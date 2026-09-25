<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

/**
 * The transactional half of issue #261's array-key contract.
 *
 * `TxnReader::batchGetFromTiKV()` builds its result map as
 * `$results[$pair->getKey()] = $pair->getValue()` and `batchGet()` re-orders it
 * into `$ordered[$key]`, so a canonical decimal-integer key is stored under its
 * `int` form — a PHP array cannot hold the string key '1000'. The declared
 * return type is `array<array-key, ?string>` for that reason; what is pinned
 * here is the behaviour a caller can rely on:
 *
 *  - every requested key has its own entry, addressed by the exact bytes that
 *    were requested (PHP casts the lookup key the same way it casts the store
 *    key);
 *  - the int keys a `foreach` yields are exactly the canonical decimal forms,
 *    and the non-canonical forms ('01000', '1e3') stay string keys and do not
 *    collide with them;
 *  - a key the transaction has already written is answered from the write set
 *    under the same rules, so read-your-writes survives the coercion.
 */
#[CoversClass(TxnReader::class)]
final class TxnReaderArrayKeyTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;
    private RegionResolver $regionResolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);

        // Two regions so batchResolveRegions() really groups the keys: a group
        // that resolved to nothing would make the assertions vacuous (see
        // docs/helpers/faq.md on scanRegions() auto-returning [] on a mock).
        $regions = [$this->region(1, '', '1'), $this->region(2, '1', '')];
        $this->pdClient->method('scanRegions')->willReturn($regions);

        // Unlike RawKvBatchArrayKeyTest, this getByKey() stub is LOAD-BEARING
        // and must not be dropped: batchResolveRegions() alone never consults
        // the cache, but TxnReader::batchGetForRegion() obtains the RegionInfo
        // for each dispatched group through
        // RegionResolver::getRegionInfo($firstKey), which asks
        // RegionCache::getByKey() first and only falls through to
        // PdClient::getRegion() on a miss. Without this stub all three tests
        // error with "RegionInfo is declared final and cannot be doubled".
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => KeyOrder::gte($key, '1') ? $regions[1] : $regions[0],
        );
        // `put()` is deliberately NOT stubbed: a createMock() is already a
        // no-op and batchResolveRegions() really does call it, so a stub would
        // be a no-op statement with no effect.
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function createReader(): TxnReader
    {
        return new TxnReader(
            startTs: 1000,
            grpc: $this->grpc,
            pdClient: $this->pdClient,
            regionResolver: $this->regionResolver,
            timeoutConfig: new TimeoutConfig(),
            lockResolver: new LockResolver(
                $this->grpc,
                $this->regionResolver,
                $this->regionCache,
                $this->pdClient,
                1000,
            ),
            regionCache: $this->regionCache,
        );
    }

    /**
     * Answer KvBatchGet with one pair per requested key, echoing 'v:' . $key,
     * so the wire keys and the returned map keys are the same byte strings.
     */
    private function stubBatchGet(): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method, BatchGetRequest $request): BatchGetResponse {
                self::assertSame('KvBatchGet', $method);

                $response = new BatchGetResponse();
                $pairs = [];
                /** @var list<string> $keys */
                $keys = iterator_to_array($request->getKeys());
                foreach ($keys as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v:' . $key);
                    $pairs[] = $pair;
                }
                $response->setPairs($pairs);

                return $response;
            },
        );
    }

    public function testBatchGetAnswersEveryKeyFormWithItsOwnValue(): void
    {
        $this->stubBatchGet();

        $state = new TransactionState();
        $results = $this->createReader()->batchGet(
            ['1000', '01000', '1e3', '-5', '0', 'user:7'],
            $state,
        );

        self::assertCount(6, $results);
        foreach (['1000', '01000', '1e3', '-5', '0', 'user:7'] as $key) {
            self::assertArrayHasKey($key, $results, sprintf('entry for key %s', var_export($key, true)));
            self::assertSame('v:' . $key, $results[$key], sprintf('value for key %s', var_export($key, true)));
        }

        // The read set the transaction tracks carries the same map shape, so a
        // consumer of getReadSet() hits the very same coercion.
        $readSet = $state->getReadSet();
        self::assertCount(6, $readSet);
        foreach (['1000', '01000', '1e3', '-5', '0', 'user:7'] as $key) {
            self::assertSame('v:' . $key, $readSet[$key] ?? null);
        }
    }

    public function testCanonicalDecimalKeysComeBackAsIntArrayKeysWithoutColliding(): void
    {
        $this->stubBatchGet();

        $results = $this->createReader()->batchGet(
            ['1000', '01000', '1e3', '-5', '0', 'user:7'],
            new TransactionState(),
        );

        $keyTypes = [];
        foreach ($results as $key => $_value) {
            $keyTypes[] = [(string) $key, gettype($key)];
        }

        // '1000', '-5' and '0' are canonical decimal-integer strings, so PHP
        // stores them as int array keys; the leading-zero and the exponent
        // form are not canonical and stay string keys.
        self::assertSame(
            [
                ['1000', 'integer'],
                ['01000', 'string'],
                ['1e3', 'string'],
                ['-5', 'integer'],
                ['0', 'integer'],
                ['user:7', 'string'],
            ],
            $keyTypes,
        );

        // Lossless coercion: the int key denotes exactly the canonical decimal
        // string it came from, so lookups by those bytes resolve…
        self::assertArrayHasKey(1000, $results);
        self::assertSame('v:1000', $results['1000']);
        self::assertSame('v:-5', $results['-5']);
        self::assertSame('v:0', $results['0']);

        // …while the non-canonical forms keep their own entries instead of
        // collapsing into the int key they resemble.
        self::assertArrayHasKey('01000', $results);
        self::assertArrayHasKey('1e3', $results);
        self::assertSame('v:01000', $results['01000']);
        self::assertSame('v:1e3', $results['1e3']);
        self::assertCount(6, $results, 'the key forms must not collide');
    }

    public function testReadYourWritesSurvivesTheArrayKeyCoercion(): void
    {
        $this->stubBatchGet();

        $state = new TransactionState();
        // The write set is a PHP array too, so '1000' is already stored as
        // int 1000 before batchGet() ever looks at it.
        $state->setWrite('1000', 'written-1000');
        $state->setWrite('01000', 'written-01000');
        $state->setWrite('1e3', 'written-1e3');

        $results = $this->createReader()->batchGet(
            ['1000', '01000', '1e3', 'user:7'],
            $state,
        );

        self::assertSame('written-1000', $results['1000']);
        self::assertSame('written-01000', $results['01000']);
        self::assertSame('written-1e3', $results['1e3']);
        self::assertSame('v:user:7', $results['user:7']);

        $keyTypes = [];
        foreach ($results as $key => $_value) {
            $keyTypes[] = [(string) $key, gettype($key)];
        }
        self::assertSame(
            [
                ['1000', 'integer'],
                ['01000', 'string'],
                ['1e3', 'string'],
                ['user:7', 'string'],
            ],
            $keyTypes,
        );
    }
}
