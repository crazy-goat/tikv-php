<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsMultiplexer;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Tests\Unit\Batch\FakeBatchCommandsTransport;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Pins the public `RawKvClient::batchGet()` contract for canonical
 * decimal-string keys (issue #202, RAW-17).
 *
 * `batchGet()` is declared `array<array-key, ?string>`, not
 * `array<string, ?string>`, because PHP stores the string key '12345' under
 * int 12345: a caller that iterates the result gets an `int` for a key it
 * passed as a `string`, and a strict comparison against the requested key
 * fails. The coercion is lossless — the lookup by the original bytes resolves
 * the same entry — and it was already documented and fixed by #261/#603; this
 * test pins the exact reproduction from the audit at the client boundary,
 * where the contract is public, rather than only at the internal
 * `RawKvBatch` seam covered by `RawKvBatchArrayKeyTest`.
 *
 * The BatchCommands multiplexer is the transport seam that makes the success
 * path reachable without ext-grpc: the unary path hardcodes a real
 * `\Grpc\Call`. The `RawKvBatch` is injected through `RawKvClient`'s test
 * seam, so the public method's normalization and delegation are exercised
 * too.
 */
#[CoversClass(RawKvClient::class)]
final class RawKvClientBatchGetArrayKeyTest extends TestCase
{
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;
    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;

    /** Records every multiplexed exchange so the dispatch can be asserted. */
    private FakeBatchCommandsTransport $transport;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->transport = new FakeBatchCommandsTransport();

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);

        // Two regions so the batch is really grouped and dispatched; the
        // split point '1' keeps '12345' and 'user:1' in different regions.
        $this->pdClient->method('scanRegions')->willReturn([
            $this->region(1, '', '1'),
            $this->region(2, '1', ''),
        ]);
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function client(): RawKvClient
    {
        // The multiplexed path never touches the unary gRPC layer.
        $this->grpc->expects($this->never())->method('getChannel');

        $batch = new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
            batchCommands: new BatchCommandsMultiplexer($this->transport),
        );

        return new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            batch: $batch,
        );
    }

    public function testCanonicalDecimalKeyComesBackAsAnIntArrayKey(): void
    {
        $result = $this->client()->batchGet(['12345', 'user:1']);

        // The exact reproduction from issue #202: array_keys() yields int
        // 12345 where the declared type used to promise string, so a strict
        // membership check against the requested key is false.
        self::assertSame([12345, 'user:1'], array_keys($result));
        self::assertSame('integer', gettype(array_keys($result)[0]));
        $this->assertNotStrictlyContained('12345', $result);

        // The coercion is lossless: both spellings address the same entry,
        // because PHP casts a string lookup exactly like the store key.
        self::assertSame('v:12345', $result['12345']);
        self::assertSame('v:12345', $result[12345]);
        self::assertSame('v:user:1', $result['user:1']);
    }

    public function testNonCanonicalDecimalKeyStaysAStringArrayKey(): void
    {
        $result = $this->client()->batchGet(['01000', '12345']);

        // '01000' is not canonical decimal, so PHP keeps it as a string key;
        // it does not collide with the int 1000 it resembles.
        self::assertSame(['01000', 12345], array_keys($result));
        self::assertSame('string', gettype(array_keys($result)[0]));
        self::assertSame('integer', gettype(array_keys($result)[1]));
        self::assertSame('v:01000', $result['01000']);
        self::assertSame('v:12345', $result[12345]);
    }

    public function testIntInputKeyIsNormalizedToItsStringBytes(): void
    {
        $result = $this->client()->batchGet([12345]);

        // RawKvClient::batchGet() accepts int keys and string-casts them
        // before validation and dispatch (issue #322); the result still comes
        // back under int 12345 because that is the array-key PHP can hold.
        self::assertSame([12345], array_keys($result));
        self::assertSame('v:12345', $result[12345]);
        self::assertSame(['12345'], $this->dispatchedKeys());
    }

    /**
     * The issue's reproduction, isolated in a helper so the parameter is a
     * plain `string` rather than the literal PHPStan can fold into the array
     * type of the result.
     *
     * @param array<array-key, ?string> $result
     */
    private function assertNotStrictlyContained(string $requestedKey, array $result): void
    {
        self::assertFalse(in_array($requestedKey, array_keys($result), true));
    }

    /**
     * @return list<string>
     */
    private function dispatchedKeys(): array
    {
        $dispatched = [];
        foreach ($this->transport->exchanges as $exchange) {
            foreach ($exchange['keysByRequest'] as $keys) {
                foreach ($keys as $key) {
                    $dispatched[] = $key;
                }
            }
        }

        return $dispatched;
    }
}
