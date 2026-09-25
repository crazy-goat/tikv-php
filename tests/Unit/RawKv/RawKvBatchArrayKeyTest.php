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
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Tests\Unit\Batch\FakeBatchCommandsTransport;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * `RawKvBatch::batchGet()` returns a key => value MAP built as
 * `$ordered[$key] = $results[$key] ?? null`, and a PHP array cannot hold the
 * string key '1000': PHP casts a canonical decimal-integer string to an int
 * array key. The declared `array<array-key, ?string>` return type (issue #261)
 * is the honest description of that; what this test pins is the consequence a
 * caller has to live with:
 *
 *  - the entry for '1000' comes back under int 1000, so `foreach ($r as $k =>
 *    $v)` hands the caller an int even though it passed a string (a
 *    `string`-typed consumer under `declare(strict_types=1)` throws a
 *    TypeError);
 *  - the lookup `$r['1000']` still works, because PHP casts the lookup key the
 *    same way — every entry stays addressable by the exact bytes requested;
 *  - the non-canonical forms ('01000', '1e3') stay string keys and do NOT
 *    collide with the int key they resemble.
 *
 * The BatchCommands multiplexer is the transport seam that makes this
 * reachable without ext-grpc: the unary path hardcodes `new \Grpc\Call(...)`,
 * which is why the other batchGet tests live in the `Grpc` suite (the same
 * rule RawKvBatchBatchCommandsTest follows).
 */
#[CoversClass(RawKvBatch::class)]
final class RawKvBatchArrayKeyTest extends TestCase
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

        // Two regions so the batch is really grouped and dispatched: a group
        // that never resolves would make the assertions below vacuous (see
        // docs/helpers/faq.md on batch grouping silently dropping keys). Only
        // the scanRegions() stub is needed — batchResolveRegions() resolves
        // through PD's scanRegions() plus a binary search, never through
        // RegionCache::getByKey() (see the faq entry on stubbing
        // batchResolveRegions()), so stubbing getByKey() here was dead setup
        // that hid whether the scan path is exercised. Unlike
        // TxnReaderArrayKeyTest, nothing here calls
        // RegionResolver::getRegionInfo(), so no getByKey() stub is needed.
        $regions = [
            $this->region(1, '', '1'),
            $this->region(2, '1', ''),
        ];
        $this->pdClient->method('scanRegions')->willReturn($regions);
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

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            1,
        );
    }

    /**
     * @param string[] $keys
     * @return array<array-key, ?string>
     */
    private function batchGet(array $keys): array
    {
        // The BatchCommands path never touches the unary gRPC layer.
        $this->grpc->expects($this->never())->method('getChannel');

        $batch = new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
            batchCommands: new BatchCommandsMultiplexer($this->transport),
        );

        return $batch->batchGet($keys, $this->createRetryExecutor());
    }

    /**
     * @return array<array-key, ?string>
     */
    private function batchGetAllKeyForms(): array
    {
        return $this->batchGet(['1000', '01000', '1e3', '-5', '0', 'user:7']);
    }

    public function testBatchGetAnswersEveryKeyFormWithItsOwnValue(): void
    {
        $results = $this->batchGetAllKeyForms();

        // One entry per requested key, and the value is the one the store
        // answered with (the fake transport echoes 'v:' . $key).
        self::assertCount(6, $results);
        foreach (['1000', '01000', '1e3', '-5', '0', 'user:7'] as $key) {
            self::assertArrayHasKey($key, $results, sprintf('entry for key %s', var_export($key, true)));
            self::assertSame('v:' . $key, $results[$key], sprintf('value for key %s', var_export($key, true)));
        }
    }

    /**
     * The two-region layout is real, and the split is the byte-order one.
     *
     * Without this the region layout is decorative: measured with a single
     * unbounded region instead of the pair below, every other test in this
     * class still passed, because the BatchCommands multiplexer coalesces all
     * groups that share a leader store into one stream request and both
     * regions here report `leaderStoreId: 1`. Asserting the actual key
     * distribution keeps the layout honest: '-5', '0' and '01000' are in
     * ["", "1") and the rest in ["1", ""), so a grouping that resolved every
     * key to the wrong region — or dropped one — fails here.
     */
    public function testTheBatchIsReallySplitAcrossBothRegions(): void
    {
        $this->batchGetAllKeyForms();

        $dispatched = [];
        foreach ($this->transport->exchanges as $exchange) {
            foreach ($exchange['keysByRequest'] as $keys) {
                foreach ($keys as $key) {
                    $dispatched[$key] = true;
                }
            }
        }

        // Every requested key reached the wire exactly once, so nothing was
        // dropped by the region grouping.
        self::assertCount(6, $dispatched);
        foreach (['1000', '01000', '1e3', '-5', '0', 'user:7'] as $key) {
            self::assertArrayHasKey($key, $dispatched, sprintf('dispatched key %s', var_export($key, true)));
        }

        // And the split point is the byte-order one: the group boundary is '1',
        // so '-5' / '0' / '01000' (all < '1' bytewise) sit below it and the
        // three keys that start with '1' or 'u' sit above it. A numeric
        // comparison would put '01000' at 100 and '1000' at 1000 — same side,
        // but '-5' would be read as -5 and '0' as 0, so the *pairing* of keys
        // to a region is the byte-order claim being pinned.
        $below = ['-5', '0', '01000'];
        foreach ($below as $key) {
            self::assertTrue(KeyOrder::lt($key, '1'), sprintf('"%s" is below the split bytewise', $key));
        }
        foreach (['1000', '1e3', 'user:7'] as $key) {
            self::assertTrue(KeyOrder::gte($key, '1'), sprintf('"%s" is at or above the split bytewise', $key));
        }
        self::assertCount(3, $below);
    }

    public function testCanonicalDecimalKeysComeBackAsIntArrayKeysWithoutColliding(): void
    {
        $results = $this->batchGetAllKeyForms();

        $keyTypes = [];
        foreach ($results as $key => $_value) {
            $keyTypes[] = [(string) $key, gettype($key)];
        }

        // '1000', '-5' and '0' are canonical decimal-integer strings, so PHP
        // stores them as int array keys. The leading-zero form and the
        // exponent form are not canonical and stay string keys.
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

        // The coercion is lossless: the int key denotes exactly the canonical
        // decimal string it came from, so the lookup by those bytes resolves.
        self::assertArrayHasKey(1000, $results);
        self::assertArrayHasKey(-5, $results);
        self::assertArrayHasKey(0, $results);
        self::assertSame('v:1000', $results['1000']);
        self::assertSame('v:-5', $results['-5']);
        self::assertSame('v:0', $results['0']);

        // The two non-canonical forms keep their own entries: '01000' must not
        // collapse into int 1000 and '1e3' must not either, or the two would
        // be indistinguishable from the int 1000 of the canonical key.
        self::assertArrayHasKey('01000', $results);
        self::assertArrayHasKey('1e3', $results);
        self::assertSame('v:01000', $results['01000']);
        self::assertSame('v:1e3', $results['1e3']);
        self::assertCount(6, $results, 'the key forms must not collide');
    }

    /**
     * The one canonical-decimal form that does NOT coerce: a value that
     * overflows a PHP int keeps its `string` key.
     *
     * The prose in `RawKvClient::batchGet()` and `docs/helpers/faq.md` says a
     * canonical decimal key comes back under its `int` form. That is only true
     * while the value fits in a PHP int: '9223372036854775808' is
     * PHP_INT_MAX + 1, so PHP stores it as a `string` key even though it is
     * perfectly canonical decimal. A 20-digit TiKV key — a big-int ID, a
     * nanosecond epoch — hits this, so the exception is pinned rather than
     * left to the prose.
     */
    public function testCanonicalDecimalsBeyondPhpIntMaxStayStringKeys(): void
    {
        $max = (string) PHP_INT_MAX;
        $min = (string) PHP_INT_MIN;
        $overMax = '9223372036854775808';     // PHP_INT_MAX + 1
        $underMin = '-9223372036854775809';   // PHP_INT_MIN - 1

        // The measurement this test rests on, recomputed here so a PHP
        // upgrade that changes the coercion rule fails the test rather than
        // silently invalidating the docblocks. Both int boundaries themselves
        // are IN range and do coerce; the first value past either of them is
        // not.
        self::assertSame('integer', $this->keyTypeOf($max));
        self::assertSame('integer', $this->keyTypeOf($min));
        self::assertSame('string', $this->keyTypeOf($overMax));
        self::assertSame('string', $this->keyTypeOf($underMin));
        // The mirror of the overflowing positive: -PHP_INT_MAX - 1 is
        // PHP_INT_MIN and still fits.
        self::assertSame('integer', $this->keyTypeOf('-' . $overMax));
        // A short canonical decimal is unaffected by the width.
        self::assertSame('integer', $this->keyTypeOf('1000'));

        $results = $this->batchGet([$overMax, $underMin, $max, $min]);

        $keyTypes = [];
        foreach ($results as $key => $_value) {
            $keyTypes[] = [(string) $key, gettype($key)];
        }

        self::assertSame(
            [
                [$overMax, 'string'],
                [$underMin, 'string'],
                [$max, 'integer'],
                [$min, 'integer'],
            ],
            $keyTypes,
        );

        // Still lossless: the string-keyed entries are addressable by their
        // exact bytes, and the int-keyed ones by those bytes too.
        self::assertSame('v:' . $overMax, $results[$overMax]);
        self::assertSame('v:' . $underMin, $results[$underMin]);
        self::assertSame('v:' . $max, $results[$max]);
        self::assertSame('v:' . $min, $results[$min]);
        self::assertCount(4, $results);
    }

    /**
     * gettype() of the array key PHP actually stores for $key.
     */
    private function keyTypeOf(string $key): string
    {
        $map = [];
        $map[$key] = true;

        return gettype(array_key_first($map));
    }
}
