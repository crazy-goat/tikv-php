<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class TestableRegionCache extends RegionCache
{
    public function __construct(
        private int $fakeTime,
        int $ttlSeconds = 600,
        ?LoggerInterface $logger = null,
        int $maxEntries = 10000,
        int $sweepInterval = 100,
    ) {
        parent::__construct($ttlSeconds, 0, $maxEntries, $sweepInterval, $logger ?? new NullLogger());
    }

    public function setTime(int $time): void
    {
        $this->fakeTime = $time;
    }

    protected function now(): int
    {
        return $this->fakeTime;
    }
}

class RegionCacheTest extends TestCase
{
    private function makeRegion(int $id, string $startKey, string $endKey = ''): RegionInfo
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

    private function makeRegionWithPeers(int $id, string $startKey, string $endKey = ''): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 10,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
            peers: [
                new PeerInfo(peerId: 10, storeId: 1),
                new PeerInfo(peerId: 20, storeId: 2),
                new PeerInfo(peerId: 30, storeId: 3),
            ],
        );
    }

    public function testImplementsInterface(): void
    {
        $cache = new RegionCache();
        $this->assertInstanceOf(RegionCacheInterface::class, $cache);
    }

    public function testGetByKeyReturnsNullOnEmptyCache(): void
    {
        $cache = new RegionCache();
        $this->assertNull($cache->getByKey('any_key'));
    }

    public function testPutAndGetByKeyHit(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $result = $cache->getByKey('m');
        $this->assertSame($region, $result);
    }

    public function testGetByKeyMissOutsideRange(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'b', 'd');
        $cache->put($region);

        $this->assertNull($cache->getByKey('a'));
        $this->assertNull($cache->getByKey('d'));
        $this->assertNull($cache->getByKey('z'));
    }

    public function testGetByKeyAtStartKeyBoundary(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'b', 'd');
        $cache->put($region);

        $result = $cache->getByKey('b');
        $this->assertSame($region, $result);
    }

    public function testGetByKeyEmptyEndKeyMeansUnbounded(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'a', '');
        $cache->put($region);

        $this->assertSame($region, $cache->getByKey('a'));
        $this->assertSame($region, $cache->getByKey('m'));
        $this->assertSame($region, $cache->getByKey('z'));
        $this->assertSame($region, $cache->getByKey('zzz'));
    }

    /**
     * The O(1) ID probe `RegionResolver::cacheScannedRegion()` asks "does the
     * cache already hold this region?" with (issue #288). A hit must answer the
     * *same value* `getByKey()` would: the stored region, unchanged.
     */
    public function testGetByIdReturnsTheStoredRegion(): void
    {
        $cache = new RegionCache();
        $first = $this->makeRegion(1, 'a', 'm');
        $second = $this->makeRegion(2, 'm', '');
        $cache->put($first);
        $cache->put($second);

        $this->assertSame($first, $cache->getById(1));
        $this->assertSame($second, $cache->getById(2));
    }

    /**
     * A miss is `null`, in every way a region can be absent: never stored, a
     * different ID than the one asked for (an ID-keyed lookup must not answer
     * with a neighbour that happens to cover the same key), invalidated, and
     * cleared.
     */
    public function testGetByIdMissReturnsNull(): void
    {
        $this->assertNull((new RegionCache())->getById(1), 'an empty cache holds nothing');

        $cache = new RegionCache();
        $region = $this->makeRegion(7, 'a', 'z');
        $cache->put($region);
        $this->assertNull($cache->getById(8), 'the ID is the lookup key, not a key range');

        $cache->invalidate(7);
        $this->assertNull($cache->getById(7), 'an invalidated region is gone');

        $cache->put($region);
        $cache->clear();
        $this->assertNull($cache->getById(7), 'a cleared cache holds nothing');
    }

    /**
     * The leader-aware half of the contract: a `switchLeader()` is stored on
     * the entry, not in the region object, so a probe that returned the raw
     * `RegionInfo` would answer with the **deposed** leader and the resolver's
     * identity test would then skip the `put()` that a PD answer reporting the
     * new leader is supposed to trigger. `getById()` must therefore agree with
     * `getByKey()` field for field, and must not hand back the same instance.
     */
    public function testGetByIdIsLeaderAwareLikeGetByKey(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);
        $this->assertTrue($cache->switchLeader(1, 3));

        $byId = $cache->getById(1);
        $byKey = $cache->getByKey('m');

        $this->assertNotNull($byId);
        $this->assertNotSame($region, $byId, 'the switched entry is not the stored region object');
        $this->assertSame(3, $byId->leaderStoreId);
        $this->assertSame(30, $byId->leaderPeerId);
        $this->assertEquals($byKey, $byId, 'both lookups must answer the same region');
    }

    /**
     * "The cache holds this region" has to mean the same thing to a probe and
     * to a reader, or the resolver's skip would keep a *dead* entry: a
     * `getByKey()` on it answers `null` anyway, so every later key pays a PD
     * round trip. An expired entry is therefore reported as a miss and dropped,
     * exactly as `getByKey()` drops it.
     */
    public function testGetByIdReportsAnExpiredEntryAsAMissAndDropsIt(): void
    {
        $cache = new TestableRegionCache(1000, 600);
        $cache->put($this->makeRegion(1, 'a', 'z'));
        $this->assertNotNull($cache->getById(1), 'within the TTL it is a hit');

        $cache->setTime(1600);

        $this->assertNull($cache->getById(1));
        $this->assertNull($cache->getByKey('m'), 'the same entry is a miss to a reader too');
        $this->assertSame(0, $cache->count(), 'and the dead entry is gone, not left to rot');
    }

    /**
     * A hit counts as a use, exactly as `getByKey()` does: a region a PD scan
     * re-confirmed should not be the next eviction candidate. Without the
     * touch, the skip in `cacheScannedRegion()` would quietly make every
     * re-scanned region the *least* recently used one.
     */
    public function testGetByIdMarksTheEntryAsRecentlyUsed(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 2);
        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        // The only access either region gets, and region 1 is the LRU before
        // it — so this is the touch that has to save it.
        $cache->getById(1);

        $cache->put($this->makeRegion(3, 'c', 'd'));

        $this->assertNotNull($cache->getById(1), 'the getById() access is what kept region 1');
        $this->assertNull($cache->getById(2), 'region 2 was the least recently used');
        $this->assertNotNull($cache->getById(3));
    }

    /**
     * The probe logs its verdict like every other cache access, but with the
     * region ID only: there is no key here, and `getByKey()` pays ~1.1 µs per
     * hit for a redacted key string that a `NullLogger` throws away.
     */
    public function testGetByIdLogsHitAndMissWithTheRegionIdOnly(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $logged = [];
        $logger->method('debug')
            ->willReturnCallback(function (string $message, array $context) use (&$logged): void {
                if (str_starts_with($message, 'Region cache') && str_ends_with($message, 'by id')) {
                    $logged[$message] = $context;
                }
            });

        $cache->getById(1);
        $cache->getById(2);

        $this->assertSame(
            ['Region cache hit by id' => ['regionId' => 1], 'Region cache miss by id' => ['regionId' => 2]],
            $logged,
        );
    }

    public function testGetRegionsInRangeReturnsContiguousChain(): void
    {
        $cache = new RegionCache();
        $region1 = $this->makeRegion(1, 'a', 'm');
        $region2 = $this->makeRegion(2, 'm', 't');
        $region3 = $this->makeRegion(3, 't', '');
        $cache->put($region1);
        $cache->put($region2);
        $cache->put($region3);

        $this->assertSame([$region1, $region2], $cache->getRegionsInRange('a', 't'));
        $this->assertSame([$region2, $region3], $cache->getRegionsInRange('m', 'z'));
        $this->assertSame([$region3], $cache->getRegionsInRange('t', ''));
    }

    public function testGetRegionsInRangeReturnsEmptyWhenChainIsIncomplete(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'm'));

        // The chain does not reach 'z': only [a, m) is cached.
        $this->assertSame([], $cache->getRegionsInRange('a', 'z'));
        // The start key is outside every cached region.
        $this->assertSame([], $cache->getRegionsInRange('x', 'z'));
    }

    public function testGetRegionsInRangeReturnsEmptyOnEmptyCache(): void
    {
        $this->assertSame([], (new RegionCache())->getRegionsInRange('a', 'z'));
    }

    public function testMultipleRegionsBinarySearch(): void
    {
        $cache = new RegionCache();
        $region1 = $this->makeRegion(1, 'a', 'd');
        $region2 = $this->makeRegion(2, 'd', 'h');
        $region3 = $this->makeRegion(3, 'h', '');

        $cache->put($region1);
        $cache->put($region2);
        $cache->put($region3);

        $this->assertSame($region1, $cache->getByKey('a'));
        $this->assertSame($region1, $cache->getByKey('c'));
        $this->assertSame($region2, $cache->getByKey('d'));
        $this->assertSame($region2, $cache->getByKey('f'));
        $this->assertSame($region3, $cache->getByKey('h'));
        $this->assertSame($region3, $cache->getByKey('z'));
    }

    public function testPutReplacesExistingRegionById(): void
    {
        $cache = new RegionCache();
        $region1 = $this->makeRegion(1, 'a', 'd');
        $cache->put($region1);

        $region2 = $this->makeRegion(1, 'a', 'f');
        $cache->put($region2);

        $this->assertSame($region2, $cache->getByKey('e'));
        $this->assertNull($cache->getByKey('g'));
    }

    public function testInvalidateRemovesRegion(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $cache->invalidate(1);

        $this->assertNull($cache->getByKey('m'));
    }

    public function testInvalidateNonExistentIsNoop(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $cache->invalidate(999);

        $this->assertSame($region, $cache->getByKey('m'));
    }

    public function testClearRemovesAll(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'd'));
        $cache->put($this->makeRegion(2, 'd', 'h'));

        $cache->clear();

        $this->assertNull($cache->getByKey('a'));
        $this->assertNull($cache->getByKey('d'));
    }

    public function testTtlExpiresEntry(): void
    {
        $cache = new TestableRegionCache(1000, 600);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $cache->setTime(1601);

        $this->assertNull($cache->getByKey('m'));
    }

    public function testTtlNotExpiredWithinWindow(): void
    {
        $cache = new TestableRegionCache(1000, 600);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $cache->setTime(1599);

        $this->assertSame($region, $cache->getByKey('m'));
    }

    public function testPutResetsExistingTtl(): void
    {
        $cache = new TestableRegionCache(1000, 600);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $cache->setTime(1500);
        $cache->put($region);

        $cache->setTime(2099);
        $this->assertSame($region, $cache->getByKey('m'));

        $cache->setTime(2101);
        $this->assertNull($cache->getByKey('m'));
    }

    public function testEmptyStartKeyRegion(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, '', 'd');
        $cache->put($region);

        $this->assertSame($region, $cache->getByKey(''));
        $this->assertSame($region, $cache->getByKey('a'));
        $this->assertNull($cache->getByKey('d'));
    }

    public function testSingleUnboundedRegion(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegion(1, '', '');
        $cache->put($region);

        $this->assertSame($region, $cache->getByKey(''));
        $this->assertSame($region, $cache->getByKey('a'));
        $this->assertSame($region, $cache->getByKey('z'));
        $this->assertSame($region, $cache->getByKey('anything'));
    }

    public function testInsertOrderDoesNotMatter(): void
    {
        $cache = new RegionCache();
        $region1 = $this->makeRegion(1, 'a', 'd');
        $region2 = $this->makeRegion(2, 'd', 'h');
        $region3 = $this->makeRegion(3, 'h', '');

        $cache->put($region3);
        $cache->put($region1);
        $cache->put($region2);

        $this->assertSame($region1, $cache->getByKey('a'));
        $this->assertSame($region1, $cache->getByKey('c'));
        $this->assertSame($region2, $cache->getByKey('d'));
        $this->assertSame($region2, $cache->getByKey('f'));
        $this->assertSame($region3, $cache->getByKey('h'));
        $this->assertSame($region3, $cache->getByKey('z'));
    }

    public function testPutLogsDebugMessage(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegion(1, 'a', 'z');

        $logger->expects($this->once())
            ->method('debug')
            ->with(
                'Region cached',
                $this->callback(fn($context): bool => $context['regionId'] === $region->regionId
                    && is_string($context['startKey'])
                    && str_contains($context['startKey'], 'bytes')
                    && is_string($context['endKey'])
                    && str_contains($context['endKey'], 'bytes')
                    && isset($context['ttl'])
                    && is_int($context['ttl']))
            );

        $cache->put($region);
    }

    public function testGetByKeyLogsCacheHit(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $logger->expects($this->once())
            ->method('debug')
            ->with(
                'Region cache hit',
                $this->callback(fn($context): bool => $context['regionId'] === $region->regionId
                    && is_string($context['key'])
                    && str_contains($context['key'], 'bytes')
                    && ! str_contains($context['key'], 'm'))
            );

        $cache->getByKey('m');
    }

    public function testGetByKeyLogsCacheMiss(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);

        $logger->expects($this->once())
            ->method('debug')
            ->with(
                'Region cache miss',
                $this->callback(fn($context): bool => is_string($context['key'])
                    && str_contains($context['key'], 'bytes')
                    && ! str_contains($context['key'], 'any_key'))
            );

        $cache->getByKey('any_key');
    }

    public function testInvalidateLogsInfo(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegion(1, 'a', 'z');
        $cache->put($region);

        $logger->expects($this->once())
            ->method('info')
            ->with('Region invalidated', ['regionId' => 1]);

        $cache->invalidate(1);
    }

    public function testSwitchLeaderSucceedsAndGetByKeyReflectsNewLeader(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $result = $cache->switchLeader(1, 3);

        $this->assertTrue($result);

        $resolved = $cache->getByKey('m');
        $this->assertNotNull($resolved);
        $this->assertSame(3, $resolved->leaderStoreId);
        $this->assertSame(30, $resolved->leaderPeerId);
        $this->assertSame(1, $resolved->regionId);
    }

    public function testSwitchLeaderWithUnknownStoreIdReturnsFalse(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $result = $cache->switchLeader(1, 99);

        $this->assertFalse($result);

        $resolved = $cache->getByKey('m');
        $this->assertNotNull($resolved);
        $this->assertSame(1, $resolved->leaderStoreId);
    }

    public function testSwitchLeaderWithUnknownRegionIdReturnsFalse(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $result = $cache->switchLeader(999, 2);

        $this->assertFalse($result);
    }

    public function testSwitchLeaderPreservesAllRegionFields(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $cache->switchLeader(1, 2);

        $resolved = $cache->getByKey('m');
        $this->assertNotNull($resolved);
        $this->assertSame(1, $resolved->regionId);
        $this->assertSame(1, $resolved->epochConfVer);
        $this->assertSame(1, $resolved->epochVersion);
        $this->assertSame('a', $resolved->startKey);
        $this->assertSame('z', $resolved->endKey);
        $this->assertCount(3, $resolved->peers);
    }

    public function testGetByKeyReturnsOriginalRegionWhenLeaderNotSwitched(): void
    {
        $cache = new RegionCache();
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $resolved = $cache->getByKey('m');
        $this->assertSame($region, $resolved);
    }

    public function testSwitchLeaderLogsInfo(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $logger->expects($this->once())
            ->method('info')
            ->with('Region leader switched', ['regionId' => 1, 'newLeaderStoreId' => 2]);

        $cache->switchLeader(1, 2);
    }

    public function testSwitchLeaderFailureDoesNotLog(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $cache = new RegionCache(logger: $logger);
        $region = $this->makeRegionWithPeers(1, 'a', 'z');
        $cache->put($region);

        $logger->expects($this->never())
            ->method('info')
            ->with('Region leader switched', $this->anything());

        $cache->switchLeader(1, 99);
    }

    public function testPutRemovesSupersededOverlappingEntryOnMerge(): void
    {
        $cache = new RegionCache();

        // Pre-merge state: region 5 owns ['a', 'm').
        $cache->put($this->makeRegion(5, 'a', 'm'));
        // Region 9 absorbs region 5's range: PD now reports ['a', 'z').
        $cache->put($this->makeRegion(9, 'a', 'z'));

        $this->assertSame(1, $cache->count());
        $resolved = $cache->getByKey('b');
        $this->assertNotNull($resolved);
        $this->assertSame(9, $resolved->regionId);
        $this->assertNull($cache->getByKey('zz')); // 'z' is now the end boundary
    }

    public function testPutRemovesSupersededEntriesOnSplit(): void
    {
        $cache = new RegionCache();

        // Pre-split state: region 1 owns ['a', 'z').
        $cache->put($this->makeRegion(1, 'a', 'z'));
        // Region 2 splits off the upper half, then region 3 the lower half.
        $cache->put($this->makeRegion(2, 'm', 'z'));
        $cache->put($this->makeRegion(3, 'a', 'm'));

        $this->assertSame(2, $cache->count());

        $resolvedLower = $cache->getByKey('a');
        $this->assertNotNull($resolvedLower);
        $this->assertSame(3, $resolvedLower->regionId);
        $resolvedUpper = $cache->getByKey('x');
        $this->assertNotNull($resolvedUpper);
        $this->assertSame(2, $resolvedUpper->regionId);
    }

    public function testPutKeepsTouchingNonOverlappingEntries(): void
    {
        $cache = new RegionCache();

        $cache->put($this->makeRegion(1, 'a', 'd'));
        $cache->put($this->makeRegion(2, 'd', 'h'));
        $cache->put($this->makeRegion(3, 'h', ''));

        $this->assertSame(3, $cache->count());
        $this->assertSame(1, $cache->getByKey('a')?->regionId);
        $this->assertSame(2, $cache->getByKey('d')?->regionId);
        $this->assertSame(3, $cache->getByKey('h')?->regionId);
    }

    public function testPutWithEqualStartKeysNewestWins(): void
    {
        $cache = new RegionCache();

        $cache->put($this->makeRegion(1, 'a', 'm'));
        $cache->put($this->makeRegion(2, 'a', 'z'));

        $this->assertSame(1, $cache->count());
        $resolved = $cache->getByKey('b');
        $this->assertNotNull($resolved);
        $this->assertSame(2, $resolved->regionId);
    }

    public function testCloneHasIndependentBoundaryTree(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'c'));
        $cache->put($this->makeRegion(2, 'c', 'e'));

        $clone = clone $cache;
        $clone->put($this->makeRegion(3, 'a', 'z'));

        $this->assertSame(2, $cache->count());
        $this->assertSame(1, $cache->getByKey('b')?->regionId);
        $this->assertSame(3, $clone->getByKey('b')?->regionId);
    }

    public function testPutReplacesLeftRegionWithoutSkippingOverlapScan(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        $cache->put($this->makeRegion(3, 'a', 'b'));

        $this->assertSame(2, $cache->count());
        $this->assertSame(3, $cache->getByKey('a')?->regionId);
        $this->assertSame(2, $cache->getByKey('b')?->regionId);
    }

    public function testEqualStartEmptyRangeReplacesOldNode(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'c'));
        $cache->put($this->makeRegion(2, 'a', 'a'));

        $this->assertSame(1, $cache->count());
        $this->assertNull($cache->getByKey('a'));
        $this->assertNull($cache->getByKey('b'));
    }

    public function testEmptyRangeDoesNotShadowCoveringRegion(): void
    {
        $cache = new RegionCache();
        $cache->put($this->makeRegion(1, 'a', 'c'));
        $cache->put($this->makeRegion(2, 'd', 'f'));
        $cache->put($this->makeRegion(3, 'b', 'b'));

        $this->assertSame(3, $cache->count());
        $this->assertSame(1, $cache->getByKey('b')?->regionId);
        $this->assertSame(2, $cache->getByKey('d')?->regionId);
    }

    public function testPutUnboundedIncomingRegionRemovesAllOverlaps(): void
    {
        $cache = new RegionCache();

        $cache->put($this->makeRegion(1, 'a', 'd'));
        $cache->put($this->makeRegion(2, 'd', 'h'));
        // Whole keyspace collapses into one region (last key range).
        $cache->put($this->makeRegion(3, '', ''));

        $this->assertSame(1, $cache->count());
        $this->assertSame(3, $cache->getByKey('a')?->regionId);
        $this->assertSame(3, $cache->getByKey('z')?->regionId);
    }

    public function testOverlappingRemovalKeepsLookupAndLruConsistent(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 10000);

        $cache->put($this->makeRegion(1, 'a', 'z'));
        $cache->put($this->makeRegion(2, 'b', 'c')); // supersedes region 1
        $cache->put($this->makeRegion(3, 'e', 'f'));
        $cache->put($this->makeRegion(4, 'b', 'f')); // supersedes 2 and 3

        $this->assertSame(1, $cache->count());
        $this->assertSame(4, $cache->getByKey('b')?->regionId);
        $this->assertSame(4, $cache->getByKey('e')?->regionId);
        $this->assertNull($cache->getByKey('a'));
        $this->assertNull($cache->getByKey('f'));
        $this->assertFalse($cache->switchLeader(1, 2));
        $this->assertFalse($cache->switchLeader(2, 2));
    }

    public function testSwitchLeaderOnEmptyCacheReturnsFalse(): void
    {
        $cache = new RegionCache();

        $this->assertFalse($cache->switchLeader(1, 2));
    }

    public function testMaxEntriesEvictsLru(): void
    {
        // Create cache with maxEntries = 3, no TTL jitter
        $cache = new TestableRegionCache(1000, 600, null, 3);

        // Insert 3 regions (fills cache)
        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));
        $cache->put($this->makeRegion(3, 'c', 'd'));

        // All 3 should be present
        $this->assertNotNull($cache->getByKey('a'));
        $this->assertNotNull($cache->getByKey('b'));
        $this->assertNotNull($cache->getByKey('c'));

        // Access region 1 to mark it recently used
        $cache->getByKey('a'); // marks region 1 as recently used (highest LRU)

        // Insert 4th region - should evict LRU (region 2, the least recently used)
        $cache->put($this->makeRegion(4, 'd', 'e'));

        // Only 3 entries should remain
        $this->assertSame(3, $cache->count());
        // Region 1 was accessed last, should still be present
        $this->assertNotNull($cache->getByKey('a'));
        // Region 2 (b-c) was LRU and should be evicted
        $this->assertNull($cache->getByKey('b'));
        // Region 4 (d-e) should be present
        $this->assertNotNull($cache->getByKey('d'));
    }

    public function testMaxEntriesEvictsOldestWhenNoAccess(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 2);

        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        // Insert 3rd - should evict the LRU (region 1, oldest access)
        $cache->put($this->makeRegion(3, 'c', 'd'));

        $this->assertSame(2, $cache->count());
        // Region 1 (a-b) should be evicted (oldest LRU)
        $this->assertNull($cache->getByKey('a'));
        // Region 2 (b-c) should remain
        $this->assertNotNull($cache->getByKey('b'));
        // Region 3 (c-d) should remain
        $this->assertNotNull($cache->getByKey('c'));
    }

    public function testSweepExpiredEntriesOnPutInterval(): void
    {
        // Create cache with sweepInterval = 2 (sweep every 2 puts), no jitter
        $cache = new TestableRegionCache(1000, 600, null, 10000, 2);

        // Insert regions
        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        // Fast-forward time past TTL
        $cache->setTime(2000);

        // This put should trigger sweep (every 2 puts)
        $cache->put($this->makeRegion(3, 'c', 'd'));

        // Sweep should have removed expired entries 1 and 2
        $this->assertNull($cache->getByKey('a'));
        $this->assertNull($cache->getByKey('b'));
        // New entry 3 should be present
        $this->assertNotNull($cache->getByKey('c'));

        // Verify cache now only has 1 entry (region 3)
        $this->assertSame(1, $cache->count());
    }

    public function testCountAfterOperations(): void
    {
        $cache = new RegionCache();

        $this->assertSame(0, $cache->count());

        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        $this->assertSame(2, $cache->count());

        $cache->invalidate(1);

        $this->assertSame(1, $cache->count());

        $cache->clear();

        $this->assertSame(0, $cache->count());
    }

    public function testIdToIndexMaintainedCorrectlyAfterMultiplePutsAndRemovals(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 10000);

        // Insert in non-sorted order (cache sorts by startKey)
        $cache->put($this->makeRegion(3, 'c', 'd'));
        $cache->put($this->makeRegion(1, 'a', 'b'));
        $cache->put($this->makeRegion(2, 'b', 'c'));

        // All should be findable
        $this->assertNotNull($cache->getByKey('a'));
        $this->assertNotNull($cache->getByKey('b'));
        $this->assertNotNull($cache->getByKey('c'));

        // Remove middle region
        $cache->invalidate(2);
        $this->assertSame(2, $cache->count());

        // Add another region (should not break anything)
        $cache->put($this->makeRegion(4, 'd', 'e'));
        $this->assertSame(3, $cache->count());

        // Verify all remaining regions accessible
        $this->assertNotNull($cache->getByKey('a'));
        $this->assertNotNull($cache->getByKey('c'));
        $this->assertNotNull($cache->getByKey('d'));
    }

    public function testSwitchLeaderAfterMultipleOperations(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 10000);

        $cache->put($this->makeRegionWithPeers(1, 'a', 'b'));
        $cache->put($this->makeRegionWithPeers(2, 'b', 'c'));
        $cache->put($this->makeRegionWithPeers(3, 'c', 'd'));

        // Remove middle one
        $cache->invalidate(2);

        // Switch leader on remaining regions
        $this->assertTrue($cache->switchLeader(1, 2));
        $this->assertTrue($cache->switchLeader(3, 1));

        $resolved1 = $cache->getByKey('a');
        $this->assertNotNull($resolved1);
        $this->assertSame(2, $resolved1->leaderStoreId);

        $resolved3 = $cache->getByKey('c');
        $this->assertNotNull($resolved3);
        $this->assertSame(1, $resolved3->leaderStoreId);
    }

    public function testSwitchLeaderUpdatesLruOrder(): void
    {
        $cache = new TestableRegionCache(1000, 600, null, 2);

        $cache->put($this->makeRegionWithPeers(1, 'a', 'b'));
        $cache->put($this->makeRegionWithPeers(2, 'b', 'c'));

        // Switch leader on region 1 makes it recently used
        $cache->switchLeader(1, 2);

        // Insert 3rd region - should evict LRU (region 2, never accessed after put)
        $cache->put($this->makeRegionWithPeers(3, 'c', 'd'));

        // Region 1 should survive (it was recently used via switchLeader)
        $this->assertNotNull($cache->getByKey('a'));
        // Region 2 should be evicted
        $this->assertNull($cache->getByKey('b'));
        // Region 3 should be present
        $this->assertNotNull($cache->getByKey('c'));
    }
}
