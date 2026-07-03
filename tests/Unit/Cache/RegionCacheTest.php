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
