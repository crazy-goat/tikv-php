<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionScanSource;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #425: the scan-path read-through.
 *
 * `RegionScanSource` is what makes a scan ask PD only for the uncovered
 * ranges, and what reports the scan-path region-cache counters. The tests
 * below pin the properties the acceptance criteria name: a warm cache costs
 * zero PD calls, a gap is fetched from PD *starting at the gap* (not at the
 * scan's start key), the fetched regions repopulate the cache, and a split or
 * merge between cache population and use is handled by the next scan.
 */
class RegionScanSourceTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private RegionCache $cache;

    private InMemoryMetrics $metrics;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->cache = new RegionCache();
        $this->metrics = new InMemoryMetrics();
    }

    private function source(): RegionScanSource
    {
        return new RegionScanSource($this->pdClient, $this->cache, $this->metrics);
    }

    private function region(int $id, string $startKey, string $endKey, int $epochVersion = 1): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: $epochVersion,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    /**
     * @param list<list<RegionInfo>> $pages
     * @return list<RegionInfo>
     */
    private function flatten(array $pages): array
    {
        return array_merge(...$pages);
    }

    public function testWarmCacheServesTheScansWithoutPd(): void
    {
        $this->cache->put($this->region(1, 'a', 'm'));
        $this->cache->put($this->region(2, 'm', 'z'));

        $this->pdClient->expects($this->never())->method('scanRegions');

        $regions = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );

        $this->assertSame([1, 2], array_column($regions, 'regionId'));
        $this->assertSame(1, $this->metrics->getCacheHits('scan'));
        $this->assertSame(0, $this->metrics->getCacheMisses('scan'));
    }

    public function testColdCacheQueriesPdAndRepopulatesTheCache(): void
    {
        $regions = [$this->region(1, 'a', 'm'), $this->region(2, 'm', 'z')];
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('a', 'z', 32)
            ->willReturn($regions);

        $served = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );

        $this->assertSame([1, 2], array_column($served, 'regionId'));
        $this->assertNotNull($this->cache->getById(1));
        $this->assertNotNull($this->cache->getById(2));
        $this->assertSame(0, $this->metrics->getCacheHits('scan'));
        $this->assertSame(1, $this->metrics->getCacheMisses('scan'));

        // A second scan is served entirely from the cache.
        $this->assertSame(
            [1, 2],
            array_column(
                $this->flatten(iterator_to_array($this->source()->batches('a', 'z', 32), false)),
                'regionId',
            ),
        );
    }

    public function testAGapIsFetchedFromItsStartKeyNotFromTheScanStart(): void
    {
        // The cache holds the first region only; the gap starts at "m".
        $this->cache->put($this->region(1, 'a', 'm'));
        $tail = [$this->region(2, 'm', 't'), $this->region(3, 't', 'z')];

        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('m', 'z', 32)
            ->willReturn($tail);

        $served = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );

        $this->assertSame([1, 2, 3], array_column($served, 'regionId'));
        // One hit for the cached run, one miss for the gap.
        $this->assertSame(1, $this->metrics->getCacheHits('scan'));
        $this->assertSame(1, $this->metrics->getCacheMisses('scan'));
    }

    public function testGapFetchedFromPdIsServedFromCacheOnTheNextScan(): void
    {
        $this->cache->put($this->region(1, 'a', 'm'));
        $tail = [$this->region(2, 'm', 'z')];

        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('m', 'z', 32)
            ->willReturn($tail);

        iterator_to_array($this->source()->batches('a', 'z', 32), false);
        iterator_to_array($this->source()->batches('a', 'z', 32), false);

        $this->assertSame(2, $this->metrics->getCacheHits('scan'));
        $this->assertSame(1, $this->metrics->getCacheMisses('scan'));
    }

    public function testSplitBetweenPopulationAndUseIsLearnedFromPdAndCached(): void
    {
        // Populated layout: one region covering the whole range.
        $this->cache->put($this->region(1, 'a', 'z'));
        // It split before the scan: the client learned this through an
        // EpochNotMatch, which invalidates the stale entry.
        $this->cache->invalidate(1);

        $left = $this->region(2, 'a', 'm', epochVersion: 2);
        $right = $this->region(3, 'm', 'z', epochVersion: 2);
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('a', 'z', 32)
            ->willReturn([$left, $right]);

        $first = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );
        $this->assertSame([2, 3], array_column($first, 'regionId'));

        // The next scan sees the split layout from the cache alone.
        $this->pdClient->expects($this->never())->method('scanRegions');
        $second = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );
        $this->assertSame([2, 3], array_column($second, 'regionId'));
    }

    public function testMergeBetweenPopulationAndUseIsLearnedFromPdAndCached(): void
    {
        // Populated layout: two regions that were merged before the scan.
        $this->cache->put($this->region(1, 'a', 'm'));
        $this->cache->put($this->region(2, 'm', 'z'));
        $this->cache->invalidate(1);
        $this->cache->invalidate(2);

        $merged = $this->region(3, 'a', 'z', epochVersion: 2);
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('a', 'z', 32)
            ->willReturn([$merged]);

        $first = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );
        $this->assertSame([3], array_column($first, 'regionId'));
        $this->assertSame($merged, $this->cache->getById(3));

        $this->pdClient->expects($this->never())->method('scanRegions');
        $second = $this->flatten(
            iterator_to_array($this->source()->batches('a', 'z', 32), false),
        );
        $this->assertSame([3], array_column($second, 'regionId'));
    }

    public function testCachedRunsAreBoundedByTheRequestedEndKey(): void
    {
        $this->cache->put($this->region(1, 'a', 'z'));

        $regions = $this->flatten(
            iterator_to_array($this->source()->batches('b', 'm', 32), false),
        );

        $this->assertSame([1], array_column($regions, 'regionId'));
    }
}
