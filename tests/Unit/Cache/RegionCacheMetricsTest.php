<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\TestCase;

/**
 * Issue #474: RegionCache::invalidate() is the single emission point for the
 * regionInvalidated() metric — callers pass their reason tag in, the cache
 * emits exactly once per ACTUAL drop (an unknown region ID emits nothing).
 * Metrics are attached in place via attachMetricsIfAbsent(); a cache shared
 * between client components is never cloned or rebound.
 */
class RegionCacheMetricsTest extends TestCase
{
    private function makeRegion(int $id, string $startKey = 'a', string $endKey = 'z'): RegionInfo
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

    public function testImplementsInterface(): void
    {
        $this->assertInstanceOf(RegionCacheInterface::class, new RegionCache());
    }

    public function testNoBackendAttachedByDefault(): void
    {
        $cache = new RegionCache();

        $this->assertNull($cache->metrics());

        // Must be a safe no-op, not a fatal (nullsafe emission).
        $cache->invalidate(1, 'region_error');
        $this->addToAssertionCount(1);
    }

    public function testInvalidateEmitsReasonExactlyOncePerActualDrop(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(1));

        $cache->invalidate(1, 'region_error');
        // Second call finds nothing to remove — no second emission.
        $cache->invalidate(1, 'region_error');

        $this->assertSame(1, $metrics->getInvalidations('region_error'));
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
    }

    public function testInvalidateOfUnknownRegionEmitsNothing(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);

        $cache->invalidate(999, 'not_leader');

        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
        $this->assertSame(0, $metrics->getInvalidations('region_error'));
    }

    public function testDefaultReasonIsRegionError(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(2, 'b', 'y'));

        $cache->invalidate(2);

        $this->assertSame(1, $metrics->getInvalidations('region_error'));
    }

    public function testAttachMetricsIfAbsentAttachesWhenNull(): void
    {
        $cache = new RegionCache();
        $metrics = new InMemoryMetrics();

        $cache->attachMetricsIfAbsent($metrics);

        $this->assertSame($metrics, $cache->metrics());
    }

    public function testAttachMetricsIfAbsentKeepsExistingBackend(): void
    {
        $existing = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $existing);

        $cache->attachMetricsIfAbsent(new InMemoryMetrics());

        $this->assertSame($existing, $cache->metrics(), 'An explicitly attached backend must win');
    }

    public function testAttachMetricsIfAbsentMutatesSameInstance(): void
    {
        $metrics = new InMemoryMetrics();
        $shared = new RegionCache();

        $shared->attachMetricsIfAbsent($metrics);

        // Same object mutated — a copy handed to another component earlier
        // would never see the wiring (that is why withMetrics() was dropped).
        $shared->put($this->makeRegion(3));
        $shared->invalidate(3, 'retry_region_error');
        $this->assertSame($shared, $shared, 'No clone returned — mutation is in place');
        $this->assertSame(1, $metrics->getInvalidations('retry_region_error'));
    }
}
