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
 * emits exactly once per drop call.
 */
class RegionCacheMetricsTest extends TestCase
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

    public function testInvalidateEmitsReasonExactlyOncePerCall(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(1, 'a', 'z'));

        $cache->invalidate(1, 'region_error');
        $cache->invalidate(1, 'region_error');

        $this->assertSame(2, $metrics->getInvalidations('region_error'));
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
    }

    public function testInvalidateOfUnknownRegionStillEmits(): void
    {
        // Choke-point contract: the caller asked for an invalidation, so the
        // metric counts it even when nothing was cached under that ID.
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);

        $cache->invalidate(999, 'not_leader');

        $this->assertSame(1, $metrics->getInvalidations('not_leader'));
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

    public function testWithMetricsReturnsNewInstanceLeavingOriginalUntouched(): void
    {
        $original = new RegionCache();
        $metrics = new InMemoryMetrics();

        $wired = $original->withMetrics($metrics);

        $this->assertNull($original->metrics(), 'Receiver must stay un-wired');
        $this->assertNotNull($wired->metrics());
        $this->assertNotSame($original, $wired);

        $wired->put($this->makeRegion(3, 'c', 'z'));
        $wired->invalidate(3, 'retry_region_error');
        $this->assertSame(1, $metrics->getInvalidations('retry_region_error'));
    }
}
