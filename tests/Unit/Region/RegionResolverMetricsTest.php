<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class RegionResolverMetricsTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
    }

    public function testCacheHitEmitsHitMetric(): void
    {
        $metrics = new InMemoryMetrics();
        $cached = $this->makeRegion(42);

        $this->regionCache->expects($this->once())->method('getByKey')->willReturn($cached);
        // PD must NOT be called on a cache hit.
        $this->pdClient->expects($this->never())->method('getRegion');

        $resolver = new RegionResolver($this->pdClient, $this->regionCache, $metrics);
        $result = $resolver->getRegionInfo('any-key');

        $this->assertSame($cached, $result);
        $this->assertSame(1, $metrics->getCacheHits('region_resolution'));
        $this->assertSame(0, $metrics->getCacheMisses('region_resolution'));
    }

    public function testCacheMissFetchesFromPdAndEmitsMissMetric(): void
    {
        $metrics = new InMemoryMetrics();
        $fresh = $this->makeRegion(99);

        $this->regionCache->expects($this->once())->method('getByKey')->willReturn(null);
        $this->pdClient->expects($this->once())->method('getRegion')->willReturn($fresh);
        $this->regionCache->expects($this->once())->method('put')->with($fresh);

        $resolver = new RegionResolver($this->pdClient, $this->regionCache, $metrics);
        $result = $resolver->getRegionInfo('any-key');

        $this->assertSame($fresh, $result);
        $this->assertSame(0, $metrics->getCacheHits('region_resolution'));
        $this->assertSame(1, $metrics->getCacheMisses('region_resolution'));
    }

    public function testNoOpMetricsIsDefaultWhenUnspecified(): void
    {
        // Construct without metrics arg — must not crash; no-op fallback used.
        // We use a cache hit to avoid PD interaction.
        $cached = $this->makeRegion(1);
        $this->regionCache->method('getByKey')->willReturn($cached);

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);
        $result = $resolver->getRegionInfo('key');

        $this->assertSame($cached, $result);
    }

    private function makeRegion(int $regionId): RegionInfo
    {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }
}
