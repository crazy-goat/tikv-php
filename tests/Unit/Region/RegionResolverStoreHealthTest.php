<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\StoreHealthTracker;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreUnavailableException;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #424 (GAP-10): an unhealthy store stops being routed to during its
 * cooldown, and the regions it led are invalidated so the next resolution can
 * pick another leader.
 */
class RegionResolverStoreHealthTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private RegionCache $regionCache;

    private InMemoryMetrics $metrics;

    private StoreHealthTracker $tracker;

    private RegionResolver $resolver;

    private int $now = 0;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->metrics = new InMemoryMetrics();
        $this->regionCache = new RegionCache(metrics: $this->metrics);
        $this->tracker = new StoreHealthTracker(
            metrics: $this->metrics,
            clockMs: fn (): int => $this->now,
        );
        $this->resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            $this->metrics,
            logger: new NullLogger(),
            storeHealth: $this->tracker,
        );
    }

    public function testThresholdFailureInvalidatesEveryRegionLedByTheStore(): void
    {
        $this->regionCache->put($this->region(1, 1, 'a', 'b'));
        $this->regionCache->put($this->region(2, 1, 'b', 'c'));
        $this->regionCache->put($this->region(3, 2, 'c', 'd'));

        $this->resolver->recordStoreFailure(1);
        $this->resolver->recordStoreFailure(1);
        $this->assertSame(1, $this->regionCache->getById(1)?->regionId);
        $this->assertSame(0, $this->metrics->getInvalidations('store_unhealthy'));

        $this->resolver->recordStoreFailure(1);

        $this->assertNull($this->regionCache->getById(1));
        $this->assertNull($this->regionCache->getById(2));
        $this->assertSame(3, $this->regionCache->getById(3)?->regionId);
        $this->assertSame(2, $this->metrics->getInvalidations('store_unhealthy'));
    }

    public function testUnhealthyStoreIsNotResolvedDuringCooldown(): void
    {
        $this->pdClient->expects($this->never())->method('getStore');
        $this->markUnhealthy();

        $this->expectException(StoreUnavailableException::class);

        $this->resolver->resolveStoreAddress(1);
    }

    public function testStoreIsResolvedAgainAfterCooldown(): void
    {
        $this->markUnhealthy();

        $store = new Store();
        $store->setId(1);
        $store->setAddress('store1:20160');
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($store);

        $this->now = 1000;

        $this->assertTrue($this->resolver->isStoreAvailable(1));
        $this->assertSame('store1:20160', $this->resolver->resolveStoreAddress(1));
    }

    public function testSuccessRecoversTheStore(): void
    {
        $this->markUnhealthy();

        $this->resolver->recordStoreSuccess(1);

        $this->assertTrue($this->resolver->isStoreAvailable(1));
        $this->assertTrue($this->tracker->isHealthy(1));
        $this->assertTrue($this->metrics->isStoreHealthy(1));
        $this->assertSame(1, $this->metrics->getStoreRecoveredCount(1));
    }

    public function testInvalidStoreIdIsIgnored(): void
    {
        $this->resolver->recordStoreFailure(0);
        $this->resolver->recordStoreSuccess(0);

        $this->assertTrue($this->resolver->isStoreAvailable(0));
        $this->assertSame(0, $this->metrics->getInvalidations('store_unhealthy'));
    }

    private function markUnhealthy(): void
    {
        $this->resolver->recordStoreFailure(1);
        $this->resolver->recordStoreFailure(1);
        $this->resolver->recordStoreFailure(1);
    }

    private function region(int $regionId, int $leaderStoreId, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $regionId,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }
}
