<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\TestCase;

/**
 * Issue #576: a region PD reports without a leader (leaderStoreId=0) must not
 * be cached. Routing it raises a fatal StoreNotFoundException, which skips the
 * retry path's invalidation, so a cached copy used to fail every request for
 * the key until the region-cache TTL ran out, even after PD recovered.
 */
final class LeaderlessRegionNotCachedTest extends TestCase
{
    public function testGetRegionInfoAsksPdAgainAfterALeaderlessAnswer(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->expects($this->exactly(2))
            ->method('getRegion')
            ->with('k')
            ->willReturnOnConsecutiveCalls($this->makeRegion(7, 0), $this->makeRegion(7, 1));

        $cache = new RegionCache();
        $resolver = new RegionResolver($pdClient, $cache);

        $leaderless = $resolver->getRegionInfo('k');
        $this->assertSame(0, $leaderless->leaderStoreId);
        $this->assertNull($cache->getByKey('k'));

        try {
            $resolver->resolveStoreAddress($leaderless->leaderStoreId);
            $this->fail('A leaderless region must still fail closed');
        } catch (StoreNotFoundException $e) {
            $this->assertSame(0, $e->storeId);
        }

        $recovered = $resolver->getRegionInfo('k');
        $this->assertSame(1, $recovered->leaderStoreId);
        $this->assertSame($recovered, $cache->getByKey('k'));
    }

    public function testBatchResolveRegionsDoesNotCacheALeaderlessScanAnswer(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->expects($this->exactly(2))
            ->method('scanRegions')
            ->willReturnOnConsecutiveCalls([$this->makeRegion(7, 0)], [$this->makeRegion(7, 1)]);

        $cache = new RegionCache();
        $resolver = new RegionResolver($pdClient, $cache);

        $first = $resolver->batchResolveRegions(['k']);
        $this->assertSame(0, $first['k']->leaderStoreId);
        $this->assertNull($cache->getByKey('k'));

        $second = $resolver->batchResolveRegions(['k']);
        $this->assertSame(1, $second['k']->leaderStoreId);
        $this->assertSame($second['k'], $cache->getByKey('k'));
    }

    private function makeRegion(int $regionId, int $leaderStoreId): RegionInfo
    {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $leaderStoreId === 0 ? 0 : 70,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
    }
}
