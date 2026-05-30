<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class RegionResolverTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $resolver;

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(1, 1, 1, 1, 1);
    }

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->resolver = new RegionResolver($this->pdClient, $this->regionCache);
    }

    public function testGetRegionInfoCacheHit(): void
    {
        $region = $this->defaultRegion();
        $this->regionCache->method('getByKey')->with('key')->willReturn($region);
        $this->pdClient->expects($this->never())->method('getRegion');

        $result = $this->resolver->getRegionInfo('key');
        $this->assertSame($region, $result);
    }

    public function testGetRegionInfoCacheMiss(): void
    {
        $region = $this->defaultRegion();
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->expects($this->once())->method('put')->with($region);
        $this->pdClient->method('getRegion')->willReturn($region);

        $result = $this->resolver->getRegionInfo('key');
        $this->assertSame($region, $result);
    }

    public function testResolveStoreAddressReturnsAddress(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $this->pdClient->method('getStore')->with(1)->willReturn($store);

        $this->assertSame('tikv1:20160', $this->resolver->resolveStoreAddress(1));
    }

    public function testResolveStoreAddressThrowsOnNullStore(): void
    {
        $this->pdClient->method('getStore')->willReturn(null);

        $this->expectException(StoreNotFoundException::class);
        $this->resolver->resolveStoreAddress(1);
    }

    public function testResolveStoreAddressThrowsOnEmptyAddress(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('');
        $this->pdClient->method('getStore')->willReturn($store);

        $this->expectException(StoreNotFoundException::class);
        $this->resolver->resolveStoreAddress(1);
    }

    public function testBatchResolveRegionsEmptyKeys(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');

        $result = $this->resolver->batchResolveRegions([]);
        $this->assertSame([], $result);
    }

    public function testBatchResolveRegionsSingleKey(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
        );

        $this->pdClient->method('scanRegions')->with('a', 'a')->willReturn([$region]);
        $this->regionCache->expects($this->once())->method('put')->with($region);

        $result = $this->resolver->batchResolveRegions(['a']);
        $this->assertSame(['a' => $region], $result);
    }

    public function testBatchResolveRegionsMultipleKeysSameRegion(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
        );

        $this->pdClient->method('scanRegions')->with('a', 'c')->willReturn([$region]);
        $this->regionCache->expects($this->once())->method('put')->with($region);

        $result = $this->resolver->batchResolveRegions(['a', 'b', 'c']);
        $this->assertCount(3, $result);
        $this->assertSame($region, $result['a']);
        $this->assertSame($region, $result['b']);
        $this->assertSame($region, $result['c']);
    }

    public function testBatchResolveRegionsMultipleKeysMultipleRegions(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: '',
        );

        $this->pdClient->method('scanRegions')->with('a', 'z')->willReturn([$region1, $region2]);
        $this->regionCache->expects($this->exactly(2))->method('put');

        $result = $this->resolver->batchResolveRegions(['a', 'm', 'z']);
        $this->assertCount(3, $result);
        $this->assertSame($region1, $result['a']);
        $this->assertSame($region2, $result['m']);
        $this->assertSame($region2, $result['z']);
    }

    public function testBatchResolveRegionsPopulatesCache(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->regionCache->expects($this->once())->method('put')->with($region);

        $this->resolver->batchResolveRegions(['a', 'b', 'c']);
    }

    public function testBatchResolveRegionsSkipsKeysOutsideRegions(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'b',
            endKey: 'y',
        );

        $this->pdClient->method('scanRegions')->with('a', 'z')->willReturn([$region]);
        $this->regionCache->expects($this->once())->method('put')->with($region);

        $result = $this->resolver->batchResolveRegions(['a', 'b', 'x', 'z']);
        $this->assertCount(2, $result);
        $this->assertArrayHasKey('b', $result);
        $this->assertArrayHasKey('x', $result);
        $this->assertArrayNotHasKey('a', $result);
        $this->assertArrayNotHasKey('z', $result);
    }
}
