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
}
