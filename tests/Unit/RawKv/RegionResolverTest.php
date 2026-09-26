<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
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

        $this->pdClient->method('scanRegions')->with('a', "a\x00")->willReturn([$region]);
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

        $this->pdClient->method('scanRegions')->with('a', "c\x00")->willReturn([$region]);
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

        $this->pdClient->method('scanRegions')->with('a', "z\x00")->willReturn([$region1, $region2]);
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

    public function testBatchResolveRegionsThrowsOnKeysOutsideRegions(): void
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

        $this->pdClient->method('scanRegions')->with('a', "z\x00")->willReturn([$region]);
        $this->regionCache->expects($this->once())->method('put')->with($region);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('"61" (1 bytes)');
        $this->resolver->batchResolveRegions(['a', 'b', 'x', 'z']);
    }

    public function testBatchResolveRegionsThrowsNamingFirstUnresolvableKey(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'b',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->regionCache->method('put');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('refusing to silently drop');
        $this->resolver->batchResolveRegions(['a', 'z']);
    }

    public function testBatchResolveRegionsThrowsWhenPdReturnsNoRegions(): void
    {
        $this->pdClient->method('scanRegions')->willReturn([]);
        $this->regionCache->method('put');

        $this->expectException(TiKvException::class);
        $this->resolver->batchResolveRegions(['a']);
    }

    /**
     * Regression test for issue #244: the maximum key of a batch may sit
     * exactly on a region boundary (the region's startKey). The scan upper
     * bound must include that region, otherwise the key found no region
     * and was silently dropped from the batch.
     */
    public function testBatchResolveRegionsIncludesRegionStartingAtMaxKey(): void
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

        // Issue #244: the previous bound ('a', 'm') made PD stop before the
        // region whose startKey is exactly 'm'.
        $this->pdClient->method('scanRegions')->with('a', "m\x00")->willReturn([$region1, $region2]);
        $this->regionCache->expects($this->exactly(2))->method('put');

        $result = $this->resolver->batchResolveRegions(['a', 'm']);
        $this->assertSame($region1, $result['a']);
        $this->assertSame($region2, $result['m']);
    }

    /**
     * Issue #188's own vector, verbatim: keys ["user:1", "user:3"] against
     * regions ["", "user:3") and ["user:3", "").
     *
     * The boundaries differ from the #244 vector above in two ways worth
     * pinning, because the scan window has to hold for both: region 1 starts
     * at the **empty** key (the first region of the keyspace, which sorts
     * before everything) and region 2 has an **unbounded** end (`''` means
     * +infinity), so neither the "a" start nor the "m" end of the other
     * vector is present here. The half-open scan must therefore ask PD for
     * ("user:1", "user:3\x00") — the `\x00` is the immediate byte successor
     * of the maximum key, so PD's "start < end_key" rule keeps the region
     * that begins exactly at "user:3".
     */
    public function testBatchResolveRegionsResolvesBothKeysWhenMaxKeyIsARegionStartKey(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'user:3',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'user:3',
            endKey: '',
        );

        // The pre-fix window ('user:1', 'user:3') made PD stop before the
        // region whose startKey is exactly 'user:3'; PHPUnit fails the test
        // on that mismatch, which is the assertion — the upper bound must
        // carry the successor byte.
        $this->pdClient->method('scanRegions')->with('user:1', "user:3\x00")->willReturn([$region1, $region2]);
        $this->regionCache->expects($this->exactly(2))->method('put');

        $result = $this->resolver->batchResolveRegions(['user:1', 'user:3']);

        $this->assertCount(2, $result);
        $this->assertSame($region1, $result['user:1']);
        $this->assertSame($region2, $result['user:3']);
    }

    /**
     * The degenerate window: one key that is itself a region start key.
     *
     * For a single key the window is [k, k."\x00"), which still contains the
     * region starting at k; pre-fix it was [k, k), PD returned an empty
     * list, the result map came back empty and batchGet() answered
     * `['user:3' => null]` (issue #188).
     *
     * The mock deliberately returns BOTH regions although the requested
     * window intersects only region 2, so the assertion is about the
     * resolver's own mapping rather than the mock's precision: a key equal
     * to a region's endKey must never fall back to that region, and the
     * region that *starts* at the key must win. Handing back only region 2
     * would let a resolver that simply trusted the first region it was given
     * pass.
     */
    public function testBatchResolveRegionsSingleKeyEqualToRegionStartKey(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'user:3',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'user:3',
            endKey: '',
        );

        // Pre-fix this call was ('user:3', 'user:3') — a window PD answers
        // with an empty list, so the key resolved to nothing at all.
        $this->pdClient->method('scanRegions')->with('user:3', "user:3\x00")->willReturn([$region1, $region2]);
        $this->regionCache->expects($this->exactly(2))->method('put');

        $result = $this->resolver->batchResolveRegions(['user:3']);

        $this->assertCount(1, $result);
        $this->assertArrayHasKey('user:3', $result);
        $this->assertSame($region2, $result['user:3'], 'the region starting at the key owns it, not its predecessor');
    }
}
