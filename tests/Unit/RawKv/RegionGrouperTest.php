<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use PHPUnit\Framework\TestCase;

class RegionGrouperTest extends TestCase
{
    public function testGroupKeysByRegionEmpty(): void
    {
        $this->assertSame(
            [],
            RegionGrouper::groupKeysByRegion([], fn(string $k): RegionInfo => new RegionInfo(1, 1, 1, 1, 1)),
        );
    }

    public function testGroupKeysByRegionSingleRegion(): void
    {
        $resolver = fn(string $k): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => new RegionInfo(1, 1, 1, 1, 1);
        $result = RegionGrouper::groupKeysByRegion(['a', 'b', 'c'], $resolver);
        $this->assertCount(1, $result);
        $this->assertSame(['a', 'b', 'c'], $result[1]['keys']);
    }

    public function testGroupKeysByRegionMultipleRegions(): void
    {
        $resolver = fn(string $k): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => new RegionInfo(
            regionId: $k === 'a' || $k === 'b' ? 1 : 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $result = RegionGrouper::groupKeysByRegion(['a', 'b', 'c'], $resolver);
        $this->assertCount(2, $result);
        $this->assertSame(['a', 'b'], $result[1]['keys']);
        $this->assertSame(['c'], $result[2]['keys']);
    }

    public function testGroupKeysByRegionPreservesOrder(): void
    {
        $resolver = fn(string $k): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => new RegionInfo(
            regionId: $k === 'c' ? 2 : 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $result = RegionGrouper::groupKeysByRegion(['a', 'b', 'c', 'd'], $resolver);
        $this->assertSame(['a', 'b', 'd'], $result[1]['keys']);
        $this->assertSame(['c'], $result[2]['keys']);
    }

    public function testGroupKeysByRegionBatchEmpty(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $resolver = new RegionResolver($pdClient, $regionCache);

        $result = RegionGrouper::groupKeysByRegionBatch([], $resolver);
        $this->assertSame([], $result);
    }

    public function testGroupKeysByRegionBatchSingleRegion(): void
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

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('scanRegions')->willReturn([$region]);

        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('put');

        $resolver = new RegionResolver($pdClient, $regionCache);
        $result = RegionGrouper::groupKeysByRegionBatch(['a', 'b', 'c'], $resolver);

        $this->assertCount(1, $result);
        $this->assertSame(['a', 'b', 'c'], $result[1]['keys']);
    }

    public function testGroupKeysByRegionBatchMultipleRegions(): void
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

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('scanRegions')->willReturn([$region1, $region2]);

        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('put');

        $resolver = new RegionResolver($pdClient, $regionCache);
        $result = RegionGrouper::groupKeysByRegionBatch(['a', 'm', 'z'], $resolver);

        $this->assertCount(2, $result);
        $this->assertSame(['a'], $result[1]['keys']);
        $this->assertSame(['m', 'z'], $result[2]['keys']);
    }

    public function testGroupKeysByRegionBatchPreservesOrder(): void
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

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('scanRegions')->willReturn([$region1, $region2]);

        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('put');

        $resolver = new RegionResolver($pdClient, $regionCache);
        $result = RegionGrouper::groupKeysByRegionBatch(['a', 'c', 'm', 'y', 'z'], $resolver);

        $this->assertCount(2, $result);
        $this->assertSame(['a', 'c'], $result[1]['keys']);
        $this->assertSame(['m', 'y', 'z'], $result[2]['keys']);
    }

    /**
     * Issue #244: a key that batchResolveRegions() cannot map to a region
     * must fail the call, never be skipped (a skipped key in a batchPut
     * batch means the write is silently lost; in a batchGet it reads back
     * as null, indistinguishable from "key not present").
     */
    public function testGroupKeysByRegionBatchThrowsOnUnresolvedKey(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('scanRegions')->willReturn([$region]);

        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('put');

        $resolver = new RegionResolver($pdClient, $regionCache);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage(KeyRedactor::redact('z'));
        RegionGrouper::groupKeysByRegionBatch(['a', 'z'], $resolver);
    }

    public function testGroupItemsByRegionThrowsOnUnresolvedKey(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('scanRegions')->willReturn([$region]);

        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('put');

        $resolver = new RegionResolver($pdClient, $regionCache);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage(KeyRedactor::redact('z'));
        RegionGrouper::groupItemsByRegion(
            [(object) ['key' => 'a'], (object) ['key' => 'z']],
            fn (object $item): string => $item->key,
            $resolver,
        );
    }
}
