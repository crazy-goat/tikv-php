<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RegionGrouper;
use PHPUnit\Framework\TestCase;

class RegionGrouperTest extends TestCase
{
    public function testGroupKeysByRegionEmpty(): void
    {
        $this->assertSame([], RegionGrouper::groupKeysByRegion([], fn(string $k) => new RegionInfo(1, 1, 1, 1, 1)));
    }

    public function testGroupKeysByRegionSingleRegion(): void
    {
        $resolver = fn(string $k) => new RegionInfo(1, 1, 1, 1, 1);
        $result = RegionGrouper::groupKeysByRegion(['a', 'b', 'c'], $resolver);
        $this->assertCount(1, $result);
        $this->assertSame(['a', 'b', 'c'], $result[1]['keys']);
    }

    public function testGroupKeysByRegionMultipleRegions(): void
    {
        $resolver = fn(string $k) => new RegionInfo(
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
        $resolver = fn(string $k) => new RegionInfo(
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
}
