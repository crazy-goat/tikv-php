<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv\Dto;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\TestCase;

class RegionInfoTest extends TestCase
{
    public function testConstructionAndProperties(): void
    {
        $region = new RegionInfo(
            regionId: 42,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
            startKey: 'aaa',
            endKey: 'zzz',
        );

        $this->assertSame(42, $region->regionId);
        $this->assertSame(7, $region->leaderPeerId);
        $this->assertSame(3, $region->leaderStoreId);
        $this->assertSame(1, $region->epochConfVer);
        $this->assertSame(10, $region->epochVersion);
        $this->assertSame('aaa', $region->startKey);
        $this->assertSame('zzz', $region->endKey);
    }

    public function testDefaultKeysAreEmptyStrings(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );

        $this->assertSame('', $region->startKey);
        $this->assertSame('', $region->endKey);
    }

    public function testIsReadonly(): void
    {
        $ref = new \ReflectionClass(RegionInfo::class);
        $this->assertTrue($ref->isReadonly());
    }
}
