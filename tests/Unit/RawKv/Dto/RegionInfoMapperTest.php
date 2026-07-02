<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv\Dto;

use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\TiKV\Client\RawKv\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfoMapper;
use PHPUnit\Framework\TestCase;

class RegionInfoMapperTest extends TestCase
{
    public function testMapsFullRegionWithLeader(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(3);
        $epoch->setVersion(17);

        $peer1 = new Peer();
        $peer1->setId(11);
        $peer1->setStoreId(1);

        $peer2 = new Peer();
        $peer2->setId(22);
        $peer2->setStoreId(2);

        $region = new Region();
        $region->setId(5);
        $region->setStartKey('a');
        $region->setEndKey('m');
        $region->setRegionEpoch($epoch);
        $region->setPeers([$peer1, $peer2]);

        $leader = new Peer();
        $leader->setId(22);
        $leader->setStoreId(2);

        $info = RegionInfoMapper::fromProto($region, $leader);

        $this->assertInstanceOf(RegionInfo::class, $info);
        $this->assertSame(5, $info->regionId);
        $this->assertSame(22, $info->leaderPeerId);
        $this->assertSame(2, $info->leaderStoreId);
        $this->assertSame(3, $info->epochConfVer);
        $this->assertSame(17, $info->epochVersion);
        $this->assertSame('a', $info->startKey);
        $this->assertSame('m', $info->endKey);
        $this->assertCount(2, $info->peers);
        $this->assertInstanceOf(PeerInfo::class, $info->peers[0]);
        $this->assertSame(11, $info->peers[0]->peerId);
        $this->assertSame(1, $info->peers[0]->storeId);
        $this->assertSame(22, $info->peers[1]->peerId);
        $this->assertSame(2, $info->peers[1]->storeId);
    }

    public function testMissingLeaderReportsUnknownStoreIdZero(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(8);
        $region->setRegionEpoch($epoch);

        $info = RegionInfoMapper::fromProto($region, null);

        // Fail closed: store id 0 ("unknown") matches no real store, so
        // resolveStoreAddress() raises StoreNotFoundException rather than
        // silently routing to a guessed store id 1.
        $this->assertSame(8, $info->regionId);
        $this->assertSame(0, $info->leaderPeerId);
        $this->assertSame(0, $info->leaderStoreId);
    }

    public function testMissingRegionEpochDefaultsToZero(): void
    {
        $region = new Region();
        $region->setId(9);

        $info = RegionInfoMapper::fromProto($region, null);

        $this->assertSame(0, $info->epochConfVer);
        $this->assertSame(0, $info->epochVersion);
    }

    public function testEmptyPeersYieldsEmptyList(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $info = RegionInfoMapper::fromProto($region, null);

        $this->assertSame([], $info->peers);
    }
}
