<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionEntry;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\TestCase;

class RegionEntryTest extends TestCase
{
    private function makeRegionWithPeers(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 10,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
            peers: [
                new PeerInfo(peerId: 10, storeId: 1),
                new PeerInfo(peerId: 20, storeId: 2),
                new PeerInfo(peerId: 30, storeId: 3),
            ],
        );
    }

    public function testConstructionSetsLeaderFromRegionInfo(): void
    {
        $region = $this->makeRegionWithPeers();
        $entry = new RegionEntry($region, 1600);

        $this->assertSame(1, $entry->getLeaderStoreId());
        $this->assertSame(10, $entry->getLeaderPeerId());
        $this->assertSame(1600, $entry->expiresAt);
    }

    public function testSwitchLeaderWithKnownStoreIdReturnsTrue(): void
    {
        $entry = new RegionEntry($this->makeRegionWithPeers(), 1600);

        $result = $entry->switchLeader(3);

        $this->assertTrue($result);
        $this->assertSame(3, $entry->getLeaderStoreId());
        $this->assertSame(30, $entry->getLeaderPeerId());
    }

    public function testSwitchLeaderWithUnknownStoreIdReturnsFalse(): void
    {
        $entry = new RegionEntry($this->makeRegionWithPeers(), 1600);

        $result = $entry->switchLeader(99);

        $this->assertFalse($result);
        $this->assertSame(1, $entry->getLeaderStoreId());
        $this->assertSame(10, $entry->getLeaderPeerId());
    }

    public function testSwitchLeaderToSecondPeer(): void
    {
        $entry = new RegionEntry($this->makeRegionWithPeers(), 1600);

        $entry->switchLeader(2);

        $this->assertSame(2, $entry->getLeaderStoreId());
        $this->assertSame(20, $entry->getLeaderPeerId());
    }

    public function testSwitchLeaderWithNoPeersReturnsFalse(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 10,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $entry = new RegionEntry($region, 1600);

        $result = $entry->switchLeader(2);

        $this->assertFalse($result);
        $this->assertSame(1, $entry->getLeaderStoreId());
    }

    public function testSwitchLeaderToSameStoreIdReturnsTrue(): void
    {
        $entry = new RegionEntry($this->makeRegionWithPeers(), 1600);

        $result = $entry->switchLeader(1);

        $this->assertTrue($result);
        $this->assertSame(1, $entry->getLeaderStoreId());
        $this->assertSame(10, $entry->getLeaderPeerId());
    }
}
