<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

final class RegionEntry
{
    private int $leaderStoreId;
    private int $leaderPeerId;

    public function __construct(
        public readonly RegionInfo $region,
        public readonly int $expiresAt,
    ) {
        $this->leaderStoreId = $region->leaderStoreId;
        $this->leaderPeerId = $region->leaderPeerId;
    }

    public function getLeaderStoreId(): int
    {
        return $this->leaderStoreId;
    }

    public function getLeaderPeerId(): int
    {
        return $this->leaderPeerId;
    }

    public function switchLeader(int $leaderStoreId): bool
    {
        foreach ($this->region->peers as $peer) {
            if ($peer->storeId === $leaderStoreId) {
                $this->leaderStoreId = $peer->storeId;
                $this->leaderPeerId = $peer->peerId;
                return true;
            }
        }

        return false;
    }
}
