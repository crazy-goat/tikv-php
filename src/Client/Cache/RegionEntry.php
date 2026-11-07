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
            if ($peer->storeId !== $leaderStoreId) {
                continue;
            }

            // A NotLeader hint may name a learner or a witness (issue #258,
            // REG-27). Neither may lead: a learner does not vote and a
            // witness cannot serve reads. Refuse the switch so the caller
            // falls back to invalidating the region and re-resolving it
            // through PD instead of routing to a peer that cannot lead.
            if (!$peer->canBeLeader()) {
                return false;
            }

            $this->leaderStoreId = $peer->storeId;
            $this->leaderPeerId = $peer->peerId;
            return true;
        }

        return false;
    }
}
