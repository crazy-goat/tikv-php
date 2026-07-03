<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

final class RegionContextFactory
{
    public static function fromRegionInfo(RegionInfo $region): Context
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer($region->epochConfVer);
        $epoch->setVersion($region->epochVersion);

        $peer = new Peer();
        $peer->setId($region->leaderPeerId);
        $peer->setStoreId($region->leaderStoreId);

        $ctx = new Context();
        $ctx->setRegionId($region->regionId);
        $ctx->setRegionEpoch($epoch);
        $ctx->setPeer($peer);

        return $ctx;
    }
}
