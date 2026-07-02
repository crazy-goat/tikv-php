<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv\Dto;

use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;

/**
 * Maps PD's protobuf Region/Peer messages onto the {@see RegionInfo} DTO.
 *
 * Shared by {@see \CrazyGoat\TiKV\Client\Connection\PdClient::getRegion()}
 * and {@see \CrazyGoat\TiKV\Client\Connection\PdClient::scanRegions()} so the
 * two RPCs share identical, explicit null-handling for a missing leader.
 *
 * A missing leader is reported as `0` ("unknown") for both leaderPeerId and
 * leaderStoreId rather than guessing store id `1`: routing a request to a
 * fabricated store silently misroutes on split/merge. Store id `0` matches
 * no real TiKV store, so {@see \CrazyGoat\TiKV\Client\Region\RegionResolver::resolveStoreAddress()}
 * raises a {@see \CrazyGoat\TiKV\Client\Exception\StoreNotFoundException} — an
 * explicit, visible failure instead of silent misrouting.
 */
final class RegionInfoMapper
{
    /**
     * @param Region $region non-null region returned by PD
     * @param Peer|null $leader region leader, or null when PD reports none
     */
    public static function fromProto(Region $region, ?Peer $leader): RegionInfo
    {
        $regionEpoch = $region->getRegionEpoch();

        $peers = [];
        foreach ($region->getPeers() as $peer) {
            $peers[] = new PeerInfo(
                peerId: (int) $peer->getId(),
                storeId: (int) $peer->getStoreId(),
            );
        }

        return new RegionInfo(
            regionId: (int) $region->getId(),
            leaderPeerId: $leader instanceof Peer ? (int) $leader->getId() : 0,
            leaderStoreId: $leader instanceof Peer ? (int) $leader->getStoreId() : 0,
            epochConfVer: $regionEpoch ? (int) $regionEpoch->getConfVer() : 0,
            epochVersion: $regionEpoch ? (int) $regionEpoch->getVersion() : 0,
            startKey: $region->getStartKey(),
            endKey: $region->getEndKey(),
            peers: $peers,
        );
    }
}
