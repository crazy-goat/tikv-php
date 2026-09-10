<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv\Dto;

use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

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
 *
 * PD reports region boundaries in the encoded key space (for TxnKV that is
 * memory-comparable encoded). When a {@see CodecInterface} is passed, the
 * boundary keys are decoded back to user-key space so entries entering
 * {@see \CrazyGoat\TiKV\Client\Cache\RegionCache} can be compared against the
 * raw user keys the client resolves (issue #415, GAP-01). Empty boundary keys
 * (unbounded range ends) stay empty — they never represent an encoded key.
 */
final class RegionInfoMapper
{
    /**
     * @param Region $region non-null region returned by PD
     * @param Peer|null $leader region leader, or null when PD reports none
     * @param CodecInterface|null $codec when provided, the region's boundary
     *        keys are decoded from PD's encoded space into user-key space
     */
    public static function fromProto(Region $region, ?Peer $leader, ?CodecInterface $codec = null): RegionInfo
    {
        $regionEpoch = $region->getRegionEpoch();

        $peers = [];
        foreach ($region->getPeers() as $peer) {
            $peers[] = new PeerInfo(
                peerId: (int) $peer->getId(),
                storeId: (int) $peer->getStoreId(),
            );
        }

        $startKey = $region->getStartKey();
        $endKey = $region->getEndKey();
        if ($codec instanceof CodecInterface) {
            // Empty keys mean "unbounded" and are never MCE-encoded — decoding
            // them would throw (the terminator is missing by design).
            $startKey = $startKey === '' ? '' : $codec->decodeRegionKey($startKey);
            $endKey = $endKey === '' ? '' : $codec->decodeRegionKey($endKey);
        }

        return new RegionInfo(
            regionId: (int) $region->getId(),
            leaderPeerId: $leader instanceof Peer ? (int) $leader->getId() : 0,
            leaderStoreId: $leader instanceof Peer ? (int) $leader->getStoreId() : 0,
            epochConfVer: $regionEpoch ? (int) $regionEpoch->getConfVer() : 0,
            epochVersion: $regionEpoch ? (int) $regionEpoch->getVersion() : 0,
            startKey: $startKey,
            endKey: $endKey,
            peers: $peers,
        );
    }
}
