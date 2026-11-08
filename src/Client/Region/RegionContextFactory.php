<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use Closure;
use CrazyGoat\Proto\Kvrpcpb\CommandPri;
use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\IsolationLevel;
use CrazyGoat\Proto\Kvrpcpb\ResourceControlContext;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\Dto\ReplicaReadTarget;

use function array_filter;
use function array_values;
use function count;
use function random_int;

final class RegionContextFactory
{
    /**
     * Resolve the peer (and store) a read request should target (issue #421).
     *
     * Leader mode always produces the leader
     * peer as before. Follower / Mixed / PreferLeader select a replica peer
     * from RegionInfo::peers, optionally restricted to stores carrying all
     * of the policy's matchStoreLabels (looked up through $storeLookup, a
     * `Closure(int $storeId): ?Store` — RegionResolver::getStore(...)).
     * PreferLeader keeps using the leader while the leader satisfies the
     * label policy; Follower/Mixed pick a replica at random to spread read
     * load. When no replica satisfies the selection — or the only replica
     * was just excluded after a DataIsNotReady error — the target degrades
     * to the leader so a label policy or a lagging replica never fails a
     * read outright.
     *
     * Context.replica_read is set when the request goes to a non-leader
     * peer; Context.stale_read follows the policy's staleRead flag (it is
     * also valid on leader-targeted stale reads).
     *
     * @param Closure(int): ?Store $storeLookup store-label source for the
     *     matchStoreLabels filter (only consulted when labels are configured)
     * @param int|null $excludedStoreId store id to exclude from replica
     *     selection — set to a store that previously answered DataIsNotReady
     *     so the next attempt falls back to another replica or the leader
     * @param int $priority Kvrpcpb CommandPri carried on the Context
     *     (issue #336). Normal (0) is the proto3 default and is therefore
     *     not serialized — a request that never had a priority keeps its
     *     original wire bytes.
     * @param RequestContextOptions|null $requestContext Optional control
     *     fields (isolation level, not-fill-cache, max execution duration,
     *     request source, resource group) applied on top of the routing
     *     Context (issue #426). Null applies none of them.
     * @param ResourceControl|null $resourceControl Resource-control metadata
     *     (issue #427): the resource group and the resource-group tag. Null
     *     (the default) writes neither field. Applied before
     *     $requestContext, so a non-default field of the latter wins when
     *     both name the same Context field.
     * @param (Closure(int): bool)|null $storeAvailable Optional health filter
     *     (issue #424): a replica whose store is in its health cooldown is
     *     skipped, so a retry does not re-select the same unhealthy replica.
     *     Null keeps the historical selection unchanged.
     * @param (Closure(int, int): int)|null $randomInt Internal test seam for
     *     the replica choice (issue #817): it receives the candidate index
     *     bounds and must return an index inside them. Null uses random_int()
     *     as before; application code must leave it null.
     */
    public static function resolveTarget(
        RegionInfo $region,
        ReplicaReadPolicy $policy,
        Closure $storeLookup,
        ?int $excludedStoreId = null,
        ?CodecInterface $codec = null,
        int $priority = CommandPri::Normal,
        ?RequestContextOptions $requestContext = null,
        ?ResourceControl $resourceControl = null,
        ?Closure $storeAvailable = null,
        ?Closure $randomInt = null,
    ): ReplicaReadTarget {
        $leader = self::leaderPeer($region);
        $leaderTarget = new ReplicaReadTarget(
            self::buildContext(
                $region,
                $leader,
                false,
                $policy->staleRead,
                $codec,
                $priority,
                $requestContext,
                $resourceControl,
            ),
            $region->leaderStoreId,
        );

        if ($policy->mode === ReplicaReadMode::Leader) {
            return $leaderTarget;
        }

        $labels = $policy->matchStoreLabels;
        $leaderAvailable = !$storeAvailable instanceof \Closure || $storeAvailable($region->leaderStoreId);

        if (
            $policy->mode === ReplicaReadMode::PreferLeader
            && $excludedStoreId !== $region->leaderStoreId
            && $leaderAvailable
            && self::storeMatchesLabels($region->leaderStoreId, $labels, $storeLookup)
        ) {
            return $leaderTarget;
        }

        $candidates = array_values(array_filter(
            $region->peers,
            static fn (PeerInfo $peer): bool => $peer->storeId !== $excludedStoreId
                && ($policy->mode === ReplicaReadMode::Mixed || $peer->storeId !== $region->leaderStoreId)
                && (!$storeAvailable instanceof \Closure || $storeAvailable($peer->storeId)),
        ));

        if ($labels !== []) {
            $candidates = self::filterByStoreLabels($candidates, $labels, $storeLookup);
        }

        if ($candidates === []) {
            // No eligible replica (label filter empty, leader excluded, or
            // the only replica just failed with DataIsNotReady): degrade to
            // the leader so the read still succeeds.
            return $leaderTarget;
        }

        $lastIndex = count($candidates) - 1;
        $index = $randomInt instanceof Closure ? $randomInt(0, $lastIndex) : random_int(0, $lastIndex);
        $peerInfo = $candidates[$index];

        return new ReplicaReadTarget(
            self::buildContext(
                $region,
                self::peerFromInfo($peerInfo),
                true,
                $policy->staleRead,
                $codec,
                $priority,
                $requestContext,
                $resourceControl,
            ),
            $peerInfo->storeId,
        );
    }

    /**
     * Build the Context of a request addressed to the region's leader.
     *
     * @param int $priority Kvrpcpb CommandPri carried on the Context
     *     (issue #336). Normal (0) is the proto3 default and is not
     *     serialized, so a request without a priority keeps its original
     *     wire bytes.
     * @param RequestContextOptions|null $requestContext Optional control
     *     fields applied on top of the routing Context (issue #426).
     * @param ResourceControl|null $resourceControl Resource-control metadata
     *     (issue #427); null writes neither resource-control field. Applied
     *     before $requestContext, so the latter wins on overlapping fields.
     */
    public static function fromRegionInfo(
        RegionInfo $region,
        ?CodecInterface $codec = null,
        int $priority = CommandPri::Normal,
        ?RequestContextOptions $requestContext = null,
        ?ResourceControl $resourceControl = null,
    ): Context {
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
        $ctx->setPriority($priority);
        $resourceControl?->apply($ctx);
        self::applyCodec($ctx, $codec);
        self::applyRequestContext($ctx, $requestContext);

        return $ctx;
    }

    private static function buildContext(
        RegionInfo $region,
        Peer $peer,
        bool $replicaRead,
        bool $staleRead,
        ?CodecInterface $codec = null,
        int $priority = CommandPri::Normal,
        ?RequestContextOptions $requestContext = null,
        ?ResourceControl $resourceControl = null,
    ): Context {
        $epoch = new RegionEpoch();
        $epoch->setConfVer($region->epochConfVer);
        $epoch->setVersion($region->epochVersion);

        $ctx = new Context();
        $ctx->setRegionId($region->regionId);
        $ctx->setRegionEpoch($epoch);
        $ctx->setPeer($peer);
        $ctx->setReplicaRead($replicaRead);
        $ctx->setStaleRead($staleRead);
        $ctx->setPriority($priority);
        $resourceControl?->apply($ctx);
        self::applyCodec($ctx, $codec);
        self::applyRequestContext($ctx, $requestContext);

        return $ctx;
    }

    /**
     * Copy the optional control fields onto the Context (issue #426).
     *
     * Each field is written only when it differs from its proto3 default, so
     * an unset option leaves the serialized Context byte-for-byte unchanged.
     */
    private static function applyRequestContext(Context $context, ?RequestContextOptions $options): void
    {
        if (!$options instanceof RequestContextOptions) {
            return;
        }

        if ($options->isolationLevel !== IsolationLevel::SI) {
            $context->setIsolationLevel($options->isolationLevel);
        }
        if ($options->notFillCache) {
            $context->setNotFillCache(true);
        }
        if ($options->maxExecutionDurationMs > 0) {
            $context->setMaxExecutionDurationMs($options->maxExecutionDurationMs);
        }
        if ($options->requestSource !== '') {
            $context->setRequestSource($options->requestSource);
        }
        if ($options->resourceGroupTag !== '') {
            $context->setResourceGroupTag($options->resourceGroupTag);
        }
        if ($options->busyThresholdMs > 0) {
            $context->setBusyThresholdMs($options->busyThresholdMs);
        }
        if ($options->resourceGroupName !== '') {
            $resourceControlContext = new ResourceControlContext();
            $resourceControlContext->setResourceGroupName($options->resourceGroupName);
            $context->setResourceControlContext($resourceControlContext);
        }
    }

    private static function applyCodec(Context $context, ?CodecInterface $codec): void
    {
        if (!$codec instanceof CodecV2) {
            return;
        }

        $context->setApiVersion($codec->getApiVersion());
        $context->setKeyspaceName($codec->getKeyspaceName());
        $context->setKeyspaceId($codec->getKeyspaceId());
    }

    private static function leaderPeer(RegionInfo $region): Peer
    {
        $peer = new Peer();
        $peer->setId($region->leaderPeerId);
        $peer->setStoreId($region->leaderStoreId);

        return $peer;
    }

    private static function peerFromInfo(PeerInfo $info): Peer
    {
        $peer = new Peer();
        $peer->setId($info->peerId);
        $peer->setStoreId($info->storeId);

        return $peer;
    }

    /**
     * Keep only peers whose store carries every requested label. Stores that
     * cannot be looked up (PD miss) are treated as non-matching.
     *
     * @param PeerInfo[] $peers
     * @param array<string, string> $labels
     * @param Closure(int): ?Store $storeLookup
     * @return PeerInfo[]
     */
    private static function filterByStoreLabels(array $peers, array $labels, Closure $storeLookup): array
    {
        return array_values(array_filter(
            $peers,
            static fn (PeerInfo $peer): bool => self::storeMatchesLabels($peer->storeId, $labels, $storeLookup),
        ));
    }

    /**
     * @param array<string, string> $labels
     * @param Closure(int): ?Store $storeLookup
     */
    private static function storeMatchesLabels(int $storeId, array $labels, Closure $storeLookup): bool
    {
        if ($labels === []) {
            return true;
        }

        $store = $storeLookup($storeId);
        if (!$store instanceof Store) {
            return false;
        }

        $storeLabels = [];
        foreach ($store->getLabels() as $label) {
            $storeLabels[(string) $label->getKey()] = (string) $label->getValue();
        }

        foreach ($labels as $key => $value) {
            if (($storeLabels[$key] ?? null) !== $value) {
                return false;
            }
        }

        return true;
    }
}
