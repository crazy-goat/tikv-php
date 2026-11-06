<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

interface RegionCacheInterface
{
    /**
     * Look up the region that contains the given key.
     */
    public function getByKey(string $key): ?RegionInfo;

    /**
     * Look up a cached region by its ID, or null when the cache does not hold
     * it — never stored, invalidated, evicted, cleared, or past its TTL. An
     * expired entry is dropped rather than reported, exactly as
     * {@see self::getByKey()} drops it.
     *
     * The answer is the same value {@see self::getByKey()} returns for the
     * entry: a *leader-aware* region, whose leaderPeerId and leaderStoreId
     * reflect every `switchLeader()` applied since it was stored, not the raw
     * stored region. A successful lookup also counts as a use for LRU, exactly
     * as `getByKey()` does.
     *
     * O(1) by construction, where `getByKey()` walks the ordered index: a
     * caller that already holds a region ID — a PD answer it is about to
     * store, for instance — must not pay a treap descent to learn what the ID
     * map already holds. Issue #288.
     */
    public function getById(int $regionId): ?RegionInfo;

    /**
     * Return the contiguous chain of cached regions covering the half-open
     * key range [startKey, endKey), in ascending startKey order.
     *
     * The chain starts at the region containing $startKey and is followed
     * through each region's end key until a region covers $endKey (an empty
     * end key means +infinity, matching
     * {@see \CrazyGoat\TiKV\Client\Connection\PdClientInterface::scanRegions()}).
     *
     * Returns an empty array when the cache does not hold the complete chain
     * (a cold or partially warm cache, or a gap), so the caller falls back to
     * PD. Scans use this to serve an entire page from the region cache
     * without a PD scanRegions() round trip per page (issue #293).
     *
     * @return list<RegionInfo>
     */
    public function getRegionsInRange(string $startKey, string $endKey): array;

    /**
     * Snapshot of every region the cache currently holds, keyed by region ID.
     *
     * This is the enumeration seam used by the region-cache refresher
     * (issue #425): a tick re-queries PD for each cached region and folds the
     * answers back through {@see self::put()}. Two details are part of the
     * contract:
     *
     * - the answer is a *snapshot*; later cache mutations do not affect it;
     * - an entry whose TTL has already elapsed but which no lookup has dropped
     *   yet is included, because a refresh is exactly what can revive it.
     *   A refresh that only saw live entries could not warm a cache that sat
     *   idle for longer than the TTL.
     *
     * The returned regions are leader-aware, exactly like
     * {@see self::getById()}: a `switchLeader()` applied since the entry was
     * stored is reflected in `leaderStoreId` / `leaderPeerId`.
     *
     * @return array<int, RegionInfo> regionId => region
     */
    public function getCachedRegions(): array;

    /**
     * Store a region in the cache.
     *
     * Implementations must not cache a region without a known leader
     * (`leaderStoreId === 0`): routing it fails with a fatal
     * StoreNotFoundException, and a region cached with no leader would be
     * re-resolved only after that failure (issues #576, #627).
     */
    public function put(RegionInfo $region): void;

    /**
     * Remove a region from the cache by its ID.
     *
     * $reason names the caller for the regionInvalidated() metric
     * (MetricsInterface); it is forwarded to the metrics backend when this
     * cache was constructed with one. Defaults to 'region_error'.
     */
    public function invalidate(int $regionId, string $reason = 'region_error'): void;

    /**
     * Switch the leader of a cached region to the peer with the given store ID.
     *
     * Returns true if the peer was found and the leader was switched.
     * Returns false if the region is not cached or the store ID is not among known peers.
     */
    public function switchLeader(int $regionId, int $leaderStoreId): bool;

    /**
     * Remove all regions from the cache.
     */
    public function clear(): void;
}
