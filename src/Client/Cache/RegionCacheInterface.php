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
     * Store a region in the cache.
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
