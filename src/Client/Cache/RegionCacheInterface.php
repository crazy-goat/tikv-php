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
     * Store a region in the cache.
     */
    public function put(RegionInfo $region): void;

    /**
     * Remove a region from the cache by its ID.
     */
    public function invalidate(int $regionId): void;

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
