<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

final readonly class RegionResolver
{
    public function __construct(
        private PdClientInterface $pdClient,
        private RegionCacheInterface $regionCache,
        private MetricsInterface $metrics = new NoOpMetrics(),
    ) {
    }

    public function getRegionInfo(string $key): RegionInfo
    {
        $region = $this->regionCache->getByKey($key);
        if ($region instanceof RegionInfo) {
            $this->metrics->regionCacheHit('region_resolution');

            return $region;
        }

        $this->metrics->regionCacheMiss('region_resolution');
        $region = $this->pdClient->getRegion($key);
        $this->regionCache->put($region);

        return $region;
    }

    /**
     * Resolve regions for a batch of keys using a single scanRegions() call
     * instead of one getRegion() per key. Populates the cache as a side effect.
     *
     * @param string[] $keys
     * @return array<string, RegionInfo> key => region mapping
     */
    public function batchResolveRegions(array $keys): array
    {
        if ($keys === []) {
            return [];
        }

        $sorted = $keys;
        sort($sorted, SORT_STRING);

        $minKey = $sorted[0];
        $maxKey = end($sorted);

        $regions = $this->pdClient->scanRegions($minKey, $maxKey);

        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

        return $this->assignKeysToRegions($keys, $regions);
    }

    /**
     * Assign keys to regions using binary search on sorted region boundaries.
     *
     * @param string[] $keys
     * @param RegionInfo[] $regions regions sorted by startKey
     * @return array<string, RegionInfo>
     */
    private function assignKeysToRegions(array $keys, array $regions): array
    {
        if ($regions === []) {
            return [];
        }

        $result = [];
        foreach ($keys as $key) {
            $region = $this->findRegionForKey($key, $regions);
            if ($region instanceof RegionInfo) {
                $result[$key] = $region;
            }
        }

        return $result;
    }

    /**
     * Find the region containing the given key using binary search.
     *
     * @param RegionInfo[] $regions regions sorted by startKey
     */
    private function findRegionForKey(string $key, array $regions): ?RegionInfo
    {
        $left = 0;
        $right = count($regions) - 1;
        $result = null;

        while ($left <= $right) {
            $mid = (int) (($left + $right) / 2);
            $region = $regions[$mid];

            if ($region->startKey <= $key) {
                $result = $region;
                $left = $mid + 1;
            } else {
                $right = $mid - 1;
            }
        }

        if ($result !== null && $result->endKey !== '' && $key >= $result->endKey) {
            return null;
        }

        return $result;
    }

    public function resolveStoreAddress(int $storeId): string
    {
        $store = $this->pdClient->getStore($storeId);
        if (!$store instanceof Store) {
            throw new StoreNotFoundException($storeId);
        }

        $address = $store->getAddress();
        if ($address === '') {
            throw new StoreNotFoundException($storeId);
        }

        return $address;
    }
}
