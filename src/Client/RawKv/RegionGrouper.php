<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;

final class RegionGrouper
{
    /**
     * @param string[] $keys
     * @param callable(string): RegionInfo $regionResolver
     * @return array<int, array{region: RegionInfo, keys: string[]}>
     */
    public static function groupKeysByRegion(array $keys, callable $regionResolver): array
    {
        $grouped = [];
        foreach ($keys as $key) {
            $region = $regionResolver($key);
            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'keys' => []];
            }
            $grouped[$regionId]['keys'][] = $key;
        }

        return $grouped;
    }

    /**
     * Group keys by region using batch resolution (single scanRegions call).
     *
     * @param string[] $keys
     * @return array<int, array{region: RegionInfo, keys: string[]}>
     */
    public static function groupKeysByRegionBatch(array $keys, RegionResolver $regionResolver): array
    {
        if ($keys === []) {
            return [];
        }

        $resolved = $regionResolver->batchResolveRegions($keys);

        $grouped = [];
        foreach ($keys as $key) {
            $region = $resolved[$key] ?? null;
            if ($region === null) {
                continue;
            }
            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'keys' => []];
            }
            $grouped[$regionId]['keys'][] = $key;
        }

        return $grouped;
    }
}
