<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
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

    /**
     * Group arbitrary items by region using a key-extractor callable.
     *
     * This is the generalised version of {@see groupKeysByRegionBatch} for
     * callers that hold non-string items (e.g. Mutation objects). Items
     * whose key could not be resolved are silently skipped (caller should
     * have validated all keys beforehand).
     *
     * Example:
     * <code>
     * RegionGrouper::groupItemsByRegion(
     *     $mutations,
     *     fn(Mutation $m) => $m->getKey(),
     *     $regionResolver,
     * );
     * </code>
     *
     * @template T of object
     * @param T[] $items
     * @param callable(T): string $keyExtractor
     * @return array<int, array{region: RegionInfo, items: T[]}>
     */
    public static function groupItemsByRegion(
        array $items,
        callable $keyExtractor,
        RegionResolver $regionResolver,
    ): array {
        if ($items === []) {
            return [];
        }

        $keys = array_map($keyExtractor, $items);
        $resolved = $regionResolver->batchResolveRegions($keys);

        $grouped = [];
        foreach ($items as $item) {
            $key = $keyExtractor($item);
            $region = $resolved[$key] ?? null;
            if ($region === null) {
                continue;
            }
            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'items' => []];
            }
            $grouped[$regionId]['items'][] = $item;
        }

        return $grouped;
    }
}
