<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

final class RegionGrouper
{
    /**
     * Read the region that {@see RegionResolver::batchResolveRegions()}
     * assigned to $key, failing closed when the map has no entry for it.
     *
     * **Every** region-grouping loop in the client reads its map through
     * this accessor, and none of them may `continue` past a miss. The
     * resolver already fails closed (issue #244), so a miss is an internal
     * error — the client's own invariant is broken, or the resolver contract
     * changed — never a key to skip. Skipping one is silent data loss:
     * `batchPut()`/`batchDelete()`/`ingest()` return `void` and would report
     * success for a write that was never sent, and `batchGet()` would hand
     * back a `null` indistinguishable from a legitimately missing key. That
     * is why the guard exists at all and why it throws: client-go's
     * `GroupKeysByRegion` propagates the resolution error instead of
     * dropping keys (issue #187).
     *
     * The key is redacted with {@see KeyRedactor::redact()} — the message
     * must name *which* key could not be routed without ever putting raw
     * key bytes in an exception message.
     *
     * @param array<array-key, RegionInfo> $resolved map returned by
     *     {@see RegionResolver::batchResolveRegions()}
     * @param array-key $key the key being grouped. `int|string`, not
     *     `string`: the map inherits PHP's array-key semantics (issue
     *     #261), so a canonical decimal key is stored under its `int` form.
     * @throws TiKvException when the map has no region for $key
     */
    public static function resolvedRegion(array $resolved, int|string $key): RegionInfo
    {
        $region = $resolved[$key] ?? null;
        if (!$region instanceof RegionInfo) {
            throw new TiKvException(sprintf(
                'Region could not be resolved for key %s; refusing to silently drop it from the batch',
                KeyRedactor::redact((string) $key),
            ));
        }

        return $region;
    }

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
            $grouped[$regionId] ??= ['region' => $region, 'keys' => []];
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
            // Fails closed rather than skipping: see resolvedRegion().
            $region = self::resolvedRegion($resolved, $key);
            $regionId = $region->regionId;
            $grouped[$regionId] ??= ['region' => $region, 'keys' => []];
            $grouped[$regionId]['keys'][] = $key;
        }

        return $grouped;
    }

    /**
     * Group arbitrary items by region using a key-extractor callable.
     *
     * This is the generalised version of {@see groupKeysByRegionBatch} for
     * callers that hold non-string items (e.g. Mutation objects). Every key
     * must resolve to a region — an unresolvable key throws a
     * {@see TiKvException} naming the key, it is never skipped
     * ({@see resolvedRegion()}; issue #244, #187).
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
            // Fails closed rather than skipping: see resolvedRegion().
            $region = self::resolvedRegion($resolved, $key);
            $regionId = $region->regionId;
            $grouped[$regionId] ??= ['region' => $region, 'items' => []];
            $grouped[$regionId]['items'][] = $item;
        }

        return $grouped;
    }
}
