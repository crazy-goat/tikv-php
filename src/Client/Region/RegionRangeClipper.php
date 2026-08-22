<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

/**
 * Clips a user-specified [start, end) key range to the boundaries of each
 * region in an ordered region list, yielding (region, clippedStart, clippedEnd)
 * tuples with consistent half-open [start, end) semantics.
 *
 * Empty end-key is treated as +infinity. Sub-ranges that fall completely
 * outside a region are skipped.
 *
 * This centralises the region-clipping logic that was duplicated across
 * RawKvScanner, RawKvRangeOps and Transaction, ensuring that boundary
 * behaviour is defined once and tested once.
 */
final readonly class RegionRangeClipper
{
    /**
     * Clip a forward [start, end) range to each region's bounds.
     *
     * Regions are expected in ascending order (as returned by
     * PdClient::scanRegions). Each yielded tuple contains:
     *   [0] => RegionInfo
     *   [1] => clipped start key
     *   [2] => clipped end key
     *
     * @param RegionInfo[] $regions
     * @return \Generator<int, array{RegionInfo, string, string}>
     */
    public function clipForward(array $regions, string $startKey, string $endKey): \Generator
    {
        foreach ($regions as $region) {
            $scanStart = strcmp($startKey, $region->startKey) > 0 ? $startKey : $region->startKey;
            $scanEnd = $endKey === ''
                ? $region->endKey
                : ($region->endKey !== '' && strcmp($endKey, $region->endKey) > 0 ? $region->endKey : $endKey);

            if (strcmp($scanStart, $scanEnd) >= 0 && $scanEnd !== '') {
                continue;
            }

            yield [$region, $scanStart, $scanEnd];
        }
    }

    /**
     * Clip a reverse [start, end) range to each region's bounds.
     *
     * Regions should be in descending order (e.g. after array_reverse).
     * The returned clipped range has the same semantics as forward
     * ([clippedEnd, clippedStart) with reverse=true on the wire), meaning
     * scanning proceeds from clippedStart downwards towards clippedEnd.
     *
     * @param RegionInfo[] $regions
     * @return \Generator<int, array{RegionInfo, string, string}>
     */
    public function clipReverse(array $regions, string $startKey, string $endKey): \Generator
    {
        foreach ($regions as $region) {
            $scanStart = ($region->endKey === '' || strcmp($startKey, $region->endKey) < 0) ? $startKey : $region->endKey;
            $scanEnd = (strcmp($endKey, $region->startKey) > 0) ? $endKey : $region->startKey;

            if (strcmp($scanEnd, $scanStart) >= 0 && $scanEnd !== '') {
                continue;
            }

            yield [$region, $scanStart, $scanEnd];
        }
    }
}
