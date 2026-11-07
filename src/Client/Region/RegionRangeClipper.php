<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyOrder;

/**
 * Clips a user-specified [start, end) key range to the boundaries of each
 * region in an ordered region list, yielding (region, clippedStart, clippedEnd)
 * tuples with consistent half-open [start, end) semantics.
 *
 * For forward clipping an empty end-key is +infinity; for reverse clipping it
 * is the start of the keyspace. A reverse request must name an explicit upper
 * bound: an empty start-key is not +infinity (RAW-09, #194), so clipReverse()
 * rejects it instead of silently clipping every region to nothing. Sub-ranges
 * that fall completely outside a region are skipped.
 *
 * This centralises the region-clipping logic that was duplicated across
 * RawKvScanner, RawKvRangeOps and Transaction, ensuring that boundary
 * behaviour is defined once and tested once.
 */
final readonly class RegionRangeClipper
{
    /**
     * Reject a reversed forward range before any PD or store RPC.
     *
     * A forward range is `[startKey, endKey)`, so `startKey` must not sort
     * after `endKey`. An empty `endKey` means +infinity and is always valid;
     * `startKey === endKey` is a valid empty range (it returns no rows and
     * deletes nothing), not a reversed one.
     *
     * @throws InvalidArgumentException When `startKey` sorts after a non-empty `endKey`
     */
    public static function assertForwardRangeOrder(string $startKey, string $endKey, string $method): void
    {
        if ($endKey !== '' && KeyOrder::gt($startKey, $endKey)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid range in %s: start key must not sort after end key',
                $method,
            ));
        }
    }

    /**
     * Reject a reversed reverse-scan range before any PD or store RPC.
     *
     * A reverse scan reads `[endKey, startKey)` descending, so `startKey` is
     * the exclusive upper bound and must not sort before `endKey`. An empty
     * `endKey` means the start of the keyspace and is always valid;
     * `startKey === endKey` is a valid empty range, not a reversed one.
     *
     * An empty `startKey` is not a +infinity upper bound (RAW-09, #194):
     * with a non-empty `endKey` it sorts before the lower bound and is
     * rejected with a message that names the empty upper bound. Only
     * `reverseScan('', '')` remains valid, as the documented empty range.
     *
     * @throws InvalidArgumentException When `startKey` is empty and `endKey`
     *         is not, or when `startKey` sorts before a non-empty `endKey`
     */
    public static function assertReverseRangeOrder(string $startKey, string $endKey, string $method): void
    {
        if ($startKey === '' && $endKey !== '') {
            throw new InvalidArgumentException(sprintf(
                'Invalid range in %s: an empty start key is not a valid upper bound; '
                . 'reverse scan requires an explicit upper bound',
                $method,
            ));
        }

        if ($endKey !== '' && KeyOrder::lt($startKey, $endKey)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid range in %s: start key must not sort before end key',
                $method,
            ));
        }
    }

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
            $scanStart = KeyOrder::gt($startKey, $region->startKey) ? $startKey : $region->startKey;
            $scanEnd = $endKey === ''
                ? $region->endKey
                : ($region->endKey !== '' && KeyOrder::gt($endKey, $region->endKey) ? $region->endKey : $endKey);

            if (KeyOrder::gte($scanStart, $scanEnd) && $scanEnd !== '') {
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
     * The request must be a valid reverse range (see
     * {@see self::assertReverseRangeOrder()}): an empty start key with a
     * non-empty end key throws rather than yielding sub-ranges whose empty
     * upper bound TiKV reads as the keyspace minimum (REG-23, #254).
     *
     * @param RegionInfo[] $regions
     * @return \Generator<int, array{RegionInfo, string, string}>
     *
     * @throws InvalidArgumentException When the range is not a valid reverse range
     */
    public function clipReverse(array $regions, string $startKey, string $endKey): \Generator
    {
        self::assertReverseRangeOrder($startKey, $endKey, 'clipReverse');

        foreach ($regions as $region) {
            $scanStart = ($region->endKey === '' || KeyOrder::lt($startKey, $region->endKey))
                ? $startKey
                : $region->endKey;
            $scanEnd = (KeyOrder::gt($endKey, $region->startKey)) ? $endKey : $region->startKey;

            if (KeyOrder::gte($scanEnd, $scanStart) && $scanEnd !== '') {
                continue;
            }

            yield [$region, $scanStart, $scanEnd];
        }
    }
}
