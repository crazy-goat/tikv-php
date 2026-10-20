<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyOrder;

/**
 * How many regions a scan asks PD for in one `ScanRegions` call (issue #282).
 *
 * A scan used to ask PD for **every** region covering the requested range
 * (`limit = 0`), even though the scan itself is capped at a few thousand
 * rows and normally stops after the first region or two. On a large cluster
 * that answer is tens of megabytes of `metapb.Region` — more than a gRPC
 * receive limit admits — and it is paid for a query that reads a single
 * region.
 *
 * The client therefore asks for a bounded number of regions and loads the
 * next batch only when the scan actually consumes the current one. The
 * bound is shared by the RawKV and the TxnKV scanners so the two paths
 * cannot drift apart.
 */
final class RegionScanBatch
{
    /**
     * Maximum number of regions requested per PD `ScanRegions` call.
     *
     * A region that contributes to a scan holds at least one row, so a scan
     * never needs more regions than it has rows left to return. The value
     * is a ceiling for the wide scans that do span many regions: it keeps a
     * PD response in the tens of kilobytes instead of the tens of megabytes
     * an unlimited request answers with.
     */
    public const MAX_REGIONS = 32;

    /**
     * The number of regions to request for a scan that may still return
     * $rowBudget rows.
     *
     * The row budget bounds the request from above (a scan cannot use more
     * regions than it has rows left to return) and the floor of one region
     * bounds it from below, so a narrow scan asks PD for a narrow region
     * list.
     */
    public static function forRowBudget(int $rowBudget): int
    {
        return max(1, min($rowBudget, self::MAX_REGIONS));
    }

    /**
     * Trim the regions of a batch to the cursor the batch was fetched at.
     *
     * A region that starts below the cursor overlaps the range the previous
     * batch already covered: it is the region a **merge** produced after that
     * batch was enumerated, so the part below the cursor was read already
     * and must not be read a second time. The trimmed region is a copy — the
     * original one is what the region cache holds.
     *
     * A region that lies entirely below the cursor is dropped: the previous
     * batch covered it.
     *
     * @param list<RegionInfo> $regions
     * @return list<RegionInfo>
     */
    public static function trimToCursor(array $regions, string $cursor): array
    {
        $trimmed = [];
        foreach ($regions as $region) {
            if (KeyOrder::gte($region->startKey, $cursor)) {
                $trimmed[] = $region;
                continue;
            }

            if ($region->endKey !== '' && KeyOrder::lte($region->endKey, $cursor)) {
                continue;
            }

            $trimmed[] = new RegionInfo(
                regionId: $region->regionId,
                leaderPeerId: $region->leaderPeerId,
                leaderStoreId: $region->leaderStoreId,
                epochConfVer: $region->epochConfVer,
                epochVersion: $region->epochVersion,
                startKey: $cursor,
                endKey: $region->endKey,
                peers: $region->peers,
            );
        }

        return $trimmed;
    }
}
