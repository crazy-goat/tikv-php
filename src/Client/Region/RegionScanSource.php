<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyOrder;

/**
 * Enumerates the regions covering a scan range, reading the region cache first
 * and asking PD only for the uncovered gaps (issue #425).
 *
 * The scanner used to treat the cache as all-or-nothing: one missing region
 * made `RegionCache::getRegionsInRange()` answer `[]`, and the whole remaining
 * window was re-scanned from PD. This class walks the cache in contiguous
 * runs and, at the first gap, issues one bounded `ScanRegions` call that
 * starts exactly at the gap — so the PD window is the uncovered range, not
 * the range the cache already covers.
 *
 * Every run of regions served without PD counts as one `regionCacheHit()`;
 * every `ScanRegions` call counts as one `regionCacheMiss()`, both under the
 * configured operation label (`'scan'` for RawKV, `'txn_scan'` for TxnKV), so
 * the scan path is visible in the same counters as point lookups.
 *
 * Termination mirrors the previous per-scanner loops and is unconditional:
 * an empty PD page, an unbounded last region, a region that reaches the
 * requested end key, a non-advancing end key, a page shorter than the limit,
 * and a non-advancing cached chain all stop the enumeration.
 *
 * @see RegionScanBatch for the batch-size bound and the cursor trimming.
 */
final readonly class RegionScanSource
{
    public function __construct(
        private PdClientInterface $pdClient,
        private RegionCacheInterface $regionCache,
        private MetricsInterface $metrics = new NoOpMetrics(),
        private string $operation = 'scan',
    ) {
    }

    /**
     * Yield bounded batches of regions covering [startKey, endKey), in
     * ascending startKey order.
     *
     * A batch served from the cache may span the whole cached chain; a batch
     * fetched from PD contains at most $batchSize regions. The generator is
     * lazy, so a consumer satisfied by the first batch never triggers the
     * next PD call.
     *
     * @param int $batchSize number of regions to request per PD call
     * @return \Generator<int, list<RegionInfo>>
     */
    public function batches(string $startKey, string $endKey, int $batchSize): \Generator
    {
        $cursor = $startKey;

        while (true) {
            // The complete-chain lookup is the cheap all-or-nothing fast path
            // (a warm cache answers the whole remainder in one call). Only
            // when it cannot cover the range does the partial-chain lookup
            // run, so the PD window starts at the first gap (issue #425).
            $cached = $this->regionCache->getRegionsInRange($cursor, $endKey);
            if ($cached !== []) {
                $this->metrics->regionCacheHit($this->operation);
                yield RegionScanBatch::trimToCursor($cached, $cursor);

                return;
            }

            $chain = $this->regionCache->getCachedChain($cursor, $endKey);
            if ($chain !== []) {
                $this->metrics->regionCacheHit($this->operation);
                $lastEndKey = $chain[count($chain) - 1]->endKey;

                // Trim only the part below a cursor produced by an earlier
                // merge, exactly as the PD path does.
                yield RegionScanBatch::trimToCursor($chain, $cursor);

                if (KeyOrder::lte($lastEndKey, $cursor)) {
                    // Defensive: a cache that cannot advance would loop.
                    return;
                }

                // A gap starts at $lastEndKey: ask PD for the uncovered
                // remainder on the next iteration.
                $cursor = $lastEndKey;
                continue;
            }

            $this->metrics->regionCacheMiss($this->operation);

            $batch = array_values($this->pdClient->scanRegions($cursor, $endKey, $batchSize));
            if ($batch === []) {
                return;
            }

            foreach ($batch as $region) {
                $this->regionCache->put($region);
            }

            yield RegionScanBatch::trimToCursor($batch, $cursor);

            $lastEndKey = $batch[count($batch) - 1]->endKey;
            if ($lastEndKey === '') {
                // The last region of the keyspace: nothing follows it.
                return;
            }
            if ($endKey !== '' && KeyOrder::gte($lastEndKey, $endKey)) {
                // The requested range is covered.
                return;
            }
            if (KeyOrder::lte($lastEndKey, $cursor)) {
                // No forward progress: stop instead of asking PD for the same
                // page again.
                return;
            }
            if (count($batch) < $batchSize) {
                // PD answered with fewer regions than requested, so the range
                // holds no more.
                return;
            }

            $cursor = $lastEndKey;
        }
    }
}
