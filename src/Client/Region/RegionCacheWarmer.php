<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use Closure;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Observability\DebugLogging;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Opt-in refresh and preload of the region cache (issue #425).
 *
 * PHP has no background threads, so this class owns no timer and starts no
 * loop. A long-running worker calls {@see self::refresh()} from its own tick
 * (the same pattern as `reapIdleChannels()`), or warms the cache at boot with
 * {@see self::preload()}. Both entry points are bounded and idempotent:
 *
 * - `refresh()` re-queries PD for the regions the cache already holds, at most
 *   once per `$refreshIntervalMs` and at most `$maxRegionsPerRefresh` entries
 *   per tick (0 disables the non-forced refresh entirely, making a plain
 *   `refresh()` a no-op);
 * - `preload()` resolves the regions of the given half-open ranges through one
 *   paginated `ScanRegions` sequence per range and stores them.
 *
 * Neither method throws on a PD failure: the failure is logged at warning
 * level and the affected cache entries are kept as they were. A failure of one
 * range does not stop the remaining ranges. Faulty *arguments* (a malformed or
 * reversed range) are a programmer error and throw `InvalidArgumentException`
 * before any RPC.
 *
 * ## Why refresh re-queries by start key
 *
 * PD exposes `GetRegion(key)`, not `GetRegion(id)`, so a cached entry is
 * refreshed by asking PD for the region that now contains that entry's own
 * start key. Three outcomes are possible:
 *
 * - PD returns the same region ID with a newer epoch or leader: `put()`
 *   updates it in place and refreshes its TTL;
 * - PD returns a different region ID (a split or merge happened): `put()`
 *   stores the new region and its overlap removal drops the superseded cached
 *   entry silently, exactly as an ordinary superseding write does (issue
 *   #238 — this is not an error-driven invalidation and emits no
 *   `regionInvalidated()` metric);
 * - PD's answer does not contain the start key (a stale PD region tree, the
 *   same shape `RegionResolver::getRegionInfo()` rejects as a retryable miss):
 *   the cached entry is dropped, so the next request resolves it again.
 *
 * A cached entry is never replaced with an answer that does not cover its own
 * start key, so a refresh cannot poison the cache with the wrong region.
 */
final class RegionCacheWarmer
{
    /**
     * Default refresh interval: 0, i.e. disabled. Refreshing costs one PD
     * `GetRegion` per cached region, so it is opt-in; `60000` (one minute,
     * the shape of client-go's `RegionsRefreshInterval`) is a reasonable
     * setting for a worker tick.
     */
    public const DEFAULT_REFRESH_INTERVAL_MS = 0;

    /**
     * Maximum number of cached regions refreshed in one tick. A tick costs
     * one PD `GetRegion` per region, so the bound keeps a large warm cache
     * from turning one worker tick into thousands of sequential round trips.
     * Successive ticks continue where the previous one stopped (round-robin),
     * so every entry is eventually refreshed.
     */
    public const DEFAULT_MAX_REGIONS_PER_REFRESH = 128;

    /**
     * Regions requested per PD `ScanRegions` call while preloading. The value
     * mirrors `RegionResolver::SCAN_PAGE_SIZE`: a page in the tens of
     * kilobytes, continued until the range is covered.
     */
    private const PRELOAD_PAGE_SIZE = 128;

    /**
     * Hard ceiling on the pages one preloaded range may fetch — the same
     * termination guarantee as `RegionResolver::MAX_SCAN_PAGES`, so a PD that
     * keeps answering with a non-advancing page cannot make the tick loop.
     */
    private const MAX_PRELOAD_PAGES = 1024;

    private int $lastRefreshAtMs = 0;

    /** Round-robin position into the next tick's refreshed slice. */
    private int $refreshOffset = 0;

    /**
     * Whether this warmer emits its debug records at all.
     *
     * A refresh tick logs one record per region it could not store, and a
     * preload logs the same for every page it reads. A logger that discards
     * the record makes those context arrays pointless, so
     * {@see DebugLogging} asks the logger once, at construction (issue
     * #715). Reconfigure the logger before the client is built — see
     * `docs/configuration.md`.
     */
    private readonly bool $debugEnabled;

    /**
     * @param int $refreshIntervalMs Minimum time between two non-forced
     *     refreshes, in milliseconds. 0 (the default) disables the non-forced
     *     refresh and turns a plain `refresh()` into a no-op — the feature is
     *     opt-in. A forced refresh ignores the interval. Must be >= 0.
     * @param (Closure(): int)|null $clockMs Monotonic clock in milliseconds
     *     (test seam); null uses `hrtime(true)`. Only differences are read, so
     *     the origin is irrelevant.
     * @param int $maxRegionsPerRefresh Upper bound on the cache entries one
     *     tick refreshes (round-robin across ticks). Must be >= 1.
     */
    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly RegionCacheInterface $regionCache,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly int $refreshIntervalMs = self::DEFAULT_REFRESH_INTERVAL_MS,
        private readonly ?Closure $clockMs = null,
        private readonly int $maxRegionsPerRefresh = self::DEFAULT_MAX_REGIONS_PER_REFRESH,
    ) {
        if ($refreshIntervalMs < 0) {
            throw new InvalidArgumentException('refreshIntervalMs must be >= 0');
        }
        if ($maxRegionsPerRefresh < 1) {
            throw new InvalidArgumentException('maxRegionsPerRefresh must be >= 1');
        }
        $this->debugEnabled = DebugLogging::handlesDebug($logger);
    }

    /**
     * Refresh a bounded slice of the cached regions from PD.
     *
     * A non-forced call is a no-op when the refresh interval is disabled (0)
     * or when less than `$refreshIntervalMs` has elapsed since the last
     * refresh; a forced call always performs the round. The first call after
     * construction always refreshes. At most `$maxRegionsPerRefresh` entries
     * are visited per call, round-robin, so a large cache costs bounded work
     * per tick and is still fully refreshed over several ticks.
     *
     * The first PD failure aborts the tick — every remaining entry would only
     * repeat the failure (one `getRegion()` already walks all PD endpoints) —
     * and keeps its cached value; the call does not throw.
     *
     * Returns the number of distinct regions the cache holds after the tick
     * that PD confirmed. The accumulated IDs are reconciled with the cache
     * once the slice is done, so a merge or split answered on a later call
     * within the tick counts once, and a leaderless PD answer (which `put()`
     * refuses to cache) counts zero.
     */
    public function refresh(bool $force = false): int
    {
        $nowMs = $this->nowMs();
        if (
            !$force
            && (
                $this->refreshIntervalMs === 0
                || $nowMs - $this->lastRefreshAtMs < $this->refreshIntervalMs
            )
        ) {
            return 0;
        }

        $this->lastRefreshAtMs = $nowMs;

        $entries = array_values($this->regionCache->getCachedRegions());
        $total = count($entries);
        if ($total === 0) {
            return 0;
        }

        $count = min($this->maxRegionsPerRefresh, $total);
        $start = $this->refreshOffset % $total;
        $this->refreshOffset = ($start + $count) % $total;

        /** @var array<int, true> $confirmed region IDs the cache still holds */
        $confirmed = [];

        foreach (array_slice($entries, $start, $count) as $region) {
            if ($region->leaderStoreId === 0) {
                // Not cacheable (issues #576, #627); a PD answer would not be
                // stored either.
                continue;
            }

            try {
                $fresh = $this->pdClient->getRegion($region->startKey);
            } catch (\Throwable $e) {
                $this->logger->warning('Region cache refresh aborted; keeping the cached entries', [
                    'regionId' => $region->regionId,
                    'error' => $e->getMessage(),
                ]);

                break;
            }

            if (!KeyOrder::inRange($region->startKey, $fresh->startKey, $fresh->endKey)) {
                // PD no longer maps this entry's own start key to its cached
                // range: keep serving it would route to a region that does not
                // own the key. Drop it and let the next request re-resolve.
                $this->regionCache->invalidate($region->regionId, 'refresh');
                continue;
            }

            // A different ID means a split or merge superseded the entry;
            // put() removes the overlapping stale entry silently (#238).
            $this->regionCache->put($fresh);
            if ($this->regionCache->getById($fresh->regionId) instanceof RegionInfo) {
                $confirmed[$fresh->regionId] = true;
            } else {
                if ($this->debugEnabled) {
                    $this->logger->debug('Refreshed region not cached (no leader)', [
                        'regionId' => $fresh->regionId,
                    ]);
                }
            }
        }

        // Reconcile with the cache *after* the whole slice: a merge or split
        // answered on a later call removes an ID confirmed earlier, so counting
        // as answers arrive would overstate what is stored.
        $refreshed = $this->countStillCached($confirmed);

        if ($refreshed > 0) {
            if ($this->debugEnabled) {
                $this->logger->debug('Refreshed cached regions', ['refreshed' => $refreshed]);
            }
        }

        return $refreshed;
    }

    /**
     * Resolve and cache the regions of the given half-open ranges without
     * serving a user read.
     *
     * Each range is a `[startKey, endKey]` pair: `endKey === ''` means
     * +infinity, and an empty range (`startKey === endKey`) caches nothing.
     * The method pages `ScanRegions` until the range is covered, bounded by
     * {@see self::MAX_PRELOAD_PAGES}.
     *
     * Returns the number of distinct regions the cache actually holds after
     * the call: a region returned by two overlapping ranges counts once, and
     * a leaderless PD answer (which `put()` refuses to cache) counts zero. A
     * PD failure is logged at warning level and only abandons the range it
     * happened in — the cache keeps whatever it already held, and the
     * remaining ranges are still attempted.
     *
     * @param array<array-key, mixed> $ranges list of `[startKey, endKey]` pairs
     *
     * @throws InvalidArgumentException when a range is not a pair of strings
     *     or is reversed (`startKey > endKey` with a non-empty endKey)
     */
    public function preload(array $ranges): int
    {
        $normalized = $this->normalizeRanges($ranges);
        /** @var array<int, true> $stored region IDs actually cached */
        $stored = [];

        foreach ($normalized as [$startKey, $endKey]) {
            if ($endKey !== '' && KeyOrder::eq($startKey, $endKey)) {
                continue;
            }

            $stored += $this->preloadRange($startKey, $endKey);
        }

        // Reconcile with the cache *after* every range: a later range whose
        // answer is a merge or split removes regions an earlier range stored,
        // so counting as answers arrive would overstate what is cached.
        return $this->countStillCached($stored);
    }

    /**
     * Count the accumulated region IDs that the cache still holds.
     *
     * @param array<int, true> $regionIds
     */
    private function countStillCached(array $regionIds): int
    {
        $stillCached = [];
        foreach (array_keys($regionIds) as $regionId) {
            if ($this->regionCache->getById($regionId) instanceof RegionInfo) {
                $stillCached[$regionId] = true;
            }
        }

        return count($stillCached);
    }

    /**
     * Page through one range and store every cacheable region PD returns.
     *
     * @return array<int, true> region IDs the cache holds (keyed for dedupe
     *     across overlapping ranges)
     */
    private function preloadRange(string $startKey, string $endKey): array
    {
        /** @var array<int, true> $stored */
        $stored = [];
        $cursor = $startKey;

        for ($page = 0; $page < self::MAX_PRELOAD_PAGES; $page++) {
            try {
                $batch = $this->pdClient->scanRegions($cursor, $endKey, self::PRELOAD_PAGE_SIZE);
            } catch (\Throwable $e) {
                $this->logger->warning('Region cache preload failed; keeping the cached entries', [
                    'error' => $e->getMessage(),
                ]);

                return $stored;
            }

            if ($batch === []) {
                return $stored;
            }

            foreach ($batch as $region) {
                if ($region->leaderStoreId === 0) {
                    // put() refuses a leaderless region (issues #576, #627);
                    // count only what the cache actually holds.
                    if ($this->debugEnabled) {
                        $this->logger->debug('Preloaded region not cached (no leader)', [
                            'regionId' => $region->regionId,
                        ]);
                    }
                    continue;
                }

                $this->regionCache->put($region);
                $stored[$region->regionId] = true;
            }

            $lastEndKey = $batch[count($batch) - 1]->endKey;
            if ($lastEndKey === '' || ($endKey !== '' && KeyOrder::gte($lastEndKey, $endKey))) {
                return $stored;
            }

            if (KeyOrder::lte($lastEndKey, $cursor) || count($batch) < self::PRELOAD_PAGE_SIZE) {
                return $stored;
            }

            $cursor = $lastEndKey;
        }

        return $stored;
    }

    /**
     * @param array<array-key, mixed> $ranges
     * @return list<array{0: string, 1: string}>
     */
    private function normalizeRanges(array $ranges): array
    {
        $normalized = [];
        foreach ($ranges as $range) {
            if (
                !is_array($range)
                || !array_key_exists(0, $range)
                || !array_key_exists(1, $range)
                || !is_string($range[0])
                || !is_string($range[1])
            ) {
                throw new InvalidArgumentException(
                    'Each preload range must be a [startKey, endKey] pair of strings',
                );
            }

            if ($range[1] !== '' && KeyOrder::gt($range[0], $range[1])) {
                throw new InvalidArgumentException(
                    'Preload range is reversed: startKey must not sort after endKey',
                );
            }

            $normalized[] = [$range[0], $range[1]];
        }

        return $normalized;
    }

    private function nowMs(): int
    {
        if ($this->clockMs instanceof Closure) {
            return ($this->clockMs)();
        }

        // A monotonic clock: only the difference between two readings is
        // used, so an NTP step cannot delay (or replay) a refresh tick.
        return intdiv(hrtime(true), 1_000_000);
    }
}
