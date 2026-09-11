<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RegionCache implements RegionCacheInterface
{
    /** @var RegionEntry[] */
    private array $entries = [];

    /** @var array<int, int> regionId => index */
    private array $idToIndex = [];

    /** @var array<int, int> regionId => LRU order (higher = more recent) */
    private array $lruOrder = [];

    private int $lruCounter = 0;

    private int $putCountSinceSweep = 0;

    public function __construct(private readonly int $ttlSeconds = 600, private readonly int $jitterSeconds = 60, private readonly int $maxEntries = 10000, private readonly int $sweepInterval = 100, private readonly LoggerInterface $logger = new NullLogger(), private ?MetricsInterface $metrics = null)
    {
    }

    /**
     * The attached metrics backend, or null when none was provided.
     */
    public function metrics(): ?MetricsInterface
    {
        return $this->metrics;
    }

    /**
     * Attach the given metrics backend if this cache does not carry one yet
     * (null); an explicitly attached backend always wins. Mutates THIS
     * instance in place — caches are shared between client components and
     * cloning a user-supplied cache would leave their wiring behind.
     */
    public function attachMetricsIfAbsent(MetricsInterface $metrics): void
    {
        $this->metrics ??= $metrics;
    }

    public function getByKey(string $key): ?RegionInfo
    {
        $index = $this->binarySearch($key);
        if ($index === null) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        $entry = $this->entries[$index];

        if ($this->isExpired($entry)) {
            $this->removeByIndex($index);
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        if ($entry->region->endKey !== '' && strcmp($key, $entry->region->endKey) >= 0) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        // Mark as recently used
        $this->lruOrder[$entry->region->regionId] = ++$this->lruCounter;

        $this->logger->debug('Region cache hit', ['key' => KeyRedactor::redact($key), 'regionId' => $entry->region->regionId]);

        return $this->resolveRegionInfo($entry);
    }

    public function getRegionsInRange(string $startKey, string $endKey): array
    {
        $regions = [];
        $cursor = $startKey;

        while (true) {
            $region = $this->getByKey($cursor);
            if (!$region instanceof RegionInfo) {
                // A gap (or an expired/cold start region): let the caller
                // fall back to a single PD scanRegions() call.
                return [];
            }

            $regions[] = $region;

            if ($region->endKey === '') {
                // Unbounded region covers any requested end key.
                return $regions;
            }

            if ($endKey !== '' && strcmp($region->endKey, $endKey) >= 0) {
                return $regions;
            }

            // Require strictly forward progress: a non-advancing end key
            // means the cached layout is inconsistent, so defer to PD.
            if (strcmp($region->endKey, $cursor) <= 0) {
                return [];
            }

            $cursor = $region->endKey;
        }
    }

    public function put(RegionInfo $region): void
    {
        $this->removeById($region->regionId);

        // Any cached entry whose range overlaps the incoming region's range is
        // superseded (issue #238): after a split or merge PD reports the new
        // region for the full range, so keeping the stale entry would make the
        // binary search resolve keys to a region that no longer owns them.
        // Superseded entries are NOT counted as invalidations — the
        // regionInvalidated() metric is reserved for error-driven drops
        // (issue #474); a fresh insert supersedes them, it does not
        // invalidate them.
        $this->removeOverlapping($region);

        $position = $this->findInsertPosition($region->startKey);
        $entry = new RegionEntry($region, $this->now() + $this->ttlSeconds + $this->jitter());

        array_splice($this->entries, $position, 0, [$entry]);

        // Update idToIndex incrementally: shift indices >= position by 1
        $this->shiftIdToIndex($position, 1);
        $this->idToIndex[$region->regionId] = $position;

        // Mark as recently used
        $this->lruOrder[$region->regionId] = ++$this->lruCounter;

        // Evict LRU if at capacity
        if (count($this->entries) > $this->maxEntries) {
            $this->evictLru();
        }

        // Periodic sweep of expired entries
        $this->putCountSinceSweep++;
        if ($this->putCountSinceSweep >= $this->sweepInterval) {
            $this->sweepExpired();
            $this->putCountSinceSweep = 0;
        }

        $this->logger->debug('Region cached', [
            'regionId' => $region->regionId,
            'startKey' => KeyRedactor::redact($region->startKey),
            'endKey' => KeyRedactor::redact($region->endKey),
            'ttl' => $entry->expiresAt - $this->now(),
        ]);
    }

    /**
     * Drop a region from the cache and, when a removal actually happened,
     * emit the regionInvalidated() metric with the caller-supplied reason —
     * exactly once per ACTUAL drop. This is the single emission point for
     * every invalidation path (issue #474) — callers must not emit the
     * metric themselves, or drops are double-counted.
     */
    public function invalidate(int $regionId, string $reason = 'region_error'): void
    {
        $this->logger->info('Region invalidated', ['regionId' => $regionId]);
        if ($this->removeById($regionId)) {
            $this->metrics?->regionInvalidated($reason);
        }
    }

    public function switchLeader(int $regionId, int $leaderStoreId): bool
    {
        $index = $this->idToIndex[$regionId] ?? null;
        if ($index === null) {
            return false;
        }

        // Mark as recently used
        $this->lruOrder[$regionId] = ++$this->lruCounter;

        $entry = $this->entries[$index];
        $result = $entry->switchLeader($leaderStoreId);
        if ($result) {
            $this->logger->info('Region leader switched', [
                'regionId' => $regionId,
                'newLeaderStoreId' => $leaderStoreId,
            ]);
        }
        return $result;
    }

    public function clear(): void
    {
        $this->entries = [];
        $this->idToIndex = [];
        $this->lruOrder = [];
        $this->lruCounter = 0;
        $this->putCountSinceSweep = 0;
    }

    public function count(): int
    {
        return count($this->entries);
    }

    protected function now(): int
    {
        return time();
    }

    /**
     * Sweep expired entries from the cache.
     * Returns the number of entries removed.
     */
    private function sweepExpired(): int
    {
        $now = $this->now();
        $removed = 0;
        $toRemove = [];

        foreach ($this->entries as $index => $entry) {
            if ($now >= $entry->expiresAt) {
                $toRemove[] = $index;
            }
        }

        // Remove in reverse order to preserve indices
        for ($i = count($toRemove) - 1; $i >= 0; $i--) {
            $this->removeByIndex($toRemove[$i]);
            $removed++;
        }

        if ($removed > 0) {
            $this->logger->debug('Swept expired region entries', ['removed' => $removed]);
        }

        return $removed;
    }

    /**
     * Evict the least recently used entry from the cache.
     */
    private function evictLru(): void
    {
        if ($this->entries === []) {
            return;
        }

        // Find the region ID with the smallest LRU order (least recently used)
        $lruId = null;
        $lruOrder = PHP_INT_MAX;

        foreach ($this->lruOrder as $regionId => $order) {
            if ($order < $lruOrder) {
                $lruOrder = $order;
                $lruId = $regionId;
            }
        }

        // Fallback: evict the first entry
        $lruId ??= $this->entries[0]->region->regionId;

        $this->logger->debug('Evicting LRU region from cache', ['regionId' => $lruId]);
        $this->removeById($lruId);
    }

    /**
     * Shift idToIndex values >= $from by $delta.
     */
    private function shiftIdToIndex(int $from, int $delta): void
    {
        foreach ($this->idToIndex as $regionId => $index) {
            if ($index >= $from) {
                $this->idToIndex[$regionId] = $index + $delta;
            }
        }
    }

    /**
     * Remove every cached entry whose [startKey, endKey) range overlaps the
     * incoming region's range. Ranges that merely touch (an entry's endKey
     * equals the incoming startKey, or vice versa) do not overlap. An empty
     * endKey means unbounded (issue #238, REG-07).
     *
     * Exploits the sorted, non-overlapping invariant of $this->entries: the
     * candidate positions are bounded by $position (the insert position of
     * the incoming region), so the scan is O(log n + k) instead of a full
     * O(n) pass over up to maxEntries entries on every put().
     */
    private function removeOverlapping(RegionInfo $region): void
    {
        $startKey = $region->startKey;
        $endKey = $region->endKey;
        $position = $this->findInsertPosition($startKey);
        $toRemove = [];

        // Walk right from the insert position: entries there have
        // startKey >= $startKey, so they overlap whenever their startKey
        // lies before the incoming endKey (or the incoming range is
        // unbounded). Stop at the first touching entry — sorted entries
        // cannot overlap, so no further entry can overlap either.
        for ($i = $position, $count = count($this->entries); $i < $count; $i++) {
            if ($endKey !== '' && strcmp($endKey, $this->entries[$i]->region->startKey) <= 0) {
                break;
            }
            $toRemove[] = $i;
        }

        // Walk left from the insert position: entries there have
        // startKey <= $startKey, so they overlap whenever their endKey lies
        // past the incoming startKey (or they are unbounded). Stop at the
        // first touching entry for the same sortedness reason.
        for ($i = $position - 1; $i >= 0; $i--) {
            $entryEnd = $this->entries[$i]->region->endKey;
            if ($entryEnd !== '' && strcmp($entryEnd, $startKey) <= 0) {
                break;
            }
            $toRemove[] = $i;
        }

        // Remove in reverse order to preserve indices
        sort($toRemove, SORT_NUMERIC);
        for ($i = count($toRemove) - 1; $i >= 0; $i--) {
            $this->logger->debug('Removing superseded overlapping region from cache', [
                'regionId' => $this->entries[$toRemove[$i]]->region->regionId,
            ]);
            $this->removeByIndex($toRemove[$i]);
        }
    }

    private function binarySearch(string $key): ?int
    {
        $left = 0;
        $right = count($this->entries) - 1;
        $result = null;

        while ($left <= $right) {
            $mid = (int) (($left + $right) / 2);
            $entry = $this->entries[$mid];

            if (strcmp($entry->region->startKey, $key) <= 0) {
                $result = $mid;
                $left = $mid + 1;
            } else {
                $right = $mid - 1;
            }
        }

        return $result;
    }

    private function findInsertPosition(string $startKey): int
    {
        $left = 0;
        $right = count($this->entries);

        while ($left < $right) {
            $mid = (int) (($left + $right) / 2);
            if (strcmp($this->entries[$mid]->region->startKey, $startKey) < 0) {
                $left = $mid + 1;
            } else {
                $right = $mid;
            }
        }

        return $left;
    }

    private function removeById(int $regionId): bool
    {
        $index = $this->idToIndex[$regionId] ?? null;
        if ($index === null) {
            return false;
        }
        $this->removeByIndex($index);

        return true;
    }

    private function removeByIndex(int $index): void
    {
        if (!isset($this->entries[$index])) {
            return;
        }

        $regionId = $this->entries[$index]->region->regionId;
        unset($this->lruOrder[$regionId]);

        array_splice($this->entries, $index, 1);

        // Remove from idToIndex
        unset($this->idToIndex[$regionId]);

        // Shift remaining indices
        $this->shiftIdToIndex($index, -1);
    }

    private function isExpired(RegionEntry $entry): bool
    {
        return $this->now() >= $entry->expiresAt;
    }

    private function jitter(): int
    {
        if ($this->jitterSeconds <= 0) {
            return 0;
        }

        return random_int(0, $this->jitterSeconds);
    }

    private function resolveRegionInfo(RegionEntry $entry): RegionInfo
    {
        if ($entry->getLeaderStoreId() === $entry->region->leaderStoreId
            && $entry->getLeaderPeerId() === $entry->region->leaderPeerId) {
            return $entry->region;
        }

        return new RegionInfo(
            regionId: $entry->region->regionId,
            leaderPeerId: $entry->getLeaderPeerId(),
            leaderStoreId: $entry->getLeaderStoreId(),
            epochConfVer: $entry->region->epochConfVer,
            epochVersion: $entry->region->epochVersion,
            startKey: $entry->region->startKey,
            endKey: $entry->region->endKey,
            peers: $entry->region->peers,
        );
    }
}
