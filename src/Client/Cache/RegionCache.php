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
    /** @var array<int, RegionEntry> */
    private array $entriesById = [];

    private ?RegionBoundaryNode $boundaryRoot = null;
    private ?RegionBoundaryNode $lastBoundary = null;

    /** @var array<int, true> insertion order: oldest key first */
    private array $lruOrder = [];

    /** @var array<int, int> regionId => entry generation */
    private array $entryVersions = [];

    private int $nextEntryVersion = 0;
    private int $priorityState;

    /** @var list<array{expiresAt: int, regionId: int, startKey: string, version: int}> */
    private array $expiryHeap = [];

    private int $putCountSinceSweep = 0;

    public function __construct(private readonly int $ttlSeconds = 600, private readonly int $jitterSeconds = 60, private readonly int $maxEntries = 10000, private readonly int $sweepInterval = 100, private readonly LoggerInterface $logger = new NullLogger(), private ?MetricsInterface $metrics = null)
    {
        $this->priorityState = random_int(1, PHP_INT_MAX);
    }

    public function __clone()
    {
        $entries = [];
        foreach ($this->entriesById as $regionId => $entry) {
            $entries[$regionId] = clone $entry;
        }
        $this->entriesById = $entries;
        $this->boundaryRoot = $this->cloneBoundaryNodes($this->boundaryRoot);
        $lastStart = $this->lastBoundary?->startKey;
        $this->lastBoundary = $lastStart === null ? null : $this->boundaryForStart($lastStart);
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
        $node = $this->predecessorForKey($key);
        while ($node instanceof RegionBoundaryNode) {
            $entry = $this->entriesById[$node->regionId] ?? null;
            if ($entry instanceof RegionEntry && !$this->isEmptyRange($entry->region)) {
                break;
            }
            $node = $this->strictPredecessor($node->startKey);
        }
        if (!$node instanceof RegionBoundaryNode) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        $entry = $this->entriesById[$node->regionId] ?? null;
        if (!$entry instanceof RegionEntry) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        if ($this->isExpired($entry)) {
            $this->removeById($entry->region->regionId);
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        if ($entry->region->endKey !== '' && strcmp($key, $entry->region->endKey) >= 0) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        $this->touch($entry->region->regionId);
        $this->logger->debug('Region cache hit', [
            'key' => KeyRedactor::redact($key),
            'regionId' => $entry->region->regionId,
        ]);

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
        $existing = $this->entriesById[$region->regionId] ?? null;
        if (
            $existing instanceof RegionEntry
            && $existing->region->startKey === $region->startKey
            && $existing->region->endKey === $region->endKey
        ) {
            // Reinserting the same ID/range is the dominant warm-cache path.
            // Its ordered position cannot change, so avoid a treap delete and
            // reinsert while still refreshing TTL, LRU recency, and the expiry
            // generation exactly as a normal put does.
            $entry = new RegionEntry(
                $region,
                $this->now() + $this->ttlSeconds + $this->jitter(),
            );
            $this->entriesById[$region->regionId] = $entry;
            $this->afterPut($region, $entry);
            return;
        }

        $this->removeById($region->regionId);

        // Any cached entry whose range overlaps the incoming region's range is
        // superseded (issue #238): after a split or merge PD reports the new
        // region for the full range, so keeping the stale entry would make the
        // ordered lookup resolve keys to a region that no longer owns them.
        // Superseded entries are NOT counted as invalidations — the
        // regionInvalidated() metric is reserved for error-driven drops
        // (issue #474); a fresh insert supersedes them, it does not
        // invalidate them.
        $this->removeOverlapping($region);

        $entry = new RegionEntry(
            $region,
            $this->now() + $this->ttlSeconds + $this->jitter(),
        );
        $this->entriesById[$region->regionId] = $entry;
        $this->insertBoundary($region->startKey, $region->regionId);
        $this->afterPut($region, $entry);
    }

    private function afterPut(RegionInfo $region, RegionEntry $entry): void
    {
        $version = ++$this->nextEntryVersion;
        $this->entryVersions[$region->regionId] = $version;
        $this->heapPush([
            'expiresAt' => $entry->expiresAt,
            'regionId' => $region->regionId,
            'startKey' => $region->startKey,
            'version' => $version,
        ]);
        if (count($this->expiryHeap) > max(64, count($this->entriesById) * 4)) {
            $this->rebuildExpiryHeap();
        }
        $this->touch($region->regionId);

        // Evict LRU if at capacity
        if (count($this->entriesById) > $this->maxEntries) {
            $this->evictLru();
        }

        // Periodic sweep of expired entries. The expiry heap makes this
        // proportional to the number of due entries instead of scanning the
        // whole cache on every interval.
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
        $entry = $this->entriesById[$regionId] ?? null;
        if (!$entry instanceof RegionEntry) {
            return false;
        }

        // Mark as recently used, preserving the historical behavior even
        // when the requested peer is not present.
        $this->touch($regionId);
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
        $this->entriesById = [];
        $this->boundaryRoot = null;
        $this->lastBoundary = null;
        $this->lruOrder = [];
        $this->entryVersions = [];
        $this->nextEntryVersion = 0;
        $this->expiryHeap = [];
        $this->putCountSinceSweep = 0;
    }

    public function count(): int
    {
        return count($this->entriesById);
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

        while ($this->expiryHeap !== [] && $this->expiryHeap[0]['expiresAt'] <= $now) {
            $expired = $this->heapPop();
            $entry = $this->entriesById[$expired['regionId']] ?? null;
            if (
                !$entry instanceof RegionEntry
                || ($this->entryVersions[$expired['regionId']] ?? null) !== $expired['version']
                || $entry->expiresAt !== $expired['expiresAt']
                || $entry->region->startKey !== $expired['startKey']
            ) {
                // Stale heap record from a replaced/evicted region.
                continue;
            }

            $this->removeById($expired['regionId']);
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
        $lruId = array_key_first($this->lruOrder);
        $lruId ??= array_key_first($this->entriesById);
        if ($lruId === null) {
            return;
        }

        $this->logger->debug('Evicting LRU region from cache', ['regionId' => $lruId]);
        $this->removeById((int) $lruId);
    }

    /**
     * Remove every cached entry whose [startKey, endKey) range overlaps the
     * incoming region's range. Ranges that merely touch (an entry's endKey
     * equals the incoming startKey, or vice versa) do not overlap. An empty
     * endKey means unbounded (issue #238, REG-07).
     */
    private function removeOverlapping(RegionInfo $region): void
    {
        $equal = $this->lowerBound($region->startKey);
        if (
            $equal instanceof RegionBoundaryNode
            && strcmp($equal->startKey, $region->startKey) === 0
        ) {
            // A treap has one node per start key; newest-put-wins must remove
            // the old node even for an empty half-open range such as [a,a).
            $this->removeById($equal->regionId);
        }

        $last = $this->lastBoundary;
        if (
            $last instanceof RegionBoundaryNode
            && strcmp($region->startKey, $last->startKey) >= 0
            && isset($this->entriesById[$last->regionId])
            && !$this->rangesOverlap($this->entriesById[$last->regionId]->region, $region)
        ) {
            // The ordered index already proves that the new range is beyond
            // every cached region (PD scans commonly insert in this order).
            return;
        }

        $candidate = $this->lowerBound($region->startKey);
        $previous = $this->strictPredecessor($region->startKey);
        if (
            $previous instanceof RegionBoundaryNode
            && isset($this->entriesById[$previous->regionId])
            && $this->rangesOverlap(
                $this->entriesById[$previous->regionId]->region,
                $region,
            )
        ) {
            $candidate = $previous;
        }

        while ($candidate instanceof RegionBoundaryNode) {
            $entry = $this->entriesById[$candidate->regionId] ?? null;
            if (!$entry instanceof RegionEntry) {
                $candidate = $this->successor($candidate->startKey);
                continue;
            }
            if (!$this->rangesOverlap($entry->region, $region)) {
                break;
            }

            $next = $this->successor($candidate->startKey);
            $this->logger->debug('Removing superseded overlapping region from cache', [
                'regionId' => $candidate->regionId,
            ]);
            $this->removeById($candidate->regionId);
            $candidate = $next;
        }
    }

    private function isEmptyRange(RegionInfo $region): bool
    {
        return $region->endKey !== '' && strcmp($region->startKey, $region->endKey) >= 0;
    }

    private function rangesOverlap(RegionInfo $left, RegionInfo $right): bool
    {
        if ($this->isEmptyRange($left) || $this->isEmptyRange($right)) {
            return false;
        }
        if ($left->endKey !== '' && strcmp($left->endKey, $right->startKey) <= 0) {
            return false;
        }
        if ($right->endKey !== '' && strcmp($right->endKey, $left->startKey) <= 0) {
            return false;
        }

        return true;
    }

    private function removeById(int $regionId): bool
    {
        $entry = $this->entriesById[$regionId] ?? null;
        if (!$entry instanceof RegionEntry) {
            return false;
        }

        $removedId = $this->deleteBoundary($entry->region->startKey);
        if ($removedId !== $regionId) {
            // The index and hash should always agree. Avoid deleting an
            // unrelated node if a subclass or future implementation breaks
            // that invariant.
            return false;
        }

        if ($this->lastBoundary instanceof RegionBoundaryNode
            && $this->lastBoundary->regionId === $regionId
        ) {
            $this->lastBoundary = $this->strictPredecessor($entry->region->startKey);
        }

        unset(
            $this->entriesById[$regionId],
            $this->entryVersions[$regionId],
            $this->lruOrder[$regionId],
        );
        return true;
    }

    private function isExpired(RegionEntry $entry): bool
    {
        return $this->now() >= $entry->expiresAt;
    }

    private function touch(int $regionId): void
    {
        unset($this->lruOrder[$regionId]);
        $this->lruOrder[$regionId] = true;
    }

    private function nextPriority(): int
    {
        $value = $this->priorityState;
        $value ^= ($value << 13) & PHP_INT_MAX;
        $value ^= $value >> 7;
        $value ^= ($value << 17) & PHP_INT_MAX;
        $this->priorityState = $value;
        return $value & PHP_INT_MAX;
    }

    private function cloneBoundaryNodes(?RegionBoundaryNode $node): ?RegionBoundaryNode
    {
        if (!$node instanceof RegionBoundaryNode) {
            return null;
        }

        $clone = new RegionBoundaryNode(
            $node->startKey,
            $node->regionId,
            $node->priority,
        );
        $clone->left = $this->cloneBoundaryNodes($node->left);
        $clone->right = $this->cloneBoundaryNodes($node->right);
        if ($clone->left instanceof RegionBoundaryNode) {
            $clone->left->parent = $clone;
        }
        if ($clone->right instanceof RegionBoundaryNode) {
            $clone->right->parent = $clone;
        }
        return $clone;
    }

    private function boundaryForStart(string $startKey): ?RegionBoundaryNode
    {
        $node = $this->boundaryRoot;
        while ($node instanceof RegionBoundaryNode) {
            $comparison = strcmp($startKey, $node->startKey);
            if ($comparison < 0) {
                $node = $node->left;
            } elseif ($comparison > 0) {
                $node = $node->right;
            } else {
                return $node;
            }
        }

        return null;
    }

    private function insertBoundary(string $startKey, int $regionId): void
    {
        $priority = $this->nextPriority();
        $node = new RegionBoundaryNode($startKey, $regionId, $priority);
        $last = $this->lastBoundary;

        if (!$last instanceof RegionBoundaryNode) {
            $this->boundaryRoot = $node;
        } elseif (strcmp($startKey, $last->startKey) > 0) {
            // Sequential PD scans are the common write pattern. Attach the
            // new maximum directly to the rightmost node and bubble it up
            // through parent links instead of traversing the treap again.
            $node->parent = $last;
            $last->right = $node;
            $this->bubbleUp($node);
            if (!$node->parent instanceof RegionBoundaryNode) {
                $this->boundaryRoot = $node;
            }
        } else {
            $this->boundaryRoot = $this->insertNode($this->boundaryRoot, $node);
            $this->boundaryRoot->parent = null;
        }

        if (
            !$last instanceof RegionBoundaryNode
            || strcmp($startKey, $last->startKey) > 0
        ) {
            $this->lastBoundary = $node;
        }
    }

    private function bubbleUp(RegionBoundaryNode $node): void
    {
        while ($node->parent instanceof RegionBoundaryNode
            && $node->priority < $node->parent->priority
        ) {
            $parent = $node->parent;
            if ($parent->left === $node) {
                $this->rotateRight($parent);
            } else {
                $this->rotateLeft($parent);
            }
        }
    }

    private function insertNode(
        ?RegionBoundaryNode $node,
        RegionBoundaryNode $inserted,
    ): RegionBoundaryNode {
        if (!$node instanceof RegionBoundaryNode) {
            return $inserted;
        }

        $comparison = strcmp($inserted->startKey, $node->startKey);
        if ($comparison < 0) {
            $node->left = $this->insertNode($node->left, $inserted);
            $node->left->parent = $node;
            if ($node->left->priority < $node->priority) {
                $node = $this->rotateRight($node);
            }
        } elseif ($comparison > 0) {
            $node->right = $this->insertNode($node->right, $inserted);
            $node->right->parent = $node;
            if ($node->right->priority < $node->priority) {
                $node = $this->rotateLeft($node);
            }
        } else {
            // Overlap removal normally deletes an equal start key first.
            // Keep the tree coherent if a caller violates that invariant.
            $node->regionId = $inserted->regionId;
        }

        return $node;
    }

    private function deleteBoundary(string $startKey): ?int
    {
        $removedId = null;
        $this->boundaryRoot = $this->deleteNode(
            $this->boundaryRoot,
            $startKey,
            $removedId,
        );
        if ($this->boundaryRoot instanceof RegionBoundaryNode) {
            $this->boundaryRoot->parent = null;
        }
        return $removedId;
    }

    private function deleteNode(
        ?RegionBoundaryNode $node,
        string $startKey,
        ?int &$removedId,
    ): ?RegionBoundaryNode {
        if (!$node instanceof RegionBoundaryNode) {
            return null;
        }

        $comparison = strcmp($startKey, $node->startKey);
        if ($comparison < 0) {
            $node->left = $this->deleteNode($node->left, $startKey, $removedId);
            return $node;
        }
        if ($comparison > 0) {
            $node->right = $this->deleteNode($node->right, $startKey, $removedId);
            return $node;
        }

        $removedId = $node->regionId;
        return $this->merge($node->left, $node->right, $node->parent);
    }

    private function merge(
        ?RegionBoundaryNode $left,
        ?RegionBoundaryNode $right,
        ?RegionBoundaryNode $parent = null,
    ): ?RegionBoundaryNode {
        if (!$left instanceof RegionBoundaryNode) {
            if ($right instanceof RegionBoundaryNode) {
                $right->parent = $parent;
            }
            return $right;
        }
        if (!$right instanceof RegionBoundaryNode) {
            $left->parent = $parent;
            return $left;
        }

        if ($left->priority < $right->priority) {
            $left->right = $this->merge($left->right, $right, $left);
            $left->parent = $parent;
            return $left;
        }

        $right->left = $this->merge($left, $right->left, $right);
        $right->parent = $parent;
        return $right;
    }

    private function rotateRight(RegionBoundaryNode $node): RegionBoundaryNode
    {
        $pivot = $node->left;
        if (!$pivot instanceof RegionBoundaryNode) {
            return $node;
        }

        $parent = $node->parent;
        $wasLeft = $parent?->left === $node;
        $node->left = $pivot->right;
        if ($node->left instanceof RegionBoundaryNode) {
            $node->left->parent = $node;
        }
        $pivot->right = $node;
        $node->parent = $pivot;
        $pivot->parent = $parent;
        if ($parent instanceof RegionBoundaryNode) {
            if ($wasLeft) {
                $parent->left = $pivot;
            } else {
                $parent->right = $pivot;
            }
        }
        return $pivot;
    }

    private function rotateLeft(RegionBoundaryNode $node): RegionBoundaryNode
    {
        $pivot = $node->right;
        if (!$pivot instanceof RegionBoundaryNode) {
            return $node;
        }

        $parent = $node->parent;
        $wasRight = $parent?->right === $node;
        $node->right = $pivot->left;
        if ($node->right instanceof RegionBoundaryNode) {
            $node->right->parent = $node;
        }
        $pivot->left = $node;
        $node->parent = $pivot;
        $pivot->parent = $parent;
        if ($parent instanceof RegionBoundaryNode) {
            if ($wasRight) {
                $parent->right = $pivot;
            } else {
                $parent->left = $pivot;
            }
        }
        return $pivot;
    }

    private function lowerBound(string $key): ?RegionBoundaryNode
    {
        $node = $this->boundaryRoot;
        $result = null;
        while ($node instanceof RegionBoundaryNode) {
            if (strcmp($node->startKey, $key) >= 0) {
                $result = $node;
                $node = $node->left;
            } else {
                $node = $node->right;
            }
        }

        return $result;
    }

    private function predecessorForKey(string $key): ?RegionBoundaryNode
    {
        $node = $this->boundaryRoot;
        $result = null;
        while ($node instanceof RegionBoundaryNode) {
            if (strcmp($node->startKey, $key) <= 0) {
                $result = $node;
                $node = $node->right;
            } else {
                $node = $node->left;
            }
        }

        return $result;
    }

    private function strictPredecessor(string $key): ?RegionBoundaryNode
    {
        $node = $this->boundaryRoot;
        $result = null;
        while ($node instanceof RegionBoundaryNode) {
            if (strcmp($node->startKey, $key) < 0) {
                $result = $node;
                $node = $node->right;
            } else {
                $node = $node->left;
            }
        }

        return $result;
    }

    private function successor(string $key): ?RegionBoundaryNode
    {
        $node = $this->boundaryRoot;
        $result = null;
        while ($node instanceof RegionBoundaryNode) {
            if (strcmp($node->startKey, $key) > 0) {
                $result = $node;
                $node = $node->left;
            } else {
                $node = $node->right;
            }
        }

        return $result;
    }

    private function rebuildExpiryHeap(): void
    {
        $this->expiryHeap = [];
        foreach ($this->entriesById as $regionId => $entry) {
            $version = $this->entryVersions[$regionId] ?? null;
            if ($version === null) {
                continue;
            }
            $this->heapPush([
                'expiresAt' => $entry->expiresAt,
                'regionId' => $regionId,
                'startKey' => $entry->region->startKey,
                'version' => $version,
            ]);
        }
    }

    /**
     * @param array{expiresAt: int, regionId: int, startKey: string, version: int} $item
     */
    private function heapPush(array $item): void
    {
        $this->expiryHeap[] = $item;
        $index = count($this->expiryHeap) - 1;
        while ($index > 0) {
            $parent = intdiv($index - 1, 2);
            if ($this->expiryHeap[$parent]['expiresAt'] <= $item['expiresAt']) {
                break;
            }
            $this->expiryHeap[$index] = $this->expiryHeap[$parent];
            $index = $parent;
        }
        $this->expiryHeap[$index] = $item;
    }

    /**
     * @return array{expiresAt: int, regionId: int, startKey: string, version: int}
     */
    private function heapPop(): array
    {
        $root = $this->expiryHeap[0];
        $last = array_pop($this->expiryHeap);
        if ($this->expiryHeap === [] || !is_array($last)) {
            /** @var array{expiresAt: int, regionId: int, startKey: string, version: int} $root */
            return $root;
        }

        $index = 0;
        $count = count($this->expiryHeap);
        while (true) {
            $left = $index * 2 + 1;
            if ($left >= $count) {
                break;
            }
            $right = $left + 1;
            $child = $right < $count
                && $this->expiryHeap[$right]['expiresAt'] < $this->expiryHeap[$left]['expiresAt']
                ? $right
                : $left;
            if ($this->expiryHeap[$child]['expiresAt'] >= $last['expiresAt']) {
                break;
            }
            $this->expiryHeap[$index] = $this->expiryHeap[$child];
            $index = $child;
        }
        $this->expiryHeap[$index] = $last;

        return $root;
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
