<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Observability\DebugLogging;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class StoreCache implements StoreCacheInterface
{
    /** @var array<int, StoreEntry> */
    private array $entries = [];

    /** @var array<int, true> least recently used store first */
    private array $lruOrder = [];

    /**
     * Whether this cache emits its debug records at all.
     *
     * A store lookup runs on every region resolution, and the debug context
     * of a hit, a miss and an eviction is an array that a level-filtered
     * logger discards. {@see DebugLogging} asks the logger once, at
     * construction, whether it handles the debug level (issue #715); the
     * answer is frozen with the flag, so reconfigure the logger before the
     * client is built — see `docs/configuration.md`.
     */
    private readonly bool $debugEnabled;

    public function __construct(
        private readonly int $ttlSeconds = 600,
        private readonly int $jitterSeconds = 60,
        private readonly int $maxEntries = 128,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
        $this->debugEnabled = DebugLogging::handlesDebug($logger);
    }

    public function get(int $storeId): ?Store
    {
        if (!isset($this->entries[$storeId])) {
            if ($this->debugEnabled) {
                $this->logger->debug('Store cache miss', ['storeId' => $storeId]);
            }
            return null;
        }

        $entry = $this->entries[$storeId];

        if ($this->now() >= $entry->expiresAt) {
            $this->remove($storeId);
            if ($this->debugEnabled) {
                $this->logger->debug('Store cache expired', ['storeId' => $storeId]);
            }
            return null;
        }

        // Mark as recently used
        $this->touch($storeId);

        if ($this->debugEnabled) {
            $this->logger->debug('Store cache hit', ['storeId' => $storeId]);
        }
        return $entry->store;
    }

    public function put(Store $store): void
    {
        $storeId = (int) $store->getId();
        $this->remove($storeId);

        // Evict LRU entry if at capacity
        if (count($this->entries) >= $this->maxEntries) {
            $this->evictLru();
        }

        $jitter = $this->jitter();
        $this->entries[$storeId] = new StoreEntry(
            $store,
            $this->now() + $this->ttlSeconds + $jitter,
        );
        $this->touch($storeId);

        if ($this->debugEnabled) {
            $this->logger->debug('Store cached', [
                'storeId' => $storeId,
                'ttl' => $this->ttlSeconds + $jitter,
            ]);
        }
    }

    public function invalidate(int $storeId): void
    {
        $this->logger->info('Store invalidated', ['storeId' => $storeId]);
        $this->remove($storeId);
    }

    public function clear(): void
    {
        $this->entries = [];
        $this->lruOrder = [];
    }

    protected function now(): int
    {
        return time();
    }

    private function jitter(): int
    {
        if ($this->jitterSeconds <= 0) {
            return 0;
        }

        return random_int(0, $this->jitterSeconds);
    }

    private function remove(int $storeId): void
    {
        unset($this->entries[$storeId], $this->lruOrder[$storeId]);
    }

    /**
     * Mark a store as the most recently used one.
     *
     * The recency order is the insertion order of `$lruOrder`, so the key
     * must be removed before it is appended; a plain assignment to an
     * existing key would leave it at its old position and turn eviction into
     * a scan for the smallest counter value (issue #255, REG-24).
     */
    private function touch(int $storeId): void
    {
        unset($this->lruOrder[$storeId]);
        $this->lruOrder[$storeId] = true;
    }

    /**
     * Evict the least recently used entry from the cache.
     *
     * O(1): `touch()` keeps `$lruOrder` in recency order, so the first key is
     * the least recently used one and no scan of the cache is needed.
     */
    private function evictLru(): void
    {
        $lruId = array_key_first($this->lruOrder) ?? array_key_first($this->entries);
        if ($lruId === null) {
            return;
        }

        if ($this->debugEnabled) {
            $this->logger->debug('Evicting LRU store from cache', ['storeId' => $lruId]);
        }
        $this->remove($lruId);
    }
}
