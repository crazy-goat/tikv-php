<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\Proto\Metapb\Store;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class StoreCache implements StoreCacheInterface
{
    /** @var StoreEntry[] */
    private array $entries = [];

    /** @var array<int, int> storeId => position in LRU order (higher = more recent) */
    private array $lruOrder = [];

    private int $lruCounter = 0;

    public function __construct(
        private readonly int $ttlSeconds = 600,
        private readonly int $jitterSeconds = 60,
        private readonly int $maxEntries = 128,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {}

    public function get(int $storeId): ?Store
    {
        if (!isset($this->entries[$storeId])) {
            $this->logger->debug('Store cache miss', ['storeId' => $storeId]);
            return null;
        }

        $entry = $this->entries[$storeId];

        if ($this->now() >= $entry->expiresAt) {
            $this->remove($storeId);
            $this->logger->debug('Store cache expired', ['storeId' => $storeId]);
            return null;
        }

        // Mark as recently used
        $this->lruOrder[$storeId] = ++$this->lruCounter;

        $this->logger->debug('Store cache hit', ['storeId' => $storeId]);
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
        $this->lruOrder[$storeId] = ++$this->lruCounter;

        $this->logger->debug('Store cached', [
            'storeId' => $storeId,
            'ttl' => $this->ttlSeconds + $jitter,
        ]);
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
        $this->lruCounter = 0;
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
     * Evict the least recently used entry from the cache.
     */
    private function evictLru(): void
    {
        if ($this->entries === []) {
            return;
        }

        // Find the store ID with the smallest LRU order (least recently used).
        $lruId = array_key_first($this->lruOrder);
        if ($lruId === null) {
            $lruId = (int) array_key_first($this->entries);
        } else {
            $lruOrder = $this->lruOrder[$lruId];
            foreach ($this->lruOrder as $storeId => $order) {
                if ($order < $lruOrder) {
                    $lruOrder = $order;
                    $lruId = $storeId;
                }
            }
        }

        $this->logger->debug('Evicting LRU store from cache', ['storeId' => $lruId]);
        $this->remove($lruId);
    }
}
