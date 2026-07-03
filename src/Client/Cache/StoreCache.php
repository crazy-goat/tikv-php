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

    /** @var int[] Insertion order of store IDs (oldest first) */
    private array $order = [];

    public function __construct(
        private readonly int $ttlSeconds = 600,
        private readonly int $jitterSeconds = 60,
        private readonly int $maxEntries = 100,
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
            $this->removeEntry($storeId);
            $this->logger->debug('Store cache expired', ['storeId' => $storeId]);
            return null;
        }

        $this->logger->debug('Store cache hit', ['storeId' => $storeId]);
        return $entry->store;
    }

    public function put(Store $store): void
    {
        $storeId = (int) $store->getId();
        unset($this->entries[$storeId]);

        // Remove from order if already present (refresh moves to end)
        $orderPos = array_search($storeId, $this->order, true);
        if ($orderPos !== false) {
            unset($this->order[$orderPos]);
        }

        // Evict oldest entries when over capacity
        while (count($this->entries) >= $this->maxEntries) {
            $oldestId = array_shift($this->order);
            if ($oldestId === null) {
                break;
            }
            unset($this->entries[$oldestId]);
            $this->logger->debug('Store cache evicted', ['storeId' => $oldestId]);
        }

        $jitter = $this->jitter();
        $this->entries[$storeId] = new StoreEntry(
            $store,
            $this->now() + $this->ttlSeconds + $jitter,
        );

        $this->order[] = $storeId;

        $this->logger->debug('Store cached', [
            'storeId' => $storeId,
            'ttl' => $this->ttlSeconds + $jitter,
        ]);
    }

    public function invalidate(int $storeId): void
    {
        $this->logger->info('Store invalidated', ['storeId' => $storeId]);
        $this->removeEntry($storeId);
    }

    public function clear(): void
    {
        $this->entries = [];
        $this->order = [];
    }

    protected function now(): int
    {
        return time();
    }

    private function removeEntry(int $storeId): void
    {
        unset($this->entries[$storeId]);

        $orderPos = array_search($storeId, $this->order, true);
        if ($orderPos !== false) {
            unset($this->order[$orderPos]);
        }
    }

    private function jitter(): int
    {
        if ($this->jitterSeconds <= 0) {
            return 0;
        }

        return random_int(0, $this->jitterSeconds);
    }
}
