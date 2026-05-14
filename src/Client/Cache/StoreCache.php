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

    public function __construct(
        private readonly int $ttlSeconds = 600,
        private readonly int $jitterSeconds = 60,
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
            unset($this->entries[$storeId]);
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

        $jitter = $this->jitter();
        $this->entries[$storeId] = new StoreEntry(
            $store,
            $this->now() + $this->ttlSeconds + $jitter,
        );

        $this->logger->debug('Store cached', [
            'storeId' => $storeId,
            'ttl' => $this->ttlSeconds + $jitter,
        ]);
    }

    public function invalidate(int $storeId): void
    {
        $this->logger->info('Store invalidated', ['storeId' => $storeId]);
        unset($this->entries[$storeId]);
    }

    public function clear(): void
    {
        $this->entries = [];
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
}
