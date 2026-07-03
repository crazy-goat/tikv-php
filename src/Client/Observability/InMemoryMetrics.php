<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * In-memory, thread-unsafe (single-process PHP) implementation of
 * MetricsInterface used for tests and as a reference for users who
 * want to roll their own backend.
 *
 * Counters are stored as `[tag => value]` maps. The recorder also
 * retains per-call RPC latency samples so assertions can be made on
 * exact counts, success/failure ratios, and mean latency.
 *
 * This class is NOT intended for production use — values are not
 * thread-safe across PHP-FPM workers and memory grows unbounded.
 * For production, implement MetricsInterface on top of Prometheus,
 * StatsD, OpenTelemetry or your backend of choice.
 */
final class InMemoryMetrics implements MetricsInterface
{
    /** @var array<string, int> */
    private array $rpcStarted = [];

    /** @var array<string, int> */
    private array $rpcSucceeded = [];

    /** @var array<string, int> */
    private array $rpcFailed = [];

    /** @var array<string, int> */
    private array $retries = [];

    /** @var array<string, int> */
    private array $cacheHits = [];

    /** @var array<string, int> */
    private array $cacheMisses = [];

    /** @var array<string, int> */
    private array $invalidations = [];

    /** @var array<string, float> */
    private array $latencySumMs = [];

    /** @var array<string, int> */
    private array $latencyCount = [];

    public function rpcStarted(string $operation): void
    {
        $this->rpcStarted[$operation] = ($this->rpcStarted[$operation] ?? 0) + 1;
    }

    public function rpcCompleted(string $operation, float $durationMs, bool $success): void
    {
        if ($success) {
            $this->rpcSucceeded[$operation] = ($this->rpcSucceeded[$operation] ?? 0) + 1;
        } else {
            $this->rpcFailed[$operation] = ($this->rpcFailed[$operation] ?? 0) + 1;
        }

        $this->latencySumMs[$operation] = ($this->latencySumMs[$operation] ?? 0.0) + $durationMs;
        $this->latencyCount[$operation] = ($this->latencyCount[$operation] ?? 0) + 1;
    }

    public function retryAttempted(string $operation): void
    {
        $this->retries[$operation] = ($this->retries[$operation] ?? 0) + 1;
    }

    public function regionCacheHit(string $operation): void
    {
        $this->cacheHits[$operation] = ($this->cacheHits[$operation] ?? 0) + 1;
    }

    public function regionCacheMiss(string $operation): void
    {
        $this->cacheMisses[$operation] = ($this->cacheMisses[$operation] ?? 0) + 1;
    }

    public function regionInvalidated(string $reason): void
    {
        $this->invalidations[$reason] = ($this->invalidations[$reason] ?? 0) + 1;
    }

    /**
     * Reset all counters. Intended for tests.
     */
    public function reset(): void
    {
        $this->rpcStarted = [];
        $this->rpcSucceeded = [];
        $this->rpcFailed = [];
        $this->retries = [];
        $this->cacheHits = [];
        $this->cacheMisses = [];
        $this->invalidations = [];
        $this->latencySumMs = [];
        $this->latencyCount = [];
    }

    public function getRpcStarted(string $operation): int
    {
        return $this->rpcStarted[$operation] ?? 0;
    }

    public function getRpcSucceeded(string $operation): int
    {
        return $this->rpcSucceeded[$operation] ?? 0;
    }

    public function getRpcFailed(string $operation): int
    {
        return $this->rpcFailed[$operation] ?? 0;
    }

    public function getRetries(string $operation): int
    {
        return $this->retries[$operation] ?? 0;
    }

    public function getCacheHits(string $operation): int
    {
        return $this->cacheHits[$operation] ?? 0;
    }

    public function getCacheMisses(string $operation): int
    {
        return $this->cacheMisses[$operation] ?? 0;
    }

    public function getInvalidations(string $reason): int
    {
        return $this->invalidations[$reason] ?? 0;
    }

    public function getMeanLatencyMs(string $operation): float
    {
        $count = $this->latencyCount[$operation] ?? 0;
        if ($count === 0) {
            return 0.0;
        }

        return ($this->latencySumMs[$operation] ?? 0.0) / $count;
    }
}
