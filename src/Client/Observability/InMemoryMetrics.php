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
 * For production, use {@see PrometheusMetrics} or implement
 * MetricsInterface on top of StatsD, OpenTelemetry or your backend of
 * choice.
 *
 * It implements {@see DetailedMetricsInterface} too, so the richer signals
 * (backoff, per-store outcomes, lock resolution, 2PC phase timings) are
 * recorded and inspectable in tests.
 */
final class InMemoryMetrics implements DetailedMetricsInterface
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

    /** @var array<int, bool> */
    private array $storeHealth = [];

    /** @var array<int, int> */
    private array $storeUnhealthyCount = [];

    /** @var array<int, int> */
    private array $storeRecoveredCount = [];

    /** @var array<string, float> */
    private array $latencySumMs = [];

    /** @var array<string, int> */
    private array $latencyCount = [];

    /** @var array<string, int> */
    private array $backoffCount = [];

    /** @var array<string, float> */
    private array $backoffSumMs = [];

    /** @var array<int, int> */
    private array $storeSuccessCount = [];

    /** @var array<int, int> */
    private array $storeFailureCount = [];

    /** @var array<int, array<string, int>> */
    private array $storeErrorKinds = [];

    /** @var array<string, int> */
    private array $lockResolutionCount = [];

    /** @var array<string, float> */
    private array $lockResolutionSumMs = [];

    /** @var array<string, int> */
    private array $twoPhaseCount = [];

    /** @var array<string, int> */
    private array $twoPhaseFailureCount = [];

    /** @var array<string, float> */
    private array $twoPhaseSumMs = [];

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

    public function storeHealthChanged(int $storeId, bool $healthy): void
    {
        $this->storeHealth[$storeId] = $healthy;

        if ($healthy) {
            $this->storeRecoveredCount[$storeId] = ($this->storeRecoveredCount[$storeId] ?? 0) + 1;
        } else {
            $this->storeUnhealthyCount[$storeId] = ($this->storeUnhealthyCount[$storeId] ?? 0) + 1;
        }
    }

    public function backoffCompleted(string $backoffType, int $attempt, float $durationMs): void
    {
        $this->backoffCount[$backoffType] = ($this->backoffCount[$backoffType] ?? 0) + 1;
        $this->backoffSumMs[$backoffType] = ($this->backoffSumMs[$backoffType] ?? 0.0) + $durationMs;
    }

    public function storeAttemptCompleted(int $storeId, bool $success, ?string $errorKind = null): void
    {
        if ($success) {
            $this->storeSuccessCount[$storeId] = ($this->storeSuccessCount[$storeId] ?? 0) + 1;
            return;
        }

        $this->storeFailureCount[$storeId] = ($this->storeFailureCount[$storeId] ?? 0) + 1;
        $kind = $errorKind ?? 'unknown';
        $this->storeErrorKinds[$storeId][$kind] = ($this->storeErrorKinds[$storeId][$kind] ?? 0) + 1;
    }

    public function lockResolved(string $result, float $durationMs): void
    {
        $this->lockResolutionCount[$result] = ($this->lockResolutionCount[$result] ?? 0) + 1;
        $this->lockResolutionSumMs[$result] = ($this->lockResolutionSumMs[$result] ?? 0.0) + $durationMs;
    }

    public function twoPhasePhaseCompleted(string $phase, float $durationMs, bool $success): void
    {
        $this->twoPhaseCount[$phase] = ($this->twoPhaseCount[$phase] ?? 0) + 1;
        $this->twoPhaseSumMs[$phase] = ($this->twoPhaseSumMs[$phase] ?? 0.0) + $durationMs;
        if (!$success) {
            $this->twoPhaseFailureCount[$phase] = ($this->twoPhaseFailureCount[$phase] ?? 0) + 1;
        }
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
        $this->storeHealth = [];
        $this->storeUnhealthyCount = [];
        $this->storeRecoveredCount = [];
        $this->latencySumMs = [];
        $this->latencyCount = [];
        $this->backoffCount = [];
        $this->backoffSumMs = [];
        $this->storeSuccessCount = [];
        $this->storeFailureCount = [];
        $this->storeErrorKinds = [];
        $this->lockResolutionCount = [];
        $this->lockResolutionSumMs = [];
        $this->twoPhaseCount = [];
        $this->twoPhaseFailureCount = [];
        $this->twoPhaseSumMs = [];
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

    /**
     * Last recorded health state for a store: null when it never changed,
     * true after a recovery, false while marked unhealthy.
     */
    public function isStoreHealthy(int $storeId): ?bool
    {
        return $this->storeHealth[$storeId] ?? null;
    }

    public function getStoreUnhealthyCount(int $storeId): int
    {
        return $this->storeUnhealthyCount[$storeId] ?? 0;
    }

    public function getStoreRecoveredCount(int $storeId): int
    {
        return $this->storeRecoveredCount[$storeId] ?? 0;
    }

    public function getMeanLatencyMs(string $operation): float
    {
        $count = $this->latencyCount[$operation] ?? 0;
        if ($count === 0) {
            return 0.0;
        }

        return ($this->latencySumMs[$operation] ?? 0.0) / $count;
    }

    public function getBackoffCount(string $backoffType): int
    {
        return $this->backoffCount[$backoffType] ?? 0;
    }

    public function getTotalBackoffMs(string $backoffType): float
    {
        return $this->backoffSumMs[$backoffType] ?? 0.0;
    }

    public function getMeanBackoffMs(string $backoffType): float
    {
        $count = $this->getBackoffCount($backoffType);
        if ($count === 0) {
            return 0.0;
        }

        return $this->getTotalBackoffMs($backoffType) / $count;
    }

    public function getStoreSuccesses(int $storeId): int
    {
        return $this->storeSuccessCount[$storeId] ?? 0;
    }

    public function getStoreFailures(int $storeId): int
    {
        return $this->storeFailureCount[$storeId] ?? 0;
    }

    /**
     * Failures of a store grouped by the error kind recorded with them.
     *
     * @return array<string, int>
     */
    public function getStoreErrorKinds(int $storeId): array
    {
        return $this->storeErrorKinds[$storeId] ?? [];
    }

    public function getLockResolutions(string $result): int
    {
        return $this->lockResolutionCount[$result] ?? 0;
    }

    public function getMeanLockResolutionMs(string $result): float
    {
        $count = $this->getLockResolutions($result);
        if ($count === 0) {
            return 0.0;
        }

        return ($this->lockResolutionSumMs[$result] ?? 0.0) / $count;
    }

    public function getTwoPhaseCount(string $phase): int
    {
        return $this->twoPhaseCount[$phase] ?? 0;
    }

    public function getTwoPhaseFailures(string $phase): int
    {
        return $this->twoPhaseFailureCount[$phase] ?? 0;
    }

    public function getMeanTwoPhaseMs(string $phase): float
    {
        $count = $this->getTwoPhaseCount($phase);
        if ($count === 0) {
            return 0.0;
        }

        return ($this->twoPhaseSumMs[$phase] ?? 0.0) / $count;
    }
}
