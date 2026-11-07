<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use Closure;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;

/**
 * Per-store health state for the circuit-breaker half of store failover
 * (issue #424, GAP-10).
 *
 * A store is marked unhealthy after {@see self::DEFAULT_FAILURE_THRESHOLD}
 * consecutive transport failures and stays unavailable for
 * {@see self::DEFAULT_COOLDOWN_MS}. Failures older than
 * {@see self::DEFAULT_FAILURE_WINDOW_MS} are forgotten, so isolated failures
 * spread over a long period never accumulate into a demotion. When the
 * cooldown elapses the store is half-open: the next request is allowed through
 * as a probe. A success resets the state, a failure starts a fresh cooldown.
 * A half-open store still reports itself unhealthy until a request succeeds;
 * the cooldown alone is not evidence of recovery.
 *
 * The tracker is deliberately time-based rather than RPC-based: PHP has no
 * background health-check loop, so the retry loop of the next request is what
 * probes a half-open store. The clock is injectable so tests can drive the
 * cooldown without sleeping.
 */
final class StoreHealthTracker
{
    /**
     * Consecutive transport failures that mark a store unhealthy. A single
     * transient failure must not blacklist a store, while a dead one should
     * stop receiving full-timeout requests quickly.
     */
    public const DEFAULT_FAILURE_THRESHOLD = 3;

    /**
     * How long a store stays unavailable after being marked unhealthy, in
     * milliseconds. Matches client-go's one-second `StoreLivenessTimeout`.
     */
    public const DEFAULT_COOLDOWN_MS = 1000;

    /**
     * How long a failure counts toward the consecutive-failure threshold, in
     * milliseconds. Failures older than this are forgotten, so two failures
     * hours apart do not demote a store that was healthy in between. The
     * default is deliberately generous: it only expires genuinely idle state,
     * not the burst of failures a real outage produces.
     */
    public const DEFAULT_FAILURE_WINDOW_MS = 60_000;

    /** @var array<int, int> storeId => consecutive failures since the last success */
    private array $consecutiveFailures = [];

    /** @var array<int, int> storeId => monotonic time of the last failure (ms) */
    private array $lastFailureMs = [];

    /** @var array<int, true> storeId => currently marked unhealthy */
    private array $unhealthy = [];

    /** @var array<int, int> storeId => monotonic time the cooldown ends (ms) */
    private array $unhealthyUntilMs = [];

    /** @var Closure(): int */
    private readonly Closure $clockMs;

    /**
     * @param Closure(): int|null $clockMs Monotonic clock in milliseconds.
     *     Null uses `hrtime(true)`; tests inject a controllable clock.
     */
    public function __construct(
        private readonly int $failureThreshold = self::DEFAULT_FAILURE_THRESHOLD,
        private readonly int $cooldownMs = self::DEFAULT_COOLDOWN_MS,
        private readonly MetricsInterface $metrics = new NoOpMetrics(),
        ?Closure $clockMs = null,
        private readonly int $failureWindowMs = self::DEFAULT_FAILURE_WINDOW_MS,
    ) {
        if ($failureThreshold < 1) {
            throw new \InvalidArgumentException('failureThreshold must be >= 1');
        }
        if ($cooldownMs < 0) {
            throw new \InvalidArgumentException('cooldownMs must be >= 0');
        }
        if ($failureWindowMs < 1) {
            throw new \InvalidArgumentException('failureWindowMs must be >= 1');
        }

        $this->clockMs = $clockMs ?? static fn (): int => (int) (hrtime(true) / 1_000_000);
    }

    /**
     * Record one successful RPC against a store.
     *
     * Resets the consecutive-failure counter and clears the unhealthy state.
     * A success while half-open is the recovery signal: it emits the healthy
     * transition to metrics.
     */
    public function recordSuccess(int $storeId): void
    {
        if ($storeId <= 0) {
            return;
        }

        $wasUnhealthy = $this->unhealthy[$storeId] ?? false;
        unset(
            $this->consecutiveFailures[$storeId],
            $this->lastFailureMs[$storeId],
            $this->unhealthy[$storeId],
            $this->unhealthyUntilMs[$storeId],
        );

        if ($wasUnhealthy) {
            $this->metrics->storeHealthChanged($storeId, true);
        }
    }

    /**
     * Record one failed RPC against a store.
     *
     * Returns true exactly when this failure crossed the threshold and the
     * store became unhealthy, so the caller can invalidate cached regions
     * led by it once instead of on every later failure.
     */
    public function recordFailure(int $storeId): bool
    {
        if ($storeId <= 0) {
            return false;
        }

        $now = $this->nowMs();
        $this->pruneExpired($now);

        $failures = ($this->consecutiveFailures[$storeId] ?? 0) + 1;
        $this->consecutiveFailures[$storeId] = $failures;
        $this->lastFailureMs[$storeId] = $now;

        if ($failures < $this->failureThreshold) {
            return false;
        }

        $wasUnhealthy = $this->unhealthy[$storeId] ?? false;
        $this->unhealthy[$storeId] = true;
        $this->unhealthyUntilMs[$storeId] = $now + $this->cooldownMs;

        if ($wasUnhealthy) {
            // A failed half-open probe starts a fresh cooldown without
            // emitting a second unhealthy transition.
            return false;
        }

        $this->metrics->storeHealthChanged($storeId, false);

        return true;
    }

    /**
     * Whether a request may be routed to the store now.
     *
     * False only while the cooldown is running. After it elapses the store is
     * half-open: requests are allowed again so the next outcome can either
     * recover it or start a new cooldown.
     */
    public function isAvailable(int $storeId): bool
    {
        if (!($this->unhealthy[$storeId] ?? false)) {
            return true;
        }

        return $this->nowMs() >= ($this->unhealthyUntilMs[$storeId] ?? 0);
    }

    /**
     * Whether the store is in the healthy state.
     *
     * Unlike {@see self::isAvailable()}, a half-open store is still reported
     * unhealthy until a request succeeds: "unknown" is not "healthy".
     */
    public function isHealthy(int $storeId): bool
    {
        return !($this->unhealthy[$storeId] ?? false);
    }

    /**
     * Consecutive failures recorded for a store since its last success.
     */
    public function getConsecutiveFailures(int $storeId): int
    {
        $this->pruneExpired($this->nowMs());

        return $this->consecutiveFailures[$storeId] ?? 0;
    }

    /**
     * Monotonic time at which the current cooldown ends, or null when the
     * store is not marked unhealthy.
     */
    public function getUnhealthyUntilMs(int $storeId): ?int
    {
        return $this->unhealthyUntilMs[$storeId] ?? null;
    }

    /**
     * Whether any store has a failure recorded since its last success.
     *
     * Used by the retry executor to skip the store-id lookup on the common
     * success path: with no failure state to reset there is nothing to
     * record.
     */
    public function hasTrackedFailures(): bool
    {
        $this->pruneExpired($this->nowMs());

        return $this->unhealthy !== [] || $this->consecutiveFailures !== [];
    }

    private function nowMs(): int
    {
        return ($this->clockMs)();
    }

    /**
     * Forget failures older than the failure window for stores that are not
     * currently unhealthy.
     *
     * An unhealthy store keeps its state until a request succeeds: the
     * cooldown only makes it routable again (half-open), it does not make it
     * healthy without evidence. Pruning those entries would silently flip a
     * store that never recovered back to healthy.
     */
    private function pruneExpired(int $nowMs): void
    {
        foreach ($this->lastFailureMs as $storeId => $lastFailureMs) {
            if ($this->unhealthy[$storeId] ?? false) {
                continue;
            }

            if ($nowMs - $lastFailureMs <= $this->failureWindowMs) {
                continue;
            }

            unset($this->consecutiveFailures[$storeId], $this->lastFailureMs[$storeId]);
        }
    }
}
