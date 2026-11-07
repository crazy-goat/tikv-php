<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Optional extension of {@see MetricsInterface} for backends that can record
 * the richer signals added for issue #434 (GAP-20): backoff durations,
 * per-store outcomes, lock-resolution results and two-phase-commit timings.
 *
 * It is a **separate** interface on purpose. Adding methods to
 * {@see MetricsInterface} would break every existing implementation, so the
 * base interface stays frozen and the client emits the extra signals only
 * when the injected backend also implements this one:
 *
 * <code>
 * if ($metrics instanceof DetailedMetricsInterface) {
 *     $metrics->backoffCompleted($type, $attempt, $sleepMs);
 * }
 * </code>
 *
 * A backend that implements only {@see MetricsInterface} keeps working
 * unchanged and simply does not receive these calls. {@see PrometheusMetrics}
 * and {@see InMemoryMetrics} implement this interface; the default
 * {@see NoOpMetrics} deliberately does not, so a base-only backend keeps the
 * per-call cost to one cheap `instanceof` probe.
 *
 * All methods are best-effort, like the base interface: implementations must
 * never throw.
 */
interface DetailedMetricsInterface extends MetricsInterface
{
    /**
     * Record the backoff a retry loop scheduled before the next attempt.
     *
     * Called once per retry, after the sleep was clamped to the operation's
     * remaining deadline (the value is therefore the time actually spent
     * waiting). $attempt is the zero-based attempt number that failed.
     */
    public function backoffCompleted(string $backoffType, int $attempt, float $durationMs): void;

    /**
     * Record one RPC attempt against a store.
     *
     * Called after every attempt the retry executor runs, when the caller
     * supplied a `targetStoreId` provider and it returned a store ID. An
     * unknown store records nothing: the retry path does not fall back to a
     * region-cache lookup just to attribute a metric. $errorKind is null on
     * success, otherwise the region error kind (e.g. "server_is_busy") or
     * "transport" for a gRPC transport failure, and "unknown" for an
     * unclassified error.
     *
     * The store ID is an integer because it is a stable cluster identifier;
     * implementations that expose it as a tag should render it as a decimal
     * string.
     */
    public function storeAttemptCompleted(int $storeId, bool $success, ?string $errorKind = null): void;

    /**
     * Record one completed lock-resolution attempt.
     *
     * $result is one of:
     *
     * - 'resolved': the lock was resolved (committed or rolled back);
     * - 'live': the transaction still holds the lock and the caller retries;
     * - 'error': the resolution RPC failed for another reason.
     */
    public function lockResolved(string $result, float $durationMs): void;

    /**
     * Record one completed two-phase-commit phase.
     *
     * $phase is 'prewrite' or 'commit'. One-phase and async-commit
     * transactions emit only the prewrite phase: they have no commit phase.
     */
    public function twoPhasePhaseCompleted(string $phase, float $durationMs, bool $success): void;
}
