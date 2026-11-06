<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Interface for emitting client-level observability events to a metrics
 * implementation (Prometheus, StatsD, OpenTelemetry, in-memory test recorder, …).
 *
 * All methods are best-effort: implementations must never throw. The
 * library guarantees a no-op default via {@see NoOpMetrics} so callers
 * who do not opt in pay zero cost.
 *
 * RPC and region-cache counters are tagged by operation; retry counters are
 * tagged by the {@see \CrazyGoat\TiKV\Client\Retry\BackoffType} case name
 * (e.g. "NotLeader", "ServerBusy"), and invalidations use their reason.
 * Implementations are free to bucket tags however they wish.
 *
 * @see NoOpMetrics for the default zero-cost implementation.
 */
interface MetricsInterface
{
    /**
     * Increment the RPC count for a given operation type.
     *
     * Called once on every outbound gRPC call regardless of whether it
     * succeeds or fails.
     */
    public function rpcStarted(string $operation): void;

    /**
     * Record the latency of a completed RPC in milliseconds.
     *
     * Called exactly once per gRPC call, after the response (or error)
     * is received. $success is false if the call raised a transport
     * or TiKV error.
     */
    public function rpcCompleted(string $operation, float $durationMs, bool $success): void;

    /**
     * Increment the retry count for a given backoff type.
     *
     * Called by RetryExecutor every time a retryable error is observed
     * and the operation is scheduled for another attempt. $operation is the
     * BackoffType case name, not the public client operation name.
     */
    public function retryAttempted(string $operation): void;

    /**
     * Increment the region-cache hit counter for a given operation type.
     *
     * Called when RegionCache::getByKey() returns a non-null region
     * without falling back to PD.
     */
    public function regionCacheHit(string $operation): void;

    /**
     * Increment the region-cache miss counter for a given operation type.
     *
     * Called when RegionCache::getByKey() returns null and the caller
     * must query PD for the region.
     */
    public function regionCacheMiss(string $operation): void;

    /**
     * Increment the region-invalidation counter.
     *
     * Called exactly once per ACTUAL region drop from the cache, from
     * {@see \CrazyGoat\TiKV\Client\Cache\RegionCache::invalidate()} — the
     * single emission point for every drop path; invalidating an ID that is
     * not cached emits nothing. $reason names the caller:
     *
     * - 'region_error:<kind>': a top-level ROUTING region error other than
     *                           NotLeader (EpochNotMatch, RegionNotFound,
     *                           KeyNotInRegion, StoreNotMatch,
     *                           RegionNotInitialized) was handled via
     *                           RegionErrorHandler::check(); <kind> is the
     *                           triggering ErrorKind value, e.g.
     *                           'region_error:epoch_not_match'. Non-routing
     *                           region errors (ServerIsBusy, DiskFull,
     *                           IsWitness, RecoveryInProgress, …) keep the
     *                           cached entry and emit nothing (issue #245)
     * - 'region_error':       the same handler drop when the region error
     *                           carries no typed ErrorKind (unknown error)
     * - 'not_leader':         a NotLeader response forced an invalidation in
     *                           RetryExecutor::handleNotLeader() (hint peer
     *                           unknown or no hint) at executor-owned call
     *                           sites; there RegionErrorHandler::check()
     *                           deliberately leaves NotLeader regions cached
     *                           so handleNotLeader can switch-or-drop. At
     *                           call sites with no enclosing retry executor,
     *                           check($…, notLeaderOwnedByRetryExecutor: false)
     *                           self-invalidates with the same reason
     *                           (pessimistic lock, primary-region commit, and
     *                           lock-resolution paths).
     * - 'retry_region_error:<kind>': RetryExecutor invalidated before
     *                           scheduling the next attempt on a retryable
     *                           ROUTING error; <kind> is the triggering
     *                           ErrorKind value, e.g.
     *                           'retry_region_error:epoch_not_match'. A
     *                           non-routing retryable error (ServerIsBusy,
     *                           DiskFull, IsWitness, RecoveryInProgress, …)
     *                           is retried with the cached region intact
     *                           (issue #245)
     * - 'retry_region_error': the same pre-retry invalidation when the error
     *                           carries no typed ErrorKind (a transport
     *                           failure or a message-classified exception), so
     *                           the kind is unknown
     * - 'fatal_region_error': RetryExecutor invalidated a routing error that
     *                           is fatal (KeyNotInRegion) or a
     *                           StoreNotFoundException for one of the
     *                           region's stores BEFORE rethrowing it, so the
     *                           stale entry cannot survive the throw (issues
     *                           #233, #627)
     * - 'lock_resolve':       LockResolver dropped the region after resolving
     *                           a lock
     *
     * Note: NotLeader responses whose hint peer is still valid only switch
     * the cached leader (switchLeader()) and do NOT invalidate, so they emit
     * nothing.
     */
    public function regionInvalidated(string $reason): void;
}
