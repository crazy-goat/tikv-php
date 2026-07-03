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
 * Counters are tagged by an operation type (e.g. "get", "put", "scan").
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
     * Increment the retry count for a given operation type.
     *
     * Called by RetryExecutor every time a retryable error is observed
     * and the operation is scheduled for another attempt.
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
     * Called by RegionCache::invalidate() and from RetryExecutor on
     * NotLeader / region-error paths.
     */
    public function regionInvalidated(string $reason): void;
}
