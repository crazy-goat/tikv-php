<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Zero-cost no-op implementation of MetricsInterface.
 *
 * Used as the default when no metrics are injected, so that callers
 * that never wire up a metrics backend pay no runtime overhead
 * (a single empty-method dispatch per call site).
 *
 * @see MetricsInterface
 */
final class NoOpMetrics implements MetricsInterface
{
    public function rpcStarted(string $operation): void
    {
    }

    public function rpcCompleted(string $operation, float $durationMs, bool $success): void
    {
    }

    public function retryAttempted(string $operation): void
    {
    }

    public function regionCacheHit(string $operation): void
    {
    }

    public function regionCacheMiss(string $operation): void
    {
    }

    public function regionInvalidated(string $reason): void
    {
    }
}
