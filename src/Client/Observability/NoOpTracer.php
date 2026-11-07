<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Zero-cost no-op implementation of {@see TraceEventInterface}.
 *
 * The default when no tracer is configured, so an application that does not
 * trace pays one empty-method dispatch per event.
 */
final class NoOpTracer implements TraceEventInterface
{
    public function traceEvent(string $category, string $event, array $fields = []): void
    {
    }
}
