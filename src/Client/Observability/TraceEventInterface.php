<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Minimal structured trace-event callback, modelled on client-go's
 * `TraceEventFunc` (issue #434, GAP-20).
 *
 * The library never depends on OpenTelemetry, OpenTracing or any other
 * tracing SDK. Instead it calls {@see self::traceEvent()} at meaningful
 * boundaries and leaves the mapping to spans to the application, which can
 * bridge the events to its own SDK in a few lines (see the
 * [tracing guide](../../../docs/advanced.md#tracing)).
 *
 * Events carry a category from {@see TraceCategory} and a short event name;
 * `$fields` holds scalar attributes (store IDs, durations, error kinds).
 * A paired `*_started` / `*_completed` event carries the same opaque
 * `eventId` field (a string) on both sides, so a bridge can correlate them
 * without relying on the category or the operation name, which are not
 * unique while several calls are in flight. Implementations must be cheap
 * and must never throw: a tracing backend is observability, not part of the
 * data path.
 */
interface TraceEventInterface
{
    /**
     * @param string $category One of the {@see TraceCategory} constants.
     * @param string $event    Short event name, e.g. "rpc_started".
     * @param array<string, string|int|float|bool|null> $fields Scalar attributes.
     *        Paired start/completion events share the same `eventId`.
     */
    public function traceEvent(string $category, string $event, array $fields = []): void;
}
