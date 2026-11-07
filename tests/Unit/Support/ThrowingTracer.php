<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;

/**
 * Tracer that always throws, to pin that every emission point swallows a
 * failing tracing backend instead of letting it replace a transport or
 * protocol exception (issue #434).
 */
final class ThrowingTracer implements TraceEventInterface
{
    public function traceEvent(string $category, string $event, array $fields = []): void
    {
        throw new \RuntimeException('tracer boom');
    }
}
