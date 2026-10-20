<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\NullLogger;

/**
 * A `NullLogger` subclass that overrides `log()` and records what it
 * receives.
 *
 * psr/log does not declare `NullLogger` final, so such a subclass exists in
 * the wild — and it *does* log, so the client must not treat it as a logger
 * that discards everything. psr/log's `debug()` routes to `log()`, so the
 * records the client emits arrive here.
 */
final class LogRecordingNullLogger extends NullLogger
{
    public int $records = 0;

    /**
     * @param mixed[] $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records++;
    }
}
