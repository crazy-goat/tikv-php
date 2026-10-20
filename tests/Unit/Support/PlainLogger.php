<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;

/**
 * A plain PSR-3 logger — one without a level API.
 *
 * Such a logger cannot be asked which levels it handles, so the client has
 * to assume that it handles `debug`: a record is never lost.
 */
final class PlainLogger extends AbstractLogger
{
    public int $records = 0;

    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records++;
    }
}
