<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;

/**
 * A logger that discards the debug records by level, the way a Monolog
 * logger at `INFO` does.
 */
final class LevelFilteredLogger extends AbstractLogger
{
    public int $records = 0;

    public function isHandling(int|string $level): bool
    {
        return $level !== LogLevel::DEBUG;
    }

    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records++;
    }
}
