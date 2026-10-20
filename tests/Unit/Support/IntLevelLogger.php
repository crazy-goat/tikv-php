<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;

/**
 * A logger whose level API has Monolog 2's signature: an `int` level.
 *
 * Monolog 2's `isHandling()` takes the numeric levels, so the probe has to
 * ask with the number and not with the PSR-3 level name.
 */
final class IntLevelLogger extends AbstractLogger
{
    public int $records = 0;

    public function isHandling(int $level): bool
    {
        return $level !== 100;
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
