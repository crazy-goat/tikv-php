<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;

/**
 * A logger whose level API takes the level by reference.
 *
 * `ReflectionMethod::invoke()` cannot pass an argument by reference, so
 * the probe used to emit "Argument #1 ($level) must be passed by reference,
 * value given" before the call failed (issue #716). No PSR-3 logger in the
 * wild has this shape, but the probe must ask it without a warning.
 */
final class ByReferenceLevelLogger extends AbstractLogger
{
    public int $records = 0;

    public function isHandling(int|string &$level): bool
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
