<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;
use RuntimeException;

/**
 * A logger whose every method throws.
 *
 * `AbstractLogger` routes emergency/alert/critical/error/warning/notice/info/
 * debug through `log()`, so making `log()` throw makes the whole PSR-3
 * surface unusable. This models a Monolog handler whose stream or transport
 * was already released during PHP shutdown (issue #287, GRPC-28).
 */
final class ThrowingLogger extends AbstractLogger
{
    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        throw new RuntimeException('the logger is unusable');
    }
}
