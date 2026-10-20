<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;

/**
 * A logger that keeps every message it is given.
 *
 * The messages are collected as plain strings: a test that asserts on a
 * log line wants the line, not the level or the context.
 */
final class RecordingLogger extends AbstractLogger
{
    /** @var list<string> */
    public array $messages = [];

    /**
     * @param mixed                $level
     * @param array<string, mixed> $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->messages[] = (string) $message;
    }
}
