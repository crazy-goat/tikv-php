<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;
use RuntimeException;

/**
 * A logger whose level API is broken — the client must survive it.
 *
 * The probe treats such a logger as "cannot be asked", so the records are
 * still built and emitted.
 */
final class BrokenLevelLogger extends AbstractLogger
{
    public int $records = 0;

    public function isHandling(int|string $level): bool
    {
        throw new RuntimeException('the level API is broken');
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
