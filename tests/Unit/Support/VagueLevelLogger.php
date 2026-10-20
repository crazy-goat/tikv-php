<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;

/**
 * A logger whose level API answers with something that is not a boolean.
 *
 * The probe treats such an answer as "cannot be asked", so the records are
 * still built and emitted.
 */
final class VagueLevelLogger extends AbstractLogger
{
    public int $records = 0;

    public function isHandling(int|string $level): int
    {
        return 1;
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
