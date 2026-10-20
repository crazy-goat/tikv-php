<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\NullLogger;

/**
 * A `NullLogger` subclass that overrides `debug()` and records what it
 * receives, but keeps psr/log's discarding `log()`.
 *
 * This is the shape the probe has to be careful about: the class it
 * extends discards everything, but the subclass records the debug records
 * it receives. A client that decides from the `log()` method alone would
 * silence it and lose the records it used to get.
 */
final class DebugRecordingNullLogger extends NullLogger
{
    public int $records = 0;

    /**
     * @param mixed[] $context
     */
    public function debug(string|\Stringable $message, array $context = []): void
    {
        $this->records++;
    }
}
