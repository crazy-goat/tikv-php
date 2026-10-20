<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\NullLogger;

/**
 * A `NullLogger` subclass that keeps psr/log's own `log()` and `debug()`.
 *
 * psr/log does not declare `NullLogger` final, so a subclass exists — but
 * this one discards every record, exactly like the class it extends. The
 * client must therefore treat it as a logger that cannot emit a debug
 * record and must not build a context for it.
 */
final class DiscardingNullLogger extends NullLogger
{
}
