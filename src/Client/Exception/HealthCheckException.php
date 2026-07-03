<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use Throwable;

/**
 * Thrown by healthCheck() when the client cannot reach PD.
 *
 * Carries the underlying transport/proto error so callers can decide
 * how to react (fail-open, fail-closed, page on-call, …). Inherits
 * from TiKvException so it composes with the existing exception
 * hierarchy (every library failure derives from TiKvException).
 */
final class HealthCheckException extends TiKvException
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
