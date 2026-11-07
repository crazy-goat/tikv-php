<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

use Psr\Log\LoggerInterface;
use Throwable;

/**
 * Best-effort logging for paths that a destructor can reach.
 *
 * PHP destroys objects during shutdown in an order that is not guaranteed,
 * and the logger's own resources (a Monolog stream, a buffered handler, a
 * network transport) may already be released when a destructor runs. A
 * logging call that throws there would escape the destructor and replace the
 * original diagnostic with `Fatal error: Uncaught ... thrown in destructor`
 * (issue #287, GRPC-28).
 *
 * Every log record emitted from a close or destructor path therefore goes
 * through {@see self::log()}, which swallows any {@see Throwable} from the
 * logger. The record is lost in that case, which is the correct trade for a
 * destructor: the operation itself has already succeeded or failed and its
 * failure is reported through its own path.
 */
final class SafeLogging
{
    private function __construct()
    {
    }

    /**
     * Emit one record, ignoring a logger that is unusable.
     *
     * @param string                $level   PSR-3 level name
     * @param string                $message Log message
     * @param array<string, mixed>  $context Log context
     */
    public static function log(
        LoggerInterface $logger,
        string $level,
        string $message,
        array $context = [],
    ): void {
        try {
            $logger->log($level, $message, $context);
        } catch (Throwable) {
            // The logger may already be torn down during shutdown; a
            // destructor must never fail because of it.
        }
    }
}
