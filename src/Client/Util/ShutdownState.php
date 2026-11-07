<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Util;

/**
 * Tracks whether PHP has entered its shutdown sequence.
 *
 * PHP calls the functions registered with `register_shutdown_function()`
 * before it destroys the remaining objects, so a flag set from one of those
 * functions tells a destructor whether it is running during shutdown. That
 * distinction matters for work a destructor would otherwise do: a rollback
 * is a network round trip, and ext-grpc may already be in or past its module
 * shutdown phase, where the behaviour of a call is undefined rather than
 * merely erroneous (issue #287, GRPC-28).
 *
 * The registration is lazy and idempotent. A client registers it when it is
 * constructed, so the flag is in place before the destructors that consult
 * it. A destructor that runs because a reference was dropped earlier sees
 * the normal `false` state and keeps its usual behaviour.
 */
final class ShutdownState
{
    private static bool $shuttingDown = false;

    private static bool $registered = false;

    private function __construct()
    {
    }

    /**
     * Register the shutdown callback once for the whole process.
     *
     * Called from the constructors of the classes whose destructors consult
     * {@see self::isShuttingDown()}. Safe to call repeatedly.
     */
    public static function ensureRegistered(): void
    {
        if (self::$registered) {
            return;
        }

        self::$registered = true;
        register_shutdown_function(static function (): void {
            self::$shuttingDown = true;
        });
    }

    /**
     * Whether PHP is currently running its shutdown sequence.
     */
    public static function isShuttingDown(): bool
    {
        return self::$shuttingDown;
    }

    /**
     * Mark the process as shutting down.
     *
     * Production code never needs this — the registered callback does it.
     * It exists so a test can exercise the shutdown-only branches without
     * ending the PHPUnit process.
     *
     * @internal
     */
    public static function markShutdown(): void
    {
        self::$shuttingDown = true;
    }

    /**
     * Clear the shutdown flag between tests.
     *
     * @internal
     */
    public static function resetForTests(): void
    {
        self::$shuttingDown = false;
    }
}
