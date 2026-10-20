<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use ReflectionMethod;
use Throwable;

/**
 * Decides whether a logger can emit a `debug` record at all.
 *
 * The hot paths of the client build a log context before every `debug()`
 * call, and that context can be expensive: the region cache redacts the
 * key (a `substr` + `bin2hex` + `sprintf`) on every lookup, every miss and
 * every `put()` (issue #300). A logger that discards the record makes that
 * work pointless, so the client asks the logger **once**, at construction,
 * whether it handles the debug level, and builds the context only when the
 * answer is yes.
 *
 * The answer is taken **once** and is not re-examined: a logger that is
 * reconfigured afterwards (a Monolog handler whose level is lowered to
 * `DEBUG`, say) keeps the answer the client was given. Configure the
 * logger before the client is built, and build a new client after
 * changing it — see `docs/configuration.md`.
 *
 * PSR-3 has no level API, so the answer comes from two sources:
 *
 * 1. **A level API, when the logger has one.** Monolog's
 *    `Logger::isHandling()` is the de-facto one, and a logger that answers
 *    "no" for `debug` (a Monolog logger at `INFO`, or one whose handler
 *    does not take the level) is asked explicitly. The probe is tolerant:
 *    the level is passed in the shapes the signature accepts (Monolog 2
 *    takes an `int`, Monolog 3 an `int|string|Level`), and a logger whose
 *    `isHandling()` throws is reported as "cannot tell" instead of
 *    breaking the client.
 * 2. **The class, when there is no level API.** A `NullLogger` discards
 *    every record — but only the logger that keeps psr/log's own `log()`
 *    *and* `debug()` implementations is known to do so. psr/log does not
 *    declare that class `final`, so a subclass that overrides either
 *    method may well record what it receives, and it is therefore
 *    treated as a logger that handles the debug level.
 *
 * A logger that cannot be asked at all (a plain PSR-3 logger without a
 * level API) is assumed to handle `debug`: the cost is then a context
 * that may be discarded, but a record is never lost.
 */
final class DebugLogging
{
    /**
     * The numeric value Monolog 2 uses for the `debug` level.
     *
     * PSR-3 defines the levels as strings only, but Monolog 2's
     * `isHandling()` takes the numeric ones.
     */
    private const DEBUG_LEVEL_NUMBER = 100;

    /**
     * Whether the given logger can emit a `debug` record.
     *
     * @param LoggerInterface $logger the logger the client was given
     */
    public static function handlesDebug(LoggerInterface $logger): bool
    {
        $level = self::probeLevel($logger);
        if ($level !== null) {
            return $level;
        }

        return !self::discardsEveryRecord($logger);
    }

    /**
     * Ask the logger whether it handles the `debug` level.
     *
     * @return bool|null null when the logger cannot be asked
     */
    private static function probeLevel(LoggerInterface $logger): ?bool
    {
        if (!method_exists($logger, 'isHandling')) {
            return null;
        }

        try {
            $method = new ReflectionMethod($logger, 'isHandling');
        } catch (Throwable) {
            return null;
        }

        // Only a public method is the logger's answer to the question; a
        // private one is an internal detail of that class.
        if (!$method->isPublic()) {
            return null;
        }

        $target = $method->isStatic() ? null : $logger;

        foreach (self::levelArguments() as $argument) {
            try {
                $answer = $method->invoke($target, $argument);
            } catch (Throwable) {
                // The signature does not accept this shape of the level
                // (Monolog 2 takes an int, a Monolog 3 enum takes a Level);
                // try the next one.
                continue;
            }

            // Only a real answer counts; anything else means the logger
            // cannot be asked.
            if (is_bool($answer)) {
                return $answer;
            }
        }

        return null;
    }

    /**
     * The shapes of the `debug` level to try, in order.
     *
     * @return list<string|int>
     */
    private static function levelArguments(): array
    {
        return [LogLevel::DEBUG, self::DEBUG_LEVEL_NUMBER];
    }

    /**
     * Whether the logger is known to discard every record.
     *
     * psr/log's `NullLogger` is not `final`, so a subclass may override
     * `log()` or `debug()` and record what it receives. Only a logger that
     * keeps psr/log's own implementation of **both** methods is known to
     * discard every record: psr/log's `debug()` routes to `log()`, so
     * overriding either one is enough to make the subclass a candidate for
     * records. The rule errs on the side of keeping the records — a
     * logger that records debug records must never be silenced.
     */
    private static function discardsEveryRecord(LoggerInterface $logger): bool
    {
        if (!$logger instanceof NullLogger) {
            return false;
        }

        return !self::overridesPsrLogMethod($logger, 'log')
            && !self::overridesPsrLogMethod($logger, 'debug');
    }

    /**
     * Whether the logger's own class overrides a method psr/log declares.
     *
     * The method is still psr/log's own when the class that declares it is
     * `NullLogger` itself or one of its parents — psr/log's
     * `AbstractLogger` declares `debug()`, which routes to `log()`. Any
     * other class is a user class that overrode the method, and a user
     * class that overrode it may record what it receives.
     */
    private static function overridesPsrLogMethod(LoggerInterface $logger, string $method): bool
    {
        try {
            $declaringClass = (new ReflectionMethod($logger, $method))->getDeclaringClass();
        } catch (Throwable) {
            // The method cannot be inspected: assume that the logger
            // records, so that no record is ever lost.
            return true;
        }

        return !is_a(NullLogger::class, $declaringClass->getName(), true);
    }
}
