<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;

/**
 * The one place the slow-operation warning is timed and written (issue #701).
 *
 * Every measured operation goes through {@see self::measure()}, which owns
 * the `hrtime()` pair, the threshold check and the context keys. The batch
 * path waits on a future instead of calling a closure directly and uses
 * {@see self::threshold()} plus {@see self::warning()} to keep the same
 * message and context.
 */
final class SlowLog
{
    /**
     * The warning message. Documented verbatim in `docs/configuration.md`,
     * so it lives here once instead of at every emission site.
     */
    public const MESSAGE = 'Slow TiKV operation';

    /**
     * Return the positive threshold for the operation, or null when slow
     * logging is disabled for it (no configuration, or a threshold of 0).
     */
    public static function threshold(?SlowLogConfig $config, SlowLogOperation $operation): ?int
    {
        if (!$config instanceof SlowLogConfig) {
            return null;
        }

        $threshold = $config->getThreshold($operation);

        return $threshold > 0 ? $threshold : null;
    }

    /**
     * Measure the execution time of a callable and log a warning if it
     * exceeds the configured threshold for the given operation type.
     *
     * @template T
     * @param callable(): T $fn
     * @return T
     */
    public static function measure(
        ?SlowLogConfig $config,
        LoggerInterface $logger,
        SlowLogOperation $operation,
        string $key,
        callable $fn,
    ): mixed {
        $threshold = self::threshold($config, $operation);
        if ($threshold === null) {
            return $fn();
        }

        $start = hrtime(true);
        try {
            return $fn();
        } finally {
            $durationMs = (hrtime(true) - $start) / 1_000_000;
            if ($durationMs > $threshold) {
                self::warning($logger, $operation, $key, $durationMs, $threshold);
            }
        }
    }

    /**
     * Emit the slow-operation warning with the documented message and
     * context keys.
     */
    public static function warning(
        LoggerInterface $logger,
        SlowLogOperation $operation,
        string $key,
        float $durationMs,
        int $thresholdMs,
    ): void {
        $logger->warning(self::MESSAGE, [
            'operation' => $operation->value,
            'key' => KeyRedactor::redact($key),
            'duration_ms' => round($durationMs, 2),
            'threshold_ms' => $thresholdMs,
        ]);
    }
}
