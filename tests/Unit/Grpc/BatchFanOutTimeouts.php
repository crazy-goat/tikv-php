<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;

/**
 * The one test-side reader of `TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS`
 * (issue #775).
 *
 * The constant stays private: it is an implementation detail of the
 * constructor's `batchDeadlineMs` validation, not public API. Both the data
 * provider in `TimeoutConfigTest` and the source-scan guard in
 * `BatchFanOutTimeoutGuardTest` go through this reader, so the list lives in
 * `src/` once and is not copied into the tests a second time.
 *
 * @internal
 */
final class BatchFanOutTimeouts
{
    /**
     * The declared fan-out per-call timeout field names, in the source order
     * of `TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS`.
     *
     * @return list<string>
     */
    public static function declared(): array
    {
        $constant = (new \ReflectionClass(TimeoutConfig::class))
            ->getReflectionConstant('BATCH_FAN_OUT_TIMEOUTS');
        if ($constant === false) {
            throw new \LogicException(
                'TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS no longer exists; the batch-deadline '
                . 'validation and the guard in BatchFanOutTimeoutGuardTest must be updated '
                . 'together (issue #775).',
            );
        }

        $fields = $constant->getValue();
        if (!is_array($fields)) {
            throw new \LogicException(
                'TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS must be a list of per-call timeout field names.',
            );
        }

        $names = [];
        foreach ($fields as $field) {
            if (!is_string($field)) {
                throw new \LogicException(
                    'TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS must contain only per-call timeout field names.',
                );
            }
            $names[] = $field;
        }

        return $names;
    }
}
