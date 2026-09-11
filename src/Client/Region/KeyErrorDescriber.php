<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

/**
 * Formats a KeyError variant field into a human-readable detail string.
 *
 * Shared by RegionErrorHandler (per-key errors inside region-error
 * responses) and TwoPhaseCommitter::handleHeartbeatError() (issue #492),
 * so both surfaces describe every KeyError variant identically.
 */
final class KeyErrorDescriber
{
    public static function describe(?object $keyError): string
    {
        if ($keyError === null) {
            return 'unknown error';
        }

        $parts = [];

        // String variant fields: include the payload text (lowercase prefix,
        // matching the historical RegionErrorHandler message format).
        foreach (['getRetryable' => 'retryable', 'getAbort' => 'abort'] as $method => $name) {
            if (method_exists($keyError, $method)) {
                $v = $keyError->$method();
                if ($v !== '' && $v !== null) {
                    $parts[] = "{$name}: {$v}";
                }
            }
        }

        // Message variant fields: variant name is the detail.
        foreach (self::MESSAGE_FIELDS as $method) {
            if (method_exists($keyError, $method)) {
                $value = $keyError->$method();
                if ($value !== null) {
                    $parts[] = substr($method, 3);
                }
            }
        }

        return $parts !== [] ? implode(', ', $parts) : 'unknown error';
    }

    private const MESSAGE_FIELDS = [
        'getLocked', 'getConflict', 'getAlreadyExist', 'getDeadlock',
        'getCommitTsExpired', 'getTxnNotFound', 'getCommitTsTooLarge',
        'getAssertionFailed', 'getPrimaryMismatch', 'getTxnLockNotFound',
    ];
}
