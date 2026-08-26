<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;

final class RegionErrorHandler
{
    /**
     * Check a response for region errors and throw if any are found.
     *
     * Inspects:
     * 1. Top-level region_error (all batch responses)
     * 2. Top-level error string (RawBatchPutResponse, RawBatchDeleteResponse)
     * 3. Per-pair KeyError in pairs (RawBatchGetResponse)
     *
     * When a $cache and $regionId are provided, the region is invalidated
     * from the cache before the exception is thrown (reason 'region_error').
     * This is the consistent behaviour expected by Transaction and
     * LockResolver callers.
     *
     * NotLeader errors are deliberately NOT invalidated here: every
     * production call site runs inside a RetryExecutor::execute() closure,
     * whose handleNotLeader() is the sole owner of NotLeader drops — it
     * switches to the hinted leader when that peer is still cached and only
     * invalidates otherwise. Invalidating here too would double-count the
     * metric and break valid-hint leader switching. See
     * MetricsInterface::regionInvalidated().
     */
    public static function check(
        object $response,
        ?RegionCacheInterface $cache = null,
        ?int $regionId = null,
    ): void {
        // 1. Top-level region error (all response types). NotLeader oneofs
        // are left for RetryExecutor::handleNotLeader() — see docblock.
        if (method_exists($response, 'getRegionError')) {
            $regionError = $response->getRegionError();
            if ($regionError !== null) {
                if (
                    $regionError->getNotLeader() === null
                    && $cache instanceof \CrazyGoat\TiKV\Client\Cache\RegionCacheInterface
                    && $regionId !== null
                ) {
                    $cache->invalidate($regionId);
                }
                throw RegionException::fromRegionError($regionError);
            }
        }

        // 2. Top-level error string (RawBatchPutResponse, RawBatchDeleteResponse)
        if (method_exists($response, 'getError')) {
            $error = $response->getError();
            if (is_string($error) && $error !== '') {
                throw new RegionException(
                    operation: 'BatchRequest',
                    message: $error,
                );
            }
        }

        // 3. Per-pair KeyError in RawBatchGetResponse pairs
        if ($response instanceof RawBatchGetResponse) {
            foreach ($response->getPairs() as $pair) {
                if ($pair->hasError()) {
                    $keyError = $pair->getError();
                    $key = $pair->getKey();
                    $message = self::describeKeyError($key, $keyError);
                    throw new RegionException(
                        operation: 'BatchGet',
                        message: $message,
                    );
                }
            }
        }
    }

    /**
     * Build a human-readable description from a KeyError.
     */
    private static function describeKeyError(string $key, ?object $keyError): string
    {
        if ($keyError === null) {
            return sprintf('per-pair error for key "%s": null', $key);
        }

        $parts = [];

        // Check oneof string fields
        if (method_exists($keyError, 'getRetryable')) {
            $v = $keyError->getRetryable();
            if ($v !== '' && $v !== null) {
                $parts[] = "retryable: {$v}";
            }
        }
        if (method_exists($keyError, 'getAbort')) {
            $v = $keyError->getAbort();
            if ($v !== '' && $v !== null) {
                $parts[] = "abort: {$v}";
            }
        }

        // Check oneof message fields for presence
        $messageFields = ['getLocked', 'getConflict', 'getAlreadyExist', 'getDeadlock',
            'getCommitTsExpired', 'getTxnNotFound', 'getCommitTsTooLarge',
            'getAssertionFailed', 'getPrimaryMismatch', 'getTxnLockNotFound'];

        foreach ($messageFields as $method) {
            if (method_exists($keyError, $method)) {
                $value = $keyError->$method();
                if ($value !== null) {
                    $name = substr($method, 3); // strip 'get'
                    $parts[] = $name;
                }
            }
        }

        $detail = $parts !== [] ? implode(', ', $parts) : 'unknown error';

        return sprintf('per-pair error for key "%s": %s', $key, $detail);
    }
}
