<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

final class RegionErrorHandler
{
    /**
     * Check a response for region errors and throw if any are found.
     *
     * Inspects:
     * 1. Top-level region_error (all responses)
     * 2. Top-level error: a string (RawBatchPutResponse, RawBatchDeleteResponse,
     *    RawGetResponse) or a KeyError (transactional responses)
     * 3. Per-pair KeyError on RawBatchGetResponse, RawScanResponse,
     *    RawBatchScanResponse, ScanResponse and BatchGetResponse
     *
     * Transactional callers must interpret a KeyError themselves (lock
     * resolution, write conflicts, GC aborts). They pass
     * $keyErrorsHandledByCaller = true, which skips checks 2 (KeyError only)
     * and 3; every other caller gets a RegionException for a populated KeyError
     * instead of a silently accepted response.
     *
     * When a $cache and $regionId are provided, the region is invalidated
     * from the cache before the exception is thrown. This is the consistent
     * behaviour expected by Transaction and LockResolver callers.
     *
     * NotLeader ownership depends on where check() runs:
     *
     * - $notLeaderOwnedByRetryExecutor = true (default): the call site sits
     *   inside a RetryExecutor::execute() closure whose handleNotLeader() is
     *   the sole owner of NotLeader drops — it switches to the hinted leader
     *   when that peer is still cached and only invalidates otherwise.
     *   Invalidating here too would double-count the metric and break
     *   valid-hint leader switching, so NotLeader oneofs are left cached.
     * - false: no retry executor owns this site (e.g. commit()'s primary-key
     *   commit or pessimisticLockBatch()), so check() self-invalidates with
     *   reason 'not_leader' before throwing — the same
     *   recovery master's unconditional invalidate() provided; without it a
     *   stale entry would survive up to TTL (~600s) and keep resolving to
     *   the moved leader.
     *
     * See MetricsInterface::regionInvalidated().
     */
    public static function check(
        object $response,
        ?RegionCacheInterface $cache = null,
        ?int $regionId = null,
        bool $notLeaderOwnedByRetryExecutor = true,
        bool $keyErrorsHandledByCaller = false,
    ): void {
        // 1. Top-level region error (all response types). NotLeader oneofs
        // are handled per $notLeaderOwnedByRetryExecutor — see docblock.
        if (method_exists($response, 'getRegionError')) {
            $regionError = $response->getRegionError();
            if ($regionError !== null) {
                $isNotLeader = $regionError->getNotLeader() !== null;
                if (
                    (!$isNotLeader || !$notLeaderOwnedByRetryExecutor)
                    && $cache instanceof \CrazyGoat\TiKV\Client\Cache\RegionCacheInterface
                    && $regionId !== null
                ) {
                    $cache->invalidate($regionId, $isNotLeader ? 'not_leader' : 'region_error');
                }
                throw RegionException::fromRegionError($regionError);
            }
        }

        // 2. Top-level error: a string (RawBatchPutResponse, RawBatchDeleteResponse,
        // RawGetResponse) or a KeyError (GetResponse, ScanResponse, CommitResponse,
        // BatchGetResponse, ...). The type is checked explicitly: a populated error
        // of a shape we do not recognise must never pass silently.
        if (method_exists($response, 'getError')) {
            $error = $response->getError();
            if (is_string($error) && $error !== '') {
                throw new RegionException(
                    operation: 'BatchRequest',
                    message: $error,
                );
            }

            if ($error instanceof KeyError && !$keyErrorsHandledByCaller) {
                throw new RegionException(
                    operation: 'KeyError',
                    message: 'key error: ' . KeyErrorDescriber::describe($error),
                );
            }
        }

        // 3. Per-pair KeyError on every response that carries pairs.
        if ($keyErrorsHandledByCaller) {
            return;
        }

        $pairs = match (true) {
            $response instanceof RawBatchGetResponse,
            $response instanceof BatchGetResponse,
            $response instanceof ScanResponse => $response->getPairs(),
            $response instanceof RawScanResponse,
            $response instanceof RawBatchScanResponse => $response->getKvs(),
            default => [],
        };

        foreach ($pairs as $pair) {
            if ($pair->hasError()) {
                throw new RegionException(
                    operation: $response instanceof RawBatchGetResponse ? 'BatchGet' : 'PerPairError',
                    message: self::describeKeyError($pair->getKey(), $pair->getError()),
                );
            }
        }
    }

    /**
     * Build a human-readable description from a KeyError.
     */
    private static function describeKeyError(string $key, ?object $keyError): string
    {
        if ($keyError === null) {
            return sprintf('per-pair error for key %s: null', KeyRedactor::redact($key));
        }

        return sprintf(
            'per-pair error for key %s: %s',
            KeyRedactor::redact($key),
            KeyErrorDescriber::describe($keyError),
        );
    }
}
