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
     * - false: no retry executor owns this site (e.g. commit()'s prewrite
     *   loop, pessimisticLockBatch(), Transaction::batchGet()), so check()
     *   self-invalidates with reason 'not_leader' before throwing — the same
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

        return sprintf('per-pair error for key "%s": %s', $key, KeyErrorDescriber::describe($keyError));
    }
}
