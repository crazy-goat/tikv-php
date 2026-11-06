<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
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
     * and 3; every other caller gets a RegionException for a top-level
     * KeyError, or a KeyErrorException for a per-pair KeyError, instead of a
     * silently accepted response.
     *
     * When a $cache and $regionId are provided, the region is invalidated
     * from the cache before the exception is thrown when the region error is
     * a ROUTING error (EpochNotMatch, RegionNotFound, KeyNotInRegion,
     * StoreNotMatch, RegionNotInitialized) or carries no typed kind. A
     * non-routing error (ServerIsBusy, DiskFull, IsWitness,
     * RecoveryInProgress, …) reached the region that owns the key, so the
     * cached entry stays — the same routing table RetryExecutor uses
     * (issue #245). This is the consistent behaviour expected by Transaction
     * and LockResolver callers.
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
                // Build the exception first: the typed ErrorKind lives on it
                // and decides whether the cached region is still correct.
                $exception = RegionException::fromRegionError($regionError);
                $isNotLeader = $exception->notLeader instanceof NotLeader;

                if ($cache instanceof RegionCacheInterface && $regionId !== null) {
                    if ($isNotLeader) {
                        // A valid-hint switch belongs to RetryExecutor's
                        // handleNotLeader(); only a non-executor site drops.
                        if (!$notLeaderOwnedByRetryExecutor) {
                            $cache->invalidate($regionId, 'not_leader');
                        }
                    } else {
                        $kind = $exception->errorKind;
                        // An unclassified region error invalidates
                        // conservatively, exactly as before. A classified
                        // non-routing kind means the request reached the
                        // region that owns the key, so keep the entry.
                        if (!$kind instanceof ErrorKind || RetryExecutor::invalidatesRoutingKind($kind)) {
                            $cache->invalidate($regionId, self::regionInvalidationReason($exception));
                        }
                    }
                }

                throw $exception;
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
                throw new KeyErrorException(sprintf(
                    '%s failed: %s',
                    $response instanceof RawBatchGetResponse ? 'BatchGet' : 'PerPairError',
                    self::describeKeyError($pair->getKey(), $pair->getError()),
                ));
            }
        }
    }

    /**
     * Metric reason for a regionInvalidated() drop owned by check().
     *
     * The handler is a separate drop path from RetryExecutor's pre-retry
     * invalidation, so it keeps its own 'region_error' prefix rather than
     * 'retry_region_error'. The kind is still tagged the same way:
     * 'region_error:epoch_not_match'. An unclassified error keeps the
     * historical 'region_error' label.
     */
    private static function regionInvalidationReason(RegionException $e): string
    {
        $kind = $e->errorKind;

        return $kind instanceof ErrorKind ? 'region_error:' . $kind->value : 'region_error';
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
