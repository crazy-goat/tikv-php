<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
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
     * (issue #245). NotLeader is the exception to the routing rule: with the
     * default $notLeaderOwnedByRetryExecutor = true it stays cached, because
     * RetryExecutor::handleNotLeader() owns that drop; only a site with no
     * retry executor invalidates it here (see below). For EpochNotMatch the
     * stale entry is also captured on the exception before the drop, so
     * RetryExecutor can seed the new regions' leader from the stale cached
     * entry's leader store and refresh the cache without PD (issue #246).
     * This is the consistent behaviour expected by Transaction and
     * LockResolver callers.
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
        bool $checkPairErrors = true,
    ): void {
        // 1. Top-level region error (all response types). NotLeader oneofs
        // are handled per $notLeaderOwnedByRetryExecutor — see docblock.
        //
        // instanceof instead of a method_exists lookup: the response types
        // are a closed set (every response the client sends), the generated
        // protobuf classes share no interface, and this check runs on the
        // universal response path (issue #303). A new response type that can
        // carry a region error must be added to this match.
        $regionError = match (true) {
            $response instanceof RawGetResponse,
            $response instanceof RawPutResponse,
            $response instanceof RawDeleteResponse,
            $response instanceof RawGetKeyTTLResponse,
            $response instanceof RawCASResponse,
            $response instanceof RawDeleteRangeResponse,
            $response instanceof RawChecksumResponse,
            $response instanceof RawScanResponse,
            $response instanceof RawBatchGetResponse,
            $response instanceof RawBatchPutResponse,
            $response instanceof RawBatchDeleteResponse,
            $response instanceof RawBatchScanResponse,
            $response instanceof GetResponse,
            $response instanceof BatchGetResponse,
            $response instanceof ScanResponse,
            $response instanceof CommitResponse,
            $response instanceof BatchRollbackResponse,
            $response instanceof CheckTxnStatusResponse,
            $response instanceof CheckSecondaryLocksResponse,
            $response instanceof ResolveLockResponse,
            $response instanceof TxnHeartBeatResponse,
            $response instanceof PrewriteResponse,
            $response instanceof PessimisticLockResponse,
            $response instanceof PessimisticRollbackResponse => $response->getRegionError(),
            default => null,
        };

        if ($regionError !== null) {
            // Build the exception first: the typed ErrorKind lives on it
            // and decides whether the cached region is still correct.
            // Capture the stale entry before invalidating it: an
            // EpochNotMatch payload carries no leader, and the retry
            // needs the stale cached entry's leader store in order to
            // cache the new regions without asking PD (issue #246). A
            // StoreNotMatch needs it too, as the fallback when the
            // payload does not name the requested store (issue #247).
            $cachedRegion = null;
            if (
                $regionId !== null
                && ($regionError->hasEpochNotMatch() || $regionError->hasStoreNotMatch())
            ) {
                $cachedRegion = $cache?->getById($regionId);
            }
            $exception = RegionException::fromRegionError($regionError, $cachedRegion);
            $isNotLeader = $exception->notLeader instanceof NotLeader;

            if ($regionId !== null) {
                if ($isNotLeader) {
                    // A valid-hint switch belongs to RetryExecutor's
                    // handleNotLeader(); only a non-executor site drops.
                    if (!$notLeaderOwnedByRetryExecutor) {
                        $cache?->invalidate($regionId, 'not_leader');
                    }
                } else {
                    $kind = $exception->errorKind;
                    // An unclassified region error invalidates
                    // conservatively, exactly as before. A classified
                    // non-routing kind means the request reached the
                    // region that owns the key, so keep the entry.
                    if (!$kind instanceof ErrorKind || RetryExecutor::invalidatesRoutingKind($kind)) {
                        $cache?->invalidate($regionId, self::regionInvalidationReason($exception));
                    }
                }
            }

            throw $exception;
        }

        // 2. Top-level error: a string (RawBatchPutResponse,
        // RawBatchDeleteResponse, RawGetResponse) or a KeyError (GetResponse,
        // ScanResponse, CommitResponse, BatchGetResponse, ...). The type is
        // checked explicitly: a populated error of a shape we do not
        // recognise must never pass silently. The match covers exactly the
        // response types that expose a singular getError() method; Prewrite,
        // PessimisticLock and PessimisticRollback expose the repeated
        // getErrors() instead and are interpreted by their callers.
        $error = match (true) {
            $response instanceof RawGetResponse,
            $response instanceof RawPutResponse,
            $response instanceof RawDeleteResponse,
            $response instanceof RawGetKeyTTLResponse,
            $response instanceof RawCASResponse,
            $response instanceof RawDeleteRangeResponse,
            $response instanceof RawChecksumResponse,
            $response instanceof RawBatchPutResponse,
            $response instanceof RawBatchDeleteResponse,
            $response instanceof GetResponse,
            $response instanceof BatchGetResponse,
            $response instanceof ScanResponse,
            $response instanceof CommitResponse,
            $response instanceof BatchRollbackResponse,
            $response instanceof CheckTxnStatusResponse,
            $response instanceof CheckSecondaryLocksResponse,
            $response instanceof ResolveLockResponse,
            $response instanceof TxnHeartBeatResponse => $response->getError(),
            default => null,
        };

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

        // 3. Per-pair KeyError on every response that carries pairs.
        if ($keyErrorsHandledByCaller || !$checkPairErrors) {
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
                throw self::perPairException($response, $pair);
            }
        }
    }

    /**
     * Check a RawBatchGetResponse and collect its pairs into a key => value
     * map in a single traversal (issue #303).
     *
     * RawKvBatch::batchGet() uses this as its one pass over the pairs: the
     * per-pair KeyError check and the value collection happen in the same
     * loop instead of walking the repeated field once in check() and again in
     * the caller. The region error is still checked first, exactly as check()
     * does, so the exception ordering is unchanged.
     *
     * @return array<array-key, ?string>
     */
    public static function checkAndCollectBatchGet(
        RawBatchGetResponse $response,
        ?RegionCacheInterface $cache = null,
        ?int $regionId = null,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): array {
        // The region error and (absent) top-level error are checked by the
        // shared path; the pair loop is deferred to the collector so the
        // repeated field is walked exactly once.
        self::check(
            $response,
            $cache,
            $regionId,
            $notLeaderOwnedByRetryExecutor,
            keyErrorsHandledByCaller: false,
            checkPairErrors: false,
        );

        return self::collectBatchGetPairs($response);
    }

    /**
     * Check the pairs of a RawBatchGetResponse and collect them into a
     * key => value map in one traversal.
     *
     * Unlike {@see self::checkAndCollectBatchGet()} this does not repeat the
     * region-error check, so it is the companion of a call site that already
     * ran {@see self::check()} with $checkPairErrors = false (the
     * BatchCommands multiplexed path in RawKvBatch).
     *
     * @return array<array-key, ?string>
     */
    public static function collectBatchGetPairs(RawBatchGetResponse $response): array
    {
        $results = [];
        foreach ($response->getPairs() as $pair) {
            if ($pair->hasError()) {
                throw self::perPairException($response, $pair);
            }
            $results[$pair->getKey()] = $pair->getValue();
        }

        return $results;
    }

    /**
     * Build the exception for a per-pair KeyError.
     */
    private static function perPairException(object $response, KvPair $pair): KeyErrorException
    {
        return new KeyErrorException(sprintf(
            '%s failed: %s',
            $response instanceof RawBatchGetResponse ? 'BatchGet' : 'PerPairError',
            self::describeKeyError($pair->getKey(), $pair->getError()),
        ));
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
