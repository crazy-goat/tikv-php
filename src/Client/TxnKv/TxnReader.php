<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

/**
 * Read-only operations for a single transaction.
 *
 * Extracted from the Transaction god object (issue #83) following the
 * same decomposition pattern as the RawKv module (RawKvCrud).
 *
 * Each instance is bound to a single transaction's start timestamp and
 * dependencies.  Methods operate on the shared TransactionState to
 * respect read-your-writes semantics.
 */
final readonly class TxnReader
{
    /**
     * @param int $startTs Transaction start timestamp (constant for the lifetime of the reader)
     */
    public function __construct(
        private int $startTs,
        private GrpcClientInterface $grpc,
        private PdClientInterface $pdClient,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private LockResolver $lockResolver,
        private RegionCacheInterface $regionCache,
        /** Read preference for reads (issue #421); commits always target the leader. */
        private ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
    ) {
    }

    /**
     * Read a single key.
     *
     * Checks the local write set first (read-your-writes), delegates to
     * TiKV via a retry-aware gRPC call.
     *
     * @throws TiKvException
     */
    public function get(
        string $key,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): ?string {
        if ($state->hasWriteSetKey($key)) {
            return $state->getWriteSetValue($key);
        }

        $excludedStore = null;

        return $retryExecutor->execute(
            $key,
            function () use ($key, $state, &$excludedStore): ?string {
                $region = $this->regionResolver->getRegionInfo($key);
                $target = RegionContextFactory::resolveTarget(
                    $region,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                );
                $excludedStore = null;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);

                $request = new GetRequest();
                $request->setContext($target->context);
                $request->setKey($key);
                $request->setVersion($this->startTs);

                try {
                    /** @var GetResponse $response */
                    $response = $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'KvGet',
                        $request,
                        GetResponse::class,
                        $this->timeoutMs('read'),
                    );

                    RegionErrorHandler::check($response, $this->regionCache, $region->regionId);
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        // The selected replica's applied index is behind: exclude
                        // it so the next attempt falls back to another replica or
                        // the leader (issue #421).
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $error = $response->getError();
                if ($error !== null) {
                    $locked = $error->getLocked();
                    if ($locked !== null) {
                        $rawPrimary = $locked->getPrimaryLock();
                        $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $key);
                        $this->lockResolver->resolveLock($lockPrimary, $locked);
                        throw new TxnRetryableException('Lock encountered, resolved - retry', BackoffType::TxnLock);
                    }

                    $retryable = $error->getRetryable();
                    if ($retryable !== '') {
                        throw new \CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException($retryable);
                    }

                    // GC has passed this transaction's start timestamp — the
                    // server names it in the abort field ("GC life time is
                    // shorter than transaction duration"). Throw the typed,
                    // non-retryable GC exception; the previous fall-through
                    // here returned the response as-if-successful and the
                    // caller read an empty value (issue #422).
                    $abort = $error->getAbort();
                    if ($abort !== '') {
                        throw $this->gcExceptionFromAbort($abort);
                    }
                }

                if ($response->getNotFound()) {
                    $state->setReadValue($key, null);
                    return null;
                }

                $value = $response->getValue();
                $state->setReadValue($key, $value);
                return $value;
            },
            $classifier
        );
    }

    /**
     * Batch-read multiple keys.
     *
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<string, ?string>
     *
     * @throws InvalidArgumentException
     * @throws TiKvException
     */
    public function batchGet(
        array $keys,
        TransactionState $state,
    ): array {
        $keys = $this->normalizeKeysToStrings($keys, 'batchGet');

        $results = [];
        $remaining = [];
        foreach ($keys as $key) {
            if ($state->hasWriteSetKey($key)) {
                $results[$key] = $state->getWriteSetValue($key);
            } else {
                $remaining[] = $key;
            }
        }

        if ($remaining !== []) {
            $remoteResults = $this->batchGetFromTiKV($remaining, $state);
            // Do not use array_merge(): it renumbers integer keys, which
            // silently drops numeric-string key results ("12345" is stored
            // as int key 12345 and would move to index 0).
            foreach ($remoteResults as $key => $value) {
                $results[$key] = $value;
            }
        }

        // Preserve input order.
        $ordered = [];
        foreach ($keys as $key) {
            $ordered[$key] = $results[$key] ?? null;
        }
        return $ordered;
    }

    /**
     * Scan keys in range [startKey, endKey).
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException
     * @throws TiKvException
     */
    public function scan(
        string $startKey,
        string $endKey,
        int $limit,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit = 10240,
    ): array {
        $limit = $this->normalizeScanLimit($limit, $maxScanLimit);

        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }
        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
            $regionLimit = $remaining > 0 ? $remaining : $limit;
            $regionResults = $this->executeScanForRegion(
                $scanStart,
                $scanEnd,
                $regionLimit,
                $retryExecutor,
                $classifier,
                $maxScanLimit,
            );
            array_push($results, ...$regionResults);

            if ($remaining > 0) {
                $remaining -= count($regionResults);
                if ($remaining <= 0) {
                    break;
                }
            }
        }

        return $this->finalizeScanResults($results, $startKey, $endKey, $limit, $state);
    }

    /**
     * @param string[] $keys
     * @return array<string, ?string>
     */
    private function batchGetFromTiKV(
        array $keys,
        TransactionState $state,
    ): array {
        $results = [];
        $resolved = $this->regionResolver->batchResolveRegions($keys);

        // Group keys by resolved region.
        $grouped = [];
        foreach ($keys as $key) {
            $region = $resolved[$key] ?? null;
            if ($region === null) {
                // Defense in depth: batchResolveRegions() already fails
                // closed, so this is unreachable unless the resolver
                // contract changes (issue #244). A silently skipped key
                // here would read back as null — indistinguishable from
                // "key not present".
                throw new TiKvException(sprintf(
                    'Region could not be resolved for key %s; refusing to silently drop it from the batch',
                    KeyRedactor::redact($key),
                ));
            }
            $regionId = $region->regionId;
            $grouped[$regionId] ??= ['region' => $region, 'keys' => []];
            $grouped[$regionId]['keys'][] = $key;
        }

        // Fan out all per-region KvBatchGet RPCs (issue #291): every send is
        // issued before any response is awaited, so the regions' read
        // latencies overlap. Responses are awaited in dispatch order, so the
        // first failing region throws the same exception the sequential loop
        // aborted with.
        $regionCalls = [];
        foreach ($grouped as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];

            $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchGetForRegionAsync($region, $regionKeys);
        }

        $batchExecutor = new BatchAsyncExecutor();
        try {
            $regionResults = $batchExecutor->executeParallel(
                $regionCalls,
                $this->timeoutConfig->batchDeadlineMs,
            );
        } catch (BatchPartialFailureException $e) {
            // batchGetFromTiKV() is reached from Transaction::batchGet()
            // with no RetryExecutor owner... preserve the sequential
            // first-abort exception semantics (issue #291).
            throw $e->getFirstRegionError();
        }

        foreach ($regionResults as $response) {
            assert($response instanceof BatchGetResponse);
            foreach ($response->getPairs() as $pair) {
                $results[$pair->getKey()] = $pair->getValue();
            }
        }

        foreach ($keys as $key) {
            if (!array_key_exists($key, $results)) {
                $results[$key] = null;
            }
            $state->setReadValue($key, $results[$key]);
        }

        return $results;
    }

    /**
     * Issue (eagerly send) one region's KvBatchGet without waiting for the
     * response (issue #291); the returned future's wait phase runs the
     * region-error check and GC-abort mapping.
     *
     * @param string[] $regionKeys
     */
    private function batchGetForRegionAsync(
        RegionInfo $region,
        array $regionKeys,
    ): CheckedGrpcFuture {
        $target = RegionContextFactory::resolveTarget(
            $region,
            $this->replicaReadPolicy,
            $this->regionResolver->getStore(...),
        );
        $address = $this->regionResolver->resolveStoreAddress($target->storeId);

        $request = new BatchGetRequest();
        $request->setContext($target->context);
        $request->setKeys($regionKeys);
        $request->setVersion($this->startTs);

        $future = $this->grpc->callAsync(
            $address,
            'tikvpb.Tikv',
            'KvBatchGet',
            $request,
            BatchGetResponse::class,
            $this->timeoutMs('batch_read'),
        );

        return CheckedGrpcFuture::fromCallable(function () use ($future, $region) {
            /** @var BatchGetResponse $response */
            $response = $future->wait();
            // batchGetFromTiKV() is reached from Transaction::batchGet()
            // with no RetryExecutor owner, so no handleNotLeader() would
            // drop a NotLeader-carrying region — check() must
            // self-invalidate (issue #474 review).
            RegionErrorHandler::check(
                $response,
                $this->regionCache,
                $region->regionId,
                notLeaderOwnedByRetryExecutor: false,
            );

            // GC abort handling (issue #422): without this, a start
            // timestamp GC has passed yields an empty pair list and every
            // key of the region silently resolves to null below — and is
            // cached into the read set, so even a caller-level retry within
            // the same transaction keeps reading null.
            $error = $response->getError();
            if ($error instanceof KeyError) {
                throw self::gcExceptionFromAbort($error->getAbort());
            }

            return $response;
        }, $future);
    }

    /**
     * Scan one clipped sub-range, continuing past regions that were split
     * after the outer region enumeration so no part of the range is dropped.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        string $startKey,
        string $endKey,
        int $limit,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit = 10240,
    ): array {
        $results = [];
        $pending = $limit;
        $cursorStart = $startKey;

        while (true) {
            // Resolve on the current start key: the sub-range starts inside
            // the region, so the cache lookup hits and only the end key
            // needs re-clipping after a split.
            $freshEndKey = '';
            $excludedStore = null;
            $batch = $retryExecutor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $maxScanLimit,
                &$excludedStore,
                &$freshEndKey,
            ): array {
                // Resolve the region on every attempt so cache invalidation
                // and leader switching performed by the retry executor take
                // effect (issue #267): a stale captured region would
                // otherwise reproduce the original error on each retry.
                $fresh = $this->regionResolver->getRegionInfo($cursorStart);
                $target = RegionContextFactory::resolveTarget(
                    $fresh,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                );
                $excludedStore = null;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);
                $freshEndKey = $fresh->endKey;

                // Re-clip the sub-range against the freshly resolved region:
                // after a split the fresh region is smaller, and TiKV rejects
                // ranges that cross region boundaries.
                $scanEnd = $freshEndKey !== '' && ($endKey === '' || $freshEndKey < $endKey)
                    ? $freshEndKey
                    : $endKey;

                $request = new ScanRequest();
                $request->setContext($target->context);
                $request->setStartKey($cursorStart);
                if ($scanEnd !== '') {
                    $request->setEndKey($scanEnd);
                }
                $request->setLimit($pending > 0 ? $pending : $maxScanLimit);
                $request->setVersion($this->startTs);

                try {
                    /** @var ScanResponse $response */
                    $response = $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'KvScan',
                        $request,
                        ScanResponse::class,
                        $this->timeoutMs('scan'),
                    );
                    RegionErrorHandler::check($response, $this->regionCache, $fresh->regionId);
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $error = $response->getError();
                if ($error !== null) {
                    $locked = $error->getLocked();
                    if ($locked !== null) {
                        $rawPrimary = $locked->getPrimaryLock();
                        $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
                        $this->lockResolver->resolveLock($lockPrimary, $locked);
                        throw new TxnRetryableException(
                            'Lock encountered during scan, resolved - retry',
                            BackoffType::TxnLock,
                        );
                    }

                    // Same GC abort mapping as get(): a scan crossing many
                    // regions is the most likely long-running read to have
                    // its start timestamp passed by GC mid-scan.
                    $abort = $error->getAbort();
                    if ($abort !== '') {
                        throw $this->gcExceptionFromAbort($abort);
                    }
                }

                $subResults = [];
                foreach ($response->getPairs() as $pair) {
                    $subResults[] = [
                        'key' => $pair->getKey(),
                        'value' => $pair->getValue(),
                    ];
                }

                return $subResults;
            }, $classifier);

            array_push($results, ...$batch);

            if ($pending > 0) {
                $pending -= count($batch);
                if ($pending <= 0) {
                    break;
                }
            }

            // Continue only when the fresh region ended inside the
            // sub-range (a split occurred) and the cursor actually
            // advanced; otherwise the whole sub-range was covered.
            if (
                $freshEndKey === ''
                || $freshEndKey <= $cursorStart
                || ($endKey !== '' && $freshEndKey >= $endKey)
            ) {
                break;
            }
            $cursorStart = $freshEndKey;
        }

        return $results;
    }

    /**
     * Build the typed exception for a KeyError abort message.
     *
     * TiKV names the GC case in the abort field ("GC life time is shorter
     * than transaction duration") when a read or commit targets a start
     * timestamp GC has already passed — map that to the dedicated,
     * non-retryable TxnAbortedByGcException (issue #422).
     *
     * Other abort texts (transaction aborts, flashbacks) have no dedicated
     * exception; they surface as a base TiKvException carrying the server
     * text. That is still strictly better than the previous behaviour, where
     * a non-GC abort fell through silently and the read returned an empty
     * value as if the key simply did not exist.
     */
    private function gcExceptionFromAbort(string $abort): TiKvException
    {
        if (str_contains($abort, 'GC life time is shorter')) {
            return new TxnAbortedByGcException($abort);
        }

        return new TiKvException($abort);
    }

    /**
     * Normalize batch keys to strings. PHP coerces integer-like string keys
     * ("12345", "0") to int when arrays are built with array_keys(); ints are
     * cast back to strings, anything else is rejected (issue #322).
     *
     * @param array<array-key, string|int> $keys
     * @return string[]
     *
     * @throws InvalidArgumentException
     */
    private function normalizeKeysToStrings(array $keys, string $operation): array
    {
        $normalized = [];
        foreach ($keys as $key) {
            if (is_int($key)) {
                $normalized[] = (string) $key;
            } elseif (is_string($key)) {
                $normalized[] = $key;
            } else {
                throw new InvalidArgumentException(sprintf(
                    '%s: keys must be strings or ints, %s given',
                    $operation,
                    get_debug_type($key),
                ));
            }
        }

        return $normalized;
    }

    /**
     * @throws InvalidArgumentException
     */
    private function normalizeScanLimit(int $limit, int $maxScanLimit): int
    {
        if ($limit < 0) {
            throw new InvalidArgumentException('Scan limit must be 0 or greater');
        }

        if ($limit === 0) {
            return $maxScanLimit;
        }

        if ($limit > $maxScanLimit) {
            throw new InvalidArgumentException(sprintf(
                'Scan limit (%d) exceeds maximum allowed scan limit of %d',
                $limit,
                $maxScanLimit,
            ));
        }

        return $limit;
    }

    /**
     * Merge TiKV scan results with the local write set to enforce
     * read-your-writes semantics, then apply the limit.
     *
     * @param array<array{key: string, value: ?string}> $results
     * @return array<array{key: string, value: ?string}>
     */
    private function finalizeScanResults(
        array $results,
        string $startKey,
        string $endKey,
        int $limit,
        TransactionState $state,
    ): array {
        $tikvMap = [];
        foreach ($results as $entry) {
            $tikvMap[$entry['key']] = $entry['value'];
        }

        // TiKV returns keys as strings, but integer-like keys ("12345",
        // "0") are stored as int keys by PHP; restore them so the state
        // lookups and output records use strings (issue #322).
        $allKeys = array_map(strval(...), array_keys($tikvMap));
        foreach ($state->getWriteKeys() as $key) {
            // Byte-order comparison: numeric strings must never be compared
            // numerically ("9" >= "10" is true under PHP's loose comparison
            // but false in TiKV's byte order) (issue #331).
            if (strcmp($key, $startKey) >= 0 && ($endKey === '' || strcmp($key, $endKey) < 0)) {
                $allKeys[] = $key;
            }
        }
        $allKeys = array_unique($allKeys);
        // Byte-order sort (SORT_STRING), never SORT_REGULAR: PHP compares
        // numeric strings numerically ("9" sorts before "10"), while TiKV
        // orders keys bytewise — pagination resumes from the last returned
        // key, so a numeric-order sort silently skips in-between keys
        // (issue #331).
        sort($allKeys, SORT_STRING);

        $merged = [];
        foreach ($allKeys as $key) {
            if (count($merged) >= $limit) {
                break;
            }

            if ($state->hasWriteSetKey($key)) {
                $writeValue = $state->getWriteSetValue($key);
                if ($writeValue !== null) {
                    $merged[] = ['key' => $key, 'value' => $writeValue];
                }
            } elseif (array_key_exists($key, $tikvMap)) {
                $merged[] = ['key' => $key, 'value' => $tikvMap[$key]];
            }
        }

        return $merged;
    }

    private function timeoutMs(string $operationType): ?int
    {
        return match ($operationType) {
            'read' => $this->timeoutConfig->readTimeoutMs,
            'batch_read' => $this->timeoutConfig->batchReadTimeoutMs,
            'scan' => $this->timeoutConfig->scanTimeoutMs,
            default => null,
        };
    }
}
