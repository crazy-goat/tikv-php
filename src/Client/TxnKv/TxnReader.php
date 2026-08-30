<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;

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

        return $retryExecutor->execute($key, function () use ($key, $state): ?string {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new GetRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);
            $request->setVersion($this->startTs);

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
        }, $classifier);
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
                continue;
            }
            $regionId = $region->regionId;
            $grouped[$regionId] ??= ['region' => $region, 'keys' => []];
            $grouped[$regionId]['keys'][] = $key;
        }

        foreach ($grouped as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new BatchGetRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKeys($regionKeys);
            $request->setVersion($this->startTs);

            /** @var BatchGetResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvBatchGet',
                $request,
                BatchGetResponse::class,
                $this->timeoutMs('batch_read'),
            );
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
            $batch = $retryExecutor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $maxScanLimit,
                &$freshEndKey,
            ): array {
                // Resolve the region on every attempt so cache invalidation
                // and leader switching performed by the retry executor take
                // effect (issue #267): a stale captured region would
                // otherwise reproduce the original error on each retry.
                $fresh = $this->regionResolver->getRegionInfo($cursorStart);
                $address = $this->regionResolver->resolveStoreAddress($fresh->leaderStoreId);
                $freshEndKey = $fresh->endKey;

                // Re-clip the sub-range against the freshly resolved region:
                // after a split the fresh region is smaller, and TiKV rejects
                // ranges that cross region boundaries.
                $scanEnd = $freshEndKey !== '' && ($endKey === '' || $freshEndKey < $endKey)
                    ? $freshEndKey
                    : $endKey;

                $request = new ScanRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($fresh));
                $request->setStartKey($cursorStart);
                if ($scanEnd !== '') {
                    $request->setEndKey($scanEnd);
                }
                $request->setLimit($pending > 0 ? $pending : $maxScanLimit);
                $request->setVersion($this->startTs);

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
            if ($key >= $startKey && ($endKey === '' || $key < $endKey)) {
                $allKeys[] = $key;
            }
        }
        $allKeys = array_unique($allKeys);
        sort($allKeys);

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
