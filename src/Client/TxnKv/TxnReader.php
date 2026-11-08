<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\CommandPri;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\BatchCallId;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\KeyErrorDescriber;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\RegionScanBatch;
use CrazyGoat\TiKV\Client\Region\RegionScanSource;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryBudget;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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
     * Re-asks allowed for a KvScan sub-range that answered empty while the
     * limit was still unsatisfied (issue #512).
     */
    private const MAX_EMPTY_SCAN_REATTEMPTS = 1;

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
        private LoggerInterface $logger = new NullLogger(),
        /**
         * Kvrpcpb CommandPri carried on every read request's Context
         * (issue #336). Normal (0) is the proto3 default and is not
         * serialized, so a transaction without a priority keeps the
         * original wire bytes.
         */
        private int $priority = CommandPri::Normal,
        /** Metrics backend for scan-path region-cache hit/miss counters (issue #425). */
        private MetricsInterface $metrics = new NoOpMetrics(),
        /** Optional Context control fields applied to every read (issue #426). */
        private ?RequestContextOptions $requestContext = null,
        /** Resource-control metadata carried on every read request (issue #427). */
        private ResourceControl $resourceControl = new ResourceControl(),
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

        // One get() call is one logical operation: a fresh budget, never the
        // one a previous read or the commit phase spent (issue #223).
        $retryExecutor = $retryExecutor->beginOperation();
        // The same budget bounds lock resolution triggered by a locked key
        // (issue #569).
        $budget = $retryExecutor->currentBudget();

        $excludedStore = null;
        $targetStoreId = null;

        return $retryExecutor->execute(
            $key,
            function () use ($key, $state, &$excludedStore, &$targetStoreId, $budget): ?string {
                $region = $this->regionResolver->getRegionInfo($key);
                $target = RegionContextFactory::resolveTarget(
                    $region,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                    priority: $this->priority,
                    requestContext: $this->requestContext,
                    resourceControl: $this->resourceControl,
                    storeAvailable: $this->regionResolver->isStoreAvailable(...),
                );
                $excludedStore = null;
                $targetStoreId = $target->storeId;
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
                        $this->timeoutMsWithinBudget($this->timeoutMs('read'), $budget),
                    );

                    RegionErrorHandler::check(
                        $response,
                        $this->regionCache,
                        $region->regionId,
                        keyErrorsHandledByCaller: true,
                    );
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
                if ($error instanceof KeyError) {
                    $this->handleReadKeyError($error, $key, 'Get', $budget);
                }

                if ($response->getNotFound()) {
                    $state->setReadValue($key, null);
                    return null;
                }

                $value = $response->getValue();
                $state->setReadValue($key, $value);
                return $value;
            },
            $classifier,
            targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            },
        );
    }

    /**
     * Batch-read multiple keys.
     *
     * Every per-region `KvBatchGet` dispatch+wait cycle runs under the
     * transaction's retry executor, with the same classifier as `get()` and
     * `scan()`: a region error (`NotLeader`, `EpochNotMatch`, `ServerIsBusy`,
     * …) invalidates or switches the cached region and the region is
     * re-resolved before the retry (issue #222). The first attempt of every
     * region is still dispatched eagerly, before any response is awaited, so
     * the client-side fan-out of issue #291 is preserved; the retries run
     * inside {@see RetryExecutor::execute()}.
     *
     * The returned map inherits PHP's array-key semantics: a canonical
     * decimal-integer key is returned under its `int` form, every other key
     * form stays a `string` key — including a canonical decimal that overflows
     * a PHP int (`'9223372036854775808'`), which no int key can denote.
     * `$results['1000']` still finds the entry stored as int 1000, because PHP
     * casts a lookup the same way. Hence `array-key`, not `string` (issue #261;
     * see {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::batchGet()} for
     * the full rule).
     *
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<array-key, ?string>
     *
     * @throws InvalidArgumentException
     * @throws TiKvException
     */
    public function batchGet(
        array $keys,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): array {
        $keys = $this->normalizeKeysToStrings($keys, 'batchGet');

        // One batchGet() call is one logical operation: every region shares
        // one retry budget (issue #223).
        $retryExecutor = $retryExecutor->beginOperation();

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
            $remoteResults = $this->batchGetFromTiKV($remaining, $state, $retryExecutor, $classifier);
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
     * The regions covering the range are loaded from PD in bounded batches
     * (issue #282): the scan asks for a bounded number of regions at a time
     * and only fetches the next batch when the current one is consumed
     * without satisfying the row limit. Asking PD for the whole range
     * answers with every region of the keyspace, which is tens of megabytes
     * for a scan that reads one region.

     * `limit: 0` asks for the whole range. The scan budget is then the
     * per-transaction cap (`$maxScanLimit`, 10240 by default), and a range
     * that holds more rows than the cap does **not** return a truncated
     * result: the scan probes for one more row and throws
     * {@see ScanLimitExceededException} when the range really holds more
     * (issue #440). A scan with an explicit `limit > 0` returns at most that
     * many rows, so the caller detects a possibly-truncated result by
     * comparing the returned count with the limit.
     *
     * With `$reverse = true` the bounds keep the RawKV reverse-scan meaning:
     * `$startKey` is the exclusive upper bound and `$endKey` the lower bound,
     * so the scan reads `[endKey, startKey)` in descending key order (issue
     * #435, GAP-21). An empty `$startKey` with a non-empty `$endKey` is
     * rejected before any RPC — reverse scan has no +infinity sentinel — and
     * `$startKey === $endKey` is the empty range.
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException
     * @throws TiKvException
     * @throws ScanLimitExceededException When `limit` is 0 and the range holds
     *         more rows than the per-transaction scan cap
     */
    public function scan(
        string $startKey,
        string $endKey,
        int $limit,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit = 10240,
        bool $reverse = false,
    ): array {
        // One scan() call is one logical operation: every page and region
        // shares one retry budget (issue #223).
        $retryExecutor = $retryExecutor->beginOperation();

        if ($reverse) {
            // A reverse scan reads [endKey, startKey) descending: an empty
            // upper bound is not +infinity and an inverted range is rejected
            // before any PD or store RPC (issue #435, GAP-21).
            RegionRangeClipper::assertReverseRangeOrder($startKey, $endKey, 'scan');
        }

        // `limit: 0` means "the whole range"; the scan budget is the cap and
        // a range that exceeds it must fail loudly instead of returning a
        // truncated result (issue #440).
        $wholeRange = $limit === 0;
        $limit = $this->normalizeScanLimit($limit, $maxScanLimit);

        if ($reverse && $startKey === $endKey) {
            // Both bounds are the keyspace minimum: the reverse range is
            // empty, so no region has to be resolved (RAW-09 parity).
            return [];
        }

        $results = [];
        $remaining = $limit;
        $batchSize = RegionScanBatch::forRowBudget($limit);

        $clipper = new RegionRangeClipper();

        if ($reverse) {
            foreach ($this->reverseRegionBatches($startKey, $endKey, $batchSize) as $regions) {
                foreach ($clipper->clipReverse($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
                    $regionResults = $this->executeScanForRegion(
                        $scanStart,
                        $scanEnd,
                        $remaining,
                        $retryExecutor,
                        $classifier,
                        $maxScanLimit,
                        true,
                    );
                    foreach ($regionResults as $row) {
                        $results[] = $row;
                    }

                    $remaining -= count($regionResults);
                    if ($remaining <= 0) {
                        break 2;
                    }
                }
            }
        } else {
            // The cache is read first and PD is asked only for the gaps
            // (issue #425), so a warm cache turns every page of a
            // transactional scan into zero PD round trips. The source trims
            // a batch to the cursor a merge may have moved it to, exactly as
            // the old loop did.
            $source = new RegionScanSource(
                $this->pdClient,
                $this->regionCache,
                $this->metrics,
                'txn_scan',
            );

            foreach ($source->batches($startKey, $endKey, $batchSize) as $regions) {
                foreach ($clipper->clipForward($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
                    $regionResults = $this->executeScanForRegion(
                        $scanStart,
                        $scanEnd,
                        $remaining,
                        $retryExecutor,
                        $classifier,
                        $maxScanLimit,
                    );
                    foreach ($regionResults as $row) {
                        $results[] = $row;
                    }

                    $remaining -= count($regionResults);
                    if ($remaining <= 0) {
                        break 2;
                    }
                }
            }
        }

        if ($wholeRange && $remaining <= 0) {
            // The whole budget was spent: the range may hold rows the scan
            // did not return. Probe before answering, so the caller learns
            // that the result was truncated instead of trusting a partial
            // range (issue #440). A reverse scan probes below its lowest row.
            if ($reverse) {
                $this->assertReverseRangeNotTruncated($results, $endKey, $retryExecutor, $classifier, $maxScanLimit);
            } else {
                $this->assertRangeNotTruncated($results, $endKey, $retryExecutor, $classifier, $maxScanLimit);
            }
        }

        // The whole-range path is not capped again here: the cap bounds the
        // TiKV read, while the write-set merge may add the transaction's own
        // in-range keys, which must not be dropped (issue #440).
        return $this->finalizeScanResults(
            $results,
            $startKey,
            $endKey,
            $wholeRange ? null : $limit,
            $state,
            $reverse,
        );
    }

    /**
     * Enumerate the regions covering the reverse range [endKey, startKey) from
     * the upper bound downwards, in bounded batches (issue #435, GAP-21).
     *
     * A reverse scan consumes a range from its upper bound, so the regions are
     * resolved from the top: the cache answers the common case and a miss asks
     * PD for the region containing the cursor, or — when the cursor sits on a
     * region boundary — for the adjacent region below through `GetPrevRegion`.
     * This is the same top-down walk RawKV's reverse scan uses (issue #198),
     * so a narrow reverse scan never enumerates the rest of the range.
     *
     * @param string $startKey exclusive upper bound
     * @param string $endKey lower bound ('' = the start of the keyspace)
     * @param int $batchSize maximum number of regions per batch
     * @return \Generator<int, list<RegionInfo>>
     */
    private function reverseRegionBatches(string $startKey, string $endKey, int $batchSize): \Generator
    {
        $cursor = $startKey;
        $batch = [];

        while ($cursor !== '') {
            $region = $this->regionResolver->getRegionBelow($cursor);
            if (!$region instanceof RegionInfo) {
                break;
            }

            // A merge after the previous batch can produce a region that
            // extends above the cursor already read; clamp it to the cursor.
            $batch[] = RegionScanBatch::trimEndToCursor($region, $cursor);

            $nextCursor = $region->startKey;
            $done = $nextCursor === ''
                || !KeyOrder::lt($nextCursor, $cursor)
                || ($endKey !== '' && KeyOrder::lte($nextCursor, $endKey));

            if (count($batch) >= $batchSize || $done) {
                yield $batch;
                $batch = [];
            }

            if ($done) {
                return;
            }

            $cursor = $nextCursor;
        }

        if ($batch !== []) {
            yield $batch;
        }
    }

    /**
     * Fail closed when an unbounded transactional scan spent its whole budget
     * and the range holds rows the scan did not return (issue #440).
     *
     * Spending the budget does not prove that rows are missing — a range
     * with exactly `maxScanLimit` rows is complete — so one extra row is
     * probed for: a `KvScan` for a single row starting at the successor of
     * the last key the scan returned. A row there means the result was
     * truncated, and the scan throws {@see ScanLimitExceededException}
     * instead of returning a partial range.
     *
     * @param list<array{key: string, value: ?string}> $results the rows the
     *        scan returned
     *
     * @throws ScanLimitExceededException
     */
    private function assertRangeNotTruncated(
        array $results,
        string $endKey,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit,
    ): void {
        if ($results === []) {
            return;
        }

        // The probe starts after the highest key the scan returned — not
        // after the last row: the rows of a region come back in ascending
        // order, but the merge with the write set happens later and the
        // probe must not depend on that.
        $highestKey = $results[0]['key'];
        foreach ($results as $row) {
            if (KeyOrder::gt($row['key'], $highestKey)) {
                $highestKey = $row['key'];
            }
        }

        $cursor = KeyOrder::successor($highestKey);

        if ($endKey !== '' && KeyOrder::gte($cursor, $endKey)) {
            // The range ends at the last row the scan returned: nothing can
            // follow it inside the range.
            return;
        }

        $probe = $this->executeScanForRegion($cursor, $endKey, 1, $retryExecutor, $classifier, $maxScanLimit);

        if ($probe !== []) {
            throw ScanLimitExceededException::forTransactionScan($maxScanLimit, count($results));
        }
    }

    /**
     * The reverse counterpart of {@see assertRangeNotTruncated()}: a reverse
     * scan returns the highest rows first, so an exhausted budget may hide
     * rows **below** the lowest key it returned (issue #435, GAP-21).
     *
     * One row is probed for in [endKey, lowestKey), the part of the range the
     * scan has not returned. A row there means the result was truncated, and
     * the scan throws {@see ScanLimitExceededException} instead of returning a
     * partial range.
     *
     * @param list<array{key: string, value: ?string}> $results the rows the
     *        scan returned, in descending key order
     *
     * @throws ScanLimitExceededException
     */
    private function assertReverseRangeNotTruncated(
        array $results,
        string $endKey,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit,
    ): void {
        if ($results === []) {
            return;
        }

        $lowestKey = $results[0]['key'];
        foreach ($results as $row) {
            if (KeyOrder::lt($row['key'], $lowestKey)) {
                $lowestKey = $row['key'];
            }
        }

        if ($lowestKey === '' || ($endKey !== '' && KeyOrder::lte($lowestKey, $endKey))) {
            // The range ends at the lowest row the scan returned: nothing can
            // follow it below the lower bound.
            return;
        }

        $probe = $this->executeScanForRegion(
            $lowestKey,
            $endKey,
            1,
            $retryExecutor,
            $classifier,
            $maxScanLimit,
            true,
        );

        if ($probe !== []) {
            throw ScanLimitExceededException::forTransactionScan($maxScanLimit, count($results));
        }
    }

    /**
     * The returned map inherits PHP's array-key semantics: a canonical
     * decimal-integer key is returned under its `int` form, every other key
     * form stays a `string` key — including a canonical decimal that overflows
     * a PHP int (`'9223372036854775808'`), which no int key can denote.
     * `$results['1000']` still finds the entry stored as int 1000, because PHP
     * casts a lookup the same way. Hence `array-key`, not `string` (issue #261;
     * see {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::batchGet()} for
     * the full rule).
     *
     * @param string[] $keys
     * @return array<array-key, ?string>
     */
    private function batchGetFromTiKV(
        array $keys,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): array {
        $results = [];
        $resolved = $this->regionResolver->batchResolveRegions($keys);

        // Group keys by resolved region.
        $grouped = [];
        foreach ($keys as $key) {
            // Fails closed rather than skipping: a skipped key here would
            // read back as null — indistinguishable from "key not present"
            // (issue #187).
            $region = RegionGrouper::resolvedRegion($resolved, $key);
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
        /** @var array<int, BatchCallId> $labels */
        $labels = [];
        foreach ($grouped as $regionId => $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];

            $slot = count($regionCalls);
            $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchGetForRegionWithRetry(
                $regionKeys,
                $retryExecutor,
                $classifier,
            );
            $labels[$slot] = new BatchCallId($regionId);
        }

        $batchExecutor = new BatchAsyncExecutor();
        try {
            $regionResults = $batchExecutor->executeParallel(
                $regionCalls,
                $this->timeoutConfig->batchDeadlineMs,
                $labels,
            );
        } catch (BatchPartialFailureException $e) {
            // The future-level RetryExecutor has already applied each
            // region's retry budget; unwrap the aggregate to preserve the
            // historical first-failing-region exception surface (issue #291).
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
     * Eagerly dispatch the first regional request and retry its wait phase
     * when response-borne errors require lock resolution (issue #210) or a
     * retryable region error (issue #222).
     *
     * `fromRetryableDispatch()` is the fan-out-preserving form of
     * {@see RetryExecutor::execute()}: the first attempt is sent during the
     * dispatch phase so every region's RPC overlaps, while the wait phase
     * runs the dispatch+wait cycle inside the executor. That is why the
     * per-region call is not wrapped in a blocking `execute()` here — doing
     * so would serialise the regions and undo issue #291.
     *
     * @param string[] $regionKeys
     * @param callable(TiKvException): RetryVerdict $classifier
     */
    private function batchGetForRegionWithRetry(
        array $regionKeys,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): CheckedGrpcFuture {
        $firstKey = $regionKeys[0] ?? '';
        // The same operation budget bounds lock resolution triggered by a
        // locked key in any region of this batch (issue #569).
        $budget = $retryExecutor->currentBudget();
        $targetStoreId = 0;

        return CheckedGrpcFuture::fromRetryableDispatch(
            function () use ($firstKey, $regionKeys, &$targetStoreId, $budget): CheckedGrpcFuture {
                return $this->batchGetForRegionAsync(
                    $this->regionResolver->getRegionInfo($firstKey),
                    $regionKeys,
                    $targetStoreId,
                    $budget,
                );
            },
            $retryExecutor,
            $firstKey,
            $classifier,
            static function () use (&$targetStoreId): int {
                return $targetStoreId;
            },
        );
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
        int &$targetStoreId = 0,
        ?RetryBudget $budget = null,
    ): CheckedGrpcFuture {
        $target = RegionContextFactory::resolveTarget(
            $region,
            $this->replicaReadPolicy,
            $this->regionResolver->getStore(...),
            priority: $this->priority,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
            storeAvailable: $this->regionResolver->isStoreAvailable(...),
        );
        $targetStoreId = $target->storeId;
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
            $this->timeoutMsWithinBudget($this->timeoutMs('batch_read'), $budget),
        );

        return CheckedGrpcFuture::fromCallable(function () use ($future, $region, $regionKeys, $budget) {
            /** @var BatchGetResponse $response */
            $response = $future->wait();
            // The wait-boundary RetryExecutor is the sole NotLeader owner
            // for this response (issue #474).
            RegionErrorHandler::check(
                $response,
                $this->regionCache,
                $region->regionId,
                notLeaderOwnedByRetryExecutor: true,
                keyErrorsHandledByCaller: true,
            );

            $error = $response->getError();
            if ($error instanceof KeyError) {
                $this->handleReadKeyError($error, $regionKeys[0] ?? '', 'BatchGet', $budget);
            }

            $pairErrors = [];
            foreach ($response->getPairs() as $index => $pair) {
                $pairError = $pair->getError();
                if ($pairError instanceof KeyError) {
                    $fallbackKey = is_int($index) ? ($regionKeys[$index] ?? $pair->getKey()) : $pair->getKey();
                    $pairErrors[] = [$pairError, $fallbackKey];
                }
            }
            $this->handleReadKeyErrors($pairErrors, 'BatchGet', $budget);

            return $response;
        }, $future);
    }

    private function lockRetryMessage(string $operation): string
    {
        return $operation === 'Get'
            ? 'Lock encountered, resolved - retry'
            : sprintf('Lock encountered during %s, resolved - retry', $operation);
    }

    /**
     * Handle the per-pair KeyErrors of one batchGet or scan response. Every
     * lock is resolved in one batch (one status check per transaction, one
     * KvResolveLock per region and transaction, issue #428) before a single
     * retry is requested. Without locks the first error raises as in
     * {@see handleReadKeyError()}.
     *
     * @param list<array{KeyError, string}> $pairErrors each error with its fallback key
     * @param RetryBudget|null $budget retry budget of the enclosing read
     *        operation; its remaining deadline bounds the lock resolution
     *        (issue #569)
     */
    private function handleReadKeyErrors(
        array $pairErrors,
        string $operation,
        ?RetryBudget $budget = null,
    ): void {
        $locks = [];
        foreach ($pairErrors as [$error, $fallbackKey]) {
            $locked = $error->getLocked();
            if ($locked !== null) {
                if ($locked->getKey() === '') {
                    $locked->setKey($fallbackKey);
                }
                $locks[] = $locked;
            }
        }

        if ($locks !== []) {
            $this->lockResolver->resolveLocks($locks, $budget?->remainingDeadlineMs() ?? 0);
            throw new TxnRetryableException($this->lockRetryMessage($operation), BackoffType::TxnLock);
        }

        foreach ($pairErrors as [$error, $fallbackKey]) {
            $this->handleReadKeyError($error, $fallbackKey, $operation, $budget);
        }
    }

    /**
     * Handle a transactional read KeyError consistently across get, batchGet,
     * and scan. A live lock resolves then requests a retry of the same read;
     * retryable conflicts, GC aborts, and unknown variants fail closed.
     */
    private function handleReadKeyError(
        KeyError $error,
        string $fallbackKey,
        string $operation,
        ?RetryBudget $budget = null,
    ): never {
        $locked = $error->getLocked();
        if ($locked !== null) {
            $rawPrimary = $locked->getPrimaryLock();
            $primary = $rawPrimary !== ''
                ? $rawPrimary
                : ($locked->getKey() !== '' ? $locked->getKey() : $fallbackKey);
            $this->lockResolver->resolveLock(
                $primary,
                $locked,
                $budget?->remainingDeadlineMs() ?? 0,
            );
            throw new TxnRetryableException($this->lockRetryMessage($operation), BackoffType::TxnLock);
        }

        $retryable = $error->getRetryable();
        if ($retryable !== '') {
            throw new TransactionConflictException($retryable);
        }

        $abort = $error->getAbort();
        if ($abort !== '') {
            throw self::gcExceptionFromAbort($abort);
        }

        throw new TiKvException(
            sprintf('%s failed: %s', $operation, KeyErrorDescriber::describe($error)),
        );
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
        bool $reverse = false,
    ): array {
        if ($reverse) {
            return $this->executeReverseScanForRegion(
                $startKey,
                $endKey,
                $limit,
                $retryExecutor,
                $classifier,
                $maxScanLimit,
            );
        }

        $results = [];
        $pending = $limit;
        $cursorStart = $startKey;
        $emptyReattempts = 0;
        // The same operation budget bounds lock resolution triggered by a
        // locked key in any page of this scan (issue #569).
        $budget = $retryExecutor->currentBudget();

        while (true) {
            // Resolve on the current start key: the sub-range starts inside
            // the region, so the cache lookup hits and only the end key
            // needs re-clipping after a split.
            $freshEndKey = '';
            $excludedStore = null;
            $targetStoreId = null;
            $batch = $retryExecutor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $maxScanLimit,
                &$excludedStore,
                &$freshEndKey,
                &$targetStoreId,
                $budget,
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
                    priority: $this->priority,
                    requestContext: $this->requestContext,
                    resourceControl: $this->resourceControl,
                    storeAvailable: $this->regionResolver->isStoreAvailable(...),
                );
                $excludedStore = null;
                $targetStoreId = $target->storeId;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);
                $freshEndKey = $fresh->endKey;

                // Re-clip the sub-range against the freshly resolved region:
                // after a split the fresh region is smaller, and TiKV rejects
                // ranges that cross region boundaries. The bounds are
                // compared bytewise — a loose `<` would compare numeric keys
                // numerically ("20" < "100" is false) and widen the wire
                // range across the region boundary (issue #186).
                $scanEnd = $freshEndKey !== '' && ($endKey === '' || KeyOrder::lt($freshEndKey, $endKey))
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
                        $this->timeoutMsWithinBudget($this->timeoutMs('scan'), $budget),
                    );
                    RegionErrorHandler::check(
                        $response,
                        $this->regionCache,
                        $fresh->regionId,
                        keyErrorsHandledByCaller: true,
                    );
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $error = $response->getError();
                if ($error instanceof KeyError) {
                    $this->handleReadKeyError($error, $cursorStart, 'Scan', $budget);
                }

                $subResults = [];
                $pairErrors = [];
                foreach ($response->getPairs() as $pair) {
                    $pairError = $pair->getError();
                    if ($pairError instanceof KeyError) {
                        $pairErrors[] = [$pairError, $pair->getKey()];
                    }
                    $subResults[] = [
                        'key' => $pair->getKey(),
                        'value' => $pair->getValue(),
                    ];
                }
                $this->handleReadKeyErrors($pairErrors, 'Scan', $budget);

                return $subResults;
            }, $classifier, targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            });

            foreach ($batch as $row) {
                $results[] = $row;
            }

            $emptyBatch = $batch === [];
            if (!$emptyBatch) {
                $emptyReattempts = 0;
            }

            if ($pending > 0) {
                $pending -= count($batch);
                if ($pending <= 0) {
                    break;
                }
            }

            // Continue only when the fresh region ended inside the
            // sub-range (a split occurred) and the cursor actually
            // advanced; otherwise the whole sub-range was covered. Both
            // bounds are compared bytewise, never with PHP's relational
            // operators, which fall back to a numeric comparison for
            // numeric strings: "9" is bytewise past the region end "10"
            // while 9 >= 10 is false, so the scan would keep going over a
            // range the region does not own (issue #186).
            if (
                $freshEndKey === ''
                || KeyOrder::lte($freshEndKey, $cursorStart)
                || ($endKey !== '' && KeyOrder::gte($freshEndKey, $endKey))
            ) {
                // An empty answer for a still-unsatisfied limit that would end
                // the scan is the one shape a transient empty KvScan batch
                // takes (issue #512). Re-ask the same range a bounded number
                // of times before accepting it as the end of the data.
                if ($emptyBatch && $pending > 0) {
                    if ($emptyReattempts < self::MAX_EMPTY_SCAN_REATTEMPTS) {
                        usleep(BackoffType::RegionMiss->sleepMs($emptyReattempts) * 1000);
                        $emptyReattempts++;
                        continue;
                    }

                    $this->logger->debug('TxnKV scan accepted an empty batch after re-attempts', [
                        'startKey' => KeyRedactor::redact($cursorStart),
                        'pending' => $pending,
                        'reattempts' => $emptyReattempts,
                    ]);
                }
                break;
            }
            $cursorStart = $freshEndKey;
            $emptyReattempts = 0;
        }

        return $results;
    }

    /**
     * Reverse-scan one clipped sub-range. The caller passes the original
     * sub-range as [startKey, endKey) == [upper bound, lower bound); the wire
     * request reads [endKey, startKey) in descending order with `reverse`.
     *
     * The sub-range is resolved on its lower bound, not its upper one: the
     * upper bound can sit exactly on a region's end boundary, where the cache
     * lookup would miss and PD would answer with the neighbouring region
     * outside the sub-range (the same choice RawKV's reverse scan makes).
     *
     * A split discovered after the sub-range was clipped leaves the remainder
     * [freshEndKey, startKey) above the batch; it belongs *before* this batch
     * in descending order, so it is scanned first and the batch is trimmed to
     * the remaining limit (issue #435, GAP-21).
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function executeReverseScanForRegion(
        string $startKey,
        string $endKey,
        int $limit,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit = 10240,
    ): array {
        $resolutionKey = $endKey;
        $freshEndKey = '';
        $emptyReattempts = 0;
        // The same operation budget bounds lock resolution triggered by a
        // locked key in this sub-range (issue #569).
        $budget = $retryExecutor->currentBudget();

        while (true) {
            $freshEndKey = '';
            $excludedStore = null;
            $targetStoreId = null;

            $batch = $retryExecutor->execute($resolutionKey, function () use (
                $startKey,
                $endKey,
                $limit,
                $maxScanLimit,
                $resolutionKey,
                &$excludedStore,
                &$freshEndKey,
                &$targetStoreId,
                $budget,
            ): array {
                // Resolve the region on every attempt so cache invalidation
                // and leader switching performed by the retry executor take
                // effect (issue #267): a stale captured region would
                // otherwise reproduce the original error on each retry.
                $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
                $target = RegionContextFactory::resolveTarget(
                    $fresh,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                    priority: $this->priority,
                    requestContext: $this->requestContext,
                    resourceControl: $this->resourceControl,
                    storeAvailable: $this->regionResolver->isStoreAvailable(...),
                );
                $excludedStore = null;
                $targetStoreId = $target->storeId;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);
                $freshEndKey = $fresh->endKey;

                // After a split the fresh region is smaller: clip the wire
                // start (upper) key down to the fresh region's end. TiKV
                // rejects ranges that cross region boundaries.
                $wireStartKey = $startKey;
                if ($freshEndKey !== '' && KeyOrder::lt($freshEndKey, $wireStartKey)) {
                    $wireStartKey = $freshEndKey;
                }

                $request = new ScanRequest();
                $request->setContext($target->context);
                $request->setStartKey($wireStartKey);
                if ($endKey !== '') {
                    $request->setEndKey($endKey);
                }
                $request->setLimit($limit > 0 ? $limit : $maxScanLimit);
                $request->setVersion($this->startTs);
                $request->setReverse(true);

                try {
                    /** @var ScanResponse $response */
                    $response = $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'KvScan',
                        $request,
                        ScanResponse::class,
                        $this->timeoutMsWithinBudget($this->timeoutMs('scan'), $budget),
                    );
                    RegionErrorHandler::check(
                        $response,
                        $this->regionCache,
                        $fresh->regionId,
                        keyErrorsHandledByCaller: true,
                    );
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $error = $response->getError();
                if ($error instanceof KeyError) {
                    $this->handleReadKeyError($error, $resolutionKey, 'Scan', $budget);
                }

                $subResults = [];
                $pairErrors = [];
                foreach ($response->getPairs() as $pair) {
                    $pairError = $pair->getError();
                    if ($pairError instanceof KeyError) {
                        $pairErrors[] = [$pairError, $pair->getKey()];
                    }
                    $subResults[] = [
                        'key' => $pair->getKey(),
                        'value' => $pair->getValue(),
                    ];
                }
                $this->handleReadKeyErrors($pairErrors, 'Scan', $budget);

                return $subResults;
            }, $classifier, targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            });

            // A transient empty answer is a diagnosed failure mode (issue
            // #512): when the region covers the whole sub-range and the
            // caller still wants rows, re-ask the same range once before
            // accepting the empty answer. This also covers the whole-range
            // truncation probe, whose sub-range is one region wide.
            $terminal = $freshEndKey === '' || KeyOrder::gte($freshEndKey, $startKey);
            if (!$terminal || $batch !== [] || $limit <= 0) {
                break;
            }
            if ($emptyReattempts < self::MAX_EMPTY_SCAN_REATTEMPTS) {
                usleep(BackoffType::RegionMiss->sleepMs($emptyReattempts) * 1000);
                $emptyReattempts++;
                continue;
            }
            $this->logger->debug('TxnKV reverse scan accepted an empty batch after re-attempts', [
                'startKey' => KeyRedactor::redact($startKey),
                'endKey' => KeyRedactor::redact($endKey),
                'reattempts' => $emptyReattempts,
            ]);
            break;
        }

        // If the fresh region covers only [endKey, freshEndKey), the higher
        // remainder [freshEndKey, startKey) belongs BEFORE this batch in the
        // reverse result order: scan it first, then trim the batch to the
        // remaining limit.
        if ($freshEndKey === '' || KeyOrder::gte($freshEndKey, $startKey)) {
            return $batch;
        }

        $upper = $this->executeReverseScanForRegion(
            $startKey,
            $freshEndKey,
            $limit,
            $retryExecutor,
            $classifier,
            $maxScanLimit,
        );
        $batchCapacity = $limit > 0 ? $limit - count($upper) : count($batch);
        if ($batchCapacity <= 0) {
            return $upper;
        }

        return [...$upper, ...array_slice($batch, 0, $batchCapacity)];
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
     * TiKV returns rows in ascending byte order — every page continues from
     * the previous region boundary and the regions are clipped in ascending
     * order — so the scanned sequence is already sorted and only the
     * write-set keys need a sort. Instead of re-sorting up to `MAX_SCAN_LIMIT`
     * scanned rows (the `array_unique()` plus full `sort()` pair this method
     * used to run, issue #302), each sorted write-set key is placed into the
     * scanned sequence with a binary search and the untouched runs are copied
     * as slices. A per-row PHP merge loop was measured slower than the native
     * `sort()` it replaces — PHP's array functions are C, a userland loop is
     * not — so the merge must stay at the slice level (issue #302).
     *
     * A `null` limit means "no limit": the unbounded (`limit: 0`) path uses
     * it, because the write-set merge must not drop the transaction's own
     * in-range keys — the scan cap already bounds the TiKV read (issue #440).
     *
     * A reverse scan returns rows in descending order (issue #435, GAP-21).
     * The merge itself is written for ascending input — that is the order the
     * binary-search splice relies on — so the scanned rows are flipped to
     * ascending, merged, and flipped back; the limit then trims the highest
     * keys, which is what a reverse scan's `limit` means.
     *
     * @param array<array{key: string, value: ?string}> $results
     * @return array<array{key: string, value: ?string}>
     */
    private function finalizeScanResults(
        array $results,
        string $startKey,
        string $endKey,
        ?int $limit,
        TransactionState $state,
        bool $reverse = false,
    ): array {
        if ($reverse) {
            $results = array_reverse($results);
        }

        $tikvMap = [];
        foreach ($results as $entry) {
            $tikvMap[$entry['key']] = $entry['value'];
        }

        // TiKV returns keys as strings, but integer-like keys ("12345",
        // "0") are stored as int keys by PHP; restore them so the state
        // lookups and output records use strings (issue #322). Iterating the
        // map preserves the insertion order, i.e. the ascending order TiKV
        // returned (issue #302).
        $tikvRows = [];
        foreach ($tikvMap as $key => $value) {
            $tikvRows[] = ['key' => (string) $key, 'value' => $value];
        }

        // Only the write-set keys are unordered: `getWriteKeys()` follows the
        // transaction's write order, not key order. Byte-order sort
        // (SORT_STRING), never SORT_REGULAR: PHP compares numeric strings
        // numerically ("9" sorts before "10"), while TiKV orders keys
        // bytewise — pagination resumes from the last returned key, so a
        // numeric-order sort silently skips in-between keys (issue #331).
        // A reverse scan's bounds are [endKey, startKey), so the in-range
        // test swaps them; the merge below still runs ascending.
        $rangeStart = $reverse ? $endKey : $startKey;
        $rangeEnd = $reverse ? $startKey : $endKey;

        $writeKeys = [];
        foreach ($state->getWriteKeys() as $key) {
            // Byte-order comparison: numeric strings must never be compared
            // numerically ("9" >= "10" is true under PHP's loose comparison
            // but false in TiKV's byte order) (issue #331).
            if (KeyOrder::inRange($key, $rangeStart, $rangeEnd)) {
                $writeKeys[] = $key;
            }
        }
        sort($writeKeys, SORT_STRING);

        if ($writeKeys === []) {
            // Nothing to merge: the scanned rows are already the result.
            // A reverse scan's limit keeps the highest keys, i.e. the tail of
            // the ascending sequence.
            $rows = match (true) {
                $limit === null => $tikvRows,
                $reverse => array_slice($tikvRows, -$limit),
                default => array_slice($tikvRows, 0, $limit),
            };

            return $reverse ? array_reverse($rows) : $rows;
        }

        $tikvCount = count($tikvRows);
        $chunks = [];
        $cursor = 0;

        foreach ($writeKeys as $writeKey) {
            // Lower bound: the first scanned row whose key is >= $writeKey.
            // Only the write set is walked here; the scanned rows between two
            // write keys are copied as one slice, so the merge costs
            // `|writeSet| * log(rows)` key comparisons instead of one per row.
            $low = $cursor;
            $high = $tikvCount;
            while ($low < $high) {
                $mid = ($low + $high) >> 1;
                if (KeyOrder::lt($tikvRows[$mid]['key'], $writeKey)) {
                    $low = $mid + 1;
                } else {
                    $high = $mid;
                }
            }

            if ($low > $cursor) {
                $chunks[] = array_slice($tikvRows, $cursor, $low - $cursor);
            }

            // The write set wins over the scanned row, and a delete (null
            // value) hides it (read-your-writes).
            $writeValue = $state->getWriteSetValue($writeKey);
            if ($writeValue !== null) {
                $chunks[] = [['key' => $writeKey, 'value' => $writeValue]];
            }

            $cursor = $low < $tikvCount && KeyOrder::eq($tikvRows[$low]['key'], $writeKey)
                ? $low + 1
                : $low;
        }

        if ($cursor < $tikvCount) {
            $chunks[] = array_slice($tikvRows, $cursor);
        }

        $merged = $chunks === [] ? [] : array_merge(...$chunks);

        // The limit is an upper bound on the merged rows, so truncating the
        // finished sequence is the same result the old early `break` produced
        // (tombstones were already skipped above, so they do not consume it).
        // A reverse scan keeps the highest keys: the tail of the ascending
        // sequence, before it is flipped back to descending.
        $rows = match (true) {
            $limit === null => $merged,
            $reverse => array_slice($merged, -$limit),
            default => array_slice($merged, 0, $limit),
        };

        return $reverse ? array_reverse($rows) : $rows;
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

    /**
     * Per-RPC deadline capped by the operation's live remaining retry budget
     * (issue #257, REG-26).
     *
     * The configured timeout is the upper bound: an in-flight read must not
     * outlive the retry deadline the executor is enforcing around it. A null
     * configured timeout keeps the gRPC-layer default; a disabled or absent
     * budget leaves the configured value unchanged. The result is at least
     * 1 ms because 0 means "no deadline" to the gRPC layer, which is the
     * unbounded behaviour this guard exists to prevent.
     */
    private function timeoutMsWithinBudget(?int $configuredMs, ?RetryBudget $budget): ?int
    {
        if ($configuredMs === null) {
            return null;
        }

        $remainingMs = $budget?->remainingDeadlineMs() ?? 0;
        if ($remainingMs <= 0) {
            return $configuredMs;
        }

        return max(1, min($configuredMs, $remainingMs));
    }
}
