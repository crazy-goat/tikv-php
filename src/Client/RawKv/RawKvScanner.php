<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Google\Protobuf\Internal\Message;
use Psr\Log\LoggerInterface;

final readonly class RawKvScanner
{
    public const MAX_SCAN_LIMIT = 10240;

    /**
     * Default number of rows an unbounded scan (limit 0) may return before
     * throwing {@see ScanLimitExceededException} (issue #191). A `limit: 0`
     * scan pages internally to honour the "whole range" contract; this guard
     * caps the accumulated result so it cannot grow without bound.
     *
     * The guard is evaluated once per internally fetched page, after that
     * page has been buffered, so it is page-granular rather than a strict
     * per-row memory bound: when `maxScanRows` is smaller than the page size
     * (`scanPageSize`, default {@see self::MAX_SCAN_LIMIT}) the scan reads a
     * whole page before throwing, and a page fanned out across
     * `maxConcurrency` regions may have further in-flight responses. Peak
     * memory is therefore the accumulated rows (up to about `maxScanRows`)
     * plus up to `maxConcurrency x scanPageSize` rows of one fetched page.
     * Configurable through `options['maxScanRows']` on
     * {@see RawKvClient::create()}.
     */
    public const DEFAULT_MAX_SCAN_ROWS = 100000;

    public function __construct(
        private PdClientInterface $pdClient,
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private int $maxBackoffMs,
        private int $serverBusyBudgetMs,
        private RegionCacheInterface $regionCache,
        private LoggerInterface $logger,
        private ?SlowLogConfig $slowLogConfig = null,
        private int $retryDeadlineMs = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS,
        private int $maxConcurrency = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY,
        /** Read preference for scans (issue #421). */
        private ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
        /** Row cap for an unbounded (limit 0) scan; exceeded throws (issue #191). */
        private int $maxScanRows = self::DEFAULT_MAX_SCAN_ROWS,
        /**
         * Per-page row budget used when a `limit: 0` scan paginates internally
         * (issue #191). Defaults to {@see self::MAX_SCAN_LIMIT}; lower values
         * are a test seam, not a production knob.
         */
        private int $scanPageSize = self::MAX_SCAN_LIMIT,
    ) {
        if ($this->maxScanRows < 1) {
            throw new InvalidArgumentException('maxScanRows must be >= 1');
        }
        if ($this->scanPageSize < 1 || $this->scanPageSize > self::MAX_SCAN_LIMIT) {
            throw new InvalidArgumentException(sprintf(
                'scanPageSize must be between 1 and %d',
                self::MAX_SCAN_LIMIT,
            ));
        }
    }

    /**
     * Range scan over [startKey, endKey).
     *
     * `limit = 0` means "the whole range": the scan pages internally at
     * {@see self::$scanPageSize} rows per RPC and buffers every row, so the
     * documented unbounded contract holds for ranges larger than one TiKV
     * page (issue #191). The accumulated buffer is capped by
     * {@see self::$maxScanRows}; once the buffered rows exceed it a
     * {@see ScanLimitExceededException} is thrown instead of silently
     * truncating. The cap is checked per fetched page (page-granular), not per
     * row, so a page may be buffered in full before the throw.
     *
     * @return array<array{key: string, value: ?string}>
     */
    public function scan(string $startKey, string $endKey, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        $limit = $this->validateScanLimit($limit);

        if ($limit === 0) {
            return $this->scanUnbounded($startKey, $endKey, $keyOnly, $columnFamily);
        }

        $executor = $this->createRetryExecutor();
        $segments = $this->clipForwardSegments($startKey, $endKey);

        return $this->scanSegmentsSequentially($executor, $segments, $limit, $keyOnly, $columnFamily);
    }

    /**
     * Page a `limit: 0` forward scan to completion, advancing the cursor past
     * the last key of each full page (mirroring {@see ScanIterator}'s
     * `lastKey . "\x00"` continuation) and stopping on the first short or
     * empty page. Rows are buffered and guarded by {@see self::$maxScanRows}.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function scanUnbounded(string $startKey, string $endKey, bool $keyOnly, string $columnFamily): array
    {
        $results = [];
        $cursor = $startKey;

        while (true) {
            $executor = $this->createRetryExecutor();
            $segments = $this->clipForwardSegments($cursor, $endKey);

            // A page spanning several regions keeps the #293 concurrent
            // fan-out; a single-region page takes the sequential path.
            $page = count($segments) > 1
                ? $this->scanSegmentsInParallel(
                    $executor,
                    $segments,
                    $endKey,
                    $this->scanPageSize,
                    $keyOnly,
                    $columnFamily,
                )
                : $this->scanSegmentsSequentially(
                    $executor,
                    $segments,
                    $this->scanPageSize,
                    $keyOnly,
                    $columnFamily,
                );

            if ($page === []) {
                break;
            }

            $this->assertWithinScanLimit(count($results) + count($page));
            array_push($results, ...$page);

            if (count($page) < $this->scanPageSize) {
                break;
            }

            $lastKey = $page[count($page) - 1]['key'];
            $cursor = $lastKey . "\x00";

            if ($endKey !== '' && strcmp($cursor, $endKey) >= 0) {
                break;
            }
        }

        return $results;
    }

    /**
     * Enumerate the regions covering the forward range and clip them to it.
     *
     * @return list<array{RegionInfo, string, string}>
     */
    private function clipForwardSegments(string $startKey, string $endKey): array
    {
        $regions = $this->resolveScanRegions($startKey, $endKey);
        $clipper = new RegionRangeClipper();

        /** @var list<array{RegionInfo, string, string}> $segments */
        $segments = iterator_to_array($clipper->clipForward($regions, $startKey, $endKey), false);

        return $segments;
    }

    /**
     * Reverse range scan over [endKey, startKey), returned in descending
     * order (`startKey` is the exclusive upper bound, `endKey` the lower).
     *
     * `limit = 0` means "the whole range": the scan pages internally at
     * {@see self::$scanPageSize} rows per RPC, moving the descending upper
     * bound down to the lowest key of each full page, and buffers every row
     * (issue #191). The buffer is capped by {@see self::$maxScanRows}; once
     * the buffered rows exceed it a {@see ScanLimitExceededException} is
     * thrown instead of silently truncating. Like the forward path, the cap
     * is checked per fetched page (page-granular), not per row.
     *
     * @return array<array{key: string, value: ?string}>
     */
    public function reverseScan(
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily = '',
    ): array {
        $limit = $this->validateScanLimit($limit);

        if ($limit === 0) {
            return $this->reverseScanUnbounded($startKey, $endKey, $keyOnly, $columnFamily);
        }

        return $this->reverseScanPage($startKey, $endKey, $limit, $keyOnly, $columnFamily);
    }

    /**
     * Page a `limit: 0` reverse scan to completion. After a full page the
     * exclusive upper bound becomes the page's lowest key, so the next page
     * reads strictly below it (equal keys cannot repeat — keys are unique).
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function reverseScanUnbounded(string $startKey, string $endKey, bool $keyOnly, string $columnFamily): array
    {
        $results = [];
        $upper = $startKey;

        while (true) {
            $page = $this->reverseScanPage($upper, $endKey, $this->scanPageSize, $keyOnly, $columnFamily);

            if ($page === []) {
                break;
            }

            $this->assertWithinScanLimit(count($results) + count($page));
            array_push($results, ...$page);

            if (count($page) < $this->scanPageSize) {
                break;
            }

            $lastKey = $page[count($page) - 1]['key'];
            if ($lastKey === '' || strcmp($lastKey, $upper) >= 0) {
                // Defensive: no forward progress would loop forever.
                break;
            }
            $upper = $lastKey;
        }

        return $results;
    }

    /**
     * Scan one bounded reverse page over the clipped regions. The wire range
     * is [endKey, startKey); enumerate it through the region cache like the
     * forward scan, then walk it in reverse.
     *
     * @param int $limit positive page budget (never the `0` "unbounded"
     *                   sentinel: the public API routes `limit: 0` to
     *                   {@see self::reverseScanUnbounded()}). Passing `0`
     *                   here would set no request limit and silently read an
     *                   unbounded region.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function reverseScanPage(
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        assert($limit > 0, 'reverseScanPage() requires a positive page budget; use reverseScanUnbounded() for limit 0');

        $executor = $this->createRetryExecutor();

        $regions = array_reverse($this->resolveScanRegions($endKey, $startKey));

        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipReverse($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
            $regionResults = $this->executeScanForRegion(
                $executor,
                $scanStart,
                $scanEnd,
                $remaining,
                $keyOnly,
                true,
                $columnFamily,
            );
            array_push($results, ...$regionResults);

            $remaining -= count($regionResults);
            if ($remaining <= 0) {
                break;
            }
        }

        return $results;
    }

    /**
     * Fail closed after a fetched page pushes the accumulated row count past
     * the configured {@see self::$maxScanRows} maximum (issue #191). Because
     * a page is fetched (and may be fanned out across regions) before this
     * runs, the observed `$rows` can exceed `maxScanRows` by up to one page.
     *
     * @throws ScanLimitExceededException
     */
    private function assertWithinScanLimit(int $rows): void
    {
        if ($rows > $this->maxScanRows) {
            throw new ScanLimitExceededException($this->maxScanRows, $rows);
        }
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function scanPrefix(string $prefix, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        return $this->scan($prefix, RawKvSplitter::calculatePrefixEndKey($prefix), $limit, $keyOnly, $columnFamily);
    }

    /**
     * Enumerate the regions covering the forward key range [startKey, endKey).
     *
     * The region cache is consulted first: when it already holds the complete
     * chain the range is served locally, so a paginated scan issues no PD RPC
     * per page (issue #293). Only an incomplete chain — a cold or partially
     * warm cache, or a gap — falls back to a single PD scanRegions() call
     * whose result is then cached.
     *
     * @return list<RegionInfo>
     */
    private function resolveScanRegions(string $startKey, string $endKey): array
    {
        $cached = $this->regionCache->getRegionsInRange($startKey, $endKey);
        if ($cached !== []) {
            return $cached;
        }

        $regions = array_values($this->pdClient->scanRegions($startKey, $endKey, 0));
        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

        return $regions;
    }

    /**
     * Scan the given clipped sub-ranges one after another, subtracting each
     * region's yield from the remaining limit.
     *
     * @param list<array{RegionInfo, string, string}> $segments
     * @return array<array{key: string, value: ?string}>
     */
    private function scanSegmentsSequentially(
        RetryExecutor $executor,
        array $segments,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        $results = [];
        $remaining = $limit;

        foreach ($segments as [, $scanStart, $scanEnd]) {
            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion(
                $executor,
                $scanStart,
                $scanEnd,
                $regionLimit,
                $keyOnly,
                false,
                $columnFamily,
            );
            array_push($results, ...$regionResults);

            if ($remaining > 0) {
                $remaining -= count($regionResults);
                if ($remaining <= 0) {
                    break;
                }
            }
        }

        return $results;
    }

    /**
     * Scan the clipped regions of an unbounded page concurrently, in bounded
     * windows of at most {@see self::$maxConcurrency} regions (issue #293).
     *
     * Within a window every region's RawScan send is issued before any
     * response is awaited, so the regions' server-side latencies overlap;
     * the window's responses are concatenated in region order and charged
     * against the page budget. A further window is never dispatched once the
     * budget is spent (the loop is gated on the remaining budget) and every
     * region is given the *remaining* budget, so the fan-out holds at most
     * `maxConcurrency x limit` rows. Dispatch is still windowed by
     * {@see BatchAsyncExecutor}, so the previous unbounded "every region
     * with the full limit" behaviour is gone. Region ranges are disjoint, so
     * restoring the order after the fact is lossless.
     *
     * Region and transport errors are not retried inside a window (the sends
     * are awaited outside a per-region retry loop): the whole page falls back
     * to {@see self::scanSegmentsSequentially()} — which re-resolves and
     * retries properly — on any failure, on a response that is not a
     * {@see RawScanResponse}, or on a non-final region that shrank after
     * enumeration (which would leave a hole before the next segment). Reads
     * are idempotent, so discarding a partial concurrent result is safe.
     *
     * @param list<array{RegionInfo, string, string}> $segments
     * @return array<array{key: string, value: ?string}>
     */
    private function scanSegmentsInParallel(
        RetryExecutor $executor,
        array $segments,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        $results = [];
        $remaining = $limit;
        $segmentCount = count($segments);
        $lastIndex = -1;
        $lastFreshEnd = '';

        for ($offset = 0; $offset < $segmentCount && $remaining > 0; $offset += $this->maxConcurrency) {
            /** @var array<int, array{RegionInfo, string, string}> $window */
            $window = array_slice($segments, $offset, $this->maxConcurrency, true);

            /** @var array<int, string> $freshEnds */
            $freshEnds = [];
            $calls = [];
            foreach ($window as $index => [, $scanStart, $scanEnd]) {
                $freshEnds[$index] = '';
                $calls[$index] = function () use (
                    $executor,
                    $scanStart,
                    $scanEnd,
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                    &$freshEnds,
                    $index,
                ): CheckedGrpcFuture {
                    return $this->sendSubRangeScanAsync(
                        $executor,
                        $scanStart,
                        $scanEnd,
                        $remaining,
                        $keyOnly,
                        $columnFamily,
                        false,
                        $freshEnds[$index],
                    );
                };
            }

            try {
                $responses = $this->createBatchExecutor()->executeParallelCapped($calls, $this->maxConcurrency);
            } catch (TiKvException) {
                return $this->scanSegmentsSequentially($executor, $segments, $limit, $keyOnly, $columnFamily);
            }

            ksort($responses);

            foreach ($responses as $index => $response) {
                if (!$response instanceof RawScanResponse || $response->getRegionError() !== null) {
                    return $this->scanSegmentsSequentially($executor, $segments, $limit, $keyOnly, $columnFamily);
                }

                $freshEnd = $freshEnds[$index] ?? '';
                $segmentEnd = $segments[$index][2];

                // A non-final region that shrank after enumeration (split)
                // leaves a hole [freshEnd, segmentEnd): the next dispatched
                // segment starts at segmentEnd, not at the fresh end key.
                // Fall back to the sequential path, which re-resolves the
                // region and continues past the split. The final segment is
                // handled by the continuation below.
                if (
                    $index !== $segmentCount - 1
                    && $freshEnd !== ''
                    && strcmp($freshEnd, $segmentEnd) < 0
                ) {
                    return $this->scanSegmentsSequentially($executor, $segments, $limit, $keyOnly, $columnFamily);
                }

                $batch = $this->parseScanPairs($response, $keyOnly);
                array_push($results, ...$batch);
                $remaining -= count($batch);
                $lastIndex = $index;
                $lastFreshEnd = $freshEnd;

                if ($remaining <= 0) {
                    break 2;
                }
            }
        }

        // Rare split-after-enumeration of the final region: it shrank, so
        // continue from its fresh end key instead of dropping the remainder.
        // Only when the final segment was actually consumed and the budget is
        // not spent; passing the real remaining budget (never 0, which the
        // downstream executeScanForRegion() would read as "unbounded") bounds
        // the continuation wire read.
        if (
            $lastIndex === $segmentCount - 1
            && $remaining > 0
            && $lastFreshEnd !== ''
            && strcmp($lastFreshEnd, $segments[$lastIndex][1]) > 0
            && ($endKey === '' || strcmp($lastFreshEnd, $endKey) < 0)
        ) {
            array_push(
                $results,
                ...$this->executeScanForRegion(
                    $executor,
                    $lastFreshEnd,
                    $endKey,
                    $remaining,
                    $keyOnly,
                    false,
                    $columnFamily,
                ),
            );
        }

        if (count($results) > $limit) {
            return array_slice($results, 0, $limit);
        }

        return $results;
    }

    /**
     * Scan multiple ranges concurrently.
     *
     * Each range's first RawScan send is issued at the wire layer before any
     * wait begins, so the ranges' server-side latencies overlap instead of
     * accumulating serially (issue #295). Ranges whose region enumeration
     * yields more than one sub-range fan out all of those sends too; after
     * the waits, a sub-range whose region returned a region error or shrank
     * after enumeration is re-run through the sequential retrying path, and
     * a split of the final sub-range continues from its fresh end key, so no
     * part of the range is dropped (issue #267 semantics).
     *
     * At most {@see self::$maxConcurrency} requests are in flight at any
     * moment, and the returned outer array preserves input range order.
     *
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     * @throws \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException When any range fails
     */
    public function batchScan(array $ranges, int $eachLimit, bool $keyOnly, string $columnFamily = ''): array
    {
        if ($ranges === []) {
            return [];
        }

        if ($eachLimit <= 0) {
            throw new InvalidArgumentException('eachLimit must be greater than 0');
        }

        if ($eachLimit > self::MAX_SCAN_LIMIT) {
            throw new InvalidArgumentException(sprintf(
                'eachLimit (%d) exceeds maximum allowed scan limit of %d',
                $eachLimit,
                self::MAX_SCAN_LIMIT,
            ));
        }

        $executor = $this->createRetryExecutor();
        $clipper = new RegionRangeClipper();

        $calls = [];
        foreach ($ranges as $index => $range) {
            [$startKey, $endKey] = $range;
            $calls[$index] = fn(): CheckedGrpcFuture => $this->scanRangeAsync(
                $executor,
                $clipper,
                $startKey,
                $endKey,
                $eachLimit,
                $keyOnly,
                $columnFamily,
            );
        }

        $executed = $this->createBatchExecutor()->executeParallelCapped($calls, $this->maxConcurrency);

        ksort($executed);

        $results = [];
        foreach ($executed as $perRange) {
            assert(is_array($perRange));
            /** @var array<array{key: string, value: ?string}> $perRange */
            $results[] = $perRange;
        }

        return $results;
    }

    public function scanIterator(
        string $startKey,
        string $endKey,
        int $batchSize,
        bool $keyOnly,
        string $columnFamily = '',
    ): ScanIterator {
        return new ScanIterator(
            $this->scan(...),
            $startKey,
            $endKey,
            $batchSize,
            $keyOnly,
            $columnFamily,
        );
    }

    public function scanPrefixIterator(
        string $prefix,
        int $batchSize,
        bool $keyOnly,
        string $columnFamily = '',
    ): ScanIterator {
        return new ScanIterator(
            $this->scan(...),
            $prefix,
            RawKvSplitter::calculatePrefixEndKey($prefix),
            $batchSize,
            $keyOnly,
            $columnFamily,
        );
    }

    /**
     * Scan one clipped sub-range, continuing past regions that were split
     * after the outer region enumeration so no part of the range is dropped.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        bool $reverse,
        string $columnFamily = '',
    ): array {
        if ($reverse) {
            return $this->executeReverseScanForSubRange(
                $executor,
                $startKey,
                $endKey,
                $limit,
                $keyOnly,
                $columnFamily,
            );
        }

        // Forward scan: iterate over the sub-range. Each iteration resolves
        // the region on every attempt so cache invalidation and leader
        // switching performed by the retry executor take effect (issue
        // #267). If a region split leaves part of the sub-range
        // un-consumed, continue from the fresh region's end key instead of
        // silently dropping the remainder.
        $results = [];
        $pending = $limit;
        $cursorStart = $startKey;

        while (true) {
            $freshEndKey = '';
            $excludedStore = null;
            $batch = $executor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $keyOnly,
                $columnFamily,
                &$excludedStore,
                &$freshEndKey,
            ): array {
                // Resolve the region on every attempt: a stale captured
                // region would otherwise reproduce the original error on
                // each retry.
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
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || strcmp($freshEndKey, $endKey) < 0)
                    ? $freshEndKey
                    : $endKey;

                $request = new RawScanRequest();
                $request->setContext($target->context);
                $request->setStartKey($cursorStart);
                if ($wireEndKey !== '') {
                    $request->setEndKey($wireEndKey);
                }
                if ($pending > 0) {
                    $request->setLimit($pending);
                }
                $request->setKeyOnly($keyOnly);
                $request->setReverse(false);
                if ($columnFamily !== '') {
                    $request->setCf($columnFamily);
                }

                try {
                    $response = $this->measure('scan', $cursorStart, fn(): RawScanResponse => $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'RawScan',
                        $request,
                        RawScanResponse::class,
                        $this->timeoutConfig->scanTimeoutMs,
                    ));
                    /** @var RawScanResponse $response */
                    RegionErrorHandler::check($response);
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        // The selected replica's applied index is behind:
                        // exclude it so the next attempt falls back to
                        // another replica or the leader (issue #421).
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $subResults = [];
                foreach ($response->getKvs() as $pair) {
                    $subResults[] = [
                        'key' => $pair->getKey(),
                        'value' => $keyOnly ? null : $pair->getValue(),
                    ];
                }

                return $subResults;
            });

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
                || strcmp($freshEndKey, $cursorStart) <= 0
                || ($endKey !== '' && strcmp($freshEndKey, $endKey) >= 0)
            ) {
                break;
            }
            $cursorStart = $freshEndKey;
        }

        return $results;
    }

    /**
     * Reverse-scan one clipped sub-range. The caller passes the original
     * sub-range as [startKey, endKey) == [upper bound, lower bound); the
     * wire request reads [endKey, startKey) in descending order.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function executeReverseScanForSubRange(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        // Reverse scans resolve on the sub-range end (the lower bound). The
        // wire start key can sit exactly on the region's end boundary,
        // where the cache lookup would miss and PD would answer with the
        // neighbouring region outside the sub-range, so the lower bound is
        // the safe resolution key.
        $resolutionKey = $endKey;
        $freshEndKey = '';
        $excludedStore = null;
        $batch = $executor->execute($resolutionKey, function () use (
            $startKey,
            $endKey,
            $resolutionKey,
            $limit,
            $keyOnly,
            $columnFamily,
            &$excludedStore,
            &$freshEndKey,
        ): array {
            // Resolve the region on every attempt: a stale captured region
            // would otherwise reproduce the original error on each retry.
            $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
            $target = RegionContextFactory::resolveTarget(
                $fresh,
                $this->replicaReadPolicy,
                $this->regionResolver->getStore(...),
                excludedStoreId: $excludedStore,
            );
            $excludedStore = null;
            $address = $this->regionResolver->resolveStoreAddress($target->storeId);
            $freshEndKey = $fresh->endKey;

            // After a split the fresh region is smaller: clip the wire
            // start (upper) key down to the fresh region's end.
            $wireStartKey = $startKey;
            if ($freshEndKey !== '' && strcmp($freshEndKey, $wireStartKey) < 0) {
                $wireStartKey = $freshEndKey;
            }

            $request = new RawScanRequest();
            $request->setContext($target->context);
            $request->setStartKey($wireStartKey);
            if ($endKey !== '') {
                $request->setEndKey($endKey);
            }
            if ($limit > 0) {
                $request->setLimit($limit);
            }
            $request->setKeyOnly($keyOnly);
            $request->setReverse(true);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            try {
                $response = $this->measure('scan', $startKey, fn(): RawScanResponse => $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'RawScan',
                    $request,
                    RawScanResponse::class,
                    $this->timeoutConfig->scanTimeoutMs,
                ));
                /** @var RawScanResponse $response */
                RegionErrorHandler::check($response);
            } catch (RegionException $e) {
                if ($e->errorKind === ErrorKind::DataIsNotReady) {
                    $excludedStore = $target->storeId;
                }
                throw $e;
            }

            $subResults = [];
            foreach ($response->getKvs() as $pair) {
                $subResults[] = [
                    'key' => $pair->getKey(),
                    'value' => $keyOnly ? null : $pair->getValue(),
                ];
            }

            return $subResults;
        });

        // If the fresh region covers only [endKey, freshEndKey), the higher
        // remainder [freshEndKey, startKey) belongs BEFORE this batch in the
        // reverse result order: scan it first, then trim the batch to the
        // remaining limit.
        if ($freshEndKey === '' || strcmp($freshEndKey, $startKey) >= 0) {
            return $batch;
        }

        $upper = $this->executeReverseScanForSubRange(
            $executor,
            $startKey,
            $freshEndKey,
            $limit,
            $keyOnly,
            $columnFamily,
        );
        $batchCapacity = $limit > 0 ? $limit - count($upper) : count($batch);
        if ($batchCapacity <= 0) {
            return $upper;
        }

        return [...$upper, ...array_slice($batch, 0, $batchCapacity)];
    }

    /**
     * Scan a single range asynchronously: resolve + clip the region set,
     * issue one RawScan send per enumerated sub-range (all before any wait),
     * and return an un-waited future whose waiter concatenates the segment
     * responses in key order, trims to the limit, and falls back to the
     * sequential retrying path for any sub-range whose region reported an
     * error or shrank after enumeration (a non-final shrunken region would
     * otherwise leave a hole before the next segment), plus a continuation
     * when the final sub-range under-delivers past its fresh end key.
     *
     * @return CheckedGrpcFuture resolving to array<array{key: string, value: ?string}>
     */
    private function scanRangeAsync(
        RetryExecutor $executor,
        RegionRangeClipper $clipper,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily = '',
    ): CheckedGrpcFuture {
        // Enumerate regions up-front (dispatch phase) so the cache is warm
        // for every sub-range send; this mirrors scan()'s outer loop.
        $regions = $this->resolveScanRegions($startKey, $endKey);

        $segments = [];
        $freshEnds = [];

        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
            /** @var string $freshEnd */
            $freshEnd = '';
            $future = $this->sendSubRangeScanAsync(
                $executor,
                $scanStart,
                $scanEnd,
                $limit,
                $keyOnly,
                $columnFamily,
                false,
                $freshEnd,
            );
            $segments[] = ['start' => $scanStart, 'end' => $scanEnd, 'future' => $future];
            $freshEnds[] = $freshEnd;
        }

        if ($segments === []) {
            return CheckedGrpcFuture::fromCallable(static fn(): array => []);
        }

        return CheckedGrpcFuture::fromCallable(
            function () use ($executor, $segments, $freshEnds, $endKey, $limit, $keyOnly, $columnFamily): array {
                $results = [];
                $remaining = $limit === 0 ? PHP_INT_MAX : $limit;
                $segmentCount = count($segments);

                $lastIndex = -1;
                foreach ($segments as $index => $segment) {
                    $response = $this->measure(
                        'scan',
                        $segment['start'],
                        fn(): mixed => $segment['future']->waitForExecutor(),
                    );
                    assert($response instanceof RawScanResponse);

                    // A non-final region that shrank after enumeration
                    // (split) leaves a hole [freshEnd, segmentEnd): the next
                    // dispatched segment starts at segmentEnd, not at the
                    // fresh end key, so the keys in between would be dropped.
                    $freshEnd = $freshEnds[$index] ?? '';
                    $shrank = $index !== $segmentCount - 1
                        && $freshEnd !== ''
                        && strcmp($freshEnd, $segment['end']) < 0;

                    if ($response->getRegionError() !== null || $shrank) {
                        // The region error or the shrink means the un-awaited
                        // send cannot be trusted to cover the sub-range, and
                        // batchScan awaits outside any per-range retry loop.
                        // Re-run the whole sub-range through the retrying
                        // sequential path instead of silently returning a
                        // partial result (issue #293; the unbounded fan-out
                        // guards the shrink the same way).
                        // executeScanForRegion() already continues past a
                        // split inside the segment, so clear the recorded
                        // fresh end to keep the outer continuation below from
                        // re-scanning the same keys.
                        $freshEnds[$index] = '';
                        $batch = $this->executeScanForRegion(
                            $executor,
                            $segment['start'],
                            $segment['end'],
                            $remaining === PHP_INT_MAX ? 0 : $remaining,
                            $keyOnly,
                            false,
                            $columnFamily,
                        );
                    } else {
                        $batch = $this->parseScanPairs($response, $keyOnly);
                    }

                    array_push($results, ...$batch);
                    $remaining -= count($batch);
                    $lastIndex = $index;

                    if ($remaining <= 0) {
                        break;
                    }
                }

                // Sequential fallback for the rare split-after-enumeration
                // case: the fresh end key of the last consumed segment still
                // sits inside the requested range, so continue from there.
                if ($remaining > 0 && $lastIndex === $segmentCount - 1) {
                    $cursorStart = $freshEnds[$lastIndex];
                    $segmentStart = $segments[$lastIndex]['start'];
                    // The cursor must have advanced past the segment start
                    // and still lie inside the requested range; otherwise the
                    // whole sub-range was covered by the dispatched segments.
                    if (
                        $cursorStart !== ''
                        && strcmp($cursorStart, $segmentStart) > 0
                        && ($endKey === '' || strcmp($cursorStart, $endKey) < 0)
                    ) {
                        $rest = $this->executeScanForRegion(
                            $executor,
                            $cursorStart,
                            $endKey,
                            $remaining === PHP_INT_MAX ? 0 : $remaining,
                            $keyOnly,
                            false,
                            $columnFamily,
                        );
                        array_push($results, ...$rest);
                        $remaining -= count($rest);
                    }
                }

                if ($limit > 0 && count($results) > $limit) {
                    return array_slice($results, 0, $limit);
                }

                return $results;
            },
        );
    }

    /**
     * Issue one RawScan send for a single sub-range and return an un-waited
     * future. The fresh region's end key (needed by the caller for split
     * detection) is written back through the by-reference parameter during
     * dispatch.
     *
     * @param bool $reverse true scans [startKey, endKey) descending
     * @param-out string $freshEndKey
     */
    private function sendSubRangeScanAsync(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
        bool $reverse,
        ?string &$freshEndKey,
    ): CheckedGrpcFuture {
        $freshEndKey = '';
        // The resolution key mirrors the sequential paths: forward scans
        // resolve on the sub-range start, reverse scans on its end (the
        // lower bound), see executeReverseScanForSubRange().
        $resolutionKey = $reverse ? $endKey : $startKey;

        /** @var CheckedGrpcFuture $future */
        $future = $executor->execute($resolutionKey, function () use (
            $startKey,
            $endKey,
            $limit,
            $keyOnly,
            $columnFamily,
            $reverse,
            $resolutionKey,
            &$freshEndKey,
        ): CheckedGrpcFuture {
            // Resolve the region on every attempt so retries pick up cache
            // invalidation and leader switching (issue #267).
            $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
            $target = RegionContextFactory::resolveTarget(
                $fresh,
                $this->replicaReadPolicy,
                $this->regionResolver->getStore(...),
            );
            $address = $this->regionResolver->resolveStoreAddress($target->storeId);
            $freshEndKey = $fresh->endKey;

            if ($reverse) {
                // Clip the wire start (upper) key down to the fresh region's
                // end after a split; the wire reads [endKey, startKey).
                $wireStartKey = $startKey;
                if ($freshEndKey !== '' && strcmp($freshEndKey, $wireStartKey) < 0) {
                    $wireStartKey = $freshEndKey;
                }
                $request = new RawScanRequest();
                $request->setContext($target->context);
                $request->setStartKey($wireStartKey);
                if ($endKey !== '') {
                    $request->setEndKey($endKey);
                }
                $request->setReverse(true);
            } else {
                // Re-clip against the freshly resolved region: TiKV rejects
                // ranges that cross region boundaries.
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || strcmp($freshEndKey, $endKey) < 0)
                    ? $freshEndKey
                    : $endKey;
                $request = new RawScanRequest();
                $request->setContext($target->context);
                $request->setStartKey($startKey);
                if ($wireEndKey !== '') {
                    $request->setEndKey($wireEndKey);
                }
                $request->setReverse(false);
            }

            // The per-segment limit is the full range limit: segments are
            // disjoint, so at most one of them can return more rows than
            // needed and the waiter trims the concatenation to the limit.
            $request->setLimit($limit);
            $request->setKeyOnly($keyOnly);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            $response = $this->grpc->callAsync(
                $address,
                'tikvpb.Tikv',
                'RawScan',
                $request,
                RawScanResponse::class,
                $this->timeoutConfig->scanTimeoutMs,
            );

            return CheckedGrpcFuture::fromCallable(
                fn(): Message => $this->measure(
                    'scan',
                    $resolutionKey,
                    fn(): Message => $response->wait(),
                ),
            );
        });

        return $future;
    }

    /**
     * Map a RawScanResponse to plain key/value pairs.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function parseScanPairs(RawScanResponse $response, bool $keyOnly): array
    {
        $pairs = [];
        foreach ($response->getKvs() as $pair) {
            $pairs[] = [
                'key' => $pair->getKey(),
                'value' => $keyOnly ? null : $pair->getValue(),
            ];
        }

        return $pairs;
    }

    private function createBatchExecutor(): BatchAsyncExecutor
    {
        return new BatchAsyncExecutor($this->logger);
    }

    /**
     * Validate a caller-supplied scan limit and return it unchanged.
     *
     * `0` is preserved (it means "the whole range"; the caller paginates
     * internally — issue #191), while an explicit limit above the per-RPC
     * {@see self::MAX_SCAN_LIMIT} is rejected because one RawScan RPC cannot
     * exceed it.
     */
    private function validateScanLimit(int $limit): int
    {
        if ($limit < 0) {
            throw new InvalidArgumentException('Scan limit must be 0 or greater');
        }

        if ($limit > self::MAX_SCAN_LIMIT) {
            throw new InvalidArgumentException(sprintf(
                'Scan limit (%d) exceeds maximum allowed scan limit of %d',
                $limit,
                self::MAX_SCAN_LIMIT,
            ));
        }

        return $limit;
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
            deadlineMs: $this->retryDeadlineMs,
        );
    }

    /**
     * Measure the execution time of a callable and log a warning if it
     * exceeds the configured threshold for the given operation type.
     *
     * @template T
     * @param callable(): T $fn
     * @return T
     */
    private function measure(string $operation, string $key, callable $fn): mixed
    {
        if (!$this->slowLogConfig instanceof SlowLogConfig) {
            return $fn();
        }

        $threshold = $this->slowLogConfig->getThreshold($operation);
        if ($threshold <= 0) {
            return $fn();
        }

        $start = hrtime(true);
        try {
            return $fn();
        } finally {
            $durationMs = (hrtime(true) - $start) / 1_000_000;
            if ($durationMs > $threshold) {
                $this->logger->warning('Slow TiKV operation', [
                    'operation' => $operation,
                    'key' => \CrazyGoat\TiKV\Client\Util\KeyRedactor::redact($key),
                    'duration_ms' => round($durationMs, 2),
                    'threshold_ms' => $threshold,
                ]);
            }
        }
    }
}
