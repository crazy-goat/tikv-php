<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use Closure;
use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\BatchCallId;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\RegionScanBatch;
use CrazyGoat\TiKV\Client\Region\RegionScanSource;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
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
        private MetricsInterface $metrics = new NoOpMetrics(),
        /** Optional Context control fields applied to every request (issue #426). */
        private ?RequestContextOptions $requestContext = null,
        /** Resource-control metadata carried on every request (issue #427). */
        private ResourceControl $resourceControl = new ResourceControl(),
        /**
         * Client-level retry classifier (issue #719); null keeps the
         * built-in classification.
         *
         * @var (\Closure(TiKvException): \CrazyGoat\TiKV\Client\Retry\RetryVerdict)|null
         */
        private ?Closure $retryClassifier = null,
        /**
         * Owner lifecycle check invoked before every unbounded scan page
         * (issue #783); null disables the re-check. RawKvClient passes its
         * ensureOpen() so a close() from a callback during a `limit: 0`
         * scan stops the next RawScan page with ClientClosedException
         * instead of opening it on a closed transport.
         *
         * @var (Closure(): void)|null
         */
        private ?Closure $ensureOpen = null,
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
     * {@see RawKvScanner::$scanPageSize} rows per RPC and buffers every row, so the
     * documented unbounded contract holds for ranges larger than one TiKV
     * page (issue #191). The accumulated buffer is capped by
     * {@see RawKvScanner::$maxScanRows}; once the buffered rows exceed it a
     * {@see ScanLimitExceededException} is thrown instead of silently
     * truncating. The cap is checked per fetched page (page-granular), not per
     * row, so a page may be buffered in full before the throw.
     *
     * A bounded scan (`limit > 0`) returns at most `limit` rows, so a result
     * whose count equals the limit may be truncated (issue #440). The
     * unbounded, constant-memory alternative is {@see self::scanIterator()}.
     *
     * A reversed range (`startKey` sorts after a non-empty `endKey`) is
     * rejected before any RPC.
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException When the range is reversed
     * @throws RetryBudgetExhaustedException When the shared retry budget is
     *         exhausted
     * @throws TiKvException When a region lookup fails closed or the scan RPC
     *         fails
     */
    public function scan(string $startKey, string $endKey, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        RegionRangeClipper::assertForwardRangeOrder($startKey, $endKey, 'scan');
        $limit = $this->validateScanLimit($limit);

        // One logical scan shares one retry budget across its pages and
        // regions; a later scan() call starts a fresh one (issue #223).
        $executor = $this->createRetryExecutor()->beginOperation();

        if ($limit === 0) {
            return $this->scanUnbounded($executor, $startKey, $endKey, $keyOnly, $columnFamily);
        }

        return $this->scanRangeSequentially($executor, $startKey, $endKey, $limit, $keyOnly, $columnFamily);
    }

    /**
     * Page a `limit: 0` forward scan to completion, advancing the cursor past
     * the last key of each full page (mirroring {@see ScanIterator}'s
     * {@see KeyOrder::successor()} continuation) and stopping on the first
     * short or empty page. Rows are buffered and guarded by
     * {@see RawKvScanner::$maxScanRows}.
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function scanUnbounded(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        $results = [];
        $cursor = $startKey;

        while (true) {
            // Re-check the owner's lifecycle before fetching a new page: a
            // close() from a callback during the previous page must stop the
            // scan here instead of opening another RawScan on a closed
            // transport (issue #783).
            if ($this->ensureOpen instanceof \Closure) {
                ($this->ensureOpen)();
            }

            $page = $this->scanPage($executor, $cursor, $endKey, $this->scanPageSize, $keyOnly, $columnFamily);

            if ($page === []) {
                break;
            }

            $this->assertWithinScanLimit(count($results) + count($page));
            foreach ($page as $row) {
                $results[] = $row;
            }

            if (count($page) < $this->scanPageSize) {
                break;
            }

            $lastKey = $page[count($page) - 1]['key'];
            $cursor = KeyOrder::successor($lastKey);

            if ($endKey !== '' && KeyOrder::gte($cursor, $endKey)) {
                break;
            }
        }

        return $results;
    }

    /**
     * Scan one page of at most $limit rows, starting at $cursor.
     *
     * The regions covering the range are loaded in bounded batches (issue
     * #282) and the page ends as soon as its row budget is spent, so a page
     * that spans several regions costs one PD round trip per batch instead
     * of one round trip for the whole keyspace. A batch covering several
     * regions keeps the #293 concurrent fan-out; a single-region batch
     * takes the sequential path.
     *
     * @param int $limit positive row budget
     * @return array<array{key: string, value: ?string}>
     */
    private function scanPage(
        RetryExecutor $executor,
        string $cursor,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        assert($limit > 0, 'scanPage() requires a positive row budget');

        $results = [];
        $remaining = $limit;
        $clipper = new RegionRangeClipper();

        foreach ($this->regionBatches($cursor, $endKey, RegionScanBatch::forRowBudget($limit)) as $batch) {
            /** @var list<array{RegionInfo, string, string}> $segments */
            $segments = iterator_to_array($clipper->clipForward($batch, $cursor, $endKey), false);
            if ($segments === []) {
                continue;
            }

            $batchResults = count($segments) > 1
                ? $this->scanSegmentsInParallel(
                    $executor,
                    $segments,
                    $segments[count($segments) - 1][2],
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                )
                : $this->scanSegmentsSequentially(
                    $executor,
                    $segments,
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                );

            foreach ($batchResults as $row) {
                $results[] = $row;
            }

            $remaining -= count($batchResults);
            if ($remaining <= 0) {
                break;
            }
        }

        return $results;
    }

    /**
     * Scan the forward range [startKey, endKey), one clipped sub-range after
     * another, subtracting each region's yield from the remaining limit.
     *
     * The regions are loaded in bounded batches (issue #282): a scan whose
     * row limit is satisfied by the first region asks PD for one batch of
     * regions and stops there, instead of asking for every region covering
     * the range.
     *
     * @param int $limit positive row budget
     * @return array<array{key: string, value: ?string}>
     */
    private function scanRangeSequentially(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        assert($limit > 0, 'scanRangeSequentially() requires a positive row budget');

        $results = [];
        $remaining = $limit;
        $clipper = new RegionRangeClipper();

        foreach ($this->regionBatches($startKey, $endKey, RegionScanBatch::forRowBudget($limit)) as $batch) {
            /** @var list<array{RegionInfo, string, string}> $segments */
            $segments = iterator_to_array($clipper->clipForward($batch, $startKey, $endKey), false);
            if ($segments === []) {
                continue;
            }

            $batchResults = $this->scanSegmentsSequentially($executor, $segments, $remaining, $keyOnly, $columnFamily);
            foreach ($batchResults as $row) {
                $results[] = $row;
            }

            $remaining -= count($batchResults);
            if ($remaining <= 0) {
                return $results;
            }
        }

        return $results;
    }

    /**
     * Enumerate the regions covering the key range [startKey, endKey) in
     * bounded batches.
     *
     * Delegates to {@see RegionScanSource}, which serves contiguous cached
     * runs locally and asks PD only for the uncovered gap at the cursor
     * (issue #425). The batches are yielded lazily, so a consumer satisfied
     * by the first batch never triggers the next PD call (issue #282/#293).
     *
     * @param int $batchSize number of regions to request per PD call
     * @return \Generator<int, list<RegionInfo>>
     */
    private function regionBatches(string $startKey, string $endKey, int $batchSize): \Generator
    {
        $source = new RegionScanSource($this->pdClient, $this->regionCache, $this->metrics);

        yield from $source->batches($startKey, $endKey, $batchSize);
    }

    /**
     * Reverse range scan over [endKey, startKey), returned in descending
     * order (`startKey` is the exclusive upper bound, `endKey` the lower).
     *
     * `limit = 0` means "the whole range": the scan pages internally at
     * {@see RawKvScanner::$scanPageSize} rows per RPC, moving the descending upper
     * bound down to the lowest key of each full page, and buffers every row
     * (issue #191). The buffer is capped by {@see RawKvScanner::$maxScanRows}; once
     * the buffered rows exceed it a {@see ScanLimitExceededException} is
     * thrown instead of silently truncating. Like the forward path, the cap
     * is checked per fetched page (page-granular), not per row.
     *
     * A bounded scan (`limit > 0`) returns at most `limit` rows, so a result
     * whose count equals the limit may be truncated (issue #440). There is
     * no reverse iterator; the unbounded, constant-memory alternative for a
     * forward range is {@see self::scanIterator()}.
     *
     * An unbounded reverse scan fans the per-region reads of a page out
     * concurrently, in windows of at most {@see RawKvScanner::$maxConcurrency}
     * regions (issue #555); a bounded scan stays sequential.
     *
     * Regions are resolved from the upper bound downwards in bounded batches
     * (issue #198): a scan satisfied by the first batch never asks PD for the
     * rest of the range.
     *
     * A reversed range (`startKey` sorts before a non-empty `endKey`) is
     * rejected before any RPC. Reverse scan has no +infinity sentinel: an
     * empty `startKey` is not an unbounded upper bound, so
     * `reverseScan('', $lower)` with a non-empty lower bound throws
     * `InvalidArgumentException` instead of issuing a `RawScan` whose empty
     * `start_key` would return nothing (RAW-09, #194). An empty `endKey` is
     * the start of the keyspace (not +infinity), so `reverseScan('', '')` is
     * the empty range.
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException When the range is reversed
     */
    public function reverseScan(
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily = '',
    ): array {
        RegionRangeClipper::assertReverseRangeOrder($startKey, $endKey, 'reverseScan');
        $limit = $this->validateScanLimit($limit);

        // [end, start) == ['', '') is empty: an empty start key is the
        // keyspace minimum, not an unbounded upper bound. Return before the
        // region enumeration — the clipper cannot skip this case because its
        // empty-end sentinel also marks a region whose end is unbounded.
        // A non-empty end key with an empty start key is a reversed range and
        // was rejected by assertReverseRangeOrder() above. Equal non-empty
        // bounds are the same empty range: skip the region lookup entirely
        // instead of resolving a region that cannot yield a row.
        if ($startKey === $endKey) {
            return [];
        }

        // One logical reverse scan shares one retry budget across its pages
        // and regions; a later reverseScan() call starts a fresh one
        // (issue #223).
        $executor = $this->createRetryExecutor()->beginOperation();

        if ($limit === 0) {
            return $this->reverseScanUnbounded($executor, $startKey, $endKey, $keyOnly, $columnFamily);
        }

        return $this->reverseScanPage($executor, $startKey, $endKey, $limit, $keyOnly, $columnFamily);
    }

    /**
     * Page a `limit: 0` reverse scan to completion. After a full page the
     * exclusive upper bound becomes the page's lowest key, so the next page
     * reads strictly below it (equal keys cannot repeat — keys are unique).
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function reverseScanUnbounded(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        bool $keyOnly,
        string $columnFamily,
    ): array {
        $results = [];
        $upper = $startKey;

        while (true) {
            $page = $this->reverseScanPage(
                $executor,
                $upper,
                $endKey,
                $this->scanPageSize,
                $keyOnly,
                $columnFamily,
                parallel: true,
            );

            if ($page === []) {
                break;
            }

            $this->assertWithinScanLimit(count($results) + count($page));
            foreach ($page as $row) {
                $results[] = $row;
            }

            if (count($page) < $this->scanPageSize) {
                break;
            }

            $lastKey = $page[count($page) - 1]['key'];
            if ($lastKey === '' || KeyOrder::gte($lastKey, $upper)) {
                // Defensive: no forward progress would loop forever.
                break;
            }
            $upper = $lastKey;
        }

        return $results;
    }

    /**
     * Scan one reverse page over the clipped regions. The wire range is
     * [endKey, startKey); enumerate it from the upper bound downwards, then
     * walk the regions in descending order.
     *
     * PD's `ScanRegions` enumerates a range from its **lower** bound only, so
     * the reverse scan used to collect the whole chain before walking it from
     * the top. The regions are now loaded lazily from the top in bounded
     * batches (issue #198): a page satisfied by its first batch never asks
     * for the next one, and a narrow reverse scan touches only the regions it
     * can consume instead of every region of the remaining range.
     *
     * A bounded reverse scan walks the clipped segments one after another.
     * An unbounded page (`$parallel`) fans them out like the forward
     * {@see self::scanSegmentsInParallel()} (issue #555): the sends of one
     * window are all issued before any response is awaited, and the
     * descending segment order is restored before the rows are concatenated.
     *
     * @param int $limit positive page budget (never the `0` "unbounded"
     *                   sentinel: the public API routes `limit: 0` to
     *                   {@see self::reverseScanUnbounded()}). Passing `0`
     *                   here would set no request limit and silently read an
     *                   unbounded region.
     * @param bool $parallel fan the page's segments out concurrently; only
     *                       the unbounded page loop sets this, the bounded
     *                       path stays sequential
     *
     * @return array<array{key: string, value: ?string}>
     */
    private function reverseScanPage(
        RetryExecutor $executor,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
        bool $parallel = false,
    ): array {
        assert($limit > 0, 'reverseScanPage() requires a positive page budget; use reverseScanUnbounded() for limit 0');

        if ($startKey === $endKey) {
            // An empty page: the unbounded loop can land exactly on the lower
            // bound after a full page, and there is nothing left to resolve.
            return [];
        }

        $results = [];
        $remaining = $limit;
        $clipper = new RegionRangeClipper();

        foreach ($this->reverseRegionBatches($startKey, $endKey, RegionScanBatch::forRowBudget($limit)) as $batch) {
            /** @var list<array{RegionInfo, string, string}> $segments */
            $segments = iterator_to_array($clipper->clipReverse($batch, $startKey, $endKey), false);
            if ($segments === []) {
                continue;
            }

            $batchResults = $parallel && count($segments) > 1
                ? $this->scanSegmentsInParallel(
                    $executor,
                    $segments,
                    $endKey,
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                    true,
                )
                : $this->scanSegmentsSequentially(
                    $executor,
                    $segments,
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                    true,
                );

            foreach ($batchResults as $row) {
                $results[] = $row;
            }

            $remaining -= count($batchResults);
            if ($remaining <= 0) {
                break;
            }
        }

        return $results;
    }

    /**
     * Enumerate the regions covering the reverse range [endKey, startKey) in
     * bounded batches, from the upper bound downwards (issue #198).
     *
     * A reverse scan consumes a range from its upper bound, so the regions
     * are resolved from the top: the cache answers the common case and a miss
     * asks PD for the region containing the cursor, or — when the cursor sits
     * on a region boundary — for the adjacent region below through
     * `GetPrevRegion`. A batch is fetched only when the previous one did not
     * spend the row budget, so a scan that fits in the first batch never
     * enumerates the rest of the range.
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
     * Fail closed after a fetched page pushes the accumulated row count past
     * the configured {@see RawKvScanner::$maxScanRows} maximum (issue #191). Because
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
     * Prefix scan over [prefix, prefix-end).
     *
     * A convenience wrapper around {@see self::scan()}, so it shares that
     * method's caps: `limit = 0` returns the whole prefix (paginated
     * internally, guarded by {@see RawKvScanner::$maxScanRows}, which throws
     * {@see ScanLimitExceededException} instead of truncating), and a bounded
     * scan returns at most `limit` rows (issue #440). A negative `limit` is
     * rejected before any RPC (see {@see self::validateScanLimit()}). The
     * unbounded, constant-memory alternative is
     * {@see self::scanPrefixIterator()}.
     *
     * @return array<array{key: string, value: ?string}>
     */
    public function scanPrefix(string $prefix, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        return $this->scan($prefix, RawKvSplitter::calculatePrefixEndKey($prefix), $limit, $keyOnly, $columnFamily);
    }

    /**
     * Collect the regions covering the key range [startKey, endKey).
     *
     * The region cache is consulted first: when it already holds the complete
     * chain the range is served locally, so a paginated scan issues no PD RPC
     * per page (issue #293). Only an incomplete chain — a cold or partially
     * warm cache, or a gap — falls back to PD, and then only $batchSize
     * regions at a time (issue #282).
     *
     * The list itself is complete: the concurrent batch scan dispatches one
     * send per region. The reverse scan no longer uses this method — it walks
     * the range from the top down through {@see self::reverseRegionBatches()}
     * (issue #198).
     *
     * @param int $batchSize number of regions to request per PD call
     * @return list<RegionInfo>
     */
    private function resolveScanRegions(string $startKey, string $endKey, int $batchSize): array
    {
        $regions = [];
        foreach ($this->regionBatches($startKey, $endKey, $batchSize) as $batch) {
            foreach ($batch as $region) {
                $regions[] = $region;
            }
        }

        return $regions;
    }

    /**
     * Scan the given clipped sub-ranges one after another, subtracting each
     * region's yield from the remaining limit.
     *
     * This is the bounded forward and reverse path, and the fallback of the
     * concurrent page fan-out when a region error, a transport error or a
     * split after enumeration makes the dispatched sends untrustworthy.
     *
     * @param list<array{RegionInfo, string, string}> $segments
     * @param int $limit positive row budget
     * @param bool $reverse true scans each segment [start, end) descending
     * @return array<array{key: string, value: ?string}>
     */
    private function scanSegmentsSequentially(
        RetryExecutor $executor,
        array $segments,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
        bool $reverse = false,
    ): array {
        assert($limit > 0, 'scanSegmentsSequentially() requires a positive row budget');

        $results = [];
        $remaining = $limit;

        foreach ($segments as [, $scanStart, $scanEnd]) {
            $regionResults = $this->executeScanForRegion(
                $executor,
                $scanStart,
                $scanEnd,
                $remaining,
                $keyOnly,
                $reverse,
                $columnFamily,
            );
            foreach ($regionResults as $row) {
                $results[] = $row;
            }

            $remaining -= count($regionResults);
            if ($remaining <= 0) {
                break;
            }
        }

        return $results;
    }

    /**
     * Scan the clipped regions of an unbounded page concurrently, in bounded
     * windows of at most {@see RawKvScanner::$maxConcurrency} regions (issues #293
     * and #555).
     *
     * Within a window every region's RawScan send is issued before any
     * response is awaited, so the regions' server-side latencies overlap;
     * the window's responses are concatenated in region order (descending
     * for a reverse scan) and charged against the page budget. A further
     * window is never dispatched once the budget is spent (the loop is
     * gated on the remaining budget) and every region is given the
     * *remaining* budget, so the fan-out holds at most
     * `maxConcurrency x limit` rows. Dispatch is still windowed by
     * {@see BatchAsyncExecutor}, so the previous unbounded "every region
     * with the full limit" behaviour is gone. Region ranges are disjoint, so
     * restoring the order after the fact is lossless.
     *
     * Region and transport errors are not retried inside a window (the sends
     * are awaited outside a per-region retry loop): the whole page falls back
     * to {@see self::scanSegmentsSequentially()} — which re-resolves and
     * retries properly — on any failure, on a response that is not a
     * {@see RawScanResponse}, or on a region that shrank after enumeration
     * (which would leave a hole in the page). Reads are idempotent, so
     * discarding a partial concurrent result is safe. A forward scan detects
     * a shrink on every segment but the final one (the final one has a
     * continuation below); a reverse scan detects it on every segment,
     * because the missing part lies above the segment and only the
     * sequential reverse path can splice it back in order.
     *
     * The batch deadline spans all windows. A {@see BatchDeadlineExceededException}
     * is never a reason to fall back (the sequential path has no deadline): it
     * is rethrown with the configured deadline.
     *
     * @param list<array{RegionInfo, string, string}> $segments
     *        clipped sub-ranges in scan order (ascending for forward,
     *        descending for reverse)
     * @param string $rangeEnd exclusive end of the forward range the segments
     *        cover; unused by a reverse scan, whose split continuation lives
     *        in {@see self::executeReverseScanForSubRange()}
     * @param bool $reverse true scans each segment [start, end) descending
     * @return array<array{key: string, value: ?string}>
     */
    private function scanSegmentsInParallel(
        RetryExecutor $executor,
        array $segments,
        string $rangeEnd,
        int $limit,
        bool $keyOnly,
        string $columnFamily,
        bool $reverse = false,
    ): array {
        $results = [];
        $remaining = $limit;
        $segmentCount = count($segments);
        $lastIndex = -1;
        $lastFreshEnd = '';
        $deadlineMs = $this->timeoutConfig->batchDeadlineMs;
        $startTimeMs = (int) (microtime(true) * 1000);

        for ($offset = 0; $offset < $segmentCount && $remaining > 0; $offset += $this->maxConcurrency) {
            // One batch deadline for ALL windows (issue #275): each window gets
            // what is left of it, so N windows cannot take N times the deadline.
            $windowDeadlineMs = $deadlineMs;
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $deadlineMs) {
                    $dispatched = array_slice($segments, 0, $offset, true);
                    $pending = array_slice($segments, $offset, null, true);
                    throw new BatchDeadlineExceededException($deadlineMs, $elapsedMs, [
                        'dispatchedRegions' => array_keys($dispatched),
                        'pendingRegions' => array_keys($pending),
                        'dispatchedRegionIds' => $this->segmentRegionIds($dispatched),
                        'pendingRegionIds' => $this->segmentRegionIds($pending),
                    ]);
                }
                $windowDeadlineMs = $deadlineMs - $elapsedMs;
            }

            /** @var array<int, array{RegionInfo, string, string}> $window */
            $window = array_slice($segments, $offset, $this->maxConcurrency, true);

            /** @var array<int, string> $freshEnds */
            $freshEnds = [];
            $calls = [];
            /** @var array<int, BatchCallId> $labels */
            $labels = [];
            foreach ($window as $index => [$region, $scanStart, $scanEnd]) {
                $freshEnds[$index] = '';
                $calls[$index] = function () use (
                    $executor,
                    $scanStart,
                    $scanEnd,
                    $remaining,
                    $keyOnly,
                    $columnFamily,
                    $reverse,
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
                        $reverse,
                        $freshEnds[$index],
                    );
                };
                $labels[$index] = new BatchCallId($region->regionId);
            }

            try {
                $responses = $this->createBatchExecutor()->executeParallelCapped(
                    $calls,
                    $this->maxConcurrency,
                    $windowDeadlineMs,
                    $labels,
                );
            } catch (BatchDeadlineExceededException) {
                // An exhausted deadline is not a reason to fall back: the
                // sequential path has no deadline at all. Report the
                // configured deadline and the segments without a result.
                $dispatched = array_slice($segments, 0, $offset + count($window), true);
                $pending = array_slice($segments, $offset, null, true);
                throw new BatchDeadlineExceededException(
                    $deadlineMs,
                    (int) (microtime(true) * 1000) - $startTimeMs,
                    [
                        'dispatchedRegions' => array_keys($dispatched),
                        'pendingRegions' => array_keys($pending),
                        'dispatchedRegionIds' => $this->segmentRegionIds($dispatched),
                        'pendingRegionIds' => $this->segmentRegionIds($pending),
                    ],
                );
            } catch (TiKvException) {
                return $this->scanSegmentsSequentially(
                    $executor,
                    $segments,
                    $limit,
                    $keyOnly,
                    $columnFamily,
                    $reverse,
                );
            }

            ksort($responses);

            foreach ($responses as $index => $response) {
                if (!$response instanceof RawScanResponse || $response->getRegionError() !== null) {
                    return $this->scanSegmentsSequentially(
                        $executor,
                        $segments,
                        $limit,
                        $keyOnly,
                        $columnFamily,
                        $reverse,
                    );
                }

                $freshEnd = $freshEnds[$index] ?? '';
                $currentSegmentEnd = $segments[$index][2];

                if ($reverse) {
                    // A reverse segment shrank when the fresh region's end
                    // key dropped below the segment's upper bound: the send
                    // was clipped to [scanEnd, freshEnd), so the part above
                    // it would be dropped. Only the sequential reverse path
                    // can splice the missing part back in descending order
                    // (executeReverseScanForSubRange() continues past the
                    // split), so re-run the whole page through it.
                    if (
                        $freshEnd !== ''
                        && KeyOrder::lt($freshEnd, $segments[$index][1])
                    ) {
                        return $this->scanSegmentsSequentially(
                            $executor,
                            $segments,
                            $limit,
                            $keyOnly,
                            $columnFamily,
                            true,
                        );
                    }
                } elseif (
                    $index !== $segmentCount - 1
                    && $freshEnd !== ''
                    && KeyOrder::lt($freshEnd, $currentSegmentEnd)
                ) {
                    // A non-final forward region that shrank after enumeration
                    // (split) leaves a hole [freshEnd, segmentEnd): the next
                    // dispatched segment starts at segmentEnd, not at the
                    // fresh end key. Fall back to the sequential path, which
                    // re-resolves the region and continues past the split.
                    // The final segment is handled by the continuation below.
                    return $this->scanSegmentsSequentially(
                        $executor,
                        $segments,
                        $limit,
                        $keyOnly,
                        $columnFamily,
                        false,
                    );
                }

                $batch = $this->parseScanPairs($response, $keyOnly);
                foreach ($batch as $row) {
                    $results[] = $row;
                }
                $remaining -= count($batch);
                $lastIndex = $index;
                $lastFreshEnd = $freshEnd;

                if ($remaining <= 0) {
                    break 2;
                }
            }
        }

        // Rare split-after-enumeration of the final forward region: it shrank,
        // so continue from its fresh end key instead of dropping the
        // remainder. A reverse shrink is handled by the fallback above (its
        // missing part is above the segment, not below the range). Only when
        // the final segment was actually consumed and the budget is not
        // spent; passing the real remaining budget (never 0, which the
        // downstream executeScanForRegion() would read as "unbounded") bounds
        // the continuation wire read.
        if (
            !$reverse
            && $lastIndex === $segmentCount - 1
            && $remaining > 0
            && $lastFreshEnd !== ''
            && KeyOrder::gt($lastFreshEnd, $segments[$lastIndex][1])
            && ($rangeEnd === '' || KeyOrder::lt($lastFreshEnd, $rangeEnd))
        ) {
            $rest = $this->executeScanForRegion(
                $executor,
                $lastFreshEnd,
                $rangeEnd,
                $remaining,
                $keyOnly,
                false,
                $columnFamily,
            );
            foreach ($rest as $row) {
                $results[] = $row;
            }
        }

        if (count($results) > $limit) {
            return array_slice($results, 0, $limit);
        }

        return $results;
    }

    /**
     * Real TiKV region IDs covered by the given segment list, unique and
     * ascending (issue #285, GRPC-26). Segments are one per region, so the
     * IDs name the parts of the scan that were or were not dispatched.
     *
     * @param array<int, array{RegionInfo, string, string}> $segments
     * @return list<int>
     */
    private function segmentRegionIds(array $segments): array
    {
        $regionIds = [];
        foreach ($segments as [$region]) {
            $regionIds[$region->regionId] = true;
        }

        $regionIds = array_keys($regionIds);
        sort($regionIds);

        return $regionIds;
    }

    /**
     * Scan multiple non-contiguous ranges with one `RawBatchScan` request per
     * region (issue #433, GAP-19).
     *
     * The client groups the requested ranges by the regions that own them,
     * clips every range to each region's bounds, and sends one
     * `RawBatchScan` per region with all of that region's clipped ranges.
     * Requests to distinct regions are dispatched concurrently, bounded by
     * {@see RawKvScanner::$maxConcurrency} (issue #295), and share one batch deadline
     * ({@see TimeoutConfig::$batchDeadlineMs}). For a range list that spans
     * N regions the call therefore costs N store round trips instead of one
     * `RawScan` per range per region, which is the point of the RPC.
     *
     * `RawBatchScan` carries one `context` (one region), so a range that
     * spans several regions is split across those regions' requests. The
     * response is a flat `kvs` list without per-range boundaries; the client
     * restores the per-range grouping from the clipped bounds. That is only
     * unambiguous when the requested ranges are pairwise disjoint, so a call
     * with overlapping ranges keeps the per-range `RawScan` path
     * ({@see self::batchScanPerRange()}). The same fallback runs when a
     * region error, a transport failure or a missing store makes the
     * un-retried region fan-out untrustworthy: reads are idempotent, so the
     * whole call is re-run through the per-range path, which re-resolves and
     * retries each range with a fresh retry budget (issue #267 semantics).
     * This is a deliberate deviation from a strict regroup-and-retry: the
     * successful region responses are discarded and re-read.
     *
     * The batch deadline ({@see TimeoutConfig::$batchDeadlineMs}) covers
     * region enumeration as well as the store fan-out. The fallback shares
     * the remaining budget rather than starting a second full one, and an
     * exhausted budget is rethrown as
     * {@see BatchDeadlineExceededException}. A PD failure during region
     * enumeration is wrapped in
     * {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException},
     * matching the pre-#433 surface.
     *
     * `eachLimit` is required and must be in `1..MAX_SCAN_LIMIT`. Unlike
     * {@see self::scan()}, `0` is rejected instead of meaning "the whole
     * range", so one `batchScan()` call cannot omit the per-range cap or
     * request an unbounded result set. Empty bounds are still allowed:
     * `batchScan([['', '']], eachLimit: 100)` scans the whole keyspace and
     * returns at most 100 rows. Each range returns at most `eachLimit` rows;
     * a returned count equal to the limit may mean the range holds more rows
     * (issue #440). TiKV applies `each_limit` per region and per range, so a
     * range that spans several regions is trimmed to `eachLimit` after the
     * per-region responses are merged.
     *
     * Every range is validated before the first send: a reversed range
     * rejects the whole call with {@see InvalidArgumentException}.
     *
     * The result is one entry per range, in input order for a list; with
     * non-sequential input keys the outer array is re-indexed in ascending
     * key order. The shape is
     * `[[['key' => ..., 'value' => ...], ...], ...]`. Each inner list holds
     * at most `eachLimit` rows in ascending key order, and `value` is `null`
     * when `$keyOnly` is true. An empty `$ranges` list returns `[]`; a range
     * that matches no rows contributes an empty inner list.
     *
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     * @throws InvalidArgumentException When `eachLimit` is invalid, or a range
     *         is malformed or reversed
     * @throws \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException When any range fails
     * @throws BatchDeadlineExceededException When the fan-out exceeds
     *         `options['timeout']['batchDeadlineMs']`
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

        // Validate every range before the first RPC and keep the caller's
        // index => [startKey, endKey] mapping for the result shape.
        /** @var array<array-key, array{0: string, 1: string}> $normalized */
        $normalized = [];
        foreach ($ranges as $index => $range) {
            [$startKey, $endKey] = $this->assertBatchRange($range, $index);
            RegionRangeClipper::assertForwardRangeOrder($startKey, $endKey, 'batchScan');
            $normalized[$index] = [$startKey, $endKey];
        }

        // The batch deadline is one wall-clock budget for the whole call: it
        // covers region enumeration as well as the store fan-out, and the
        // fallback only gets what is left (issue #433 review).
        $batchDeadlineMs = $this->timeoutConfig->batchDeadlineMs;
        $startedAtMs = $batchDeadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;

        // A flat RawBatchScan response carries no per-range boundaries, so
        // the grouping can only be restored when the ranges are disjoint.
        if (!$this->rangesAreDisjoint($normalized)) {
            return $this->batchScanPerRange(
                $normalized,
                $eachLimit,
                $keyOnly,
                $columnFamily,
                $this->remainingBatchDeadlineMs($startedAtMs, $batchDeadlineMs),
            );
        }

        try {
            $groups = $this->groupBatchScanRangesByRegion($normalized, $startedAtMs, $batchDeadlineMs);
        } catch (BatchDeadlineExceededException $e) {
            throw $e;
        } catch (TiKvException $e) {
            // Region enumeration used to run inside the per-range callables,
            // where BatchAsyncExecutor wrapped a PD failure in
            // BatchPartialFailureException. Keep that documented surface
            // instead of leaking the base exception (issue #433 review).
            throw new BatchPartialFailureException([0 => $e], count($normalized));
        }

        if ($groups === []) {
            return $this->emptyBatchScanResult($normalized);
        }

        try {
            return $this->batchScanByRegion(
                $groups,
                $normalized,
                $eachLimit,
                $keyOnly,
                $columnFamily,
                $this->remainingBatchDeadlineMs($startedAtMs, $batchDeadlineMs),
            );
        } catch (BatchPartialFailureException) {
            // BatchDeadlineExceededException is deliberately not caught here:
            // the remaining budget is exhausted, so a fallback would throw
            // immediately; it propagates instead of starting a second budget.
            //
            // Deliberate deviation from a strict regroup-and-retry: instead
            // of regrouping only the failed regions and re-dispatching
            // RawBatchScan, re-run the whole call through the per-range
            // path. It re-resolves and retries every range with a fresh
            // retry budget and the remaining batch deadline. Reads are
            // idempotent, so discarding the successful region responses is
            // safe.
            return $this->batchScanPerRange(
                $normalized,
                $eachLimit,
                $keyOnly,
                $columnFamily,
                $this->remainingBatchDeadlineMs($startedAtMs, $batchDeadlineMs),
            );
        }
    }

    /**
     * Whether every non-empty range is pairwise disjoint.
     *
     * The `RawBatchScan` response is a flat `kvs` list, so a key that belongs
     * to two overlapping ranges cannot be attributed to one of them. Empty
     * ranges (`startKey === endKey` with a non-empty end) match no key and
     * are ignored.
     *
     * @param array<array-key, array{0: string, 1: string}> $ranges
     */
    private function rangesAreDisjoint(array $ranges): bool
    {
        /** @var list<array{0: string, 1: string}> $intervals */
        $intervals = [];
        foreach ($ranges as [$startKey, $endKey]) {
            if ($endKey !== '' && KeyOrder::eq($startKey, $endKey)) {
                continue;
            }
            $intervals[] = [$startKey, $endKey];
        }

        $count = count($intervals);
        if ($count < 2) {
            return true;
        }

        usort(
            $intervals,
            static fn(array $a, array $b): int => KeyOrder::cmp($a[0], $b[0]),
        );

        $maxEnd = $intervals[0][1];
        for ($i = 1; $i < $count; ++$i) {
            [$startKey, $endKey] = $intervals[$i];
            if ($maxEnd === '' || KeyOrder::gt($maxEnd, $startKey)) {
                return false;
            }
            if ($endKey === '' || KeyOrder::gt($endKey, $maxEnd)) {
                $maxEnd = $endKey;
            }
        }

        return true;
    }

    /**
     * Group every range's clipped sub-ranges by the region that owns them.
     *
     * Within a region the entries are sorted by start key. That keeps each
     * range's keys contiguous in the flat response and, crucially, puts a
     * clipped range with an empty end key (+infinity) last: TiKV reads an
     * empty end key on a non-final range as "up to the next range's start".
     *
     * The batch deadline is checked before every range's region enumeration,
     * so a cold cache cannot spend an unbounded number of PD round trips
     * before the store fan-out starts.
     *
     * @param array<array-key, array{0: string, 1: string}> $ranges
     * @return array<int, array{
     *     region: RegionInfo,
     *     ranges: list<array{index: int|string, start: string, end: string}>
     * }>
     */
    private function groupBatchScanRangesByRegion(array $ranges, int $startedAtMs, int $batchDeadlineMs): array
    {
        $clipper = new RegionRangeClipper();
        /** @var array<int, array{
         *     region: RegionInfo,
         *     ranges: list<array{index: int|string, start: string, end: string}>
         * }> $groups
         */
        $groups = [];

        foreach ($ranges as $index => [$startKey, $endKey]) {
            $this->assertBatchDeadlineNotExceeded($startedAtMs, $batchDeadlineMs);
            $regions = $this->resolveScanRegions($startKey, $endKey, RegionScanBatch::MAX_REGIONS);
            foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $scanStart, $scanEnd]) {
                $regionId = $region->regionId;
                $groups[$regionId] ??= ['region' => $region, 'ranges' => []];
                $groups[$regionId]['ranges'][] = [
                    'index' => $index,
                    'start' => $scanStart,
                    'end' => $scanEnd,
                ];
            }
        }

        foreach ($groups as $regionId => $group) {
            usort(
                $group['ranges'],
                static fn(array $a, array $b): int => KeyOrder::cmp($a['start'], $b['start']),
            );
            $groups[$regionId]['ranges'] = $group['ranges'];
        }

        return $groups;
    }

    /**
     * Dispatch one `RawBatchScan` per region and merge the flat responses
     * back into the caller's per-range result shape.
     *
     * Regions are dispatched in ascending start-key order, so merging their
     * responses in slot order yields ascending rows per range without a
     * sort. A range that spans several regions can return up to
     * `eachLimit` rows from each region (TiKV applies `each_limit` per
     * region), so each range is trimmed to `eachLimit` after the merge.
     *
     * @param array<int, array{
     *     region: RegionInfo,
     *     ranges: list<array{index: int|string, start: string, end: string}>
     * }> $groups
     * @param array<array-key, array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     *
     * @throws BatchPartialFailureException When a region send fails
     * @throws BatchDeadlineExceededException When the batch deadline expires
     */
    private function batchScanByRegion(
        array $groups,
        array $ranges,
        int $eachLimit,
        bool $keyOnly,
        string $columnFamily,
        int $batchDeadlineMs,
    ): array {
        $regionIds = array_keys($groups);
        usort(
            $regionIds,
            fn(int $a, int $b): int => KeyOrder::cmp(
                $groups[$a]['region']->startKey,
                $groups[$b]['region']->startKey,
            ),
        );

        $calls = [];
        /** @var array<int, BatchCallId> $labels */
        $labels = [];
        /** @var array<int, int> $slotToRegionId */
        $slotToRegionId = [];
        foreach ($regionIds as $regionId) {
            $slot = count($calls);
            $group = $groups[$regionId];
            $calls[$slot] = fn(): CheckedGrpcFuture => $this->sendBatchScanAsync(
                $group['region'],
                $group['ranges'],
                $eachLimit,
                $keyOnly,
                $columnFamily,
            );
            $labels[$slot] = new BatchCallId($regionId);
            $slotToRegionId[$slot] = $regionId;
        }

        $responses = $this->createBatchExecutor()->executeParallelCapped(
            $calls,
            $this->maxConcurrency,
            $batchDeadlineMs,
            $labels,
        );

        /** @var array<array-key, list<array{key: string, value: ?string}>> $results */
        $results = [];
        foreach ($ranges as $index => $_) {
            $results[$index] = [];
        }

        foreach ($responses as $slot => $response) {
            if (!$response instanceof RawBatchScanResponse) {
                throw new TiKvException('RawBatchScan returned an unexpected response type');
            }
            $this->collectBatchScanRows(
                $response,
                $groups[$slotToRegionId[$slot]]['ranges'],
                $eachLimit,
                $keyOnly,
                $results,
            );
        }

        // TiKV's each_limit is per region and per range; the documented
        // contract is a per-range cap, so trim the ascending merge.
        foreach ($results as $index => $rows) {
            if (count($rows) > $eachLimit) {
                $results[$index] = array_slice($rows, 0, $eachLimit);
            }
        }

        ksort($results);

        return array_values($results);
    }

    /**
     * Issue one `RawBatchScan` for a region's clipped ranges.
     *
     * @param list<array{index: int|string, start: string, end: string}> $groupRanges
     */
    private function sendBatchScanAsync(
        RegionInfo $region,
        array $groupRanges,
        int $eachLimit,
        bool $keyOnly,
        string $columnFamily,
    ): CheckedGrpcFuture {
        $target = RegionContextFactory::resolveTarget(
            $region,
            $this->replicaReadPolicy,
            $this->regionResolver->getStore(...),
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
            storeAvailable: $this->regionResolver->isStoreAvailable(...),
        );
        $address = $this->regionResolver->resolveStoreAddress($target->storeId);

        $request = new RawBatchScanRequest();
        $request->setContext($target->context);

        $protoRanges = [];
        foreach ($groupRanges as $entry) {
            $keyRange = new KeyRange();
            $keyRange->setStartKey($entry['start']);
            if ($entry['end'] !== '') {
                $keyRange->setEndKey($entry['end']);
            }
            $protoRanges[] = $keyRange;
        }
        $request->setRanges($protoRanges);
        $request->setEachLimit($eachLimit);
        $request->setKeyOnly($keyOnly);
        if ($columnFamily !== '') {
            $request->setCf($columnFamily);
        }

        $future = $this->grpc->callAsync(
            $address,
            'tikvpb.Tikv',
            'RawBatchScan',
            $request,
            RawBatchScanResponse::class,
            $this->timeoutConfig->scanTimeoutMs,
        );

        $regionId = $region->regionId;
        $firstKey = $groupRanges[0]['start'];

        return CheckedGrpcFuture::fromCallable(
            fn(): Message => $this->measure(
                'scan',
                $firstKey,
                function () use ($future, $regionId): Message {
                    $response = $future->wait();
                    // No RetryExecutor owns this send, so NotLeader must
                    // invalidate the cached entry itself; the fallback path
                    // then re-resolves from PD.
                    RegionErrorHandler::check(
                        $response,
                        $this->regionCache,
                        $regionId,
                        notLeaderOwnedByRetryExecutor: false,
                    );

                    return $response;
                },
            ),
            $future,
        );
    }

    /**
     * Split one region's flat `RawBatchScan` response back into the caller's
     * per-range lists.
     *
     * The response concatenates the requested ranges' results in request
     * order, and the group entries are sorted by start key, so a simple
     * membership walk restores the grouping. A range yields at most
     * `$eachLimit` rows from this region (TiKV's per-region cap); the
     * caller trims the cross-region merge afterwards.
     *
     * @param list<array{index: int|string, start: string, end: string}> $groupRanges
     * @param array<array-key, list<array{key: string, value: ?string}>> $results
     */
    private function collectBatchScanRows(
        RawBatchScanResponse $response,
        array $groupRanges,
        int $eachLimit,
        bool $keyOnly,
        array &$results,
    ): void {
        $kvs = $response->getKvs();
        $count = count($kvs);
        $position = 0;

        foreach ($groupRanges as $entry) {
            $consumed = 0;
            while ($position < $count && $consumed < $eachLimit) {
                $pair = $kvs[$position];
                $key = $pair->getKey();
                if (!KeyOrder::inRange($key, $entry['start'], $entry['end'])) {
                    break;
                }

                $results[$entry['index']][] = [
                    'key' => $key,
                    'value' => $keyOnly ? null : $pair->getValue(),
                ];
                ++$position;
                ++$consumed;
            }
        }
    }

    /**
     * Milliseconds elapsed since a batch-deadline start stamp.
     */
    private function elapsedMs(int $startedAtMs): int
    {
        return (int) (microtime(true) * 1000) - $startedAtMs;
    }

    /**
     * Throw when the batch deadline is enabled and already exhausted.
     *
     * @throws BatchDeadlineExceededException
     */
    private function assertBatchDeadlineNotExceeded(int $startedAtMs, int $batchDeadlineMs): void
    {
        if ($batchDeadlineMs <= 0) {
            return;
        }

        $elapsedMs = $this->elapsedMs($startedAtMs);
        if ($elapsedMs >= $batchDeadlineMs) {
            throw new BatchDeadlineExceededException($batchDeadlineMs, $elapsedMs);
        }
    }

    /**
     * The part of the batch deadline still available to the next phase, or
     * `0` when the deadline is disabled.
     *
     * @throws BatchDeadlineExceededException When the budget is exhausted
     */
    private function remainingBatchDeadlineMs(int $startedAtMs, int $batchDeadlineMs): int
    {
        if ($batchDeadlineMs <= 0) {
            return 0;
        }

        $elapsedMs = $this->elapsedMs($startedAtMs);
        $remainingMs = $batchDeadlineMs - $elapsedMs;
        if ($remainingMs <= 0) {
            throw new BatchDeadlineExceededException($batchDeadlineMs, $elapsedMs);
        }

        return $remainingMs;
    }

    /**
     * The per-range `RawScan` path: one scan per range, fanned out
     * concurrently (issue #295).
     *
     * Used for overlapping ranges, where a flat `RawBatchScan` response
     * cannot be split unambiguously, and as the recovery path when the
     * region-grouped fan-out fails. Every range independently resolves and
     * retries its own regions (issue #267).
     *
     * The retry budget starts here, not before the region-grouped path, so
     * a slow `RawBatchScan` fan-out cannot leave the fallback with an
     * already-exhausted budget (issue #433 review). The caller passes the
     * batch deadline that is still available to this path.
     *
     * @param array<array-key, array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     */
    private function batchScanPerRange(
        array $ranges,
        int $eachLimit,
        bool $keyOnly,
        string $columnFamily,
        int $batchDeadlineMs,
    ): array {
        // One per-range fallback is one logical operation: all ranges share
        // one retry budget (issue #223).
        $executor = $this->createRetryExecutor()->beginOperation();
        $clipper = new RegionRangeClipper();

        $calls = [];
        foreach ($ranges as $index => [$startKey, $endKey]) {
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

        $executed = $this->createBatchExecutor()->executeParallelCapped(
            $calls,
            $this->maxConcurrency,
            $batchDeadlineMs,
        );

        ksort($executed);

        $results = [];
        foreach ($executed as $perRange) {
            assert(is_array($perRange));
            /** @var array<array{key: string, value: ?string}> $perRange */
            $results[] = $perRange;
        }

        return $results;
    }

    /**
     * One empty list per requested range, preserving the historical result
     * order (input order for a list, ascending key order for associative
     * input).
     *
     * @param array<array-key, array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     */
    private function emptyBatchScanResult(array $ranges): array
    {
        $results = [];
        foreach ($ranges as $index => $_) {
            $results[$index] = [];
        }

        ksort($results);

        return array_values($results);
    }

    /**
     * Validate one `[startKey, endKey]` pair of a batchScan() call before it
     * is destructured. Destructuring a malformed range would raise an
     * "Undefined array key 1" warning and then a TypeError; rejecting it
     * here gives the caller a descriptive exception instead (issue #516).
     *
     * @return array{0: string, 1: string}
     *
     * @throws InvalidArgumentException When the range is not a two-element
     *         array of strings
     */
    private function assertBatchRange(mixed $range, int|string $index): array
    {
        if (
            !is_array($range)
            || count($range) !== 2
            || !array_key_exists(0, $range)
            || !array_key_exists(1, $range)
            || !is_string($range[0])
            || !is_string($range[1])
        ) {
            throw new InvalidArgumentException(sprintf(
                'Invalid range in batchScan at index %s: expected a [startKey, endKey] pair of strings',
                (string) $index,
            ));
        }

        return [$range[0], $range[1]];
    }

    /**
     * @param (callable(): void)|null $guard Invoked before every page fetch
     *        (see {@see ScanIterator::__construct()})
     */
    public function scanIterator(
        string $startKey,
        string $endKey,
        int $batchSize,
        bool $keyOnly,
        string $columnFamily = '',
        ?callable $guard = null,
    ): ScanIterator {
        RegionRangeClipper::assertForwardRangeOrder($startKey, $endKey, 'scanIterator');

        return new ScanIterator(
            $this->scan(...),
            $startKey,
            $endKey,
            $batchSize,
            $keyOnly,
            $columnFamily,
            $guard,
        );
    }

    /**
     * @param (callable(): void)|null $guard Invoked before every page fetch
     *        (see {@see ScanIterator::__construct()})
     */
    public function scanPrefixIterator(
        string $prefix,
        int $batchSize,
        bool $keyOnly,
        string $columnFamily = '',
        ?callable $guard = null,
    ): ScanIterator {
        return new ScanIterator(
            $this->scan(...),
            $prefix,
            RawKvSplitter::calculatePrefixEndKey($prefix),
            $batchSize,
            $keyOnly,
            $columnFamily,
            $guard,
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
            $targetStoreId = null;
            $batch = $executor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $keyOnly,
                $columnFamily,
                &$excludedStore,
                &$freshEndKey,
                &$targetStoreId,
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
                // ranges that cross region boundaries.
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || KeyOrder::lt($freshEndKey, $endKey))
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
            }, targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            });

            foreach ($batch as $row) {
                $results[] = $row;
            }

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
                || KeyOrder::lte($freshEndKey, $cursorStart)
                || ($endKey !== '' && KeyOrder::gte($freshEndKey, $endKey))
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
        $targetStoreId = null;
        $batch = $executor->execute($resolutionKey, function () use (
            $startKey,
            $endKey,
            $resolutionKey,
            $limit,
            $keyOnly,
            $columnFamily,
            &$excludedStore,
            &$freshEndKey,
            &$targetStoreId,
        ): array {
            // Resolve the region on every attempt: a stale captured region
            // would otherwise reproduce the original error on each retry.
            $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
            $target = RegionContextFactory::resolveTarget(
                $fresh,
                $this->replicaReadPolicy,
                $this->regionResolver->getStore(...),
                excludedStoreId: $excludedStore,
                requestContext: $this->requestContext,
                resourceControl: $this->resourceControl,
                storeAvailable: $this->regionResolver->isStoreAvailable(...),
            );
            $excludedStore = null;
            $targetStoreId = $target->storeId;
            $address = $this->regionResolver->resolveStoreAddress($target->storeId);
            $freshEndKey = $fresh->endKey;

            // After a split the fresh region is smaller: clip the wire
            // start (upper) key down to the fresh region's end.
            $wireStartKey = $startKey;
            if ($freshEndKey !== '' && KeyOrder::lt($freshEndKey, $wireStartKey)) {
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
        }, targetStoreId: static function () use (&$targetStoreId): ?int {
            return $targetStoreId;
        });

        // If the fresh region covers only [endKey, freshEndKey), the higher
        // remainder [freshEndKey, startKey) belongs BEFORE this batch in the
        // reverse result order: scan it first, then trim the batch to the
        // remaining limit.
        if ($freshEndKey === '' || KeyOrder::gte($freshEndKey, $startKey)) {
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
        // for every sub-range send; this mirrors scan()'s outer loop. The
        // regions are loaded in bounded batches (issue #282), so no single
        // PD response holds every region of the range.
        $regions = $this->resolveScanRegions($startKey, $endKey, RegionScanBatch::MAX_REGIONS);

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
                        && KeyOrder::lt($freshEnd, $segment['end']);

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

                    foreach ($batch as $row) {
                        $results[] = $row;
                    }
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
                        && KeyOrder::gt($cursorStart, $segmentStart)
                        && ($endKey === '' || KeyOrder::lt($cursorStart, $endKey))
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
                        foreach ($rest as $row) {
                            $results[] = $row;
                        }
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
        $targetStoreId = null;
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
            &$targetStoreId,
        ): CheckedGrpcFuture {
            // Resolve the region on every attempt so retries pick up cache
            // invalidation and leader switching (issue #267).
            $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
            $target = RegionContextFactory::resolveTarget(
                $fresh,
                $this->replicaReadPolicy,
                $this->regionResolver->getStore(...),
                requestContext: $this->requestContext,
                resourceControl: $this->resourceControl,
                storeAvailable: $this->regionResolver->isStoreAvailable(...),
            );
            $targetStoreId = $target->storeId;
            $address = $this->regionResolver->resolveStoreAddress($target->storeId);
            $freshEndKey = $fresh->endKey;

            if ($reverse) {
                // Clip the wire start (upper) key down to the fresh region's
                // end after a split; the wire reads [endKey, startKey).
                $wireStartKey = $startKey;
                if ($freshEndKey !== '' && KeyOrder::lt($freshEndKey, $wireStartKey)) {
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
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || KeyOrder::lt($freshEndKey, $endKey))
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
        }, targetStoreId: static function () use (&$targetStoreId): ?int {
            return $targetStoreId;
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
        // The parallel paths wait on the raw response without check(), so a
        // per-pair KeyError must be rejected here instead of being copied
        // through as an empty value (issue #281).
        RegionErrorHandler::check($response);

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
     *
     * A negative limit is rejected before it can reach
     * {@see RawScanRequest::setLimit()}, a `uint32` protobuf field where
     * `-1` serialises to `4294967295` and turns the scan effectively
     * unbounded (issue #332).
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
            metrics: $this->metrics,
            classifier: $this->retryClassifier,
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
