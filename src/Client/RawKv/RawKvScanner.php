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
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Google\Protobuf\Internal\Message;
use Psr\Log\LoggerInterface;

final readonly class RawKvScanner
{
    public const MAX_SCAN_LIMIT = 10240;

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
    ) {
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function scan(string $startKey, string $endKey, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        $limit = $this->validateScanLimit($limit);
        $executor = $this->createRetryExecutor();

        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }
        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
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
        $executor = $this->createRetryExecutor();

        $regions = $this->pdClient->scanRegions($endKey, $startKey, 0);
        $regions = array_reverse($regions);
        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipReverse($regions, $startKey, $endKey) as [, $scanStart, $scanEnd]) {
            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion(
                $executor,
                $scanStart,
                $scanEnd,
                $regionLimit,
                $keyOnly,
                true,
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
     * @return array<array{key: string, value: ?string}>
     */
    public function scanPrefix(string $prefix, int $limit, bool $keyOnly, string $columnFamily = ''): array
    {
        return $this->scan($prefix, RawKvSplitter::calculatePrefixEndKey($prefix), $limit, $keyOnly, $columnFamily);
    }

    /**
     * Scan multiple ranges concurrently.
     *
     * Each range's first RawScan send is issued at the wire layer before any
     * wait begins, so the ranges' server-side latencies overlap instead of
     * accumulating serially (issue #295). Ranges whose region enumeration
     * yields more than one sub-range fan out all of those sends too; a range
     * that comes up short after the waits (a region split after enumeration)
     * falls back to the sequential continuation from scan() so no part of
     * the range is dropped (issue #267 semantics).
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
            $batch = $executor->execute($cursorStart, function () use (
                $cursorStart,
                $endKey,
                $pending,
                $keyOnly,
                $columnFamily,
                &$freshEndKey,
            ): array {
                // Resolve the region on every attempt: a stale captured
                // region would otherwise reproduce the original error on
                // each retry.
                $fresh = $this->regionResolver->getRegionInfo($cursorStart);
                $address = $this->regionResolver->resolveStoreAddress($fresh->leaderStoreId);
                $freshEndKey = $fresh->endKey;

                // Re-clip the sub-range against the freshly resolved region:
                // after a split the fresh region is smaller, and TiKV rejects
                // ranges that cross region boundaries.
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || $freshEndKey < $endKey)
                    ? $freshEndKey
                    : $endKey;

                $request = new RawScanRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($fresh));
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
        $batch = $executor->execute($resolutionKey, function () use (
            $startKey,
            $endKey,
            $resolutionKey,
            $limit,
            $keyOnly,
            $columnFamily,
            &$freshEndKey,
        ): array {
            // Resolve the region on every attempt: a stale captured region
            // would otherwise reproduce the original error on each retry.
            $fresh = $this->regionResolver->getRegionInfo($resolutionKey);
            $address = $this->regionResolver->resolveStoreAddress($fresh->leaderStoreId);
            $freshEndKey = $fresh->endKey;

            // After a split the fresh region is smaller: clip the wire
            // start (upper) key down to the fresh region's end.
            $wireStartKey = $startKey;
            if ($freshEndKey !== '' && $freshEndKey < $wireStartKey) {
                $wireStartKey = $freshEndKey;
            }

            $request = new RawScanRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($fresh));
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
        if ($freshEndKey === '' || $freshEndKey >= $startKey) {
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
     * sequential continuation when the segments under-deliver (split after
     * enumeration).
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
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

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

                $lastIndex = -1;
                foreach ($segments as $index => $segment) {
                    $response = $this->measure(
                        'scan',
                        $segment['start'],
                        fn(): mixed => $segment['future']->waitForExecutor(),
                    );
                    assert($response instanceof RawScanResponse);

                    $batch = $this->parseScanPairs($response, $keyOnly);
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
                if ($remaining > 0 && $lastIndex === count($segments) - 1) {
                    $cursorStart = $freshEnds[$lastIndex];
                    $segmentStart = $segments[$lastIndex]['start'];
                    // The cursor must have advanced past the segment start
                    // and still lie inside the requested range; otherwise the
                    // whole sub-range was covered by the dispatched segments.
                    if (
                        $cursorStart !== ''
                        && $cursorStart > $segmentStart
                        && ($endKey === '' || $cursorStart < $endKey)
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
            $address = $this->regionResolver->resolveStoreAddress($fresh->leaderStoreId);
            $freshEndKey = $fresh->endKey;

            if ($reverse) {
                // Clip the wire start (upper) key down to the fresh region's
                // end after a split; the wire reads [endKey, startKey).
                $wireStartKey = $startKey;
                if ($freshEndKey !== '' && $freshEndKey < $wireStartKey) {
                    $wireStartKey = $freshEndKey;
                }
                $request = new RawScanRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($fresh));
                $request->setStartKey($wireStartKey);
                if ($endKey !== '') {
                    $request->setEndKey($endKey);
                }
                $request->setReverse(true);
            } else {
                // Re-clip against the freshly resolved region: TiKV rejects
                // ranges that cross region boundaries.
                $wireEndKey = $freshEndKey !== '' && ($endKey === '' || $freshEndKey < $endKey)
                    ? $freshEndKey
                    : $endKey;
                $request = new RawScanRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($fresh));
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

    private function validateScanLimit(int $limit): int
    {
        if ($limit < 0) {
            throw new InvalidArgumentException('Scan limit must be 0 or greater');
        }

        if ($limit === 0) {
            return self::MAX_SCAN_LIMIT;
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
