<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\ChecksumAlgorithm;
use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumRequest;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Psr\Log\LoggerInterface;

final readonly class RawKvRangeOps
{
    public function __construct(
        private PdClientInterface $pdClient,
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private RegionCacheInterface $regionCache,
        private TimeoutConfig $timeoutConfig,
        private int $maxBackoffMs,
        private int $serverBusyBudgetMs,
        private LoggerInterface $logger,
        private ?SlowLogConfig $slowLogConfig = null,
        private int $retryDeadlineMs = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS,
        private int $maxConcurrency = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY,
    ) {
    }

    /**
     * Delete [startKey, endKey) across every region the range spans.
     *
     * Per-region RawDeleteRange requests are fanned out at the wire layer:
     * all sends inside a concurrency window are issued before any wait
     * begins, so server-side latencies overlap instead of accumulating.
     * At most {@see self::$maxConcurrency} requests are in flight at any
     * moment (issue #295).
     *
     * Failure semantics differ from a sequential loop: region errors no
     * longer abort at the first failing region but surface together as a
     * {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException}.
     * deleteRange is idempotent, so retrying the whole operation remains safe.
     */
    public function deleteRange(string $startKey, string $endKey, string $columnFamily = ''): void
    {
        if ($startKey === $endKey) {
            return;
        }

        $executor = $this->createRetryExecutor();
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $clipper = new RegionRangeClipper();

        $calls = [];
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $rangeStart, $rangeEnd]) {
            $calls[] = fn(): CheckedGrpcFuture => $this->deleteRangeWithRetry(
                $executor,
                $region,
                $rangeStart,
                $rangeEnd,
                $columnFamily,
            );
        }

        $this->createBatchExecutor()->executeParallelCapped($calls, $this->maxConcurrency);
    }

    public function deletePrefix(string $prefix, string $columnFamily = ''): void
    {
        $this->deleteRange($prefix, RawKvSplitter::calculatePrefixEndKey($prefix), $columnFamily);
    }

    /**
     * Checksum [startKey, endKey) across every region the range spans and
     * merge the per-region CRC64-XOR results. Requests are fanned out with
     * the same bounded concurrency as {@see self::deleteRange()} (issue #295).
     *
     * Region errors surface as a
     * {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException};
     * checksum is idempotent, so retrying the whole operation remains safe.
     */
    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $executor = $this->createRetryExecutor();
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $clipper = new RegionRangeClipper();

        $calls = [];
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $rangeStart, $rangeEnd]) {
            $calls[] = fn(): CheckedGrpcFuture => $this->checksumWithRetry(
                $executor,
                $region,
                $rangeStart,
                $rangeEnd,
            );
        }

        $responses = $this->createBatchExecutor()->executeParallelCapped($calls, $this->maxConcurrency);

        $mergedChecksum = 0;
        $mergedTotalKvs = 0;
        $mergedTotalBytes = 0;

        foreach ($responses as $response) {
            assert($response instanceof RawChecksumResponse);
            $mergedChecksum ^= (int) $response->getChecksum();
            $mergedTotalKvs += (int) $response->getTotalKvs();
            $mergedTotalBytes += (int) $response->getTotalBytes();
        }

        return new ChecksumResult(
            checksum: $mergedChecksum,
            totalKvs: $mergedTotalKvs,
            totalBytes: $mergedTotalBytes,
        );
    }

    /**
     * Issue one RawDeleteRange send for a single clipped sub-range and
     * return an un-waited future so the batch executor can fan out all
     * regions' sends before awaiting any of them.
     */
    private function deleteRangeWithRetry(
        RetryExecutor $executor,
        RegionInfo $region,
        string $startKey,
        string $endKey,
        string $columnFamily = '',
    ): CheckedGrpcFuture {
        /** @var CheckedGrpcFuture $future */
        $future = $executor->execute($startKey, function () use (
            $region,
            $startKey,
            $endKey,
            $columnFamily,
        ): CheckedGrpcFuture {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawDeleteRangeRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartKey($startKey);
            $request->setEndKey($endKey);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            $response = $this->grpc->callAsync(
                $address,
                'tikvpb.Tikv',
                'RawDeleteRange',
                $request,
                RawDeleteRangeResponse::class,
                $this->timeoutConfig->deleteRangeTimeoutMs,
            );

            return CheckedGrpcFuture::fromCallable(
                fn(): RawDeleteRangeResponse => $this->measure(
                    'delete_range',
                    $startKey,
                    function () use ($response): RawDeleteRangeResponse {
                        /** @var RawDeleteRangeResponse $resolved */
                        $resolved = $response->wait();
                        RegionErrorHandler::check($resolved);

                        $error = $resolved->getError();
                        if ($error !== '') {
                            throw new RegionException('RawDeleteRange', $error);
                        }

                        return $resolved;
                    },
                ),
            );
        });

        return $future;
    }

    /**
     * Issue one RawChecksum send for a single clipped sub-range and return
     * an un-waited future (fan-out pattern, see deleteRangeWithRetry()).
     */
    private function checksumWithRetry(
        RetryExecutor $executor,
        RegionInfo $region,
        string $startKey,
        string $endKey,
    ): CheckedGrpcFuture {
        /** @var CheckedGrpcFuture $future */
        $future = $executor->execute($startKey, function () use ($region, $startKey, $endKey): CheckedGrpcFuture {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $range = new KeyRange();
            $range->setStartKey($startKey);
            if ($endKey !== '') {
                $range->setEndKey($endKey);
            }

            $request = new RawChecksumRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setAlgorithm(ChecksumAlgorithm::Crc64_Xor);
            $request->setRanges([$range]);

            $response = $this->grpc->callAsync(
                $address,
                'tikvpb.Tikv',
                'RawChecksum',
                $request,
                RawChecksumResponse::class,
                $this->timeoutConfig->checksumTimeoutMs,
            );

            return CheckedGrpcFuture::fromCallable(
                fn(): RawChecksumResponse => $this->measure(
                    'checksum',
                    $startKey,
                    function () use ($response): RawChecksumResponse {
                        /** @var RawChecksumResponse $resolved */
                        $resolved = $response->wait();
                        RegionErrorHandler::check($resolved);

                        $error = $resolved->getError();
                        if ($error !== '') {
                            throw new RegionException('RawChecksum', $error);
                        }

                        return $resolved;
                    },
                ),
            );
        });

        return $future;
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

    private function createBatchExecutor(): BatchAsyncExecutor
    {
        return new BatchAsyncExecutor($this->logger);
    }
}
