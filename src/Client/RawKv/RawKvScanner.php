<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
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
        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $scanStart, $scanEnd]) {
            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion(
                $executor,
                $region,
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

        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipReverse($regions, $startKey, $endKey) as [$region, $scanStart, $scanEnd]) {
            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion(
                $executor,
                $region,
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
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
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

        $results = [];
        foreach ($ranges as $range) {
            [$startKey, $endKey] = $range;
            $results[] = $this->scan($startKey, $endKey, $eachLimit, $keyOnly, $columnFamily);
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
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        RetryExecutor $executor,
        RegionInfo $region,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        bool $reverse,
        string $columnFamily = '',
    ): array {
        return $executor->execute($startKey, function () use (
            $region,
            $startKey,
            $endKey,
            $limit,
            $keyOnly,
            $reverse,
            $columnFamily,
        ): array {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawScanRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartKey($startKey);
            if ($endKey !== '') {
                $request->setEndKey($endKey);
            }
            if ($limit > 0) {
                $request->setLimit($limit);
            }
            $request->setKeyOnly($keyOnly);
            $request->setReverse($reverse);
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

            $results = [];
            foreach ($response->getKvs() as $pair) {
                $results[] = [
                    'key' => $pair->getKey(),
                    'value' => $keyOnly ? null : $pair->getValue(),
                ];
            }

            return $results;
        });
    }

    private function validateScanLimit(int $limit): int
    {
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
        if (!$this->slowLogConfig instanceof \CrazyGoat\TiKV\Client\Grpc\SlowLogConfig) {
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
