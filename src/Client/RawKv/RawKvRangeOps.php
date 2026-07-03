<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\ChecksumAlgorithm;
use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumRequest;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
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
    ) {
    }

    public function deleteRange(string $startKey, string $endKey, string $columnFamily = ''): void
    {
        if ($startKey === $endKey) {
            return;
        }

        $executor = $this->createRetryExecutor();
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $clipper = new RegionRangeClipper();

        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $rangeStart, $rangeEnd]) {
            $this->executeDeleteRangeForRegion($executor, $region, $rangeStart, $rangeEnd, $columnFamily);
        }
    }

    public function deletePrefix(string $prefix, string $columnFamily = ''): void
    {
        $this->deleteRange($prefix, RawKvSplitter::calculatePrefixEndKey($prefix), $columnFamily);
    }

    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $executor = $this->createRetryExecutor();
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $clipper = new RegionRangeClipper();

        $mergedChecksum = 0;
        $mergedTotalKvs = 0;
        $mergedTotalBytes = 0;

        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $rangeStart, $rangeEnd]) {
            $result = $this->executeChecksumForRegion($executor, $region, $rangeStart, $rangeEnd);
            $mergedChecksum ^= $result->checksum;
            $mergedTotalKvs += $result->totalKvs;
            $mergedTotalBytes += $result->totalBytes;
        }

        return new ChecksumResult(
            checksum: $mergedChecksum,
            totalKvs: $mergedTotalKvs,
            totalBytes: $mergedTotalBytes,
        );
    }

    private function executeDeleteRangeForRegion(
        RetryExecutor $executor,
        RegionInfo $region,
        string $startKey,
        string $endKey,
        string $columnFamily = '',
    ): void {
        $executor->execute($startKey, function () use ($region, $startKey, $endKey, $columnFamily): null {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawDeleteRangeRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartKey($startKey);
            $request->setEndKey($endKey);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            /** @var RawDeleteRangeResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawDeleteRange',
                $request,
                RawDeleteRangeResponse::class,
                $this->timeoutConfig->deleteRangeTimeoutMs,
            );
            RegionErrorHandler::check($response);

            $error = $response->getError();
            if ($error !== '') {
                throw new RegionException('RawDeleteRange', $error);
            }

            return null;
        });
    }

    private function executeChecksumForRegion(
        RetryExecutor $executor,
        RegionInfo $region,
        string $startKey,
        string $endKey,
    ): ChecksumResult {
        return $executor->execute($startKey, function () use ($region, $startKey, $endKey): ChecksumResult {
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

            /** @var RawChecksumResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawChecksum',
                $request,
                RawChecksumResponse::class,
                $this->timeoutConfig->deleteRangeTimeoutMs,
            );
            RegionErrorHandler::check($response);

            $error = $response->getError();
            if ($error !== '') {
                throw new RegionException('RawChecksum', $error);
            }

            return new ChecksumResult(
                checksum: (int) $response->getChecksum(),
                totalKvs: (int) $response->getTotalKvs(),
                totalBytes: (int) $response->getTotalBytes(),
            );
        });
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
}
