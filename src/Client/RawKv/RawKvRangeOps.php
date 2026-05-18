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
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
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

    public function deleteRange(string $startKey, string $endKey): void
    {
        if ($startKey === $endKey) {
            return;
        }

        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);

        foreach ($regions as $region) {
            $rangeStart = $startKey > $region->startKey ? $startKey : $region->startKey;
            $rangeEnd = ($endKey === '' || ($region->endKey !== '' && $endKey > $region->endKey))
                ? $region->endKey
                : $endKey;

            if ($rangeStart >= $rangeEnd && $rangeEnd !== '') {
                continue;
            }

            $this->executeDeleteRangeForRegion($region, $rangeStart, $rangeEnd);
        }
    }

    public function deletePrefix(string $prefix): void
    {
        $this->deleteRange($prefix, RawKvSplitter::calculatePrefixEndKey($prefix));
    }

    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);

        $mergedChecksum = 0;
        $mergedTotalKvs = 0;
        $mergedTotalBytes = 0;

        foreach ($regions as $region) {
            $rangeStart = $startKey > $region->startKey ? $startKey : $region->startKey;
            $rangeEnd = ($endKey === '' || ($region->endKey !== '' && $endKey > $region->endKey))
                ? $region->endKey
                : $endKey;

            if ($rangeStart >= $rangeEnd && $rangeEnd !== '') {
                continue;
            }

            $result = $this->executeChecksumForRegion($region, $rangeStart, $rangeEnd);
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

    private function executeDeleteRangeForRegion(RegionInfo $region, string $startKey, string $endKey): void
    {
        $executor = new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
        );

        $executor->execute($startKey, function () use ($region, $startKey, $endKey): null {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawDeleteRangeRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setStartKey($startKey);
            $request->setEndKey($endKey);

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

    private function executeChecksumForRegion(RegionInfo $region, string $startKey, string $endKey): ChecksumResult
    {
        $executor = new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
        );

        return $executor->execute($startKey, function () use ($region, $startKey, $endKey): ChecksumResult {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $range = new KeyRange();
            $range->setStartKey($startKey);
            if ($endKey !== '') {
                $range->setEndKey($endKey);
            }

            $request = new RawChecksumRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
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
}
