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
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
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
     * @param string[] $keys
     * @return array<string, ?string>
     *
     * @throws TiKvException
     */
    public function batchGet(
        array $keys,
        TransactionState $state,
    ): array {
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
            $results = array_merge($results, $remoteResults);
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
        $results = [];
        $remaining = $limit;

        $clipper = new RegionRangeClipper();
        foreach ($clipper->clipForward($regions, $startKey, $endKey) as [$region, $scanStart, $scanEnd]) {
            $regionLimit = $remaining > 0 ? $remaining : $limit;
            $regionResults = $this->executeScanForRegion(
                $region,
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
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'keys' => []];
            }
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
            RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

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
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        RegionInfo $region,
        string $startKey,
        string $endKey,
        int $limit,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $maxScanLimit = 10240,
    ): array {
        return $retryExecutor->execute($startKey, function () use (
            $region,
            $startKey,
            $endKey,
            $limit,
            $maxScanLimit,
        ): array {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new ScanRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartKey($startKey);
            if ($endKey !== '') {
                $request->setEndKey($endKey);
            }
            $request->setLimit($limit > 0 ? $limit : $maxScanLimit);
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
            RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

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
            }

            $results = [];
            foreach ($response->getPairs() as $pair) {
                $results[] = [
                    'key' => $pair->getKey(),
                    'value' => $pair->getValue(),
                ];
            }

            return $results;
        }, $classifier);
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

        $allKeys = array_keys($tikvMap);
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
