<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\ChecksumAlgorithm;
use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawCASRequest;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumRequest;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
use Grpc\Call;
use Grpc\Timeval;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class RawKvClient
{
    public const MAX_SCAN_LIMIT = 10240;

    private bool $closed = false;

    /**
     * Create a client connected to a PD cluster.
     *
     * @param string[] $pdEndpoints PD addresses (currently only the first is used)
     * @param array<string, mixed> $options Client options, including 'tls' for TLS configuration
     */
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self
    {
        $resolvedLogger = $logger ?? new NullLogger();

        $tlsConfig = null;
        if (isset($options['tls']) && is_array($options['tls'])) {
            $tlsOptions = $options['tls'];
            $builder = new TlsConfigBuilder();

            if (isset($tlsOptions['caCert']) && is_string($tlsOptions['caCert'])) {
                $builder->withCaCert($tlsOptions['caCert']);
            }

            if (
                isset($tlsOptions['clientCert']) && is_string($tlsOptions['clientCert']) &&
                isset($tlsOptions['clientKey']) && is_string($tlsOptions['clientKey'])
            ) {
                $builder->withClientCert($tlsOptions['clientCert'], $tlsOptions['clientKey']);
            }

            $tlsConfig = $builder->build();
        }

        $grpc = new GrpcClient($resolvedLogger, $tlsConfig);
        $pdClient = new PdClient($grpc, $pdEndpoints[0], $resolvedLogger);

        return new self($pdClient, $grpc, new RegionCache(logger: $resolvedLogger), logger: $resolvedLogger);
    }

    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly int $serverBusyBudgetMs = 600000, // 10 minutes separate budget
    ) {
    }

    // ========================================================================
    // Single-key operations
    // ========================================================================

    public function get(string $key): ?string
    {
        $this->ensureOpen();

        return $this->executeWithRetry($key, function () use ($key): ?string {
            $region = $this->getRegionInfo($key);
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawGetRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKey($key);

            /** @var RawGetResponse $response */
            $response = $this->grpc->call($address, 'tikvpb.Tikv', 'RawGet', $request, RawGetResponse::class);
            RegionErrorHandler::check($response);

            $value = $response->getValue();
            return $value !== '' ? $value : null;
        });
    }

    /**
     * Store a key-value pair in TiKV.
     *
     * @param int $ttl Time-to-live in seconds (0 = no expiration)
     */
    public function put(string $key, string $value, int $ttl = 0): void
    {
        $this->ensureOpen();

        $this->executeWithRetry($key, function () use ($key, $value, $ttl): null {
            $region = $this->getRegionInfo($key);
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawPutRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKey($key);
            $request->setValue($value);
            if ($ttl > 0) {
                $request->setTtl($ttl);
            }

            $response = $this->grpc->call($address, 'tikvpb.Tikv', 'RawPut', $request, RawPutResponse::class);
            RegionErrorHandler::check($response);
            return null;
        });
    }

    public function delete(string $key): void
    {
        $this->ensureOpen();

        $this->executeWithRetry($key, function () use ($key): null {
            $region = $this->getRegionInfo($key);
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawDeleteRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKey($key);

            $response = $this->grpc->call($address, 'tikvpb.Tikv', 'RawDelete', $request, RawDeleteResponse::class);
            RegionErrorHandler::check($response);
            return null;
        });
    }

    /**
     * Get the remaining TTL (time-to-live) for a key.
     *
     * @return int|null Remaining TTL in seconds, or null if key not found or has no TTL
     */
    public function getKeyTTL(string $key): ?int
    {
        $this->ensureOpen();

        return $this->executeWithRetry($key, function () use ($key): ?int {
            $region = $this->getRegionInfo($key);
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawGetKeyTTLRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKey($key);

            /** @var RawGetKeyTTLResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawGetKeyTTL',
                $request,
                RawGetKeyTTLResponse::class,
            );
            RegionErrorHandler::check($response);

            $error = $response->getError();
            if ($error !== '') {
                throw new RegionException('RawGetKeyTTL', $error);
            }

            if ($response->getNotFound()) {
                return null;
            }

            $ttl = (int) $response->getTtl();
            return $ttl > 0 ? $ttl : null;
        });
    }

    /**
     * Atomic Compare-And-Swap operation.
     *
     * Atomically compares the current value of a key with an expected value,
     * and if they match, replaces it with a new value.
     *
     * @param string|null $expectedValue Expected current value, or null if the key should not exist
     * @param int $ttl Time-to-live in seconds for the new value (0 = no expiration)
     */
    public function compareAndSwap(string $key, ?string $expectedValue, string $newValue, int $ttl = 0): CasResult
    {
        $this->ensureOpen();

        return $this->executeWithRetry(
            $key,
            function () use ($key, $expectedValue, $newValue, $ttl): CasResult {
                $region = $this->getRegionInfo($key);
                $address = $this->resolveStoreAddress($region->leaderStoreId);

                $request = new RawCASRequest();
                $request->setContext(RegionContext::fromRegionInfo($region));
                $request->setKey($key);
                $request->setValue($newValue);

                if ($expectedValue === null) {
                    $request->setPreviousNotExist(true);
                } else {
                    $request->setPreviousNotExist(false);
                    $request->setPreviousValue($expectedValue);
                }

                if ($ttl > 0) {
                    $request->setTtl($ttl);
                }

                /** @var RawCASResponse $response */
                $response = $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'RawCompareAndSwap',
                    $request,
                    RawCASResponse::class,
                );
                RegionErrorHandler::check($response);

                $error = $response->getError();
                if ($error !== '') {
                    throw new RegionException('RawCompareAndSwap', $error);
                }

                return new CasResult(
                    swapped: $response->getSucceed(),
                    previousValue: $response->getPreviousNotExist() ? null : $response->getPreviousValue(),
                );
            },
        );
    }

    /**
     * Atomically put a value only if the key does not already exist.
     *
     * @return string|null null if inserted successfully, or the existing value
     */
    public function putIfAbsent(string $key, string $value, int $ttl = 0): ?string
    {
        $result = $this->compareAndSwap($key, null, $value, $ttl);

        return $result->swapped ? null : $result->previousValue;
    }

    // ========================================================================
    // Batch operations
    // ========================================================================

    /**
     * Batch get multiple keys from TiKV.
     *
     * @param string[] $keys
     * @return array<string, ?string> Values indexed by key (null for missing keys)
     */
    public function batchGet(array $keys): array
    {
        $this->ensureOpen();

        if ($keys === []) {
            return [];
        }

        $keysByRegion = $this->groupKeysByRegion($keys);

        // Execute all regions in parallel
        $regionCalls = [];
        foreach ($keysByRegion as $regionId => $regionData) {
            $regionCalls[$regionId] = (fn(): GrpcFuture => $this->executeBatchGetForRegionAsync(
                $regionData['region'],
                $regionData['keys'],
            ));
        }

        $executor = new BatchAsyncExecutor($this->logger);

        $regionResults = $executor->executeParallel($regionCalls);

        // Merge results from all regions
        $results = [];
        foreach ($regionResults as $regionResult) {
            /** @var RawBatchGetResponse $response */
            $response = $regionResult;
            foreach ($response->getPairs() as $pair) {
                $results[$pair->getKey()] = $pair->getValue() !== '' ? $pair->getValue() : null;
            }
        }

        $ordered = [];
        foreach ($keys as $key) {
            $ordered[$key] = $results[$key] ?? null;
        }

        return $ordered;
    }

    /**
     * Batch put multiple key-value pairs to TiKV.
     *
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl Time-to-live in seconds per key or uniformly applied (0 = no expiration)
     */
    public function batchPut(array $keyValuePairs, int|array $ttl = 0): void
    {
        $this->ensureOpen();

        if ($keyValuePairs === []) {
            return;
        }

        if (is_array($ttl)) {
            $count = count($keyValuePairs);
            if (count($ttl) !== $count) {
                throw new InvalidArgumentException(sprintf(
                    'TTL array count (%d) must match key-value pairs count (%d)',
                    count($ttl),
                    $count,
                ));
            }
            if (array_is_list($ttl)) {
                $ttl = array_combine(array_keys($keyValuePairs), $ttl);
            }
        }

        $pairsByRegion = [];
        foreach ($keyValuePairs as $key => $value) {
            $region = $this->getRegionInfo($key);
            $regionId = $region->regionId;
            if (!isset($pairsByRegion[$regionId])) {
                $pairsByRegion[$regionId] = ['region' => $region, 'pairs' => []];
                if (is_array($ttl)) {
                    $pairsByRegion[$regionId]['ttls'] = [];
                }
            }
            $pair = new KvPair();
            $pair->setKey($key);
            $pair->setValue($value);
            $pairsByRegion[$regionId]['pairs'][] = $pair;
            if (is_array($ttl)) {
                $pairsByRegion[$regionId]['ttls'][] = $ttl[$key];
            }
        }

        // Execute all regions in parallel
        $regionCalls = [];
        foreach ($pairsByRegion as $regionId => $regionData) {
            $regionCalls[$regionId] = (fn(): GrpcFuture => $this->executeBatchPutForRegionAsync(
                $regionData['region'],
                $regionData['pairs'],
                $regionData['ttls'] ?? $ttl,
            ));
        }

        $executor = new BatchAsyncExecutor($this->logger);
        $executor->executeParallel($regionCalls);
    }

    /**
     * Batch delete multiple keys from TiKV.
     *
     * @param string[] $keys
     */
    public function batchDelete(array $keys): void
    {
        $this->ensureOpen();

        if ($keys === []) {
            return;
        }

        $keysByRegion = $this->groupKeysByRegion($keys);

        // Execute all regions in parallel
        $regionCalls = [];
        foreach ($keysByRegion as $regionId => $regionData) {
            $regionCalls[$regionId] = (fn(): GrpcFuture => $this->executeBatchDeleteForRegionAsync(
                $regionData['region'],
                $regionData['keys'],
            ));
        }

        $executor = new BatchAsyncExecutor($this->logger);
        $executor->executeParallel($regionCalls);
    }

    // ========================================================================
    // Scan operations
    // ========================================================================

    /**
     * Scan a range of keys [startKey, endKey).
     *
     * @param int $limit Maximum results (0 = unlimited)
     * @return array<array{key: string, value: ?string}>
     */
    public function scan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $limit = $this->validateScanLimit($limit);

        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $results = [];
        $remaining = $limit;

        foreach ($regions as $region) {
            $scanStart = $startKey > $region->startKey ? $startKey : $region->startKey;
            $scanEnd = $endKey === ''
                ? $region->endKey
                : ($region->endKey !== '' && $endKey > $region->endKey ? $region->endKey : $endKey);

            if ($scanStart >= $scanEnd && $scanEnd !== '') {
                continue;
            }

            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion($region, $scanStart, $scanEnd, $regionLimit, $keyOnly, false);
            $results = array_merge($results, $regionResults);

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
     * Scan keys with a given prefix.
     *
     * @return array<array{key: string, value: ?string}>
     */
    public function scanPrefix(string $prefix, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        return $this->scan($prefix, $this->calculatePrefixEndKey($prefix), $limit, $keyOnly);
    }

    /**
     * Reverse scan a range of keys in descending order.
     *
     * Per kvrpcpb.proto: startKey = upper bound (exclusive), endKey = lower bound (inclusive).
     *
     * @return array<array{key: string, value: ?string}>
     */
    public function reverseScan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $limit = $this->validateScanLimit($limit);

        $regions = $this->pdClient->scanRegions($endKey, $startKey, 0);
        $regions = array_reverse($regions);

        $results = [];
        $remaining = $limit;

        foreach ($regions as $region) {
            $scanStartKey = ($region->endKey === '' || $startKey < $region->endKey) ? $startKey : $region->endKey;
            $scanEndKey = ($endKey > $region->startKey) ? $endKey : $region->startKey;

            if ($scanEndKey >= $scanStartKey && $scanEndKey !== '') {
                continue;
            }

            $regionLimit = $remaining === 0 ? PHP_INT_MAX : $remaining;
            $regionResults = $this->executeScanForRegion(
                $region,
                $scanStartKey,
                $scanEndKey,
                $regionLimit,
                $keyOnly,
                true,
            );
            $results = array_merge($results, $regionResults);

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
     * Scan multiple non-contiguous key ranges.
     *
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     */
    public function batchScan(array $ranges, int $eachLimit, bool $keyOnly = false): array
    {
        $this->ensureOpen();

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
            /** @phpstan-ignore function.alreadyNarrowedType, booleanOr.alwaysFalse, notIdentical.alwaysFalse */
            if (!is_array($range) || count($range) !== 2) {
                throw new InvalidArgumentException('Each range must be an array of [startKey, endKey]');
            }
            [$startKey, $endKey] = $range;
            $results[] = $this->scan($startKey, $endKey, $eachLimit, $keyOnly);
        }

        return $results;
    }

    // ========================================================================
    // Range operations
    // ========================================================================

    /**
     * Delete all keys in range [startKey, endKey).
     */
    public function deleteRange(string $startKey, string $endKey): void
    {
        $this->ensureOpen();

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

    /**
     * Delete all keys with the given prefix.
     */
    public function deletePrefix(string $prefix): void
    {
        $this->ensureOpen();

        if ($prefix === '') {
            throw new InvalidArgumentException('Prefix must not be empty -- refusing to delete all keys');
        }

        $this->deleteRange($prefix, $this->calculatePrefixEndKey($prefix));
    }

    /**
     * Compute a CRC64-XOR checksum over all key-value pairs in [startKey, endKey).
     */
    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $this->ensureOpen();

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

    // ========================================================================
    // Lifecycle
    // ========================================================================

    public function close(): void
    {
        if (!$this->closed) {
            $this->grpc->close();
            $this->pdClient->close();
            $this->closed = true;
        }
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ClientClosedException();
        }
    }

    /**
     * Validate and normalize scan limit.
     *
     * - 0 (unlimited) is capped to MAX_SCAN_LIMIT
     * - > MAX_SCAN_LIMIT throws InvalidArgumentException
     */
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

    private function getRegionInfo(string $key): RegionInfo
    {
        $region = $this->regionCache->getByKey($key);
        if ($region instanceof RegionInfo) {
            return $region;
        }

        $region = $this->pdClient->getRegion($key);
        $this->regionCache->put($region);

        return $region;
    }

    private function resolveStoreAddress(int $storeId): string
    {
        $store = $this->pdClient->getStore($storeId);
        if (!$store instanceof \CrazyGoat\Proto\Metapb\Store) {
            throw new StoreNotFoundException($storeId);
        }

        $address = $store->getAddress();
        if ($address === '') {
            throw new StoreNotFoundException($storeId);
        }

        return $address;
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
     */
    private function executeWithRetry(string $key, callable $operation): mixed
    {
        $totalBackoffMs = 0;
        $serverBusyBackoffMs = 0;
        $attempt = 0;

        while (true) {
            try {
                return $operation();
            } catch (TiKvException $e) {
                $backoffType = $this->handleNotLeader($e, $key);

                if (!$backoffType instanceof BackoffType) {
                    $backoffType = $this->classifyError($e);

                    if (!$backoffType instanceof BackoffType) {
                        $this->logger->error('Fatal error, not retrying', ['key' => $key, 'error' => $e->getMessage()]);
                        throw $e;
                    }

                    $cached = $this->regionCache->getByKey($key);
                    if ($cached instanceof RegionInfo) {
                        $this->regionCache->invalidate($cached->regionId);
                        $this->logger->info('Invalidated region on retry', [
                            'key' => $key,
                            'regionId' => $cached->regionId,
                        ]);

                        if ($e instanceof GrpcException) {
                            try {
                                $address = $this->resolveStoreAddress($cached->leaderStoreId);
                                $this->grpc->closeChannel($address);
                            } catch (StoreNotFoundException) {
                            }
                        }
                    }
                }

                $sleepMs = $backoffType->sleepMs($attempt);

                // ServerBusy uses separate budget
                if ($backoffType === BackoffType::ServerBusy) {
                    $serverBusyBackoffMs += $sleepMs;
                    if ($serverBusyBackoffMs > $this->serverBusyBudgetMs) {
                        $this->logger->error('ServerBusy budget exhausted', [
                            'key' => $key,
                            'attempt' => $attempt,
                            'serverBusyBackoffMs' => $serverBusyBackoffMs,
                            'serverBusyBudgetMs' => $this->serverBusyBudgetMs,
                        ]);
                        throw $e;
                    }
                } else {
                    $totalBackoffMs += $sleepMs;
                    if ($totalBackoffMs > $this->maxBackoffMs) {
                        $this->logger->error('Retry budget exhausted', [
                            'key' => $key,
                            'attempt' => $attempt,
                            'totalBackoffMs' => $totalBackoffMs,
                            'maxBackoffMs' => $this->maxBackoffMs,
                        ]);
                        throw $e;
                    }
                }

                $this->logger->warning('Retrying operation', [
                    'key' => $key,
                    'attempt' => $attempt,
                    'backoffType' => $backoffType->name,
                    'sleepMs' => $sleepMs,
                    'totalBackoffMs' => $totalBackoffMs,
                ]);

                if ($sleepMs > 0) {
                    usleep($sleepMs * 1000);
                }

                $attempt++;
            }
        }
    }

    private function handleNotLeader(TiKvException $e, string $key): ?BackoffType
    {
        if (!$e instanceof RegionException || !$e->notLeader instanceof NotLeader) {
            return null;
        }

        $regionId = (int) $e->notLeader->getRegionId();
        $leader = $e->notLeader->getLeader();

        if ($leader !== null) {
            $leaderStoreId = (int) $leader->getStoreId();
            $switched = $this->regionCache->switchLeader($regionId, $leaderStoreId);
            if (!$switched) {
                $this->regionCache->invalidate($regionId);
                $this->logger->info('NotLeader hint peer unknown, invalidated region', [
                    'key' => $key,
                    'regionId' => $regionId,
                    'hintStoreId' => $leaderStoreId,
                ]);
            }
        } else {
            $this->regionCache->invalidate($regionId);
            $this->logger->info('NotLeader without hint, invalidated region', [
                'key' => $key,
                'regionId' => $regionId,
            ]);
        }

        return BackoffType::NotLeader;
    }

    private function classifyError(TiKvException $e): ?BackoffType
    {
        $message = $e->getMessage();

        if (str_contains($message, 'RaftEntryTooLarge')) {
            return null;
        }
        if (str_contains($message, 'KeyNotInRegion')) {
            return null;
        }

        if (str_contains($message, 'EpochNotMatch') || str_contains($message, 'epoch not match')) {
            return BackoffType::None;
        }
        if (str_contains($message, 'ServerIsBusy')) {
            return BackoffType::ServerBusy;
        }
        if (str_contains($message, 'StaleCommand')) {
            return BackoffType::StaleCmd;
        }
        if (str_contains($message, 'RegionNotFound')) {
            return BackoffType::RegionMiss;
        }
        if (str_contains($message, 'NotLeader')) {
            return BackoffType::NotLeader;
        }

        // Additional region error types
        if (str_contains($message, 'DiskFull')) {
            return BackoffType::DiskFull;
        }
        if (str_contains($message, 'RegionNotInitialized')) {
            return BackoffType::RegionNotInitialized;
        }
        if (str_contains($message, 'ReadIndexNotReady')) {
            return BackoffType::ReadIndexNotReady;
        }
        if (str_contains($message, 'ProposalInMergingMode')) {
            return BackoffType::ProposalInMergingMode;
        }
        if (str_contains($message, 'RecoveryInProgress')) {
            return BackoffType::RecoveryInProgress;
        }
        if (str_contains($message, 'IsWitness')) {
            return BackoffType::IsWitness;
        }
        if (str_contains($message, 'MaxTimestampNotSynced')) {
            return BackoffType::MaxTimestampNotSynced;
        }

        // Fatal errors (non-retryable)
        if (str_contains($message, 'FlashbackInProgress')) {
            return null;
        }
        if (str_contains($message, 'FlashbackNotPrepared')) {
            return null;
        }

        if ($e instanceof GrpcException) {
            return BackoffType::TiKvRpc;
        }

        return null;
    }

    /**
     * @param string[] $keys
     * @return array<int, array{region: RegionInfo, keys: string[]}>
     */
    private function groupKeysByRegion(array $keys): array
    {
        $grouped = [];
        foreach ($keys as $key) {
            $region = $this->getRegionInfo($key);
            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'keys' => []];
            }
            $grouped[$regionId]['keys'][] = $key;
        }

        return $grouped;
    }

    private function calculatePrefixEndKey(string $prefix): string
    {
        if ($prefix === '') {
            return '';
        }

        $lastByte = ord($prefix[strlen($prefix) - 1]);

        if ($lastByte === 255) {
            $trimmed = rtrim($prefix, "\xff");
            if ($trimmed === '') {
                return '';
            }
            $lastByte = ord($trimmed[strlen($trimmed) - 1]);
            return substr($trimmed, 0, -1) . chr($lastByte + 1);
        }

        return substr($prefix, 0, -1) . chr($lastByte + 1);
    }

    /**
     * @param array<string> $keys
     */
    private function executeBatchGetForRegionAsync(RegionInfo $region, array $keys): GrpcFuture
    {
        return $this->executeWithRetryAsync(function () use ($region, $keys): GrpcFuture {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawBatchGetRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKeys($keys);

            $call = new Call(
                $this->grpc->getChannel($address),
                '/tikvpb.Tikv/RawBatchGet',
                Timeval::infFuture(),
            );

            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
                \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);

            return new GrpcFuture($call, RawBatchGetResponse::class);
        });
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
     */
    private function executeWithRetryAsync(callable $operation): mixed
    {
        // For async operations, we return the future immediately
        // Retry logic will be applied when wait() is called on the future
        return $operation();
    }

    /**
     * @param KvPair[] $pairs
     * @param int|int[] $ttl
     */
    private function executeBatchPutForRegionAsync(RegionInfo $region, array $pairs, int|array $ttl): GrpcFuture
    {
        return $this->executeWithRetryAsync(function () use ($region, $pairs, $ttl): GrpcFuture {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawBatchPutRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setPairs($pairs);
            if (is_array($ttl)) {
                $request->setTtls($ttl);
            } elseif ($ttl > 0) {
                $request->setTtls([$ttl]);
            }

            $call = new Call(
                $this->grpc->getChannel($address),
                '/tikvpb.Tikv/RawBatchPut',
                Timeval::infFuture(),
            );

            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
                \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);

            return new GrpcFuture($call, RawBatchPutResponse::class);
        });
    }

    /**
     * @param string[] $keys
     */
    private function executeBatchDeleteForRegionAsync(RegionInfo $region, array $keys): GrpcFuture
    {
        return $this->executeWithRetryAsync(function () use ($region, $keys): GrpcFuture {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawBatchDeleteRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setKeys($keys);

            $call = new Call(
                $this->grpc->getChannel($address),
                '/tikvpb.Tikv/RawBatchDelete',
                Timeval::infFuture(),
            );

            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
                \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);

            return new GrpcFuture($call, RawBatchDeleteResponse::class);
        });
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        RegionInfo $region,
        string $startKey,
        string $endKey,
        int $limit,
        bool $keyOnly,
        bool $reverse,
    ): array {
        $callback = function () use ($region, $startKey, $endKey, $limit, $keyOnly, $reverse): array {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

            $request = new RawScanRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
            $request->setStartKey($startKey);
            if ($endKey !== '') {
                $request->setEndKey($endKey);
            }
            if ($limit > 0) {
                $request->setLimit($limit);
            }
            $request->setKeyOnly($keyOnly);
            $request->setReverse($reverse);

            /** @var RawScanResponse $response */
            $response = $this->grpc->call($address, 'tikvpb.Tikv', 'RawScan', $request, RawScanResponse::class);
            RegionErrorHandler::check($response);

            $results = [];
            foreach ($response->getKvs() as $pair) {
                $results[] = [
                    'key' => $pair->getKey(),
                    'value' => $keyOnly ? null : $pair->getValue(),
                ];
            }

            return $results;
        };

        return $this->executeWithRetry($startKey, $callback);
    }

    private function executeDeleteRangeForRegion(RegionInfo $region, string $startKey, string $endKey): void
    {
        $this->executeWithRetry($startKey, function () use ($region, $startKey, $endKey): null {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

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
        $callback = function () use ($region, $startKey, $endKey): ChecksumResult {
            $address = $this->resolveStoreAddress($region->leaderStoreId);

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
            $response = $this->grpc->call($address, 'tikvpb.Tikv', 'RawChecksum', $request, RawChecksumResponse::class);
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
        };

        return $this->executeWithRetry($startKey, $callback);
    }
}
