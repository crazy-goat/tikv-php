<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Grpc\Call;
use Grpc\Timeval;
use Psr\Log\LoggerInterface;

final readonly class RawKvBatch
{
    private const MAX_BATCH_LIMIT = 512;
    private const MAX_BATCH_PUT_SIZE = 16384;
    private const MAX_BATCH_GET_SIZE = 16384;
    private const MAX_BATCH_DELETE_SIZE = 16384;

    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private LoggerInterface $logger,
        private ?SlowLogConfig $slowLogConfig = null,
    ) {
    }

    /**
     * @param string[] $keys
     * @return array<string, ?string>
     */
    public function batchGet(array $keys, RetryExecutor $retryExecutor, string $columnFamily = ''): array
    {
        if ($keys === []) {
            return [];
        }

        $keysByRegion = RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);

        $batchExecutor = new BatchAsyncExecutor($this->logger);

        $regionCalls = [];
        foreach ($keysByRegion as $regionData) {
            $subBatches = RawKvSplitter::splitIntoBatches(
                $regionData['keys'],
                self::MAX_BATCH_LIMIT,
                self::MAX_BATCH_GET_SIZE,
                strlen(...),
            );
            foreach ($subBatches as $subBatch) {
                $regionCalls[] = fn(): RawBatchGetResponse => $this->batchGetWithRetry(
                    $subBatch,
                    $retryExecutor,
                    $columnFamily,
                );
            }
        }

        $regionResults = $batchExecutor->executeParallel($regionCalls);

        $results = [];
        foreach ($regionResults as $response) {
            assert($response instanceof RawBatchGetResponse);
            foreach ($response->getPairs() as $pair) {
                $results[$pair->getKey()] = $pair->getValue();
            }
        }

        $ordered = [];
        foreach ($keys as $key) {
            $ordered[$key] = $results[$key] ?? null;
        }

        return $ordered;
    }

    /**
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl
     */
    public function batchPut(
        array $keyValuePairs,
        int|array $ttl,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): void {
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

        $keys = array_keys($keyValuePairs);
        $resolved = $this->regionResolver->batchResolveRegions($keys);

        $pairsByRegion = [];
        foreach ($keyValuePairs as $key => $value) {
            $region = $resolved[$key] ?? null;
            if ($region === null) {
                continue;
            }
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

        $batchExecutor = new BatchAsyncExecutor($this->logger);

        $regionCalls = [];
        foreach ($pairsByRegion as $regionData) {
            $regionPairs = $regionData['pairs'];
            $regionTtls = $regionData['ttls'] ?? [];

            $subBatches = RawKvSplitter::splitPairsIntoBatches(
                $regionPairs,
                self::MAX_BATCH_LIMIT,
                self::MAX_BATCH_PUT_SIZE,
                $regionTtls,
            );

            foreach ($subBatches as $subBatch) {
                $batchTtls = $subBatch['ttls'];
                $batchTtl = $batchTtls !== [] ? $batchTtls : (is_int($ttl) ? $ttl : 0);
                $regionCalls[] = fn(): null => $this->batchPutWithRetry(
                    $subBatch['pairs'],
                    $batchTtl,
                    $retryExecutor,
                    $forCas,
                    $columnFamily,
                );
            }
        }

        $batchExecutor->executeParallel($regionCalls);
    }

    /**
     * @param string[] $keys
     */
    public function batchDelete(
        array $keys,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): void {
        if ($keys === []) {
            return;
        }

        $keysByRegion = RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);

        $batchExecutor = new BatchAsyncExecutor($this->logger);

        $regionCalls = [];
        foreach ($keysByRegion as $regionData) {
            $subBatches = RawKvSplitter::splitIntoBatches(
                $regionData['keys'],
                self::MAX_BATCH_LIMIT,
                self::MAX_BATCH_DELETE_SIZE,
                strlen(...),
            );
            foreach ($subBatches as $subBatch) {
                $regionCalls[] = fn(): null => $this->batchDeleteWithRetry(
                    $subBatch,
                    $retryExecutor,
                    $forCas,
                    $columnFamily,
                );
            }
        }

        $batchExecutor->executeParallel($regionCalls);
    }

    // ========================================================================
    // Async per-region methods
    // ========================================================================

    /**
     * @param string[] $keys
     */
    private function executeBatchGetForRegionAsync(
        RegionInfo $region,
        array $keys,
        string $columnFamily = '',
    ): GrpcFuture {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchGetRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setKeys($keys);
        if ($columnFamily !== '') {
            $request->setCf($columnFamily);
        }

        $batchReadTimeout = $this->timeoutMs('batch_read');
        $deadline = $batchReadTimeout !== null
            ? Timeval::now()->add(new Timeval($batchReadTimeout * 1000))
            : Timeval::infFuture();

        $call = new Call(
            $this->grpc->getChannel($address),
            '/tikvpb.Tikv/RawBatchGet',
            $deadline,
        );

        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);

        return new GrpcFuture($call, RawBatchGetResponse::class);
    }

    /**
     * @param KvPair[] $pairs
     * @param int|int[] $ttl
     */
    private function executeBatchPutForRegionAsync(
        RegionInfo $region,
        array $pairs,
        int|array $ttl,
        bool $forCas = false,
        string $columnFamily = '',
    ): GrpcFuture {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchPutRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setPairs($pairs);
        if (is_array($ttl)) {
            $request->setTtls($ttl);
        } elseif ($ttl > 0) {
            $request->setTtls(array_fill(0, count($pairs), $ttl));
        }
        if ($forCas) {
            $request->setForCas(true);
        }
        if ($columnFamily !== '') {
            $request->setCf($columnFamily);
        }

        $batchWriteTimeout = $this->timeoutMs('batch_write');
        $deadline = $batchWriteTimeout !== null
            ? Timeval::now()->add(new Timeval($batchWriteTimeout * 1000))
            : Timeval::infFuture();

        $call = new Call(
            $this->grpc->getChannel($address),
            '/tikvpb.Tikv/RawBatchPut',
            $deadline,
        );

        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);

        return new GrpcFuture($call, RawBatchPutResponse::class);
    }

    /**
     * @param string[] $keys
     */
    private function executeBatchDeleteForRegionAsync(
        RegionInfo $region,
        array $keys,
        bool $forCas = false,
        string $columnFamily = '',
    ): GrpcFuture {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchDeleteRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setKeys($keys);
        if ($forCas) {
            $request->setForCas(true);
        }
        if ($columnFamily !== '') {
            $request->setCf($columnFamily);
        }

        $batchWriteTimeout = $this->timeoutMs('batch_write');
        $deadline = $batchWriteTimeout !== null
            ? Timeval::now()->add(new Timeval($batchWriteTimeout * 1000))
            : Timeval::infFuture();

        $call = new Call(
            $this->grpc->getChannel($address),
            '/tikvpb.Tikv/RawBatchDelete',
            $deadline,
        );

        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);

        return new GrpcFuture($call, RawBatchDeleteResponse::class);
    }

    private function timeoutMs(string $operationType): ?int
    {
        return match ($operationType) {
            'batch_read' => $this->timeoutConfig->batchReadTimeoutMs,
            'batch_write' => $this->timeoutConfig->batchWriteTimeoutMs,
            default => null,
        };
    }

    // ========================================================================
    // Retry wrappers
    // ========================================================================

    /**
     * @param string[] $keys
     */
    private function batchGetWithRetry(
        array $keys,
        RetryExecutor $retryExecutor,
        string $columnFamily = '',
    ): RawBatchGetResponse {
        $key = $keys[0] ?? '';
        return $retryExecutor->execute($key, function () use (
            $keys,
            $key,
            $columnFamily,
        ): RawBatchGetResponse {
            $fresh = $this->resolveRegion($key);

            // Fast path: all keys still belong to the same region.
            $allInRegion = true;
            foreach ($keys as $k) {
                if (!$this->keyInRegion($k, $fresh)) {
                    $allInRegion = false;
                    break;
                }
            }

            if ($allInRegion) {
                $future = $this->executeBatchGetForRegionAsync($fresh, $keys, $columnFamily);
                $response = $this->measure(
                    'batch_read',
                    $key,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchGetResponse $response */
                RegionErrorHandler::check($response);
                return $response;
            }

            // Multi-region path: a region split/merge occurred since the
            // initial grouping. Re-resolve all keys and dispatch each
            // sub-group to its own region, then merge the results.
            $resolved = $this->regionResolver->batchResolveRegions($keys);

            $groups = [];
            foreach ($keys as $k) {
                $r = $resolved[$k] ?? null;
                if ($r === null) {
                    continue;
                }
                $gid = $r->regionId;
                if (!isset($groups[$gid])) {
                    $groups[$gid] = ['region' => $r, 'keys' => []];
                }
                $groups[$gid]['keys'][] = $k;
            }

            $allPairs = [];
            foreach ($groups as $group) {
                $f = $group['region'];
                $future = $this->executeBatchGetForRegionAsync($f, $group['keys'], $columnFamily);
                $response = $this->measure(
                    'batch_read',
                    $key,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchGetResponse $response */
                RegionErrorHandler::check($response);
                foreach ($response->getPairs() as $pair) {
                    $allPairs[] = $pair;
                }
            }

            $merged = new RawBatchGetResponse();
            $merged->setPairs($allPairs);
            return $merged;
        });
    }

    /**
     * @param KvPair[] $pairs
     * @param int|int[] $ttl
     */
    private function batchPutWithRetry(
        array $pairs,
        int|array $ttl,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): null {
        $firstKey = $pairs !== [] ? $pairs[0]->getKey() : '';
        return $retryExecutor->execute($firstKey, function () use (
            $pairs,
            $ttl,
            $firstKey,
            $forCas,
            $columnFamily,
        ): null {
            $fresh = $this->resolveRegion($firstKey);

            // Fast path: all pairs still belong to the same region.
            $allInRegion = true;
            foreach ($pairs as $pair) {
                if (!$this->keyInRegion($pair->getKey(), $fresh)) {
                    $allInRegion = false;
                    break;
                }
            }

            if ($allInRegion) {
                $future = $this->executeBatchPutForRegionAsync($fresh, $pairs, $ttl, $forCas, $columnFamily);
                $response = $this->measure(
                    'batch_write',
                    $firstKey,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchPutResponse $response */
                RegionErrorHandler::check($response);
                return null;
            }

            // Multi-region path: re-resolve all keys and re-dispatch.
            $keys = array_map(fn(KvPair $p): string => $p->getKey(), $pairs);
            $resolved = $this->regionResolver->batchResolveRegions($keys);

            $hasPerKeyTtl = is_array($ttl);
            $groups = [];
            foreach ($pairs as $i => $pair) {
                $k = $pair->getKey();
                $r = $resolved[$k] ?? null;
                if ($r === null) {
                    continue;
                }
                $gid = $r->regionId;
                if (!isset($groups[$gid])) {
                    $groups[$gid] = ['region' => $r, 'pairs' => [], 'ttls' => []];
                }
                $groups[$gid]['pairs'][] = $pair;
                if ($hasPerKeyTtl) {
                    $groups[$gid]['ttls'][] = $ttl[$i];
                }
            }

            foreach ($groups as $group) {
                $f = $group['region'];
                $batchTtls = $group['ttls'];
                $batchTtl = $batchTtls !== [] ? $batchTtls : (is_int($ttl) ? $ttl : 0);
                $future = $this->executeBatchPutForRegionAsync($f, $group['pairs'], $batchTtl, $forCas, $columnFamily);
                $response = $this->measure(
                    'batch_write',
                    $firstKey,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchPutResponse $response */
                RegionErrorHandler::check($response);
            }

            return null;
        });
    }

    /**
     * @param string[] $keys
     */
    private function batchDeleteWithRetry(
        array $keys,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): null {
        $key = $keys[0] ?? '';
        return $retryExecutor->execute($key, function () use (
            $keys,
            $key,
            $forCas,
            $columnFamily,
        ): null {
            $fresh = $this->resolveRegion($key);

            // Fast path: all keys still belong to the same region.
            $allInRegion = true;
            foreach ($keys as $k) {
                if (!$this->keyInRegion($k, $fresh)) {
                    $allInRegion = false;
                    break;
                }
            }

            if ($allInRegion) {
                $future = $this->executeBatchDeleteForRegionAsync($fresh, $keys, $forCas, $columnFamily);
                $response = $this->measure(
                    'batch_write',
                    $key,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchDeleteResponse $response */
                RegionErrorHandler::check($response);
                return null;
            }

            // Multi-region path: re-resolve all keys and re-dispatch.
            $resolved = $this->regionResolver->batchResolveRegions($keys);

            $groups = [];
            foreach ($keys as $k) {
                $r = $resolved[$k] ?? null;
                if ($r === null) {
                    continue;
                }
                $gid = $r->regionId;
                if (!isset($groups[$gid])) {
                    $groups[$gid] = ['region' => $r, 'keys' => []];
                }
                $groups[$gid]['keys'][] = $k;
            }

            foreach ($groups as $group) {
                $f = $group['region'];
                $future = $this->executeBatchDeleteForRegionAsync($f, $group['keys'], $forCas, $columnFamily);
                $response = $this->measure(
                    'batch_write',
                    $key,
                    fn(): \Google\Protobuf\Internal\Message => $future->wait(),
                );
                /** @var RawBatchDeleteResponse $response */
                RegionErrorHandler::check($response);
            }

            return null;
        });
    }

    /**
     * Check whether a key falls within the half-open range [startKey, endKey)
     * of the given region. An empty endKey represents +infinity.
     */
    private function keyInRegion(string $key, RegionInfo $region): bool
    {
        return $key >= $region->startKey
            && ($region->endKey === '' || $key < $region->endKey);
    }

    /**
     * Resolve the current region for a single key. Unlike the old code, this
     * never returns the stale original when the region has changed — it always
     * returns the freshly resolved region so the caller can re-group keys.
     */
    private function resolveRegion(string $key): RegionInfo
    {
        return $this->regionResolver->getRegionInfo($key);
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
