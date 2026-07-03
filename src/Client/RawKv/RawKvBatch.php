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
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
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
use Google\Protobuf\Internal\Message;
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
                $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchGetWithRetry(
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
                $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchPutWithRetry(
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
                $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchDeleteWithRetry(
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
    // Async per-region methods (issue send only, return un-waited future)
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
    // Retry wrappers - return un-waited CheckedGrpcFuture so the executor's
    // dispatch phase issues all gRPC sends before any wait begins (true
    // client-side fan-out at the wire layer for the common single-region
    // case). Region-error checking runs once at the executor wait boundary
    // via CheckedGrpcFuture::waitForExecutor().
    // ========================================================================

    /**
     * @param string[] $keys
     */
    private function batchGetWithRetry(
        array $keys,
        RetryExecutor $retryExecutor,
        string $columnFamily = '',
    ): CheckedGrpcFuture {
        $key = $keys[0] ?? '';
        /** @var CheckedGrpcFuture $future */
        $future = $retryExecutor->execute($key, function () use (
            $keys,
            $key,
            $columnFamily,
        ): CheckedGrpcFuture {
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
                $inner = $this->executeBatchGetForRegionAsync($fresh, $keys, $columnFamily);
                return $this->wrapWithLogging(CheckedGrpcFuture::fromGrpcFuture($inner), 'batch_read', $key);
            }

            // Multi-region path (rare error-recovery case after a split/merge):
            // dispatch every sub-region's send up-front, then merge their
            // responses during the wait phase. Because all sends are issued
            // before any wait, server-side latencies overlap.
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

            $innerFutures = [];
            foreach ($groups as $group) {
                $innerFutures[] = $this->executeBatchGetForRegionAsync($group['region'], $group['keys'], $columnFamily);
            }

            $waiter = function () use ($innerFutures): Message {
                $allPairs = [];
                foreach ($innerFutures as $future) {
                    /** @var RawBatchGetResponse $response */
                    $response = $future->wait();
                    RegionErrorHandler::check($response);
                    foreach ($response->getPairs() as $pair) {
                        $allPairs[] = $pair;
                    }
                }
                $merged = new RawBatchGetResponse();
                $merged->setPairs($allPairs);
                return $merged;
            };
            return $this->wrapWithLogging(
                CheckedGrpcFuture::fromCallable($waiter),
                'batch_read',
                $key,
            );
        });

        return $future;
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
    ): CheckedGrpcFuture {
        $firstKey = $pairs !== [] ? $pairs[0]->getKey() : '';
        /** @var CheckedGrpcFuture $future */
        $future = $retryExecutor->execute($firstKey, function () use (
            $pairs,
            $ttl,
            $firstKey,
            $forCas,
            $columnFamily,
        ): CheckedGrpcFuture {
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
                $inner = $this->executeBatchPutForRegionAsync($fresh, $pairs, $ttl, $forCas, $columnFamily);
                return $this->wrapWithLogging(CheckedGrpcFuture::fromGrpcFuture($inner), 'batch_write', $firstKey);
            }

            // Multi-region path: dispatch every sub-region's send up-front.
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

            $innerFutures = [];
            foreach ($groups as $group) {
                $batchTtls = $group['ttls'];
                $batchTtl = $batchTtls !== [] ? $batchTtls : (is_int($ttl) ? $ttl : 0);
                $innerFutures[] = $this->executeBatchPutForRegionAsync(
                    $group['region'],
                    $group['pairs'],
                    $batchTtl,
                    $forCas,
                    $columnFamily,
                );
            }

            $waiter = function () use ($innerFutures): Message {
                foreach ($innerFutures as $future) {
                    /** @var RawBatchPutResponse $response */
                    $response = $future->wait();
                    RegionErrorHandler::check($response);
                }
                return new RawBatchPutResponse();
            };
            return $this->wrapWithLogging(
                CheckedGrpcFuture::fromCallable($waiter),
                'batch_write',
                $firstKey,
            );
        });

        return $future;
    }

    /**
     * @param string[] $keys
     */
    private function batchDeleteWithRetry(
        array $keys,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): CheckedGrpcFuture {
        $key = $keys[0] ?? '';
        /** @var CheckedGrpcFuture $future */
        $future = $retryExecutor->execute($key, function () use (
            $keys,
            $key,
            $forCas,
            $columnFamily,
        ): CheckedGrpcFuture {
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
                $inner = $this->executeBatchDeleteForRegionAsync($fresh, $keys, $forCas, $columnFamily);
                return $this->wrapWithLogging(CheckedGrpcFuture::fromGrpcFuture($inner), 'batch_write', $key);
            }

            // Multi-region path: dispatch every sub-region's send up-front.
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

            $innerFutures = [];
            foreach ($groups as $group) {
                $innerFutures[] = $this->executeBatchDeleteForRegionAsync(
                    $group['region'],
                    $group['keys'],
                    $forCas,
                    $columnFamily,
                );
            }

            $waiter = function () use ($innerFutures): Message {
                foreach ($innerFutures as $future) {
                    /** @var RawBatchDeleteResponse $response */
                    $response = $future->wait();
                    RegionErrorHandler::check($response);
                }
                return new RawBatchDeleteResponse();
            };
            return $this->wrapWithLogging(
                CheckedGrpcFuture::fromCallable($waiter),
                'batch_write',
                $key,
            );
        });

        return $future;
    }

    /**
     * Wrap a {@see CheckedGrpcFuture} so that the wait phase is timed against
     * the {@see SlowLogConfig} threshold and a warning is logged when the
     * elapsed time exceeds it. Slow-log measurement belongs at the wait
     * boundary because the interesting business latency is the round-trip,
     * not the dispatch microseconds.
     */
    private function wrapWithLogging(
        CheckedGrpcFuture $inner,
        string $operation,
        string $key,
    ): CheckedGrpcFuture {
        if (!$this->slowLogConfig instanceof SlowLogConfig) {
            return $inner;
        }
        $threshold = $this->slowLogConfig->getThreshold($operation);
        if ($threshold <= 0) {
            return $inner;
        }
        $logger = $this->logger;
        $logCallback = function (float $durationMs) use ($threshold, $operation, $key, $logger): void {
            $logger->warning('Slow TiKV operation', [
                'operation' => $operation,
                'key' => \CrazyGoat\TiKV\Client\Util\KeyRedactor::redact($key),
                'duration_ms' => round($durationMs, 2),
                'threshold_ms' => $threshold,
            ]);
        };
        return CheckedGrpcFuture::fromCallable(
            function () use ($inner, $threshold, $logCallback): Message {
                $start = hrtime(true);
                try {
                    return $inner->waitForExecutor();
                } finally {
                    $durationMs = (hrtime(true) - $start) / 1_000_000;
                    if ($durationMs > $threshold) {
                        $logCallback($durationMs);
                    }
                }
            },
        );
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
}
