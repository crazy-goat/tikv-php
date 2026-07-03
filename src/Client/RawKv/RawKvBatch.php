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
                    $regionData['region'],
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
                    $regionData['region'],
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
                    $regionData['region'],
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
        RegionInfo $region,
        array $keys,
        RetryExecutor $retryExecutor,
        string $columnFamily = '',
    ): RawBatchGetResponse {
        $key = $keys[0] ?? '';
        return $retryExecutor->execute($key, function () use (
            $region,
            $keys,
            $key,
            $columnFamily,
        ): RawBatchGetResponse {
            $fresh = $this->resolveRegion($region, $key);
            $future = $this->executeBatchGetForRegionAsync($fresh, $keys, $columnFamily);
            /** @var RawBatchGetResponse $response */
            $response = $future->wait();
            RegionErrorHandler::check($response);
            return $response;
        });
    }

    /**
     * @param KvPair[] $pairs
     * @param int|int[] $ttl
     */
    private function batchPutWithRetry(
        RegionInfo $region,
        array $pairs,
        int|array $ttl,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): null {
        $firstKey = $pairs !== [] ? $pairs[0]->getKey() : '';
        return $retryExecutor->execute($firstKey, function () use (
            $region,
            $pairs,
            $ttl,
            $firstKey,
            $forCas,
            $columnFamily,
        ): null {
            $fresh = $this->resolveRegion($region, $firstKey);
            $future = $this->executeBatchPutForRegionAsync($fresh, $pairs, $ttl, $forCas, $columnFamily);
            $response = $future->wait();
            RegionErrorHandler::check($response);
            return null;
        });
    }

    /**
     * @param string[] $keys
     */
    private function batchDeleteWithRetry(
        RegionInfo $region,
        array $keys,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): null {
        $key = $keys[0] ?? '';
        return $retryExecutor->execute($key, function () use (
            $region,
            $keys,
            $key,
            $forCas,
            $columnFamily,
        ): null {
            $fresh = $this->resolveRegion($region, $key);
            $future = $this->executeBatchDeleteForRegionAsync($fresh, $keys, $forCas, $columnFamily);
            $response = $future->wait();
            RegionErrorHandler::check($response);
            return null;
        });
    }

    private function resolveRegion(RegionInfo $original, string $key): RegionInfo
    {
        $current = $this->regionResolver->getRegionInfo($key);
        if ($current->regionId === $original->regionId) {
            return $current;
        }
        return $original;
    }
}
