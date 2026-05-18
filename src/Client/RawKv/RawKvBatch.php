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
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
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
    public function batchGet(array $keys): array
    {
        if ($keys === []) {
            return [];
        }

        $keysByRegion = RegionGrouper::groupKeysByRegion($keys, $this->regionResolver->getRegionInfo(...));

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
                $regionCalls[] = (fn(): GrpcFuture => $this->executeBatchGetForRegionAsync(
                    $regionData['region'],
                    $subBatch,
                ));
            }
        }

        $regionResults = $batchExecutor->executeParallel($regionCalls);

        $results = [];
        foreach ($regionResults as $response) {
            assert($response instanceof RawBatchGetResponse);
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
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl
     */
    public function batchPut(array $keyValuePairs, int|array $ttl): void
    {
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
            $region = $this->regionResolver->getRegionInfo($key);
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
                $regionCalls[] = (fn(): GrpcFuture => $this->executeBatchPutForRegionAsync(
                    $regionData['region'],
                    $subBatch['pairs'],
                    $batchTtls !== [] ? $batchTtls : (is_int($ttl) ? $ttl : 0),
                ));
            }
        }

        $batchExecutor->executeParallel($regionCalls);
    }

    /**
     * @param string[] $keys
     */
    public function batchDelete(array $keys): void
    {
        if ($keys === []) {
            return;
        }

        $keysByRegion = RegionGrouper::groupKeysByRegion($keys, $this->regionResolver->getRegionInfo(...));

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
                $regionCalls[] = (fn(): GrpcFuture => $this->executeBatchDeleteForRegionAsync(
                    $regionData['region'],
                    $subBatch,
                ));
            }
        }

        $batchExecutor->executeParallel($regionCalls);
    }

    /**
     * @param string[] $keys
     */
    private function executeBatchGetForRegionAsync(RegionInfo $region, array $keys): GrpcFuture
    {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchGetRequest();
        $request->setContext(RegionContext::fromRegionInfo($region));
        $request->setKeys($keys);

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
    private function executeBatchPutForRegionAsync(RegionInfo $region, array $pairs, int|array $ttl): GrpcFuture
    {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchPutRequest();
        $request->setContext(RegionContext::fromRegionInfo($region));
        $request->setPairs($pairs);
        if (is_array($ttl)) {
            $request->setTtls($ttl);
        } elseif ($ttl > 0) {
            $request->setTtls([$ttl]);
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
    private function executeBatchDeleteForRegionAsync(RegionInfo $region, array $keys): GrpcFuture
    {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new RawBatchDeleteRequest();
        $request->setContext(RegionContext::fromRegionInfo($region));
        $request->setKeys($keys);

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
}
