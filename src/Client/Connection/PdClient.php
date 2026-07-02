<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\Proto\Pdpb\GetRegionRequest;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\GetStoreRequest;
use CrazyGoat\Proto\Pdpb\GetStoreResponse;
use CrazyGoat\Proto\Pdpb\RequestHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsRequest;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\TiKV\Client\Cache\StoreCacheInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfoMapper;
use Google\Protobuf\Internal\Message;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class PdClient implements PdClientInterface
{
    private ?int $clusterId = null;
    private ?TimestampOracle $tso = null;

    public function __construct(
        private readonly GrpcClientInterface $grpc,
        private readonly string $pdAddress,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?StoreCacheInterface $storeCache = null,
    ) {
    }

    public function getTimestamp(): int
    {
        if (!$this->tso instanceof TimestampOracle) {
            $this->tso = new TimestampOracle(
                $this->grpc,
                $this->pdAddress,
                $this,
                $this->logger,
            );
        }

        return $this->tso->getTimestamp();
    }

    public function getRegion(string $key): RegionInfo
    {
        $request = new GetRegionRequest();
        $request->setHeader($this->createHeader());
        $request->setRegionKey($key);

        /** @var GetRegionResponse $response */
        $response = $this->callWithClusterIdRetry(
            'GetRegion',
            $request,
            GetRegionResponse::class,
        );

        $region = $response->getRegion();
        if (!$region instanceof \CrazyGoat\Proto\Metapb\Region) {
            // Fail closed: a fabricated regionId=0/leaderStoreId=1 would be
            // cached and silently misroute requests. Throw so the failure is
            // visible instead of corrupting the region cache.
            throw new TiKvException('PD GetRegion returned no region for key');
        }

        return RegionInfoMapper::fromProto($region, $response->getLeader());
    }

    public function getStore(int $storeId): ?Store
    {
        if ($this->storeCache instanceof StoreCacheInterface) {
            $cached = $this->storeCache->get($storeId);
            if ($cached instanceof Store) {
                return $cached;
            }
        }

        $request = new GetStoreRequest();
        $request->setHeader($this->createHeader());
        $request->setStoreId($storeId);

        /** @var GetStoreResponse $response */
        $response = $this->callWithClusterIdRetry(
            'GetStore',
            $request,
            GetStoreResponse::class,
        );

        $store = $response->getStore();
        if ($store instanceof Store && $this->storeCache instanceof StoreCacheInterface) {
            $this->storeCache->put($store);
        }

        return $store;
    }

    /**
     * @return RegionInfo[]
     */
    public function scanRegions(string $startKey, string $endKey, int $limit = 0): array
    {
        $request = new ScanRegionsRequest();
        $request->setHeader($this->createHeader());
        $request->setStartKey($startKey);
        $request->setEndKey($endKey);
        $request->setLimit($limit);

        /** @var ScanRegionsResponse $response */
        $response = $this->callWithClusterIdRetry(
            'ScanRegions',
            $request,
            ScanRegionsResponse::class,
        );

        $regions = [];
        $regionMetas = $response->getRegionMetas();
        $leaders = $response->getLeaders();

        foreach ($regionMetas as $index => $region) {
            /** @var \CrazyGoat\Proto\Metapb\Peer|null $leader */
            $leader = $leaders[$index] ?? null;
            $regions[] = RegionInfoMapper::fromProto($region, $leader);
        }

        return $regions;
    }

    public function getClusterId(): ?int
    {
        return $this->clusterId;
    }

    public function setClusterId(int $clusterId): void
    {
        $this->clusterId = $clusterId;
    }

    public function close(): void
    {
        $this->grpc->close();
    }

    private function createHeader(): RequestHeader
    {
        $header = new RequestHeader();
        $header->setClusterId($this->clusterId ?? 0);
        return $header;
    }

    /**
     * Execute a PD gRPC call with automatic cluster ID mismatch retry.
     *
     * On first connect the client sends cluster_id=0. PD may reject with
     * "mismatch cluster id, need X but got 0". We extract X, cache it,
     * and retry exactly once.
     *
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    private function callWithClusterIdRetry(
        string $method,
        Message $request,
        string $responseClass,
    ): Message {
        $this->logger->debug('PD gRPC call', ['method' => $method, 'address' => $this->pdAddress]);
        try {
            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                $method,
                $request,
                $responseClass,
            );

            $this->learnClusterId($response);

            return $response;
        } catch (GrpcException $e) {
            $extractedId = $this->extractClusterIdFromError($e->getMessage());
            if ($extractedId !== null) {
                $this->logger->warning(
                    'Cluster ID mismatch, retrying',
                    ['method' => $method, 'clusterId' => $extractedId],
                );
                $this->clusterId = $extractedId;
                /** @phpstan-ignore method.notFound */
                $request->setHeader($this->createHeader());

                $response = $this->grpc->call(
                    $this->pdAddress,
                    'pdpb.PD',
                    $method,
                    $request,
                    $responseClass,
                );

                $this->learnClusterId($response);

                return $response;
            }

            throw $e;
        }
    }

    /**
     * Learn cluster ID from a successful PD response header.
     */
    private function learnClusterId(Message $response): void
    {
        if ($this->clusterId !== null) {
            return;
        }

        if (method_exists($response, 'getHeader')) {
            $header = $response->getHeader();
            if (is_object($header) && method_exists($header, 'getClusterId')) {
                /** @var int $clusterId */
                $clusterId = $header->getClusterId();
                $this->clusterId = $clusterId;
                $this->logger->info('Learned cluster ID', ['clusterId' => $clusterId]);
            }
        }
    }

    private function extractClusterIdFromError(string $message): ?int
    {
        if (!str_contains($message, 'mismatch cluster id')) {
            return null;
        }
        if (preg_match('/need (\d+) but got/', $message, $matches)) {
            return (int) $matches[1];
        }

        return null;
    }
}
