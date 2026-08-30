<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\Proto\Pdpb\GetAllStoresRequest;
use CrazyGoat\Proto\Pdpb\GetAllStoresResponse;
use CrazyGoat\Proto\Pdpb\GetGCSafePointRequest;
use CrazyGoat\Proto\Pdpb\GetGCSafePointResponse;
use CrazyGoat\Proto\Pdpb\GetMembersRequest;
use CrazyGoat\Proto\Pdpb\GetMembersResponse;
use CrazyGoat\Proto\Pdpb\GetRegionRequest;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\GetStoreRequest;
use CrazyGoat\Proto\Pdpb\GetStoreResponse;
use CrazyGoat\Proto\Pdpb\RequestHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsRequest;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointRequest;
use CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse;
use CrazyGoat\TiKV\Client\Cache\StoreCacheInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfoMapper;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
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

    public function getTimestamp(?int $timeoutMs = null): int
    {
        if (!$this->tso instanceof TimestampOracle) {
            $this->tso = new TimestampOracle(
                $this->grpc,
                $this->pdAddress,
                $this->getClusterId(...),
                $this->setClusterId(...),
                $this->logger,
            );
        }

        return $this->tso->getTimestamp($timeoutMs);
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
     * @return Store[]
     */
    /**
     * @return Store[]
     */
    public function getAllStores(): array
    {
        $request = new GetAllStoresRequest();
        $request->setHeader($this->createHeader());
        $request->setExcludeTombstoneStores(true);

        /** @var GetAllStoresResponse $response */
        $response = $this->callWithClusterIdRetry(
            'GetAllStores',
            $request,
            GetAllStoresResponse::class,
        );

        /** @var Store[] */
        return iterator_to_array($response->getStores());
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

    /**
     * Probe the PD member list to verify connectivity & discover cluster ID.
     *
     * Issues a `GetMembers` RPC against the configured PD address and learns
     * the cluster ID from the response header (if present). Returns the
     * cluster ID on success; returns null if the response carried no cluster
     * ID header. Unlike getRegion(), this call does NOT look up any user
     * data and never fails with "no region" — making it suitable as a
     * health check.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD-level error
     */
    public function ping(): ?int
    {
        $request = new GetMembersRequest();
        $request->setHeader($this->createHeader());

        /** @var GetMembersResponse $response */
        $response = $this->callWithClusterIdRetry(
            'GetMembers',
            $request,
            GetMembersResponse::class,
        );

        $this->learnClusterId($response);

        return $this->clusterId;
    }

    /**
     * Fetch the cluster's current GC safe point from PD.
     *
     * {@inheritdoc} The full contract (including the fail-closed error
     * handling) is documented on PdClientInterface.
     */
    public function getGCSafePoint(): int
    {
        $request = new GetGCSafePointRequest();
        $request->setHeader($this->createHeader());

        /** @var GetGCSafePointResponse $response */
        $response = $this->callWithClusterIdRetry(
            'GetGCSafePoint',
            $request,
            GetGCSafePointResponse::class,
        );

        $headerError = $this->headerErrorMessage($response);
        if ($headerError !== null) {
            throw new TiKvException(sprintf('PD GetGCSafePoint failed: %s', $headerError));
        }

        return (int) $response->getSafePoint();
    }

    /**
     * Register or refresh a service GC safe point with PD.
     *
     * {@inheritdoc}
     */
    public function updateServiceGCSafePoint(string $serviceId, int $safePoint, int $ttlSeconds): ?int
    {
        if ($serviceId === '') {
            throw new \InvalidArgumentException('serviceId must be a non-empty string');
        }
        if ($ttlSeconds < 0 && $safePoint !== 0) {
            // Removal (ttl < 0) is defined against the registration, not a
            // new hold: PD ignores the safe point for ttl < 0, but a caller
            // passing both a removal and a positive hold is almost certainly
            // confusing two operations.
            throw new \InvalidArgumentException(
                'safePoint must be 0 when ttlSeconds is negative (removal)',
            );
        }

        $request = new UpdateServiceGCSafePointRequest();
        $request->setHeader($this->createHeader());
        $request->setServiceId($serviceId);
        $request->setSafePoint((string) $safePoint);
        $request->setTtl((string) $ttlSeconds);

        try {
            /** @var UpdateServiceGCSafePointResponse $response */
            $response = $this->callWithClusterIdRetry(
                'UpdateServiceGCSafePoint',
                $request,
                UpdateServiceGCSafePointResponse::class,
            );
        } catch (GrpcException $e) {
            if ($this->isUnsupportedFeatureError($e->getMessage())) {
                $this->logger->warning(
                    'PD does not support service GC safe points; GC will not be held back',
                    ['serviceId' => $serviceId, 'error' => $e->getMessage()],
                );

                return null;
            }

            throw $e;
        }

        $headerError = $this->headerErrorMessage($response);
        if ($headerError !== null) {
            if ($this->isUnsupportedFeatureError($headerError)) {
                $this->logger->warning(
                    'PD does not support service GC safe points; GC will not be held back',
                    ['serviceId' => $serviceId, 'error' => $headerError],
                );

                return null;
            }

            throw new TiKvException(sprintf('PD UpdateServiceGCSafePoint failed: %s', $headerError));
        }

        return (int) $response->getMinSafePoint();
    }

    public function setClusterId(int $clusterId): void
    {
        $this->clusterId = $clusterId;
    }

    public function close(): void
    {
        // PdClient does NOT own the GrpcClient — it is shared and managed
        // by the high-level client (RawKvClient or TxnKvClient). Only
        // close PdClient-specific resources.
        $this->storeCache?->clear();
        $this->tso = null;
        $this->clusterId = null;
    }

    private function createHeader(): RequestHeader
    {
        $header = new RequestHeader();
        $header->setClusterId($this->clusterId ?? 0);
        return $header;
    }

    /**
     * Extract a PD-level error message from a response header, if present.
     *
     * PD reports request failures inside the response header (pdpb.Error)
     * rather than as gRPC status errors. A header with no error message is
     * a successful response.
     */
    private function headerErrorMessage(Message $response): ?string
    {
        if (!method_exists($response, 'getHeader')) {
            return null;
        }

        $header = $response->getHeader();
        if (!$header instanceof \CrazyGoat\Proto\Pdpb\ResponseHeader) {
            return null;
        }

        $error = $header->getError();
        if (!$error instanceof \CrazyGoat\Proto\Pdpb\Error) {
            return null;
        }

        $message = $error->getMessage();
        if ($message === '') {
            // An error entry with no message carries no information — a
            // type without text is treated as no error, matching how the
            // other header-carrying PD responses in this client behave.
            return null;
        }

        return $message;
    }

    /**
     * True when an error text means "this PD does not implement the
     * requested feature" (old PD versions / GC v1 clusters).
     */
    private function isUnsupportedFeatureError(string $message): bool
    {
        return str_contains($message, 'Unknown method')
            || str_contains($message, 'unknown method')
            || str_contains($message, 'Unimplemented')
            || str_contains($message, 'unimplemented')
            || str_contains($message, 'not supported')
            || str_contains($message, 'Not Supported');
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
        if (preg_match('/mismatch cluster id.*?need (\d+) but got/', $message, $matches)) {
            return (int) $matches[1];
        }

        return null;
    }
}
