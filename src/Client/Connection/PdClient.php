<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceRequest;
use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceResponse;
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
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
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

    /** Whether leader discovery has already run for this client. */
    private bool $leaderDiscovered = false;

    /** @var list<string> all configured PD endpoints */
    private readonly array $pdAddresses;
    /** Address currently used for PD RPCs (leader when discovery succeeded). */
    private string $currentAddress;

    /**
     * @param string|list<string> $pdAddresses one PD address or a list of
     *        PD cluster endpoints (issue #416, GAP-02)
     */
    public function __construct(
        private readonly GrpcClientInterface $grpc,
        string|array $pdAddresses,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?StoreCacheInterface $storeCache = null,
        private readonly ?int $lowResMaxStalenessMs = null,
        private readonly ?int $tsoPoolSize = null,
        /**
         * Key codec for PD region lookups.
         *
         * TxnKV clients inject a codec with Mode::Txn so region-lookup keys are
         * memory-comparable encoded (matching the encoded space TiKV reports
         * region boundaries in); RawKV uses the default passthrough codec.
         */
        private readonly CodecInterface $codec = new CodecV1(),
    ) {
        if (is_string($pdAddresses)) {
            $pdAddresses = [$pdAddresses];
        }
        $addresses = [];
        foreach ($pdAddresses as $address) {
            // @phpstan-ignore function.alreadyNarrowedType (runtime guard for untyped callers)
            if (is_string($address) && $address !== '') {
                $addresses[] = $address;
            }
        }
        if ($addresses === []) {
            throw new \InvalidArgumentException('At least one non-empty PD address is required');
        }
        $this->pdAddresses = $addresses;
        $this->currentAddress = $addresses[0];
    }

    /**
     * Address the next PD RPC will be sent to (the discovered leader when
     * leader discovery has run). TimestampOracle reads this through the
     * shared closure so TSO follows the same leader as PdClient.
     */
    public function currentPdAddress(): string
    {
        return $this->currentAddress;
    }

    public function getTimestamp(?int $timeoutMs = null): int
    {
        return $this->oracle()->getTimestamp($timeoutMs);
    }

    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array
    {
        return $this->oracle()->getTimestampBatch($count, $timeoutMs);
    }

    public function getLowResolutionTimestamp(?int $timeoutMs = null): int
    {
        return $this->oracle()->getLowResolutionTimestamp($timeoutMs);
    }

    private function oracle(): TimestampOracle
    {
        if (!$this->tso instanceof TimestampOracle) {
            $this->tso = new TimestampOracle(
                $this->grpc,
                fn (): string => $this->currentAddress,
                $this->getClusterId(...),
                $this->setClusterId(...),
                $this->logger,
                $this->lowResMaxStalenessMs,
                poolSize: $this->tsoPoolSize,
                onTransportFailure: $this->markAddressBad(...),
            );
        }

        return $this->tso;
    }

    public function getRegion(string $key): RegionInfo
    {
        $request = new GetRegionRequest();
        $request->setHeader($this->createHeader());
        $request->setRegionKey($this->codec->encodeRegionKey($key));

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

        return RegionInfoMapper::fromProto($region, $response->getLeader(), $this->codec);
    }

    public function getKeyspaceId(string $name): int
    {
        $request = new LoadKeyspaceRequest();
        $request->setHeader($this->createHeader());
        $request->setName($name);

        /** @var LoadKeyspaceResponse $response */
        $response = $this->callWithClusterIdRetry(
            'LoadKeyspace',
            $request,
            LoadKeyspaceResponse::class,
            'keyspacepb.Keyspace',
        );
        $keyspace = $response->getKeyspace();
        if ($keyspace === null) {
            throw new TiKvException('PD LoadKeyspace returned no keyspace');
        }

        return (int) $keyspace->getId();
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
        [$encodedStart, $encodedEnd] = $this->codec->encodeRange($startKey, $endKey);

        $request = new ScanRegionsRequest();
        $request->setHeader($this->createHeader());
        $request->setStartKey($encodedStart);
        $request->setEndKey($encodedEnd);
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
            $regions[] = RegionInfoMapper::fromProto($region, $leader, $this->codec);
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

        return $this->uint64ToInt($response->getSafePoint(), 'GC safe point');
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
        if ($ttlSeconds <= 0 && $safePoint !== 0) {
            // Removal (ttl <= 0 — PD removes the service safe point for any
            // non-positive TTL) is defined against the existing
            // registration, not a new hold: PD ignores the safe point for
            // ttl <= 0, but a caller passing both a removal and a positive
            // hold is almost certainly confusing two operations.
            throw new \InvalidArgumentException(
                'safePoint must be 0 when ttlSeconds is <= 0 (removal)',
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

        return $this->uint64ToInt($response->getMinSafePoint(), 'min GC safe point');
    }

    /**
     * Convert a uint64 proto scalar to a PHP int, rejecting values above
     * PHP_INT_MAX.
     *
     * Real TSO-derived safe points (~1e17) sit far below 2^63, so the cast
     * is safe in practice — but the generated gencode intval()s an
     * out-of-range uint64 into a *negative* PHP int, and silently returning
     * a negative safe point would be worse than failing loudly.
     */
    private function uint64ToInt(int|string $raw, string $what): int
    {
        if (is_string($raw) && !preg_match('/^-?\d+$/', $raw)) {
            throw new TiKvException(sprintf('PD returned a non-numeric %s: %s', $what, $raw));
        }

        $value = (int) $raw;
        if (is_string($raw) && $raw !== (string) $value) {
            // A decimal string that does not round-trip through int is
            // either out of the 64-bit range (clamped to PHP_INT_MAX) or
            // non-canonical — both are protocol violations, not safe points.
            throw new TiKvException(sprintf('PD returned an invalid %s: %s', $what, $raw));
        }

        if ($value < 0) {
            // Either an out-of-range uint64 wrapped negative, or PD sent
            // nonsense; both are protocol violations, not safe points.
            throw new TiKvException(sprintf('PD returned an invalid %s: %s', $what, (string) $raw));
        }

        return $value;
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
        // Reset the failover state too so a closed-then-reused client
        // re-runs leader discovery and starts from the first configured
        // endpoint again (the PD leader may have changed while closed).
        $this->leaderDiscovered = false;
        $this->currentAddress = $this->pdAddresses[0];
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
     * rather than as gRPC status errors. A header carrying a pdpb.Error is
     * an error even when its message text is empty (a typed-but-silent
     * error is still an error — treating it as success would turn a
     * documented fail-closed method into a source of fabricated defaults).
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

        return $message !== '' ? $message : 'unknown PD error (header error with empty message)';
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
     * Execute a PD gRPC call with automatic cluster ID mismatch retry and
     * multi-endpoint failover (issue #416, GAP-02).
     *
     * Two layers:
     *  - inner ({@see callCurrentAddressWithClusterIdRetry}): the historical
     *    "mismatch cluster id, need X but got 0" retry against one address;
     *  - outer: on any remaining transport failure the current address is
     *    marked bad, the PD leader is re-discovered via GetMembers against
     *    the remaining configured endpoints, and the call is retried against
     *    the new address — at most once per configured endpoint.
     *
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    private function callWithClusterIdRetry(
        string $method,
        Message $request,
        string $responseClass,
        string $service = 'pdpb.PD',
    ): Message {
        $this->discoverLeaderOnce();

        $attempted = [];
        while (true) {
            $attempted[] = $this->currentAddress;
            try {
                return $this->callCurrentAddressWithClusterIdRetry($method, $request, $responseClass, $service);
            } catch (GrpcException $e) {
                if ($this->extractClusterIdFromError($e->getMessage()) !== null) {
                    // The inner layer already retried the mismatch; a mismatch
                    // that survives it is not a transport failure and must not
                    // trigger failover.
                    throw $e;
                }

                $next = $this->failoverAfterTransportFailure($e, $method, $attempted);
                if ($next === null) {
                    throw $e;
                }
                $this->currentAddress = $next;
            }
        }
    }

    /**
     * On first use with multiple endpoints, prefer the PD leader.
     *
     * Discovery is best-effort: any failure (single endpoint, unreachable
     * peers) leaves the current address in place — the failover path will
     * rediscover on the first transport failure.
     */
    private function discoverLeaderOnce(): void
    {
        if ($this->leaderDiscovered || count($this->pdAddresses) < 2) {
            return;
        }

        $this->leaderDiscovered = true;
        try {
            $leader = $this->discoverLeaderAddress([]);
            if ($leader !== null) {
                $this->currentAddress = $leader;
            }
        } catch (GrpcException | TiKvException) {
            // Nearly unreachable: discoverLeaderAddress() catches these per
            // endpoint. Kept as defense in depth — discovery is opportunistic
            // and must never break the PD call it precedes; fall back to the
            // configured order (the failover path will rediscover on the
            // first transport failure).
        }
    }

    /**
     * Mark the current address bad and find another member to try.
     *
     * Runs GetMembers against every configured endpoint that has not been
     * tried yet; on success switches to the discovered leader (when it is
     * one of our endpoints and not already failed) or to the endpoint that
     * answered the discovery call. Returns null when every endpoint is down.
     *
     * @param list<string> $attempted addresses already tried for this call
     */
    private function failoverAfterTransportFailure(GrpcException $e, string $method, array $attempted): ?string
    {
        $this->logger->warning('PD endpoint failed, attempting failover', [
            'method' => $method,
            'failedAddress' => $this->currentAddress,
            'error' => $e->getMessage(),
        ]);

        try {
            $next = $this->discoverLeaderAddress($attempted);
        } catch (GrpcException | TiKvException $discoveryError) {
            // Nearly unreachable: discoverLeaderAddress() catches these per
            // endpoint. Kept as defense in depth — a throw here would abort
            // the failover instead of reporting the original transport error.
            $this->logger->warning('PD leader rediscovery failed', [
                'error' => $discoveryError->getMessage(),
            ]);
            return null;
        }

        if ($next === null) {
            return null;
        }

        $this->logger->info('PD failover selected new address', ['newAddress' => $next]);

        return $next;
    }

    /**
     * Ask PD for its member list and resolve the leader's client URL.
     *
     * Tries each configured endpoint not in $excluded until one answers.
     * Returns the leader member's client URL when it maps to one of our
     * configured endpoints (and is not excluded); otherwise the URL of the
     * endpoint that answered (PD followers answer GetMembers too and carry
     * the current leader in the response). Returns null when the response
     * carries no usable leader/members.
     *
     * @param list<string> $excluded
     */
    private function discoverLeaderAddress(array $excluded): ?string
    {
        foreach ($this->pdAddresses as $candidate) {
            if (in_array($candidate, $excluded, true)) {
                continue;
            }

            try {
                $response = $this->callGetMembers($candidate);
            } catch (GrpcException | TiKvException) {
                continue;
            }

            $leaderUrl = $this->resolveLeaderUrl($response);
            if (
                $leaderUrl !== null
                && !in_array($leaderUrl, $excluded, true)
                && in_array($leaderUrl, $this->pdAddresses, true)
            ) {
                return $leaderUrl;
            }

            // No usable leader mapping — the endpoint that answered is as
            // good a next hop as any (it is reachable and knows the leader).
            // $candidate cannot be excluded here: the loop top skipped
            // excluded candidates already.
            return $candidate;
        }

        return null;
    }

    private function callGetMembers(string $address): GetMembersResponse
    {
        $request = new GetMembersRequest();
        $request->setHeader($this->createHeader());

        /** @var GetMembersResponse $response */
        $response = $this->grpc->call(
            $address,
            'pdpb.PD',
            'GetMembers',
            $request,
            GetMembersResponse::class,
        );

        $this->learnClusterId($response);

        return $response;
    }

    /**
     * Resolve the client URL of the member whose ID equals
     * GetMembersResponse.leader.member_id.
     *
     * Returns the leader's first client URL that maps to one of the
     * configured endpoints when one exists; otherwise the leader's first
     * client URL — which may NOT be one of the configured endpoints (PD may
     * advertise addresses we were not given). The caller must therefore
     * validate the returned URL against the configured endpoints before
     * using it. Returns null when there is no leader member, the leader is
     * absent from the member list, or the leader has no client URLs.
     */
    private function resolveLeaderUrl(GetMembersResponse $response): ?string
    {
        $leader = $response->getLeader();
        if (!$leader instanceof \CrazyGoat\Proto\Pdpb\Member) {
            return null;
        }

        $leaderMemberId = (string) $leader->getMemberId();
        foreach ($response->getMembers() as $member) {
            if ((string) $member->getMemberId() !== $leaderMemberId) {
                continue;
            }

            $urls = [];
            foreach ($member->getClientUrls() as $url) {
                $urls[] = (string) $url;
            }

            foreach ($urls as $url) {
                if (in_array($url, $this->pdAddresses, true)) {
                    return $url;
                }
            }

            return $urls[0] ?? null;
        }

        return null;
    }

    /**
     * Called by TimestampOracle (via the onTransportFailure callback) when a
     * TSO RPC fails on a transport level so the next call — PD's or TSO's —
     * targets another endpoint.
     */
    private function markAddressBad(): void
    {
        $failed = $this->currentAddress;
        $next = $this->discoverLeaderAddress([$failed]);
        if ($next !== null) {
            $this->currentAddress = $next;
            $this->logger->info('PD failover after TSO transport failure', ['newAddress' => $next]);
        }
    }

    /**
     * Execute a PD gRPC call against the current address with automatic
     * cluster ID mismatch retry.
     *
     * On first connect the client sends cluster_id=0. PD may reject with
     * "mismatch cluster id, need X but got 0". We extract X, cache it,
     * and retry exactly once.
     *
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    private function callCurrentAddressWithClusterIdRetry(
        string $method,
        Message $request,
        string $responseClass,
        string $service = 'pdpb.PD',
    ): Message {
        $this->logger->debug('PD gRPC call', ['method' => $method, 'address' => $this->currentAddress]);
        try {
            $response = $this->grpc->call(
                $this->currentAddress,
                $service,
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
                if (method_exists($request, 'setHeader')) {
                    $request->setHeader($this->createHeader());
                }

                $response = $this->grpc->call(
                    $this->currentAddress,
                    $service,
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
