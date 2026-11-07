<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use Closure;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsMultiplexer;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Cache\StoreCacheInterface;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\HealthCheckException;
use CrazyGoat\TiKV\Client\Exception\IngestTooLargeException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\PdException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcBatchCommandsTransport;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\RegionCacheWarmer;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * RawKV client.
 *
 * ## Exceptions shared by region-resolved operations
 *
 * A method that resolves a region or a store through PD can additionally
 * throw {@see PdException} (PD answered with an application-level error such
 * as NOT_BOOTSTRAPPED), {@see InvalidStoreAddressException} (PD returned a
 * store address outside the configured policy) and the base
 * {@see TiKvException} (a fail-closed internal error). This applies to a
 * resolution that runs before a batch fan-out; a resolution that runs inside
 * {@see \CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor} reports its failure
 * through {@see BatchPartialFailureException} instead. A per-pair `KeyError`
 * on a scan response (and on a multiplexed batch-get response) is raised as
 * {@see \CrazyGoat\TiKV\Client\Exception\KeyErrorException} (a TiKvException
 * subclass). With `options['apiVersion'] => 2`, every operation that sends or
 * decodes a key can also throw
 * {@see \CrazyGoat\TiKV\Client\Exception\KeyOutOfBoundsException} when the
 * key is outside the configured keyspace. Those types are not repeated on
 * every method below.
 *
 * ## Retry budget and missing stores
 *
 * A method that runs its operation directly through the internal
 * {@see RetryExecutor} declares {@see RetryBudgetExhaustedException} (the
 * attempt cap or the wall-clock deadline was exhausted) and
 * {@see StoreNotFoundException} (PD no longer knows the store backing the
 * region).
 *
 * The fanned-out batch and range operations (`batchGet()`, `batchPut()`,
 * `batchDelete()`, `batchScan()`, `deleteRange()`, `deletePrefix()` and
 * `checksum()`) run their per-region retrying futures through
 * {@see \CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor}, which reports every
 * TiKvException — including a region's `RetryBudgetExhaustedException` or
 * `StoreNotFoundException` — inside {@see BatchPartialFailureException}. They
 * declare the wrapper for those per-region failures; inspect
 * `getRegionErrors()` for the cause. With
 * `options['batchCommands']` enabled, the multiplexed entry setup resolves
 * store addresses before that wrapper and can still throw
 * `StoreNotFoundException` directly.
 *
 * `ingest()` runs each region group through {@see RetryExecutor} with a fresh
 * budget per group, so it can throw {@see RetryBudgetExhaustedException}
 * directly (when no region group had been applied yet) or wrap a failure in
 * {@see \CrazyGoat\TiKV\Client\Exception\IngestPartialFailureException} once
 * at least one group has landed. The store-mode switch fan-out still reports
 * its failures through {@see BatchPartialFailureException}; a missing store
 * during region resolution is retried and, when fatal, escapes directly.
 */
final class RawKvClient
{
    public const MAX_SCAN_LIMIT = 10240;

    public const MAX_BATCH_LIMIT = 512;

    public const MAX_BATCH_PUT_SIZE = 16384;

    public const MAX_BATCH_GET_SIZE = 16384;

    public const MAX_BATCH_DELETE_SIZE = 16384;

    public const MAX_KEY_SIZE = 8388608;

    public const MAX_VALUE_SIZE = 4194304;

    public const MIN_KEY_SIZE = 1;

    public const OP_READ = 'read';
    public const OP_WRITE = 'write';
    public const OP_BATCH_READ = 'batch_read';
    public const OP_BATCH_WRITE = 'batch_write';
    public const OP_SCAN = 'scan';
    public const OP_DELETE_RANGE = 'delete_range';
    public const OP_CHECKSUM = 'checksum';
    public const OP_INGEST = 'ingest';

    public const OPT_TIMEOUT = 'timeout';
    public const OPT_SLOW_LOG = 'slowLog';
    public const OPT_METRICS = 'metrics';

    /** API protocol version: 0/1 (V1, default) or 2 (keyspace-aware). */
    public const OPT_API_VERSION = 'apiVersion';

    /** API V2 keyspace name; omitted or empty means DEFAULT. */
    public const OPT_KEYSPACE = 'keyspace';

    /**
     * Default wall-clock deadline for one client operation's retry loop
     * (issue #294). Bounds the blocking usleep() backoff so a sustained
     * ServerIsBusy episode cannot pin a PHP-FPM worker for minutes inside a
     * single request; the previous unbounded worst case was ~10 minutes
     * (serverBusyBudgetMs = 600000, since reduced — see
     * DEFAULT_SERVER_BUSY_BUDGET_MS).
     *
     * Kept as an alias of the canonical RetryExecutor constant so existing
     * references do not break; do not introduce further copies.
     */
    public const DEFAULT_RETRY_DEADLINE_MS = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS;

    /**
     * Default ServerBusy backoff budget (issue #294). Reduced from the
     * previous 600000 ms: the budget caps cumulative blocking sleep charged
     * to ServerBusy backoff inside ONE operation, i.e. it is effectively a
     * worker-occupancy setting. Ten minutes pinned PHP-FPM workers for the
     * whole duration of a cluster-wide overload; one minute, combined with
     * the DEFAULT_RETRY_DEADLINE_MS wall-clock bound, still rides out short
     * load spikes while shedding sustained ones in seconds. Long-running CLI
     * workers that prefer to wait out long ServerBusy episodes can raise it
     * via the constructor argument.
     */
    public const DEFAULT_SERVER_BUSY_BUDGET_MS = 60000;

    /**
     * options[] key for the per-operation retry deadline in milliseconds
     * (see OPT_TIMEOUT / TimeoutConfig for the gRPC deadlines).
     */
    public const OPT_RETRY_DEADLINE = 'retryDeadlineMs';

    /**
     * options[] key for the maximum number of simultaneously in-flight
     * requests used by the fanned-out batch and range operations (batchGet,
     * batchPut, batchDelete, deleteRange, checksum, batchScan, SST ingest
     * mode switches) — issues #295 and #264.
     */
    public const OPT_MAX_CONCURRENCY = 'maxConcurrency';

    /**
     * options[] key for the read preference (issue #421): a
     * {@see ReplicaReadPolicy} instance selecting which replica serves
     * reads (leader default, follower, mixed, prefer-leader), optional
     * store-label matching and the stale-read flag. Writes always target
     * the leader.
     */
    public const OPT_REPLICA_READ = 'replicaRead';

    /**
     * options[] key for the optional `kvrpcpb.Context` control fields
     * (issue #426, GAP-12): a
     * {@see \CrazyGoat\TiKV\Client\Region\RequestContextOptions} instance
     * carrying the isolation level, not-fill-cache flag, server-side
     * max-execution duration, request source, resource group tag, busy
     * threshold and resource group name applied to every request this
     * client sends. Omitted or null (the default) applies none of them and
     * leaves the serialized Context unchanged.
     */
    public const OPT_REQUEST_CONTEXT = 'requestContext';

    /**
     * options[] key for the maximum number of rows an unbounded scan
     * (`limit: 0`) may return before throwing
     * {@see \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException}
     * (issue #191). `limit: 0` pages
     * internally so the documented "whole range" contract holds, but the
     * accumulated result still lives in one PHP array; this guard caps it
     * (silently truncating instead is forbidden). The guard is evaluated per
     * internally fetched page — after the page has been buffered — so it is
     * page-granular, not a strict per-row memory bound: with a guard smaller
     * than the page size (`RawKvScanner::MAX_SCAN_LIMIT`, 10240) a whole page
     * is read before the throw, and `getScannedRows()` can exceed
     * `getMaxRows()`. Must be `>= 1`. Bounded scans (`limit > 0`) are
     * unaffected.
     */
    public const OPT_MAX_SCAN_ROWS = 'maxScanRows';

    /**
     * options[] key for the experimental BatchCommands multiplexing
     * (issue #418, GAP-04). When true, the RawKV batch fan-outs
     * (batchGet/batchPut/batchDelete) multiplex their per-region
     * sub-requests over one bidirectional BatchCommands stream per store
     * address instead of issuing one unary RPC per sub-batch; operations
     * not representable in the BatchCommands oneof, region errors and
     * transport failures automatically fall back to the unary path. Must
     * be a boolean, default false — the stream path is experimental this
     * cycle and the unary fan-out remains the default.
     */
    public const OPT_BATCH_COMMANDS = 'batchCommands';

    /**
     * options[] key for the region-cache refresh interval in milliseconds
     * (issue #425). A long-running worker calls
     * {@see RawKvClient::refreshRegionCache()} from its own tick; the method
     * performs at most one refresh per interval. 0 (the default) disables the
     * throttled refresh entirely, so nothing changes for an application that
     * does not opt in; a forced refresh still works. Must be >= 0.
     */
    public const OPT_REGION_CACHE_REFRESH_MS = 'regionCacheRefreshMs';

    /** Default region-cache refresh interval: disabled (issue #425). */
    public const DEFAULT_REGION_CACHE_REFRESH_MS = RegionCacheWarmer::DEFAULT_REFRESH_INTERVAL_MS;

    /**
     * options[] key for the resource group every request belongs to (issue
     * #427). Set on `Context.resource_control_context.resource_group_name`.
     * Default: unset = no resource-control fields are sent. The name must be
     * 1 to 64 characters of letters, digits, underscores or hyphens.
     *
     * @see ResourceControl
     */
    public const OPT_RESOURCE_GROUP = ResourceControl::OPTION_RESOURCE_GROUP;

    /**
     * options[] key for an opaque resource-group tag (issue #427), carried on
     * `Context.resource_group_tag` for query attribution. Default: unset =
     * the field is not sent. Must be a non-empty string.
     */
    public const OPT_RESOURCE_GROUP_TAG = ResourceControl::OPTION_RESOURCE_GROUP_TAG;

    /**
     * options[] key for the maximum payload bytes (keys plus values) of one
     * `ingest()` call (issue #297). `0` (the default) means unlimited.
     * Exceeding the limit throws
     * {@see \CrazyGoat\TiKV\Client\Exception\IngestTooLargeException} before
     * the cluster is switched into import mode and before any RPC is issued,
     * so the caller can split the payload into smaller batches.
     */
    public const OPT_MAX_INGEST_SIZE = 'maxIngestSize';

    /** Default bound on in-flight requests for fanned-out batch and range operations. */
    public const DEFAULT_MAX_CONCURRENCY = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY;

    /** Default row cap for an unbounded (`limit: 0`) scan (issue #191). */
    public const DEFAULT_MAX_SCAN_ROWS = RawKvScanner::DEFAULT_MAX_SCAN_ROWS;

    private bool $closed = false;

    private bool $atomicForCAS = false;

    private string $columnFamily = '';

    private readonly RawKvCrud $crud;
    private readonly RawKvAtomic $atomic;
    private readonly RawKvBatch $batch;
    private readonly RawKvScanner $scanner;
    private readonly RawKvRangeOps $rangeOps;
    private readonly SstIngestor $ingestor;
        /** Wall-clock deadline (ms) applied to every RetryExecutor this client builds. */
    private readonly int $retryDeadlineMs;
    /** Max in-flight requests for fanned-out batch and range operations (#295, #264). */
    private readonly int $maxConcurrency;

    /** Opt-in region-cache refresh and preload (#425). */
    private readonly RegionCacheWarmer $regionCacheWarmer;

    /**
     * The resolver every component of this client shares, so per-store
     * health state learned by one retry loop is visible to the next
     * (issue #424).
     */
    private readonly RegionResolver $regionResolver;

    /**
     * @param string[] $pdEndpoints
     * @param array<string, mixed> $options Client options. Besides the
     *        RawKV-specific ones, the shared transport options are accepted:
     *        'tls' (TLS configuration), 'allowInsecure' (fail-closed TLS,
     *        default true), 'maxChannels' and 'idleTtlMs' (channel pool),
     *        'grpc' (gRPC channel arguments) and 'grpcClient' (a transport
     *        built by the application — the client uses it and never closes
     *        it, so one transport can be shared with other clients).
     *
     * @throws InvalidArgumentException if PD endpoints array is empty or an
     *         option value is invalid
     * @throws PdException When the API V2 keyspace lookup gets a PD
     *         application-level error
     * @throws GrpcException When the API V2 keyspace lookup fails at the
     *         transport level
     * @throws TiKvException When the API V2 keyspace lookup fails closed
     */
    public static function create(
        array $pdEndpoints,
        ?LoggerInterface $logger = null,
        array $options = []
    ): self {
        // V1 RawKV passes keys through byte-for-byte; API V2 adds its
        // keyspace prefix at the shared transport boundary.
        $bundle = ConnectionFactory::create(
            $pdEndpoints,
            $logger,
            $options,
            new CodecV1(Mode::Raw),
            Mode::Raw,
        );

        return new self(
            $bundle->pdClient,
            new ApiV2GrpcClient($bundle->grpc, $bundle->codec, Mode::Raw),
            new RegionCache(logger: $bundle->logger, metrics: $bundle->metrics),
            logger: $bundle->logger,
            timeoutConfig: $bundle->timeoutConfig,
            slowLogConfig: $bundle->slowLogConfig,
            metrics: $bundle->metrics,
            allowedStoreHosts: $bundle->allowedStoreHosts,
            storeHostPolicy: $bundle->storeHostPolicy,
            pdEndpoints: $bundle->pdEndpoints,
            allowedStorePorts: $bundle->allowedStorePorts,
            retryDeadlineMs: self::resolveRetryDeadline($options),
            maxConcurrency: self::resolveMaxConcurrency($options),
            replicaReadPolicy: self::resolveReplicaReadPolicy($options),
            maxScanRows: self::resolveMaxScanRows($options),
            batchCommands: self::resolveBatchCommands($options),
            ownsGrpcClient: $bundle->ownsGrpcClient,
            codec: $bundle->codec,
            regionCacheRefreshMs: self::resolveRegionCacheRefreshMs($options),
            maxIngestSize: self::resolveMaxIngestSize($options),
            storeCache: $bundle->storeCache,
            requestContext: self::resolveRequestContext($options),
            resourceControl: self::resolveResourceControl($options),
        );
    }

    /**
     * @throws InvalidArgumentException When retryDeadlineMs, maxConcurrency or
     *         maxScanRows is out of range
     */
    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly int $serverBusyBudgetMs = self::DEFAULT_SERVER_BUSY_BUDGET_MS,
        TimeoutConfig $timeoutConfig = new TimeoutConfig(),
        ?RegionResolver $regionResolver = null,
        ?RawKvCrud $crud = null,
        ?RawKvAtomic $atomic = null,
        ?RawKvBatch $batch = null,
        ?RawKvScanner $scanner = null,
        ?RawKvRangeOps $rangeOps = null,
        private readonly ?SlowLogConfig $slowLogConfig = null,
        private readonly MetricsInterface $metrics = new NoOpMetrics(),
        /** @var list<string> */
        private readonly array $allowedStoreHosts = [],
        /** @var (Closure(string): bool)|null */
        private readonly ?Closure $storeHostPolicy = null,
        /** @var list<string> */
        private readonly array $pdEndpoints = [],
        /** @var list<int>|null */
        private readonly ?array $allowedStorePorts = null,
        int $retryDeadlineMs = self::DEFAULT_RETRY_DEADLINE_MS,
        int $maxConcurrency = self::DEFAULT_MAX_CONCURRENCY,
        /** Read preference applied to every read issued by this client (issue #421). */
        private readonly ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
        int $maxScanRows = self::DEFAULT_MAX_SCAN_ROWS,
        bool $batchCommands = false,
        /**
         * Whether this client owns the gRPC transport and may close it.
         *
         * true (the default) for a client that built the transport itself —
         * and for a client assembled by hand, which therefore closes the
         * transport it was given (issue #204 tracks the change to close only
         * what a client owns).
         * false when the transport was supplied by the application
         * (options['grpcClient']): the application stays its owner, so the
         * client never closes it and the same transport can be shared with
         * other clients.
         */
        private readonly bool $ownsGrpcClient = true,
        /**
         * Key codec supplied by the construction path. `epochNotMatchCodec()`
         * derives the EpochNotMatch payload codec from it (issue #246): V1
         * RawKV passes a passthrough `CodecV1(Mode::Raw)`, while API V2
         * passes `CodecV2` and the derived payload codec is null because
         * `ApiV2GrpcClient` already decodes response region boundaries.
         * Null itself also means passthrough for a hand-built client.
         */
        private readonly ?CodecInterface $codec = null,
        /**
         * Minimum interval between two non-forced
         * {@see self::refreshRegionCache()} calls, in milliseconds (issue
         * #425). 0 (the default) disables the throttled refresh; see
         * {@see self::OPT_REGION_CACHE_REFRESH_MS}.
         */
        int $regionCacheRefreshMs = self::DEFAULT_REGION_CACHE_REFRESH_MS,
        /**
         * Pre-built warmer (test seam). When null, the client builds one from
         * its PD client, region cache and logger.
         */
        ?RegionCacheWarmer $regionCacheWarmer = null,
        /**
         * Maximum payload bytes of one ingest() call; 0 = unlimited
         * (issue #297).
         */
        private readonly int $maxIngestSize = 0,
        /**
         * Store-address cache the retry executor drops when a StoreNotMatch
         * or a transport failure proves the cached address wrong (issue
         * #247). Null for a hand-built client: the resolver then has no
         * store cache to invalidate.
         */
        private readonly ?StoreCacheInterface $storeCache = null,
        /**
         * Optional Context control fields applied to every request
         * (issue #426). Null (the default) leaves the serialized Context
         * byte-for-byte unchanged.
         */
        private readonly ?RequestContextOptions $requestContext = null,
        /**
         * Resource-control metadata applied to every request this client
         * builds (issue #427). An empty instance (the default) leaves both
         * resource-control fields unset and keeps the request bytes
         * unchanged.
         */
        private readonly ResourceControl $resourceControl = new ResourceControl(),
    ) {
        if ($regionCacheRefreshMs < 0) {
            throw new InvalidArgumentException('regionCacheRefreshMs must be >= 0');
        }
        if ($retryDeadlineMs < 0) {
            throw new InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        if ($maxConcurrency < 1) {
            throw new InvalidArgumentException('maxConcurrency must be >= 1');
        }
        if ($maxScanRows < 1) {
            throw new InvalidArgumentException('maxScanRows must be >= 1');
        }
        if ($maxIngestSize < 0) {
            throw new InvalidArgumentException('maxIngestSize must be >= 0');
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        $this->maxConcurrency = $maxConcurrency;
        if ($regionCache instanceof RegionCache) {
            // Issue #474: regionInvalidated() is emitted from inside
            // RegionCache::invalidate() — give a user-supplied RegionCache
            // this client's metrics backend unless it already carries one.
            // A custom RegionCacheInterface implementation owns its own
            // metric behaviour and is left untouched. Mutates the shared
            // cache in place — never rebind or clone it.
            $regionCache->attachMetricsIfAbsent($metrics);
        }
        $regionResolver ??= new RegionResolver(
            $pdClient,
            $regionCache,
            $metrics,
            $this->allowedStoreHosts,
            $this->storeHostPolicy,
            $this->logger,
            $this->pdEndpoints,
            $this->allowedStorePorts,
            $this->epochNotMatchCodec(),
            $this->storeCache,
        );
        $this->regionResolver = $regionResolver;
        $this->regionCacheWarmer = $regionCacheWarmer
            ?? new RegionCacheWarmer($pdClient, $regionCache, $this->logger, $regionCacheRefreshMs);
        $this->crud = $crud ?? new RawKvCrud(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
            $this->replicaReadPolicy,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );
        $this->atomic = $atomic ?? new RawKvAtomic(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );
        $this->batch = $batch ?? new RawKvBatch(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
            $this->replicaReadPolicy,
            $this->maxConcurrency,
            // Experimental BatchCommands multiplexing (issue #418): only
            // wired when the flag is on; the stream pool lives with the
            // transport and reuses the GrpcClient's pooled channels.
            $batchCommands ? new BatchCommandsMultiplexer(
                GrpcBatchCommandsTransport::forClient($grpc),
            ) : null,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );
        $this->scanner = $scanner ?? new RawKvScanner(
            $pdClient,
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $maxBackoffMs,
            $serverBusyBudgetMs,
            $regionCache,
            $this->logger,
            $this->slowLogConfig,
            $this->retryDeadlineMs,
            $this->maxConcurrency,
            $this->replicaReadPolicy,
            $maxScanRows,
            metrics: $metrics,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );
        $this->rangeOps = $rangeOps ?? new RawKvRangeOps(
            $pdClient,
            $grpc,
            $regionResolver,
            $regionCache,
            $timeoutConfig,
            $maxBackoffMs,
            $serverBusyBudgetMs,
            $this->logger,
            $this->slowLogConfig,
            $this->retryDeadlineMs,
            $this->maxConcurrency,
            metrics: $metrics,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );
        $this->ingestor = new SstIngestor(
            $grpc,
            $pdClient,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $regionCache,
            $this->maxConcurrency,
            $this->maxIngestSize,
            $this->requestContext,
            $this->resourceControl,
            maxBackoffMs: $maxBackoffMs,
            serverBusyBudgetMs: $serverBusyBudgetMs,
            retryDeadlineMs: $this->retryDeadlineMs,
            metrics: $metrics,
        );
    }

    // ========================================================================
    // Atomic for CAS mode
    // ========================================================================

    public function setAtomicForCAS(bool $enabled): self
    {
        $this->atomicForCAS = $enabled;

        return $this;
    }

    public function isAtomicForCAS(): bool
    {
        return $this->atomicForCAS;
    }

    // ========================================================================
    // Column family
    // ========================================================================

    public function setColumnFamily(string $cf): self
    {
        $this->columnFamily = $cf;

        return $this;
    }

    public function getColumnFamily(): string
    {
        return $this->columnFamily;
    }

    // ========================================================================
    // Validation
    // ========================================================================

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ClientClosedException();
        }
    }

    /**
     * Normalize batch keys to strings. PHP coerces integer-like string keys
     * ("12345", "0") to int when arrays are built with array_keys(); ints are
     * cast back to strings, anything else is rejected so the string[] contract
     * is not silently weakened (issue #322).
     *
     * @param array<array-key, string|int> $keys
     * @return string[]
     *
     * @throws InvalidArgumentException
     */
    private function normalizeBatchKeys(array $keys): array
    {
        $normalized = [];
        foreach ($keys as $key) {
            if (is_int($key)) {
                $normalized[] = (string) $key;
            } elseif (is_string($key)) {
                $normalized[] = $key;
            } else {
                throw new InvalidArgumentException(sprintf(
                    'Batch keys must be strings or ints, %s given',
                    get_debug_type($key),
                ));
            }
        }

        return $normalized;
    }

    private function validateKeyNotEmpty(string $key, string $method): void
    {
        if ($key === '') {
            throw new InvalidArgumentException("Key must not be empty in {$method}");
        }
    }

    private function validateKeySize(string $key, string $method): void
    {
        $len = strlen($key);
        if ($len > self::MAX_KEY_SIZE) {
            throw new InvalidArgumentException(sprintf(
                'Key size (%d) exceeds maximum allowed size (%d) in %s',
                $len,
                self::MAX_KEY_SIZE,
                $method,
            ));
        }
    }

    private function validateValueSize(string $value, string $method): void
    {
        $len = strlen($value);
        if ($len > self::MAX_VALUE_SIZE) {
            throw new InvalidArgumentException(sprintf(
                'Value size (%d) exceeds maximum allowed size (%d) in %s',
                $len,
                self::MAX_VALUE_SIZE,
                $method,
            ));
        }
    }

    /**
     * Resolve options['retryDeadlineMs'] (see OPT_RETRY_DEADLINE) for
     * create(): wall-clock bound on one operation's retry loop. 0 disables
     * the deadline; a negative value is rejected.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveRetryDeadline(array $options): int
    {
        if (!array_key_exists(self::OPT_RETRY_DEADLINE, $options)) {
            return self::DEFAULT_RETRY_DEADLINE_MS;
        }

        $deadlineMs = $options[self::OPT_RETRY_DEADLINE];
        if (!is_int($deadlineMs)) {
            throw new InvalidArgumentException(sprintf(
                "options['retryDeadlineMs'] must be an int (milliseconds), %s given",
                get_debug_type($deadlineMs),
            ));
        }
        if ($deadlineMs < 0) {
            throw new InvalidArgumentException("options['retryDeadlineMs'] must be >= 0");
        }

        return $deadlineMs;
    }

    /**
     * Resolve options['maxConcurrency'] (see OPT_MAX_CONCURRENCY) for
     * create(): the bound on simultaneously in-flight requests used by the
     * fanned-out batch and range operations (issues #295 and #264). Must
     * be >= 1.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveMaxConcurrency(array $options): int
    {
        if (!array_key_exists(self::OPT_MAX_CONCURRENCY, $options)) {
            return self::DEFAULT_MAX_CONCURRENCY;
        }

        $maxConcurrency = $options[self::OPT_MAX_CONCURRENCY];
        if (!is_int($maxConcurrency)) {
            throw new InvalidArgumentException(sprintf(
                "options['maxConcurrency'] must be an int, %s given",
                get_debug_type($maxConcurrency),
            ));
        }
        if ($maxConcurrency < 1) {
            throw new InvalidArgumentException("options['maxConcurrency'] must be >= 1");
        }

        return $maxConcurrency;
    }

    /**
     * Resolve options['batchCommands'] (see OPT_BATCH_COMMANDS) for
     * create(): the experimental BatchCommands stream multiplexing flag
     * (issue #418). Must be a boolean, default false.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveBatchCommands(array $options): bool
    {
        if (!array_key_exists(self::OPT_BATCH_COMMANDS, $options)) {
            return false;
        }

        $batchCommands = $options[self::OPT_BATCH_COMMANDS];
        if (!is_bool($batchCommands)) {
            throw new InvalidArgumentException(sprintf(
                "options['batchCommands'] must be a boolean, %s given",
                get_debug_type($batchCommands),
            ));
        }

        return $batchCommands;
    }

    /**
     * Resolve options['regionCacheRefreshMs'] (see
     * OPT_REGION_CACHE_REFRESH_MS) for create(): the minimum interval between
     * two non-forced region-cache refreshes (issue #425). 0 disables it.
     * Must be >= 0.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveRegionCacheRefreshMs(array $options): int
    {
        if (!array_key_exists(self::OPT_REGION_CACHE_REFRESH_MS, $options)) {
            return self::DEFAULT_REGION_CACHE_REFRESH_MS;
        }

        $refreshMs = $options[self::OPT_REGION_CACHE_REFRESH_MS];
        if (!is_int($refreshMs)) {
            throw new InvalidArgumentException(sprintf(
                "options['regionCacheRefreshMs'] must be an int (milliseconds), %s given",
                get_debug_type($refreshMs),
            ));
        }
        if ($refreshMs < 0) {
            throw new InvalidArgumentException("options['regionCacheRefreshMs'] must be >= 0");
        }

        return $refreshMs;
    }

    /**
     * Resolve options['maxScanRows'] (see OPT_MAX_SCAN_ROWS) for create():
     * the row cap for an unbounded (`limit: 0`) scan (issue #191). Must be
     * >= 1.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveMaxScanRows(array $options): int
    {
        if (!array_key_exists(self::OPT_MAX_SCAN_ROWS, $options)) {
            return self::DEFAULT_MAX_SCAN_ROWS;
        }

        $maxScanRows = $options[self::OPT_MAX_SCAN_ROWS];
        if (!is_int($maxScanRows)) {
            throw new InvalidArgumentException(sprintf(
                "options['maxScanRows'] must be an int, %s given",
                get_debug_type($maxScanRows),
            ));
        }
        if ($maxScanRows < 1) {
            throw new InvalidArgumentException("options['maxScanRows'] must be >= 1");
        }

        return $maxScanRows;
    }

    /**
     * Resolve options['resourceGroup'] and options['resourceGroupTag'] (see
     * OPT_RESOURCE_GROUP / OPT_RESOURCE_GROUP_TAG) for create(): resource
     * control metadata applied to every request (issue #427). Both default
     * to unset, which writes no resource-control field at all.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveResourceControl(array $options): ResourceControl
    {
        return ResourceControl::fromOptions($options);
    }

    /**
     * Resolve options['maxIngestSize'] (see OPT_MAX_INGEST_SIZE) for
     * create(): the opt-in payload bound of one ingest() call (issue #297).
     * 0 means unlimited; a negative value is rejected.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveMaxIngestSize(array $options): int
    {
        if (!array_key_exists(self::OPT_MAX_INGEST_SIZE, $options)) {
            return 0;
        }

        $maxIngestSize = $options[self::OPT_MAX_INGEST_SIZE];
        if (!is_int($maxIngestSize)) {
            throw new InvalidArgumentException(sprintf(
                "options['maxIngestSize'] must be an int, %s given",
                get_debug_type($maxIngestSize),
            ));
        }
        if ($maxIngestSize < 0) {
            throw new InvalidArgumentException("options['maxIngestSize'] must be >= 0");
        }

        return $maxIngestSize;
    }

    /**
     * Resolve options['replicaRead'] (see OPT_REPLICA_READ) for create():
     * the read preference applied to reads (issue #421). Must be a
     * ReplicaReadPolicy instance when set.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveReplicaReadPolicy(array $options): ReplicaReadPolicy
    {
        if (!array_key_exists(self::OPT_REPLICA_READ, $options)) {
            return new ReplicaReadPolicy();
        }

        $policy = $options[self::OPT_REPLICA_READ];
        if (!$policy instanceof ReplicaReadPolicy) {
            throw new InvalidArgumentException(sprintf(
                "options['replicaRead'] must be a %s, %s given",
                ReplicaReadPolicy::class,
                get_debug_type($policy),
            ));
        }

        return $policy;
    }

    /**
     * Resolve options['requestContext'] (see OPT_REQUEST_CONTEXT) for
     * create(): the optional Context control fields applied to every
     * request (issue #426). Must be a RequestContextOptions instance when
     * set; absent or null means "apply none".
     *
     * @param array<string, mixed> $options
     */
    private static function resolveRequestContext(array $options): ?RequestContextOptions
    {
        if (!array_key_exists(self::OPT_REQUEST_CONTEXT, $options) || $options[self::OPT_REQUEST_CONTEXT] === null) {
            return null;
        }

        $requestContext = $options[self::OPT_REQUEST_CONTEXT];
        if (!$requestContext instanceof RequestContextOptions) {
            throw new InvalidArgumentException(sprintf(
                "options['requestContext'] must be a %s, %s given",
                RequestContextOptions::class,
                get_debug_type($requestContext),
            ));
        }

        return $requestContext;
    }

    /**
     * The codec for EpochNotMatch payload boundaries, or null when they are
     * already in user-key space (issue #246).
     *
     * ApiV2GrpcClient decodes every response region boundary before the
     * error reaches the client, so applying a V2 codec a second time would
     * fail or corrupt the cached range.
     */
    private function epochNotMatchCodec(): ?CodecInterface
    {
        return $this->codec instanceof CodecV2 ? null : $this->codec;
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
            deadlineMs: $this->retryDeadlineMs,
            metrics: $this->metrics,
        );
    }

    // ========================================================================
    // Single-key operations
    // ========================================================================

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
     */
    public function get(string $key): ?string
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'get');
        $this->validateKeySize($key, 'get');

        return $this->crud->get($key, $this->createRetryExecutor(), $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
     */
    public function put(string $key, string $value, int $ttl = 0): void
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'put');
        $this->validateKeySize($key, 'put');
        $this->validateValueSize($value, 'put');

        $this->crud->put($key, $value, $ttl, $this->createRetryExecutor(), $this->atomicForCAS, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
     */
    public function delete(string $key): void
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'delete');
        $this->validateKeySize($key, 'delete');

        $this->crud->delete($key, $this->createRetryExecutor(), $this->atomicForCAS, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
     */
    public function getKeyTTL(string $key): ?int
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'getKeyTTL');
        $this->validateKeySize($key, 'getKeyTTL');

        return $this->crud->getKeyTTL($key, $this->createRetryExecutor(), $this->columnFamily);
    }

    // ========================================================================
    // Atomic operations
    // ========================================================================

    /**
     * Atomically compare-and-swap a raw value.
     *
     * NOT IDEMPOTENT: if the server applies the CAS but the response is lost
     * (transport error), the outcome is indeterminate — the swap may have
     * succeeded even though this call throws. Transport-level gRPC errors
     * (e.g. DEADLINE_EXCEEDED, UNAVAILABLE) are therefore NOT retried and
     * propagate as CasIndeterminateException, which preserves the original
     * GrpcException as `getPrevious()`; re-read the key to determine the
     * actual outcome (issue #239). Region errors (NotLeader, EpochNotMatch,
     * …) are rejected before the write is proposed and are still retried
     * automatically. A transport failure while resolving the region or store
     * (PD unreachable), or an application-level rejection of the request
     * itself, happens before the swap is applied, so it stays a plain
     * GrpcException and means the swap definitively did not happen.
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws RegionException
     * @throws CasIndeterminateException NOT retried — the transport failure
     *         leaves the swap outcome indeterminate (the write may have been
     *         applied); the original GrpcException is preserved as `previous`
     * @throws GrpcException Only when the failure happened before the CAS
     *         request was applied (e.g. PD unreachable during region lookup,
     *         or an application-level rejection such as UNAUTHENTICATED);
     *         the swap definitively did not happen
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
     */
    public function compareAndSwap(string $key, ?string $expectedValue, string $newValue, int $ttl = 0): CasResult
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'compareAndSwap');
        $this->validateKeySize($key, 'compareAndSwap');
        $this->validateValueSize($newValue, 'compareAndSwap');

        if (!$this->atomicForCAS) {
            throw new InvalidStateException('CompareAndSwap requires atomic mode (enable via setAtomicForCAS(true))');
        }

        return $this->atomic->compareAndSwap(
            $key,
            $expectedValue,
            $newValue,
            $ttl,
            $this->createRetryExecutor(),
            $this->columnFamily,
        );
    }

    /**
     * Put a value only if the key does not already exist (via CAS).
     *
     * NOT IDEMPOTENT — see compareAndSwap() for the indeterminate-outcome
     * contract: a thrown CasIndeterminateException means the outcome is
     * unknown (the write may have been applied); re-read the key to decide.
     * Distributed lock users must treat it as indeterminate, never as
     * "not acquired" (issue #239). A plain GrpcException on this method
     * means the request was never sent or was rejected before it was applied,
     * so the lock was definitively not acquired.
     *
     * @return null|string Null if the key did not exist and the value was stored; the
     *              existing value otherwise
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws RegionException
     * @throws CasIndeterminateException NOT retried — the transport failure
     *         leaves the swap outcome indeterminate (the value may have been
     *         stored); the original GrpcException is preserved as `previous`
     * @throws GrpcException Only when the failure happened before the CAS
     *         request was applied (e.g. PD unreachable during region lookup,
     *         or an application-level rejection such as UNAUTHENTICATED);
     *         the value definitively was not stored
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for the key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs the region
     * @throws RetryBudgetExhaustedException When the retry attempt cap or the
     *         wall-clock deadline is exhausted
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
     * The returned map inherits PHP's array-key semantics: only a canonical
     * decimal-integer key (`'12345'`, `'0'`, `'-5'`) **that also fits in a PHP
     * int** is returned under its `int` form. Every other key form stays a
     * `string` key — leading zeros `'01000'`, `'1e3'`, `'+1000'`, `'-0'`,
     * non-numeric, and equally a canonical decimal that overflows, such as
     * `'9223372036854775808'` (PHP_INT_MAX + 1), which a 20-digit TiKV key
     * (big-int IDs, nanosecond epochs) can be. PHP casts a string lookup the
     * same way, so every entry remains addressable by the exact bytes that
     * were requested — `$results['1000']` finds the entry stored as int 1000.
     * The key type is therefore `array-key`, not `string` (issue #261; the full
     * rule is in `docs/helpers/faq.md`).
     *
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<array-key, ?string>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the batch fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws KeyErrorException With `options['batchCommands']` enabled, a
     *     per-pair key error on the multiplexed response escapes directly; on
     *     the default unary path it is wrapped in `BatchPartialFailureException`
     * @throws StoreNotFoundException With `options['batchCommands']` enabled,
     *     the multiplexed entry setup resolves store addresses outside the
     *     per-region retry wrapper. On the default unary path a missing store
     *     is reported inside `BatchPartialFailureException`
     * @throws TiKvException when a key's region cannot be resolved. Fail-closed:
     *     the call transmits nothing rather than applying a subset of the batch
     *     (issue #187).
     */
    public function batchGet(array $keys): array
    {
        $this->ensureOpen();

        if ($keys === []) {
            return [];
        }

        $keys = $this->normalizeBatchKeys($keys);

        foreach ($keys as $key) {
            $this->validateKeyNotEmpty($key, 'batchGet');
            $this->validateKeySize($key, 'batchGet');
        }

        return $this->batch->batchGet($keys, $this->createRetryExecutor(), $this->columnFamily);
    }

    /**
     * Write multiple key-value pairs.
     *
     * Keys inherit PHP's array-key semantics: only canonical decimal-integer
     * strings (`'12345'`, `'0'`) are coerced to int array keys, while
     * leading-zero forms (`'00'`, `'0123'`) stay string keys. Each key is
     * string-cast before it reaches the wire, so a coerced key is written
     * under its original bytes, but use per-key `put()` when a canonical
     * decimal key must stay distinct from its integer form (issue #192/RAW-07).
     *
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the batch fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws StoreNotFoundException With `options['batchCommands']` enabled,
     *     the multiplexed entry setup resolves store addresses outside the
     *     per-region retry wrapper. On the default unary path a missing store
     *     is reported inside `BatchPartialFailureException`
     * @throws TiKvException when a key's region cannot be resolved. Fail-closed:
     *     the call transmits nothing rather than applying a subset of the batch
     *     (issue #187).
     */
    public function batchPut(array $keyValuePairs, int|array $ttl = 0): void
    {
        $this->ensureOpen();

        if ($keyValuePairs === []) {
            return;
        }

        foreach ($keyValuePairs as $key => $value) {
            // PHP coerces integer-like string keys ("12345", "0") to int
            // array keys; string-cast before validating (issue #322).
            $key = (string) $key;
            $this->validateKeyNotEmpty($key, 'batchPut');
            $this->validateKeySize($key, 'batchPut');
            $this->validateValueSize($value, 'batchPut');
        }

        $this->batch->batchPut(
            $keyValuePairs,
            $ttl,
            $this->createRetryExecutor(),
            $this->atomicForCAS,
            $this->columnFamily,
        );
    }

    /**
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the batch fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws StoreNotFoundException With `options['batchCommands']` enabled,
     *     the multiplexed entry setup resolves store addresses outside the
     *     per-region retry wrapper. On the default unary path a missing store
     *     is reported inside `BatchPartialFailureException`
     * @throws TiKvException when a key's region cannot be resolved. Fail-closed:
     *     the call transmits nothing rather than applying a subset of the batch
     *     (issue #187).
     */
    public function batchDelete(array $keys): void
    {
        $this->ensureOpen();

        if ($keys === []) {
            return;
        }

        $keys = $this->normalizeBatchKeys($keys);

        foreach ($keys as $key) {
            $this->validateKeyNotEmpty($key, 'batchDelete');
            $this->validateKeySize($key, 'batchDelete');
        }

        $this->batch->batchDelete($keys, $this->createRetryExecutor(), $this->atomicForCAS, $this->columnFamily);
    }

    // ========================================================================
    // Scan operations
    // ========================================================================

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException When batchSize is below 1 or above
     *         MAX_SCAN_LIMIT, or when the range is reversed (`$startKey` sorts
     *         after a non-empty `$endKey`). The scan RPCs run during
     *         iteration; the exceptions they raise are documented on
     *         {@see ScanIterator}.
     */
    public function scanIterator(
        string $startKey,
        string $endKey,
        int $batchSize = 1024,
        bool $keyOnly = false,
    ): ScanIterator {
        $this->ensureOpen();

        return $this->scanner->scanIterator(
            $startKey,
            $endKey,
            $batchSize,
            $keyOnly,
            $this->columnFamily,
            $this->ensureOpen(...),
        );
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException When batchSize is below 1 or above
     *         MAX_SCAN_LIMIT. The scan RPCs run during iteration; the
     *         exceptions they raise are documented on {@see ScanIterator}.
     */
    public function scanPrefixIterator(string $prefix, int $batchSize = 1024, bool $keyOnly = false): ScanIterator
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefixIterator(
            $prefix,
            $batchSize,
            $keyOnly,
            $this->columnFamily,
            $this->ensureOpen(...),
        );
    }

    /**
     * Range scan over [startKey, endKey).
     *
     * `limit = 0` means **the whole range**: the call pages internally
     * ({@see RawKvClient::MAX_SCAN_LIMIT} = 10240 rows per RPC) and collects
     * every row, so it is no longer silently truncated at 10240 (issue #191).
     * Because the whole result is still buffered in one PHP array, the total
     * is bounded by `options['maxScanRows']` (default
     * {@see RawKvClient::DEFAULT_MAX_SCAN_ROWS} = 100000); exceeding it
     * throws {@see \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException}
     * rather than truncating (issue #440).
     *
     * **A scan never truncates silently** (issue #440). An unbounded scan
     * that hits the `maxScanRows` guard throws, so a returned result is the
     * whole range; a bounded scan (`limit > 0`) returns at most `limit` rows,
     * so a result whose count equals the limit may be truncated — compare
     * the count with the limit to find out.
     *
     * For ranges larger than `maxScanRows`, or to keep memory flat at all,
     * use {@see RawKvClient::scanIterator()} instead: it pages the range with
     * constant memory and has no row cap.
     *
     * @param int $limit Maximum rows to return; 0 = the whole range
     *                   (paginated internally, guarded by maxScanRows). A
     *                   negative value throws InvalidArgumentException before
     *                   any RPC
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException When the limit is invalid or the range
     *         is reversed (`$startKey` sorts after a non-empty `$endKey`)
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchDeadlineExceededException When a page fanned out across
     *         several regions exceeds `options['timeout']['batchDeadlineMs']`
     * @throws KeyErrorException When the response carries a per-pair key error
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for a key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs a region
     * @throws RetryBudgetExhaustedException When a region's retry attempt cap
     *         or the operation's wall-clock deadline is exhausted
     * @throws \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException When
     *         limit is 0 and more than `options['maxScanRows']` rows match
     */
    public function scan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $this->validateKeySize($startKey, 'scan');
        if ($endKey !== '') {
            $this->validateKeySize($endKey, 'scan');
        }

        return $this->scanner->scan($startKey, $endKey, $limit, $keyOnly, $this->columnFamily);
    }

    /**
     * Prefix scan: convenience wrapper around {@see RawKvClient::scan()}.
     *
     * `limit = 0` returns the whole prefix (paginated internally at
     * {@see RawKvClient::MAX_SCAN_LIMIT} = 10240 rows per RPC and guarded by
     * `options['maxScanRows']`, see {@see RawKvClient::scan()}); exceeding
     * that guard throws
     * {@see \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException}
     * instead of truncating (issue #440). A bounded scan (`limit > 0`)
     * returns at most `limit` rows, so a result whose count equals the limit
     * may be truncated. A negative `limit` throws `InvalidArgumentException`
     * before any RPC.
     *
     * For large prefixes prefer {@see RawKvClient::scanPrefixIterator()},
     * which pages the prefix with constant memory and has no row cap.
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchDeadlineExceededException When a page fanned out across
     *         several regions exceeds `options['timeout']['batchDeadlineMs']`
     * @throws KeyErrorException When the response carries a per-pair key error
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for a key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs a region
     * @throws RetryBudgetExhaustedException When a region's retry attempt cap
     *         or the operation's wall-clock deadline is exhausted
     * @throws \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException When
     *         limit is 0 and more than `options['maxScanRows']` rows match
     */
    public function scanPrefix(string $prefix, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefix($prefix, $limit, $keyOnly, $this->columnFamily);
    }

    /**
     * Reverse range scan over [endKey, startKey), returned in descending
     * order (`startKey` = exclusive upper bound, `endKey` = lower bound).
     *
     * `limit = 0` means **the whole range**: the call pages internally
     * (moving the descending upper bound to the lowest key of each full
     * page, {@see RawKvClient::MAX_SCAN_LIMIT} = 10240 rows per RPC) and
     * collects every row (issue #191). The total is bounded by
     * `options['maxScanRows']` (default
     * {@see RawKvClient::DEFAULT_MAX_SCAN_ROWS} = 100000); exceeding it
     * throws {@see \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException}
     * rather than truncating (issue #440).
     *
     * **A scan never truncates silently** (issue #440). An unbounded scan
     * that hits the `maxScanRows` guard throws; a bounded scan (`limit > 0`)
     * returns at most `limit` rows, so a result whose count equals the limit
     * may be truncated — compare the count with the limit to find out.
     *
     * There is no reverse iterator, so for a very large reverse range raise
     * `maxScanRows` consciously or page it in the caller; the unbounded
     * forward alternative is {@see RawKvClient::scanIterator()}. An unbounded
     * reverse scan fans its per-region reads out concurrently in windows of
     * at most `options['maxConcurrency']` regions (default 16).
     *
     * Reverse scan has no +infinity upper-bound sentinel: `$startKey` is an
     * explicit exclusive upper bound, and an empty `$startKey` with a
     * non-empty `$endKey` is a reversed range that throws
     * {@see InvalidArgumentException} before any RPC. This differs from the
     * forward operations, where an empty start key means the start of the
     * keyspace. `reverseScan('', '')` is the documented empty range.
     *
     * @param int $limit Maximum rows to return; 0 = the whole range
     *                   (paginated internally, guarded by maxScanRows). A
     *                   negative value throws InvalidArgumentException before
     *                   any RPC
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException When the limit is invalid, `$startKey`
     *         is empty with a non-empty `$endKey`, or the range is reversed
     *         (`$startKey` sorts before a non-empty `$endKey`)
     * @throws RegionException
     * @throws GrpcException
     * @throws KeyErrorException When the response carries a per-pair key error
     * @throws TiKvException When a region lookup fails closed (for example PD
     *         returned no region for a key)
     * @throws StoreNotFoundException When PD no longer knows the store that
     *         backs a region
     * @throws RetryBudgetExhaustedException When a region's retry attempt cap
     *         or the operation's wall-clock deadline is exhausted
     * @throws \CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException When
     *         limit is 0 and more than `options['maxScanRows']` rows match
     */
    public function reverseScan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $this->validateKeySize($startKey, 'reverseScan');
        if ($endKey !== '') {
            $this->validateKeySize($endKey, 'reverseScan');
        }

        return $this->scanner->reverseScan($startKey, $endKey, $limit, $keyOnly, $this->columnFamily);
    }

    /**
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException When `eachLimit` is invalid or a range
     *         is reversed (`$startKey` sorts after a non-empty `$endKey`)
     * @throws BatchPartialFailureException A per-range or per-region failure —
     *     including a `RegionException`, a `GrpcException`, a region's
     *     exhausted retry budget or a missing store — is reported inside
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the range fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     */
    public function batchScan(array $ranges, int $eachLimit, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        if ($ranges === []) {
            return [];
        }

        return $this->scanner->batchScan($ranges, $eachLimit, $keyOnly, $this->columnFamily);
    }

    // ========================================================================
    // Range operations
    // ========================================================================

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException When the range is reversed
     *         (`$startKey` sorts after a non-empty `$endKey`)
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the range fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws TiKvException When the pre-fan-out PD region enumeration throws
     *     the base type (for example from a custom `PdClientInterface`)
     */
    public function deleteRange(string $startKey, string $endKey): void
    {
        $this->ensureOpen();

        $this->rangeOps->deleteRange($startKey, $endKey, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the range fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws TiKvException When the pre-fan-out PD region enumeration throws
     *     the base type (for example from a custom `PdClientInterface`)
     */
    public function deletePrefix(string $prefix): void
    {
        $this->ensureOpen();

        if ($prefix === '') {
            throw new InvalidArgumentException('Prefix must not be empty -- refusing to delete all keys');
        }

        if (RawKvSplitter::calculatePrefixEndKey($prefix) === '') {
            throw new InvalidArgumentException(
                'Prefix consists entirely of 0xFF bytes and has no representable'
                . ' upper bound; refusing to delete to end of keyspace'
            );
        }

        $this->rangeOps->deletePrefix($prefix, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException When the range is reversed
     *         (`$startKey` sorts after a non-empty `$endKey`)
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException A region whose retry loop exhausted
     *     its budget contributes a `RetryBudgetExhaustedException` to
     *     `getRegionErrors()`; it is wrapped, not thrown directly
     * @throws BatchDeadlineExceededException When the range fan-out exceeds
     *     `options['timeout']['batchDeadlineMs']`
     * @throws TiKvException When the pre-fan-out PD region enumeration throws
     *     the base type (for example from a custom `PdClientInterface`)
     */
    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $this->ensureOpen();

        return $this->rangeOps->checksum($startKey, $endKey);
    }

    // ========================================================================
    // SST Ingest (bulk import)
    // ========================================================================

    /**
     * Bulk-import key-value pairs into TiKV via SST ingestion.
     *
     * This bypasses the normal Raft write path and directly ingests pre-sorted
     * data into TiKV regions, achieving much higher throughput for large data
     * loads.
     *
     * The key-value pairs are sorted by key, grouped by region, and written
     * as SST files via the TiKV ImportSST service. All TiKV stores are
     * switched to import mode during the operation and switched back to
     * normal mode on completion, including on any exception. Process death
     * (OOM kill, deploy restart, `kill -9`) bypasses that `finally` and
     * leaves every store in import mode; see the operations guide for the
     * manual recovery.
     *
     * Each region group is retried on retryable region errors (`NotLeader`,
     * `EpochNotMatch`, `ServerIsBusy`, …) against a freshly resolved region,
     * with a fresh retry budget per group (a long import is not capped by
     * `retryDeadlineMs`).
     * The import is still **not atomic across regions**: when a later group
     * fails permanently, the earlier groups keep their data and the failure
     * is reported as {@see \CrazyGoat\TiKV\Client\Exception\IngestPartialFailureException},
     * which lists the applied region IDs. Re-running the same key-value pairs
     * is the recommended recovery (it is idempotent; a TTL restarts).
     *
     * Keys inherit PHP's array-key semantics: only canonical decimal-integer
     * strings (`'12345'`, `'0'`) are coerced to int array keys, while
     * leading-zero forms (`'00'`, `'0123'`) stay string keys. Each key is
     * string-cast before it reaches the wire, so a coerced key is written
     * under its original bytes, but use per-key `put()` when a canonical
     * decimal key must stay distinct from its integer form (issue #192/RAW-07).
     *
     * @param array<string, string> $keyValuePairs Key-value pairs (sorted or unsorted)
     * @param int|null $ttl Time-to-live in seconds (null = no TTL)
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException When a key is empty or a key/value
     *         exceeds the configured size limit
     * @throws IngestTooLargeException When the payload exceeds
     *         `options['maxIngestSize']`, before any RPC is issued (issue #297)
     * @throws GrpcException
     * @throws RegionException
     * @throws InvalidStoreAddressException When PD returns a store address
     *         that fails validation (malformed or outside the allowed set)
     * @throws StoreNotFoundException When PD no longer knows a store that must
     *         receive an SST. Region resolution is retried; this is the fatal
     *         verdict the retry executor rethrows
     * @throws RetryBudgetExhaustedException When a region's retry loop
     *         exhausts its attempt cap or wall-clock deadline before any region
     *         group was applied (issue #197)
     * @throws \CrazyGoat\TiKV\Client\Exception\IngestPartialFailureException
     *         When at least one region group was already ingested and a later
     *         one failed permanently; the exception lists the applied region
     *         IDs (issue #197)
     * @throws BatchDeadlineExceededException When the store-mode fan-out
     *         exceeds `options['timeout']['batchDeadlineMs']`
     * @throws TiKvException when a key's region cannot be resolved. Fail-closed:
     *     the call transmits nothing rather than applying a subset of the pairs
     *     (issue #187).
     */
    public function ingest(array $keyValuePairs, ?int $ttl = null): void
    {
        $this->ensureOpen();

        if ($keyValuePairs === []) {
            return;
        }

        foreach ($keyValuePairs as $key => $value) {
            // PHP coerces integer-like string keys ("12345", "0") to int
            // array keys; string-cast before validating (issue #322).
            $key = (string) $key;
            $this->validateKeyNotEmpty($key, 'ingest');
            $this->validateKeySize($key, 'ingest');
            $this->validateValueSize($value, 'ingest');
        }

        $this->ingestor->ingest($keyValuePairs, $ttl);
    }

    // ========================================================================
    // Cluster ID
    // ========================================================================

    /**
     * Get the learned cluster ID, or null if not yet discovered.
     */
    public function getClusterId(): ?int
    {
        return $this->pdClient->getClusterId();
    }

    // ========================================================================
    // Health Check
    // ========================================================================

    /**
     * Probe the PD cluster by issuing a {@see GetMembers} RPC and return
     * the learned cluster ID.
     *
     * Use as a lightweight health check in load-balancer probes,
     * Kubernetes readiness checks, etc. The call does not touch any
     * user data and never fails with "no region" — it only fails when
     * PD is unreachable or rejects the request.
     *
     * Returns the cluster ID on success. Returns null if PD responded
     * but the response header did not carry a cluster ID or reported `0`
     * (callers may treat null as "reachable but identity unknown"). When
     * `options['expectedClusterId']` is set, a response without a
     * cluster ID is a protocol violation and the probe fails with
     * HealthCheckException instead of returning null.
     *
     * @throws ClientClosedException When the client has been closed
     * @throws HealthCheckException When the PD probe fails
     */
    public function healthCheck(): ?int
    {
        $this->ensureOpen();

        try {
            return $this->pdClient->ping();
        } catch (ClientClosedException $e) {
            throw $e;
        } catch (\Throwable $e) {
            throw new HealthCheckException(
                sprintf('PD health check failed: %s', $e->getMessage()),
                $e,
            );
        }
    }

    // ========================================================================
    // Metrics
    // ========================================================================

    /**
     * Return the metrics implementation in use.
     *
     * The returned object is the same instance passed via
     * RawKvClient::create() options['metrics'], or the no-op default
     * when no metrics backend was configured. Use to inspect counters
     * in-process (e.g. {@see InMemoryMetrics}) without consuming the
     * exported Prometheus / StatsD stream.
     */
    public function getMetrics(): MetricsInterface
    {
        return $this->metrics;
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    /**
     * Drop the cached gRPC channels that were idle longer than `idleTtlMs`.
     *
     * The transport sweeps by itself when a request arrives. A long-running
     * worker that can be quiet for a long time calls this from its own tick.
     * No-op if the client is closed.
     */
    public function reapIdleChannels(): void
    {
        if ($this->closed) {
            return;
        }

        $this->grpc->reapIdleChannels();
    }

    /**
     * Refresh the cached regions from PD (issue #425).
     *
     * PHP has no background threads, so nothing runs on its own: a long-lived
     * worker calls this from its own tick. A non-forced call performs at most
     * one refresh per `options['regionCacheRefreshMs']` (default 0, i.e.
     * disabled) and returns 0 when the interval has not elapsed.
     * `$force = true` bypasses that interval only; it does not remove the
     * per-tick bound below.
     *
     * One call re-resolves at most
     * `RegionCacheWarmer::DEFAULT_MAX_REGIONS_PER_REFRESH` (128) cached
     * regions through a PD `GetRegion` on each region's start key,
     * round-robin: successive calls continue where the previous one stopped,
     * so the whole cache is refreshed over several ticks rather than in one
     * unbounded burst. Within that slice a newer epoch or leader updates the
     * entry in place, a split or merge supersedes it (the replacement is
     * stored and the stale entry is dropped silently), and an answer that no
     * longer covers the entry's start key drops it. The first PD failure
     * aborts the tick, is logged at warning level and keeps the cached
     * entries, so this method never throws. No-op after `close()`.
     *
     * @return int number of distinct regions the cache still holds after the
     *         tick (a merge or split arriving later in the tick counts once,
     *         a leaderless answer counts zero)
     */
    public function refreshRegionCache(bool $force = false): int
    {
        if ($this->closed) {
            return 0;
        }

        return $this->regionCacheWarmer->refresh($force);
    }

    /**
     * Resolve and cache the regions of the given ranges without serving a
     * user read (issue #425).
     *
     * Each range is a `[startKey, endKey]` pair of half-open bounds, where an
     * empty `endKey` means +infinity (the same convention as `scan()`). Use it
     * at worker boot to warm the keys the worker will touch. A PD failure is
     * logged and leaves the existing cache untouched; it is never thrown.
     * No-op after `close()`.
     *
     * @param array<array-key, mixed> $ranges list of `[startKey, endKey]` pairs
     * @return int number of regions cached
     *
     * @throws InvalidArgumentException when a range is not a pair of strings or
     *         is reversed (`startKey` sorts after a non-empty `endKey`)
     */
    public function preloadRegions(array $ranges): int
    {
        if ($this->closed) {
            return 0;
        }

        return $this->regionCacheWarmer->preload($ranges);
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        try {
            $this->regionCache->clear();
        } catch (\Throwable $e) {
            $this->logger->error('Failed to clear region cache', ['exception' => $e]);
        }

        try {
            // A transport supplied by the application (options['grpcClient'])
            // belongs to the application — closing it here would break every
            // other client sharing it.
            if ($this->ownsGrpcClient) {
                $this->grpc->close();
            }
        } catch (\Throwable $e) {
            $this->logger->error('Failed to close gRPC client', ['exception' => $e]);
        }

        try {
            $this->pdClient->close();
        } catch (\Throwable $e) {
            $this->logger->error('Failed to close PD client', ['exception' => $e]);
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
