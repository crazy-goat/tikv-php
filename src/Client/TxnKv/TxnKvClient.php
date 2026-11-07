<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use Closure;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Cache\StoreCacheInterface;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\SafePointCache;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\HealthCheckException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\RegionCacheWarmer;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TxnKvClient
{
    public const OPT_TIMEOUT = 'timeout';
    public const OPT_METRICS = 'metrics';

    /** API protocol version: 0/1 (V1, default) or 2 (keyspace-aware). */
    public const OPT_API_VERSION = 'apiVersion';

    /** API V2 keyspace name; omitted or empty means DEFAULT. */
    public const OPT_KEYSPACE = 'keyspace';

    /**
     * options[] key for the per-operation retry deadline in milliseconds —
     * the wall-clock bound on the retry executor's blocking backoff loop
     * (issue #294). 0 disables the deadline (not recommended under PHP-FPM).
     */
    public const OPT_RETRY_DEADLINE = 'retryDeadlineMs';

    /**
     * options[] key for per-client GC safe-point validation (issue #422).
     * true (default): every begin() validates the fresh start timestamp
     * against a cached GC safe point and throws TxnAbortedByGcException
     * when PD's safe point has already passed it. false: no validation.
     */
    public const OPT_GC_SAFE_POINT_VALIDATION = 'gcSafePointValidation';

    /**
     * options[] key for the read preference (issue #421): a
     * \CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy instance selecting
     * which replica serves the transaction's reads (leader default,
     * follower, mixed, prefer-leader), optional store-label matching and
     * the stale-read flag. Commits always target the leader.
     */
    public const OPT_REPLICA_READ = 'replicaRead';

    /**
     * options[] key for the client-wide resource group (issue #427). Every
     * transaction begun by this client inherits it unless begin() overrides
     * it. Sent as `Context.resource_control_context.resource_group_name`.
     * Default: unset = no resource-control fields. The name must be 1 to 64
     * characters of letters, digits, underscores or hyphens.
     */
    public const OPT_RESOURCE_GROUP = ResourceControl::OPTION_RESOURCE_GROUP;

    /**
     * options[] key for the client-wide opaque resource-group tag (issue
     * #427), sent as `Context.resource_group_tag`. Default: unset = the
     * field is not sent. Must be a non-empty string.
     */
    public const OPT_RESOURCE_GROUP_TAG = ResourceControl::OPTION_RESOURCE_GROUP_TAG;

    /**
     * options[] key for the optional `kvrpcpb.Context` control fields
     * (issue #426, GAP-12): a
     * {@see \CrazyGoat\TiKV\Client\Region\RequestContextOptions} instance
     * carrying the isolation level, not-fill-cache flag, server-side
     * max-execution duration, request source, resource group tag, busy
     * threshold and resource group name applied to every request the
     * transactions of this client send. Omitted or null (the default)
     * applies none of them and leaves the serialized Context unchanged.
     *
     * A transaction may replace it through the `requestContext` option of
     * {@see self::begin()}; the transaction-level value wins.
     */
    public const OPT_REQUEST_CONTEXT = 'requestContext';

    /**
     * options[] key for the GC safe-point cache refresh interval in
     * milliseconds (issue #422). Default 30000 (30s). Must be >= 1.
     */
    public const OPT_GC_SAFE_POINT_REFRESH_MS = 'gcSafePointRefreshMs';

    /**
     * options[] key for the region-cache refresh interval in milliseconds
     * (issue #425). A long-running worker calls
     * {@see TxnKvClient::refreshRegionCache()} from its own tick; the method
     * performs at most one refresh per interval. 0 (the default) disables the
     * throttled refresh entirely, so nothing changes for an application that
     * does not opt in; a forced refresh still works. Must be >= 0.
     */
    public const OPT_REGION_CACHE_REFRESH_MS = 'regionCacheRefreshMs';

    /** Default region-cache refresh interval: disabled (issue #425). */
    public const DEFAULT_REGION_CACHE_REFRESH_MS = RegionCacheWarmer::DEFAULT_REFRESH_INTERVAL_MS;

    /**
     * begin() options[] key for one-phase commit (issue #419): a
     * single-region transaction under the key limit commits in one
     * prewrite round trip (try_one_pc). false (default): always two-phase.
     */
    public const OPT_ENABLE_1PC = 'enable1Pc';

    /**
     * begin() options[] key for async commit (issue #419): small
     * transactions commit without the commit phase — the commit timestamp
     * is derived from the prewrite's min_commit_ts and readers resolve the
     * primary lock via CheckTxnStatus / CheckSecondaryLocks. false
     * (default): always the full two-phase commit.
     */
    public const OPT_ENABLE_ASYNC_COMMIT = 'enableAsyncCommit';

    /**
     * options[] key controlling when pessimistic write locks are acquired.
     * true (default) acquires the physical lock inside set()/delete(); false
     * preserves the legacy deferred lock-at-commit behaviour.
     */
    public const OPT_EAGER_PESSIMISTIC_LOCKS = 'eagerPessimisticLocks';

    /**
     * begin() options[] key for the pessimistic-lock wait timeout in
     * milliseconds (issue #443, DIV-07): how long TiKV may block on a
     * contended key before it answers with a "locked" error, which the
     * client then retries within its lock-wait budget. Default:
     * {@see Transaction::DEFAULT_LOCK_WAIT_TIME_MS} (1000 ms, the same value
     * TiKV applies by default). 0 means "let TiKV use its own default
     * timeout" — the only value that is not shrunk as the budget is
     * consumed. Must be >= 0.
     */
    public const OPT_LOCK_WAIT_TIME_MS = 'lockWaitTimeMs';

    /**
     * begin() options[] key for the optimistic lock TTL baseline in
     * milliseconds (issue #429). The prewrite adds 10 ms per mutation to it
     * and caps the result at 120000 ms. Default: 3000. Must be between 1 and
     * 120000.
     */
    public const OPT_OPTIMISTIC_LOCK_TTL_MS = 'optimisticLockTtlMs';

    /**
     * begin() options[] key for the pessimistic lock TTL in milliseconds
     * (issue #429). A pessimistic transaction extends the primary lock
     * automatically once half of this TTL has elapsed. Default: 30000. Must
     * be between 1 and 120000.
     */
    public const OPT_PESSIMISTIC_LOCK_TTL_MS = 'pessimisticLockTtlMs';

    /**
     * begin() options[] key for the maximum number of entries in the
     * transaction's write set (issue #430). 0 (default) = unlimited.
     * Exceeding the limit throws
     * {@see \CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException}
     * from set()/delete() before any RPC is issued.
     */
    public const OPT_MAX_TXN_ENTRY_COUNT = 'maxTxnEntryCount';

    /**
     * begin() options[] key for the maximum bytes of one write-set entry,
     * measured as key length plus value length (issue #430). 0 (default) =
     * unlimited. A delete contributes its key length only.
     */
    public const OPT_MAX_TXN_ENTRY_SIZE = 'maxTxnEntrySize';

    /**
     * begin() options[] key for the maximum bytes of the whole write set,
     * measured as the sum of every entry's key plus value length (issue
     * #430). 0 (default) = unlimited.
     */
    public const OPT_MAX_TXN_TOTAL_SIZE = 'maxTxnTotalSize';

    /**
     * begin() options[] key for the maximum number of entries in the
     * transaction's read set (issue #297). 0 (default) = unlimited.
     * Exceeding the limit throws
     * {@see \CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException}
     * from get()/batchGet() before the fetched value is retained.
     */
    public const OPT_MAX_READ_SET_ENTRIES = 'maxReadSetEntries';

    /**
     * begin() options[] key for the maximum bytes of the whole read set,
     * measured as the sum of every entry's key plus value length (issue
     * #297). 0 (default) = unlimited.
     */
    public const OPT_MAX_READ_SET_TOTAL_SIZE = 'maxReadSetTotalSize';

    /**
     * begin() options[] key for read-set tracking (issue #297). Defaults to
     * false: `get()`/`batchGet()` do not retain values in `getReadSet()`,
     * because nothing in the commit path consumes the read set and keeping it
     * on the hot read path makes a long-lived transaction's memory grow
     * without bound. Must be a bool. Passing true opts in to the map and to
     * the `maxReadSetEntries` / `maxReadSetTotalSize` limits.
     */
    public const OPT_TRACK_READ_SET = 'trackReadSet';

    /**
     * begin() options[] key for the maximum bytes of one prewrite/commit
     * request per region (issue #430). Default:
     * {@see TwoPhaseCommitter::DEFAULT_TXN_BATCH_SIZE} (16384, client-go's
     * `DefTxnCommitBatchSize`). Must be >= 1.
     */
    public const OPT_MAX_TXN_BATCH_SIZE = 'maxTxnBatchSize';

    /**
     * begin() options[] key for the maximum mutations/keys of one
     * prewrite/commit request per region (issue #430). Default:
     * {@see TwoPhaseCommitter::DEFAULT_TXN_BATCH_COUNT} (512, RawKV's
     * `MAX_BATCH_LIMIT`). Must be >= 1.
     */
    public const OPT_MAX_TXN_BATCH_COUNT = 'maxTxnBatchCount';

    /**
     * options[] key for the maximum staleness (ms) of the low-resolution
     * TSO timestamp cache (issue #420, GAP-06). Used only by
     * staleness-tolerant consumers (lock resolution `current_ts`);
     * start/commit timestamps come from PD (pooled via
     * {@see self::OPT_TSO_POOL_SIZE}, not a fresh RPC per call). Default:
     * unset = no caching (getLowResolutionTimestamp() fetches fresh).
     * Must be >= 0; 0 bounds staleness but never returns stale data.
     */
    public const OPT_LOW_RES_TIMESTAMP_MAX_STALENESS_MS = 'lowResTimestampMaxStalenessMs';

    /**
     * options[] key for the PD TSO timestamp pool size (issue #292): the
     * number of consecutive timestamps requested per pooled `Tso` RPC.
     * `getTimestamp()` hands out pooled values and refills when the pool is
     * exhausted, so N calls cost roughly N / poolSize round trips instead
     * of N. Default: 64 (see Connection\TimestampOracle); 1 disables
     * pooling (one `Tso` RPC per call). Must be >= 1 and
     * <= {@see \CrazyGoat\TiKV\Client\Connection\TimestampOracle::MAX_TIMESTAMP_POOL_SIZE}
     * (1000). Accepted on the shared connection factory
     * (`ConnectionFactory::create()`, reached via both
     * `RawKvClient::create()` and `TxnKvClient::create()`), but it only
     * affects transaction timestamp consumers (TxnKv) — RawKV has no
     * transaction timestamp.
     */
    public const OPT_TSO_POOL_SIZE = 'tsoPoolSize';

    /**
     * Default service ID used by {@see TxnKvClient::holdGcSafePoint()} and
     * {@see TxnKvClient::releaseGcSafePoint()}. Distinct per client instance
     * so two clients never overwrite each other's registration.
     */
    private const SERVICE_ID_PREFIX = 'tikv-php-txnkv';

    private bool $closed = false;
    private readonly RegionResolver $regionResolver;

    /** Opt-in region-cache refresh and preload (#425). */
    private readonly RegionCacheWarmer $regionCacheWarmer;

    /**
     * Transactions begun by this client that have not been closed yet,
     * tracked weakly so tracking does not extend their lifetime. close()
     * rolls the still-active ones back before releasing the transport and
     * PD client they depend on (issue #226, TXN-21).
     *
     * @var list<\WeakReference<Transaction>>
     */
    private array $liveTransactions = [];

    /**
     * Final transaction outcomes shared by the LockResolver of every
     * transaction of this client (issue #428). Bounded, FIFO.
     */
    private readonly ResolvedTxnStatusCache $resolvedStatusCache;
    /** Wall-clock deadline (ms) handed to every Transaction this client begins. */
    private readonly int $retryDeadlineMs;
    /** Per-instance default service ID for service safe-point registrations. */
    private readonly string $serviceId;

    /**
     * @param string[] $pdEndpoints PD addresses; all are used for leader
     *        discovery and failover (issue #416)
     * @param array<string, mixed> $options Client options, including 'tls' for TLS
     *                                      configuration, 'timeout' for timeout config,
     *                                      and 'metrics' for the metrics backend.
     *                                      The shared transport options are
     *                                      accepted too: 'allowInsecure'
     *                                      (fail-closed TLS, default true),
     *                                      'maxChannels' and 'idleTtlMs'
     *                                      (channel pool), 'grpc' (gRPC channel
     *                                      arguments) and 'grpcClient' (a transport
     *                                      built by the application — the client
     *                                      uses it and never closes it, so one
     *                                      transport can be shared with other
     *                                      clients)
     */
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self
    {
        // V1 TxnKV resolves regions in the memory-comparable-encoded key
        // space; API V2 adds its mode/keyspace prefix at the shared
        // transport boundary. PD boundaries are decoded before caching.
        $bundle = ConnectionFactory::create(
            $pdEndpoints,
            $logger,
            $options,
            new CodecV1(Mode::Txn),
            Mode::Txn,
        );

        $safePointCache = self::resolveSafePointValidation($options)
            ? new SafePointCache(
                static fn (): int => $bundle->pdClient->getGCSafePoint(),
                self::resolveGcSafePointRefreshMs($options),
                $bundle->logger,
            )
            : null;

        return new self(
            $bundle->pdClient,
            new ApiV2GrpcClient($bundle->grpc, $bundle->codec, Mode::Txn),
            logger: $bundle->logger,
            timeoutConfig: $bundle->timeoutConfig,
            allowedStoreHosts: $bundle->allowedStoreHosts,
            storeHostPolicy: $bundle->storeHostPolicy,
            pdEndpoints: $bundle->pdEndpoints,
            allowedStorePorts: $bundle->allowedStorePorts,
            retryDeadlineMs: self::resolveRetryDeadline($options),
            safePointCache: $safePointCache,
            replicaReadPolicy: self::resolveReplicaReadPolicy($options),
            metrics: $bundle->metrics,
            ownsGrpcClient: $bundle->ownsGrpcClient,
            codec: $bundle->codec,
            regionCacheRefreshMs: self::resolveRegionCacheRefreshMs($options),
            storeCache: $bundle->storeCache,
            requestContext: self::resolveRequestContext($options),
            resourceControl: ResourceControl::fromOptions($options),
        );
    }

    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        ?RegionResolver $regionResolver = null,
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly TimeoutConfig $timeoutConfig = new TimeoutConfig(),
        /** @var list<string> */
        private readonly array $allowedStoreHosts = [],
        /** @var (Closure(string): bool)|null */
        private readonly ?Closure $storeHostPolicy = null,
        /** @var list<string> */
        private readonly array $pdEndpoints = [],
        /** @var list<int>|null */
        private readonly ?array $allowedStorePorts = null,
        int $retryDeadlineMs = Transaction::DEFAULT_RETRY_DEADLINE_MS,
        /** GC safe-point cache for begin() validation; null = validation disabled. */
        private readonly ?SafePointCache $safePointCache = null,
        /** Read preference applied to every transaction's reads (issue #421). */
        private readonly ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
        /** Metrics backend used by region resolution, retries, and transactions. */
        private readonly MetricsInterface $metrics = new NoOpMetrics(),
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
         * Optional clock (milliseconds) for the lock TTL bookkeeping, handed
         * to every transaction. Test seam (issue #429); defaults to the wall clock.
         */
        private readonly ?Closure $clock = null,
        /**
         * Key codec supplied by the construction path. The derived
         * EpochNotMatch payload codec (issue #246) is V1 TxnKV's
         * memory-comparable codec. For API V2 this property is `CodecV2`,
         * but the derived payload codec is null because `ApiV2GrpcClient`
         * already decodes response region boundaries; applying the V2 codec
         * again would fail or corrupt the cached range.
         */
        private readonly ?CodecInterface $codec = new CodecV1(Mode::Txn),
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
         * Store-address cache the retry executor drops when a StoreNotMatch
         * or a transport failure proves the cached address wrong (issue
         * #247). Null for a hand-built client: the resolver then has no
         * store cache to invalidate.
         */
        private readonly ?StoreCacheInterface $storeCache = null,
        /**
         * Optional Context control fields applied to every request of every
         * transaction (issue #426). Null (the default) leaves the serialized
         * Context byte-for-byte unchanged; begin() may replace it per
         * transaction.
         */
        private readonly ?RequestContextOptions $requestContext = null,
        /**
         * Client-wide resource-control metadata (issue #427), inherited by
         * every transaction and overridable per begin() call. Empty by
         * default, which sends no resource-control field.
         */
        private readonly ResourceControl $resourceControl = new ResourceControl(),
    ) {
        if ($regionCacheRefreshMs < 0) {
            throw new InvalidArgumentException('regionCacheRefreshMs must be >= 0');
        }
        if ($retryDeadlineMs < 0) {
            throw new InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        $this->resolvedStatusCache = new ResolvedTxnStatusCache();
        $this->serviceId = self::SERVICE_ID_PREFIX . '-' . substr(md5(uniqid('svc', true)), 0, 8);
        if ($this->regionCache instanceof RegionCache) {
            // Issue #474: regionInvalidated() is emitted from inside
            // RegionCache::invalidate() — give a user-supplied RegionCache
            // this client's metrics backend unless it already carries one.
            // A custom RegionCacheInterface implementation owns its own
            // metric behaviour and is left untouched. Mutates the shared
            // cache in place — never rebind or clone it.
            $this->regionCache->attachMetricsIfAbsent($this->metrics);
        }
        // ApiV2GrpcClient already decodes response region boundaries, so a
        // V2 codec must not be applied to an EpochNotMatch payload a second
        // time (issue #246).
        $payloadCodec = $this->codec instanceof CodecV2 ? null : $this->codec;
        $this->regionResolver = $regionResolver
            ?? new RegionResolver(
                $this->pdClient,
                $this->regionCache,
                $this->metrics,
                $this->allowedStoreHosts,
                $this->storeHostPolicy,
                $this->logger,
                $this->pdEndpoints,
                $this->allowedStorePorts,
                $payloadCodec,
                $this->storeCache,
            );
        $this->regionCacheWarmer = $regionCacheWarmer
            ?? new RegionCacheWarmer($this->pdClient, $this->regionCache, $this->logger, $regionCacheRefreshMs);
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
            return Transaction::DEFAULT_RETRY_DEADLINE_MS;
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
     * Resolve options['gcSafePointValidation'] (see OPT_GC_SAFE_POINT_VALIDATION)
     * for create(): whether begin() validates the start timestamp against the
     * cluster's GC safe point. Defaults to true.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveSafePointValidation(array $options): bool
    {
        if (!array_key_exists(self::OPT_GC_SAFE_POINT_VALIDATION, $options)) {
            return true;
        }

        $enabled = $options[self::OPT_GC_SAFE_POINT_VALIDATION];
        if (!is_bool($enabled)) {
            throw new InvalidArgumentException(sprintf(
                "options['gcSafePointValidation'] must be a bool, %s given",
                get_debug_type($enabled),
            ));
        }

        return $enabled;
    }

    /**
     * Resolve options['gcSafePointRefreshMs'] (see OPT_GC_SAFE_POINT_REFRESH_MS)
     * for create(): how long the cached GC safe point stays fresh. Must be >= 1.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveGcSafePointRefreshMs(array $options): int
    {
        if (!array_key_exists(self::OPT_GC_SAFE_POINT_REFRESH_MS, $options)) {
            return SafePointCache::DEFAULT_REFRESH_INTERVAL_MS;
        }

        $refreshMs = $options[self::OPT_GC_SAFE_POINT_REFRESH_MS];
        if (!is_int($refreshMs)) {
            throw new InvalidArgumentException(sprintf(
                "options['gcSafePointRefreshMs'] must be an int (milliseconds), %s given",
                get_debug_type($refreshMs),
            ));
        }
        if ($refreshMs < 1) {
            throw new InvalidArgumentException("options['gcSafePointRefreshMs'] must be >= 1");
        }

        return $refreshMs;
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
     * Begin a new transaction.
     *
     * The transaction mode defaults to **pessimistic**: `pessimistic` is
     * `true` unless the caller passes it explicitly. That is the opposite of
     * the TiKV documentation and of client-go and client-rust, where
     * optimistic is the default (issue #439). The default is kept as it is,
     * because changing it is a breaking change that belongs in a major
     * release — new code should state the mode with
     * {@see self::beginOptimistic()} or {@see self::beginPessimistic()},
     * which are thin wrappers over this method and share its option
     * handling.
     *
     * @param array{
     *     pessimistic?: bool,
     *     priority?: int,
     *     resourceGroup?: string,
     *     resourceGroupTag?: string,
     *     enable1Pc?: bool,
     *     enableAsyncCommit?: bool,
     *     eagerPessimisticLocks?: bool,
     *     lockWaitTimeMs?: int,
     *     optimisticLockTtlMs?: int,
     *     pessimisticLockTtlMs?: int,
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     *     requestContext?: RequestContextOptions|null,
     * } $options
     *     `pessimistic` selects the transaction mode: true (the default)
     *     takes the physical lock inside set()/delete(), false defers it to
     *     commit().
     *     `priority` maps onto the Kvrpcpb CommandPri enum: 0 = Normal
     *     (default), 1 = Low, 2 = High. It is carried on the Context of
     *     every read and write request the transaction sends — its reads
     *     (get, batchGet, scan) as well as its prewrite, pessimistic-lock,
     *     commit, heartbeat and rollback RPCs (issue #441, issue #336). The
     *     lock-resolution RPCs are always Normal: they resolve locks of
     *     other transactions.
     *     `lockWaitTimeMs` bounds how long TiKV waits for a contended
     *     `optimisticLockTtlMs` (default 3000) and `pessimisticLockTtlMs`
     *     (default 30000) set the lock TTLs in milliseconds, each between 1 and
     *     120000 (issue #429). A pessimistic transaction extends its primary
     *     lock automatically once half of the TTL has elapsed.
     *     `maxTxnEntryCount`, `maxTxnEntrySize` and `maxTxnTotalSize` (each 0 =
     *     unlimited) bound the transaction's write set (issue #430);
     *     `trackReadSet` (default false) opts in to read-set tracking;
     *     `maxReadSetEntries` and `maxReadSetTotalSize` (each 0 = unlimited)
     *     bound the tracked read set (issue #297);
     *     `maxTxnBatchSize` (default 16384 bytes) and `maxTxnBatchCount`
     *     (default 512) split each region's prewrite mutations and commit keys
     *     into multiple requests. Exceeding a size limit throws
     *     {@see \CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException}
     *     from set()/delete() before any RPC is issued, and from
     *     get()/batchGet() before the fetched value is retained.
     *     `requestContext` replaces the client-level Context control fields
     *     (see {@see self::OPT_REQUEST_CONTEXT}) for this transaction only
     *     (issue #426). Omit it to inherit the client-level value; pass null
     *     to disable the controls for this transaction.
     */
    public function begin(array $options = []): Transaction
    {
        $this->ensureOpen();

        $pessimistic = (bool) ($options['pessimistic'] ?? true);
        $priority = (int) ($options['priority'] ?? 0);
        $resourceControl = $this->resourceControl->withOverrides(
            ResourceControl::fromOptions($options, 'begin() options'),
        );
        $requestContext = array_key_exists('requestContext', $options)
            ? $this->assertRequestContextOption($options['requestContext'])
            : $this->requestContext;
        $enable1Pc = $this->resolveEnableFlag($options, self::OPT_ENABLE_1PC);
        $enableAsyncCommit = $this->resolveEnableFlag($options, self::OPT_ENABLE_ASYNC_COMMIT);
        $eagerPessimisticLocks = $this->resolveEagerPessimisticLocks($options);
        $lockWaitTimeMs = $this->resolveLockWaitTimeMs($options);
        $optimisticLockTtlMs = $this->resolveLockTtlMs(
            $options,
            self::OPT_OPTIMISTIC_LOCK_TTL_MS,
            TwoPhaseCommitter::DEFAULT_OPTIMISTIC_LOCK_TTL_MS,
        );
        $pessimisticLockTtlMs = $this->resolveLockTtlMs(
            $options,
            self::OPT_PESSIMISTIC_LOCK_TTL_MS,
            TwoPhaseCommitter::DEFAULT_PESSIMISTIC_LOCK_TTL_MS,
        );
        $maxTxnEntryCount = $this->resolveIntOption($options, self::OPT_MAX_TXN_ENTRY_COUNT, 0, 0);
        $maxTxnEntrySize = $this->resolveIntOption($options, self::OPT_MAX_TXN_ENTRY_SIZE, 0, 0);
        $maxTxnTotalSize = $this->resolveIntOption($options, self::OPT_MAX_TXN_TOTAL_SIZE, 0, 0);
        $trackReadSet = $this->resolveEnableFlag($options, self::OPT_TRACK_READ_SET);
        $maxReadSetEntries = $this->resolveIntOption($options, self::OPT_MAX_READ_SET_ENTRIES, 0, 0);
        $maxReadSetTotalSize = $this->resolveIntOption($options, self::OPT_MAX_READ_SET_TOTAL_SIZE, 0, 0);
        $maxTxnBatchSize = $this->resolveIntOption(
            $options,
            self::OPT_MAX_TXN_BATCH_SIZE,
            TwoPhaseCommitter::DEFAULT_TXN_BATCH_SIZE,
            1,
        );
        $maxTxnBatchCount = $this->resolveIntOption(
            $options,
            self::OPT_MAX_TXN_BATCH_COUNT,
            TwoPhaseCommitter::DEFAULT_TXN_BATCH_COUNT,
            1,
        );

        // Read the local clock BEFORE the start timestamp and the GC check
        // (a PD fetch on a cache miss): TiKV counts a lock TTL from the start
        // timestamp, so an earlier reading keeps the tracked primary-lock
        // expiry no later than the real one (issue #429).
        $startedAtMs = $this->clock instanceof Closure
            ? ($this->clock)()
            : (int) (microtime(true) * 1000);

        $startTs = $this->pdClient->getTimestamp();

        $this->validateStartTsAgainstGcSafePoint($startTs);

        $txnId = uniqid('txn-', true);

        $this->logger->info('Transaction started', [
            'txnId' => $txnId,
            'startTs' => $startTs,
            'pessimistic' => $pessimistic,
            'eagerPessimisticLocks' => $eagerPessimisticLocks,
        ]);

        $lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            $startTs,
            timeoutConfig: $this->timeoutConfig,
            logger: $this->logger,
            statusCache: $this->resolvedStatusCache,
            requestContext: $requestContext,
            resourceControl: $resourceControl,
        );

        $transaction = new Transaction(
            txnId: $txnId,
            startTs: $startTs,
            pessimistic: $pessimistic,
            priority: $priority,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: $this->maxBackoffMs,
            logger: $this->logger,
            timeoutConfig: $this->timeoutConfig,
            metrics: $this->metrics,
            retryDeadlineMs: $this->retryDeadlineMs,
            replicaReadPolicy: $this->replicaReadPolicy,
            enable1Pc: $enable1Pc,
            enableAsyncCommit: $enableAsyncCommit,
            eagerPessimisticLocks: $eagerPessimisticLocks,
            lockWaitTimeMs: $lockWaitTimeMs,
            optimisticLockTtlMs: $optimisticLockTtlMs,
            pessimisticLockTtlMs: $pessimisticLockTtlMs,
            clock: $this->clock,
            startedAtMs: $startedAtMs,
            maxTxnEntryCount: $maxTxnEntryCount,
            maxTxnEntrySize: $maxTxnEntrySize,
            maxTxnTotalSize: $maxTxnTotalSize,
            maxTxnBatchSize: $maxTxnBatchSize,
            maxTxnBatchCount: $maxTxnBatchCount,
            maxReadSetEntries: $maxReadSetEntries,
            maxReadSetTotalSize: $maxReadSetTotalSize,
            trackReadSet: $trackReadSet,
            requestContext: $requestContext,
            resourceControl: $resourceControl,
        );

        $this->pruneFinishedTransactions();
        $this->liveTransactions[] = \WeakReference::create($transaction);

        return $transaction;
    }

    /**
     * Begin a new transaction in optimistic mode.
     *
     * A thin wrapper over {@see self::begin()} that fixes the transaction
     * mode: the physical locks are taken during the prewrite at commit()
     * instead of inside set()/delete() (issue #439). Every other option is
     * handled by begin(), so the two entry points cannot drift apart.
     *
     * The mode belongs to the method: a `pessimistic` entry in $options that
     * contradicts it is rejected with an InvalidArgumentException instead of
     * being silently overridden.
     *
     * `lockWaitTimeMs` bounds how long TiKV waits for a contended lock
     * (issue #443, DIV-07). It has **no effect** in the optimistic mode: the
     * transaction takes no pessimistic lock, so it never waits for one.
     *
     * @param array{
     *     pessimistic?: bool,
     *     priority?: int,
     *     resourceGroup?: string,
     *     resourceGroupTag?: string,
     *     enable1Pc?: bool,
     *     enableAsyncCommit?: bool,
     *     eagerPessimisticLocks?: bool,
     *     lockWaitTimeMs?: int,
     *     optimisticLockTtlMs?: int,
     *     pessimisticLockTtlMs?: int,
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     *     requestContext?: RequestContextOptions|null,
     * } $options
     */
    public function beginOptimistic(array $options = []): Transaction
    {
        return $this->beginWithMode($options, false, 'beginOptimistic()');
    }

    /**
     * Begin a new transaction in pessimistic mode.
     *
     * A thin wrapper over {@see self::begin()} that fixes the transaction
     * mode: the physical lock of each key is taken inside set()/delete(),
     * before the call returns (issue #439). Every other option is handled by
     * begin(), so the two entry points cannot drift apart.
     *
     * The mode belongs to the method: a `pessimistic` entry in $options that
     * contradicts it is rejected with an InvalidArgumentException instead of
     * being silently overridden.
     *
     * `lockWaitTimeMs` bounds how long TiKV waits for a contended
     * pessimistic lock before it answers with a "locked" error (issue #443,
     * DIV-07).
     *
     * @param array{
     *     pessimistic?: bool,
     *     priority?: int,
     *     resourceGroup?: string,
     *     resourceGroupTag?: string,
     *     enable1Pc?: bool,
     *     enableAsyncCommit?: bool,
     *     eagerPessimisticLocks?: bool,
     *     lockWaitTimeMs?: int,
     *     optimisticLockTtlMs?: int,
     *     pessimisticLockTtlMs?: int,
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     *     requestContext?: RequestContextOptions|null,
     * } $options
     */
    public function beginPessimistic(array $options = []): Transaction
    {
        return $this->beginWithMode($options, true, 'beginPessimistic()');
    }

    /**
     * Begin a transaction with a fixed mode, through begin() so that the
     * option handling and the validation stay in one place (issue #439).
     *
     * The closed-client check comes first, so a closed client is reported as
     * such and not as a contradicting option — the same order begin() uses.
     *
     * @param array{
     *     pessimistic?: bool,
     *     priority?: int,
     *     resourceGroup?: string,
     *     resourceGroupTag?: string,
     *     enable1Pc?: bool,
     *     enableAsyncCommit?: bool,
     *     eagerPessimisticLocks?: bool,
     *     lockWaitTimeMs?: int,
     *     optimisticLockTtlMs?: int,
     *     pessimisticLockTtlMs?: int,
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     *     requestContext?: RequestContextOptions|null,
     * } $options
     */
    private function beginWithMode(array $options, bool $pessimistic, string $method): Transaction
    {
        $this->ensureOpen();

        if (array_key_exists('pessimistic', $options) && (bool) $options['pessimistic'] !== $pessimistic) {
            throw new InvalidArgumentException(sprintf(
                "%s fixes the transaction mode; options['pessimistic'] must not contradict it",
                $method,
            ));
        }

        $options['pessimistic'] = $pessimistic;

        return $this->begin($options);
    }

    /**
     * Resolve a boolean begin() option (enable1Pc / enableAsyncCommit, issue
     * #419, and trackReadSet, issue #297). Defaults to false; a non-bool
     * value is rejected.
     *
     * @param array<string, mixed> $options
     */
    private function resolveEnableFlag(array $options, string $key): bool
    {
        if (!array_key_exists($key, $options)) {
            return false;
        }

        $value = $options[$key];
        if (!is_bool($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be a bool, %s given",
                $key,
                get_debug_type($value),
            ));
        }

        return $value;
    }

    /**
     * Resolve the eager pessimistic-lock compatibility switch. The default
     * follows the official clients; false retains the pre-#437 deferred
     * lock-at-commit behaviour.
     *
     * @param array<string, mixed> $options
     */
    private function resolveEagerPessimisticLocks(array $options): bool
    {
        if (!array_key_exists(self::OPT_EAGER_PESSIMISTIC_LOCKS, $options)) {
            return true;
        }

        $value = $options[self::OPT_EAGER_PESSIMISTIC_LOCKS];
        if (!is_bool($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be a bool, %s given",
                self::OPT_EAGER_PESSIMISTIC_LOCKS,
                get_debug_type($value),
            ));
        }

        return $value;
    }

    /**
     * Resolve the begin() option 'lockWaitTimeMs' (see
     * {@see self::OPT_LOCK_WAIT_TIME_MS}): how long TiKV may wait for a
     * contended pessimistic lock, in milliseconds. 0 means "use TiKV's own
     * default timeout"; a negative value is rejected.
     *
     * @param array<string, mixed> $options
     */
    private function resolveLockWaitTimeMs(array $options): int
    {
        if (!array_key_exists(self::OPT_LOCK_WAIT_TIME_MS, $options)) {
            return Transaction::DEFAULT_LOCK_WAIT_TIME_MS;
        }

        $value = $options[self::OPT_LOCK_WAIT_TIME_MS];
        if (!is_int($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be an int (milliseconds), %s given",
                self::OPT_LOCK_WAIT_TIME_MS,
                get_debug_type($value),
            ));
        }
        if ($value < 0) {
            throw new InvalidArgumentException("options['lockWaitTimeMs'] must be >= 0");
        }

        return $value;
    }

    /**
     * Resolve a lock TTL begin() option (optimisticLockTtlMs /
     * pessimisticLockTtlMs, issue #429). A non-int or out-of-range value is
     * rejected, as is a non-positive timeout elsewhere (issue #276).
     *
     * @param array<string, mixed> $options
     */
    private function resolveLockTtlMs(array $options, string $key, int $default): int
    {
        if (!array_key_exists($key, $options)) {
            return $default;
        }

        $value = $options[$key];
        if (!is_int($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be an int (milliseconds), %s given",
                $key,
                get_debug_type($value),
            ));
        }
        if ($value < 1 || $value > TwoPhaseCommitter::MAX_LOCK_TTL_MS) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be between 1 and %d",
                $key,
                TwoPhaseCommitter::MAX_LOCK_TTL_MS,
            ));
        }

        return $value;
    }

    /**
     * Resolve an integer begin() option, rejecting a non-int or a value
     * below $minimum. The transaction size and batching options use this
     * (issue #430); the per-option minimum is 0 for the opt-in limits
     * (0 = unlimited) and 1 for the batch bounds.
     *
     * @param array<string, mixed> $options
     */
    private function resolveIntOption(array $options, string $key, int $default, int $minimum): int
    {
        if (!array_key_exists($key, $options)) {
            return $default;
        }

        $value = $options[$key];
        if (!is_int($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be an int, %s given",
                $key,
                get_debug_type($value),
            ));
        }
        if ($value < $minimum) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be >= %d",
                $key,
                $minimum,
            ));
        }

        return $value;
    }

    /**
     * Resolve options['replicaRead'] (see OPT_REPLICA_READ) for create():
     * the read preference applied to transaction reads (issue #421). Must
     * be a ReplicaReadPolicy instance when set.
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
     * create(): the optional Context control fields applied to every request
     * (issue #426). Must be a RequestContextOptions instance when set;
     * absent or null means "apply none".
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
     * Validate a begin() `requestContext` option (issue #426): it must be a
     * RequestContextOptions instance or null (null disables the client-level
     * controls for this transaction).
     */
    private function assertRequestContextOption(mixed $requestContext): ?RequestContextOptions
    {
        if ($requestContext === null || $requestContext instanceof RequestContextOptions) {
            return $requestContext;
        }

        throw new InvalidArgumentException(sprintf(
            "begin() option 'requestContext' must be a %s or null, %s given",
            RequestContextOptions::class,
            get_debug_type($requestContext),
        ));
    }

    /**
     * Get the learned cluster ID, or null if not yet discovered.
     */
    public function getClusterId(): ?int
    {
        return $this->pdClient->getClusterId();
    }

    /**
     * Return the PD client this client uses for region discovery and
     * timestamps.
     *
     * The returned object is the same instance the client built (or was
     * given), so a caller that needs PD — service GC safe points, cluster
     * inspection, a test that splits regions — reuses this connection
     * instead of opening a second one with a second TSO stream. This client
     * owns it: do not close it yourself. It is valid only while this client
     * is open, because {@see self::close()} closes it — treat the reference
     * as stale afterwards.
     */
    public function pdClient(): PdClientInterface
    {
        return $this->pdClient;
    }

    /**
     * Register a service GC safe point so GC holds back past $safePoint for
     * the given TTL (issue #422).
     *
     * A long-running read (large scan, report job, batch export) whose
     * duration may exceed the cluster's gc_life_time should call this
     * before starting, and refresh it periodically before the TTL lapses
     * (or call {@see releaseGcSafePoint()} on orderly shutdown). While a
     * service safe point is active and fresh, GC will not advance past it
     * and reads at startTs >= safePoint keep working; without it, such a
     * read fails partway with TxnAbortedByGcException once GC catches up.
     *
     * @param int $safePoint The timestamp to hold GC at (typically the
     *                       transaction's startTs — see
     *                       Transaction::getStartTs()).
     * @param int $ttlSeconds How long PD should honour the hold. Use a TTL
     *                        comfortably longer than the refresh period so
     *                        a crashed worker cannot block GC forever.
     * @param string|null $serviceId Override the default per-instance
     *                               service ID.
     *
     * @return int|null The resulting min safe point across all registered
     *                  services (the value GC is actually held at), or null
     *                  when this PD does not support service safe points.
     *
     * @throws ClientClosedException When the client has been closed
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function holdGcSafePoint(int $safePoint, int $ttlSeconds, ?string $serviceId = null): ?int
    {
        $this->ensureOpen();

        return $this->pdClient->updateServiceGCSafePoint(
            $serviceId ?? $this->serviceId,
            $safePoint,
            $ttlSeconds,
        );
    }

    /**
     * Remove this client's service GC safe point registration, letting GC
     * advance again (orderly shutdown of a long-running job).
     *
     * Sends a TTL of -1, which PD treats as a removal of the service safe
     * point (any non-positive TTL removes it). Returns null (nothing to
     * act on) when PD does not support service safe points.
     *
     * @throws ClientClosedException When the client has been closed
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function releaseGcSafePoint(?string $serviceId = null): ?int
    {
        $this->ensureOpen();

        return $this->pdClient->updateServiceGCSafePoint(
            $serviceId ?? $this->serviceId,
            0,
            -1,
        );
    }

    /**
     * Probe the PD cluster by issuing a {@see GetMembers} RPC and return
     * the learned cluster ID.
     *
     * Use as a lightweight health check. Does not touch any user data.
     *
     * Returns the cluster ID on success, or null if PD responded without a
     * cluster-ID header or with cluster ID `0`. When
     * `options['expectedClusterId']` is set, a response without a cluster ID
     * fails with HealthCheckException instead of returning null.
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

    /**
     * Return the metrics implementation in use. Same semantics as
     * {@see RawKvClient::getMetrics()}.
     */
    public function getMetrics(): MetricsInterface
    {
        return $this->metrics;
    }

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
     * Same contract as {@see RawKvClient::refreshRegionCache()}: PHP has no
     * background threads, so a long-lived worker calls this from its own tick.
     * A non-forced call performs at most one refresh per
     * `options['regionCacheRefreshMs']` (default 0, i.e. disabled) and returns
     * 0 when the interval has not elapsed; `$force = true` bypasses that
     * interval only, not the per-tick bound.
     *
     * One call re-resolves at most
     * `RegionCacheWarmer::DEFAULT_MAX_REGIONS_PER_REFRESH` (128) cached
     * regions, round-robin across calls, so successive ticks walk the whole
     * cache instead of refreshing it in one unbounded burst. The first PD
     * failure aborts the tick, is logged and keeps the cached entries, so this
     * method never throws. No-op after `close()`.
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
     * Same contract as {@see RawKvClient::preloadRegions()}: each range is a
     * `[startKey, endKey]` pair of half-open bounds, where an empty `endKey`
     * means +infinity. A PD failure is logged and never thrown; the existing
     * cache is left untouched. No-op after `close()`.
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

    /**
     * Release the client's resources.
     *
     * Transactions this client began that are still active are rolled back
     * first, while the transport and the PD client they depend on are still
     * open (issue #226, TXN-21). A rollback failure is logged and does not
     * abort the shutdown; the transaction keeps its destructor fallback.
     * The region cache is then cleared, the owned transport is closed and the
     * PD client is closed.
     *
     * Idempotent and never throws. The transaction rollback can perform
     * network I/O and block while its retry loop runs.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        $this->rollbackLiveTransactions();

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

    /**
     * Roll back every transaction this client began that is still active.
     *
     * Runs before the region cache, transport and PD client are released, so
     * an abandoned transaction does not leave its locks behind until the TTL
     * expires (issue #226, TXN-21). A failure is logged, not propagated:
     * close() must never throw, and the transaction's own destructor keeps
     * its rollback fallback.
     */
    private function rollbackLiveTransactions(): void
    {
        $references = $this->liveTransactions;
        $this->liveTransactions = [];

        foreach ($references as $reference) {
            $transaction = $reference->get();
            // A closed transaction always carries a terminal status, so the
            // status check also excludes an already committed or rolled back
            // one (see Transaction::__destruct()).
            if (
                !$transaction instanceof Transaction
                || $transaction->getStatus() !== TransactionStatus::Active
            ) {
                continue;
            }

            try {
                $transaction->rollback();
            } catch (\Throwable $e) {
                $this->logger->error('Transaction rollback failed during client close', [
                    'txnId' => $transaction->getTxnId(),
                    'exception' => $e,
                ]);
            }
        }
    }

    /**
     * Drop weak references whose transaction was destroyed or reached a
     * terminal status, so the list only tracks transactions that can still
     * be rolled back.
     */
    private function pruneFinishedTransactions(): void
    {
        $live = [];
        foreach ($this->liveTransactions as $reference) {
            $transaction = $reference->get();
            if (
                $transaction instanceof Transaction
                && $transaction->getStatus() === TransactionStatus::Active
            ) {
                $live[] = $reference;
            }
        }

        $this->liveTransactions = $live;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ClientClosedException();
        }
    }

    /**
     * Validate a freshly fetched start timestamp against the cluster's GC
     * safe point (issue #422).
     *
     * Throws TxnAbortedByGcException when the safe point has already
     * passed the timestamp — such a transaction would fail on its first
     * read with "GC life time is shorter than transaction duration", so
     * failing here is earlier, typed, and loses nothing.
     *
     * A PD fetch failure degrades to a warning log (fail-open): validation
     * must not invent a new hard failure mode for clients on clusters
     * where the safe-point RPC is unavailable.
     */
    private function validateStartTsAgainstGcSafePoint(int $startTs): void
    {
        $cache = $this->safePointCache;
        if (!$cache instanceof SafePointCache) {
            return;
        }

        try {
            $safePoint = $cache->get();
        } catch (TiKvException $e) {
            $this->logger->warning('GC safe-point check skipped: PD fetch failed', [
                'startTs' => $startTs,
                'error' => $e->getMessage(),
            ]);

            return;
        }

        if ($startTs < $safePoint) {
            throw new TxnAbortedByGcException(sprintf(
                'Transaction start timestamp %d is below the GC safe point %d '
                . '(data at this timestamp has been garbage collected). '
                . 'Start a new transaction, or hold GC back with '
                . 'TxnKvClient::holdGcSafePoint() for long-running reads.',
                $startTs,
                $safePoint,
            ));
        }
    }
}
