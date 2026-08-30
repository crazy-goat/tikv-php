<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use Closure;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\SafePointCache;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\HealthCheckException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TxnKvClient
{
    public const OPT_TIMEOUT = 'timeout';
    public const OPT_METRICS = 'metrics';

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
     * options[] key for the GC safe-point cache refresh interval in
     * milliseconds (issue #422). Default 30000 (30s). Must be >= 1.
     */
    public const OPT_GC_SAFE_POINT_REFRESH_MS = 'gcSafePointRefreshMs';

    /**
     * Default service ID used by {@see TxnKvClient::holdGcSafePoint()} and
     * {@see TxnKvClient::releaseGcSafePoint()}. Distinct per client instance
     * so two clients never overwrite each other's registration.
     */
    private const SERVICE_ID_PREFIX = 'tikv-php-txnkv';

    private bool $closed = false;
    private readonly RegionResolver $regionResolver;
    private readonly MetricsInterface $metrics;
    /** Wall-clock deadline (ms) handed to every Transaction this client begins. */
    private readonly int $retryDeadlineMs;
    /** Per-instance default service ID for service safe-point registrations. */
    private readonly string $serviceId;

    /**
     * @param string[] $pdEndpoints PD addresses (currently only the first is used)
     * @param array<string, mixed> $options Client options, including 'tls' for TLS
     *                                      configuration, 'timeout' for timeout config,
     *                                      and 'metrics' for the metrics backend
     */
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self
    {
        $bundle = ConnectionFactory::create($pdEndpoints, $logger, $options);

        $safePointCache = self::resolveSafePointValidation($options)
            ? new SafePointCache(
                static fn (): int => $bundle->pdClient->getGCSafePoint(),
                self::resolveGcSafePointRefreshMs($options),
                $bundle->logger,
            )
            : null;

        return new self(
            $bundle->pdClient,
            $bundle->grpc,
            logger: $bundle->logger,
            timeoutConfig: $bundle->timeoutConfig,
            allowedStoreHosts: $bundle->allowedStoreHosts,
            storeHostPolicy: $bundle->storeHostPolicy,
            pdEndpoints: $bundle->pdEndpoints,
            allowedStorePorts: $bundle->allowedStorePorts,
            retryDeadlineMs: self::resolveRetryDeadline($options),
            safePointCache: $safePointCache,
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
    ) {
        if ($retryDeadlineMs < 0) {
            throw new InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        $this->serviceId = self::SERVICE_ID_PREFIX . '-' . substr(md5(uniqid('svc', true)), 0, 8);
        $this->metrics = new NoOpMetrics();
        if ($this->regionCache instanceof RegionCache) {
            // Issue #474: regionInvalidated() is emitted from inside
            // RegionCache::invalidate() — give a user-supplied RegionCache
            // this client's metrics backend unless it already carries one.
            // A custom RegionCacheInterface implementation owns its own
            // metric behaviour and is left untouched. Mutates the shared
            // cache in place — never rebind or clone it.
            $this->regionCache->attachMetricsIfAbsent($this->metrics);
        }
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
            );
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
     * @param array{pessimistic?: bool, priority?: int} $options
     */
    public function begin(array $options = []): Transaction
    {
        $this->ensureOpen();

        $pessimistic = (bool) ($options['pessimistic'] ?? true);
        $priority = (int) ($options['priority'] ?? 0);

        $startTs = $this->pdClient->getTimestamp();

        $this->validateStartTsAgainstGcSafePoint($startTs);

        $txnId = uniqid('txn-', true);

        $this->logger->info('Transaction started', [
            'txnId' => $txnId,
            'startTs' => $startTs,
            'pessimistic' => $pessimistic,
        ]);

        $lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            $startTs,
            timeoutConfig: $this->timeoutConfig,
            maxBackoffMs: $this->maxBackoffMs,
            logger: $this->logger,
        );

        return new Transaction(
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
        );
    }

    /**
     * Get the learned cluster ID, or null if not yet discovered.
     */
    public function getClusterId(): ?int
    {
        return $this->pdClient->getClusterId();
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
     * Returns null (nothing to act on) when PD does not support service
     * safe points.
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
            $this->grpc->close();
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
