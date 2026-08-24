<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use Closure;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\HealthCheckException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

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
     * requests used by the fanned-out range operations (deleteRange,
     * checksum, batchScan, SST ingest mode switches) — issue #295.
     */
    public const OPT_MAX_CONCURRENCY = 'maxConcurrency';

    /** Default bound on in-flight requests for fanned-out range operations. */
    public const DEFAULT_MAX_CONCURRENCY = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY;

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
    /** Max in-flight requests for fanned-out range operations (issue #295). */
    private readonly int $maxConcurrency;

    /**
     * @param string[] $pdEndpoints
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException if PD endpoints array is empty
     */
    public static function create(
        array $pdEndpoints,
        ?LoggerInterface $logger = null,
        array $options = []
    ): self {
        $bundle = ConnectionFactory::create($pdEndpoints, $logger, $options);

        return new self(
            $bundle->pdClient,
            $bundle->grpc,
            new RegionCache(logger: $bundle->logger),
            logger: $bundle->logger,
            timeoutConfig: $bundle->timeoutConfig,
            slowLogConfig: $bundle->slowLogConfig,
            allowedStoreHosts: $bundle->allowedStoreHosts,
            storeHostPolicy: $bundle->storeHostPolicy,
            pdEndpoints: $bundle->pdEndpoints,
            allowedStorePorts: $bundle->allowedStorePorts,
            retryDeadlineMs: self::resolveRetryDeadline($options),
            maxConcurrency: self::resolveMaxConcurrency($options),
        );
    }

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
    ) {
        if ($retryDeadlineMs < 0) {
            throw new InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        if ($maxConcurrency < 1) {
            throw new InvalidArgumentException('maxConcurrency must be >= 1');
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        $this->maxConcurrency = $maxConcurrency;
        $regionResolver ??= new RegionResolver(
            $pdClient,
            $regionCache,
            $metrics,
            $this->allowedStoreHosts,
            $this->storeHostPolicy,
            $this->logger,
            $this->pdEndpoints,
            $this->allowedStorePorts,
        );
        $this->crud = $crud ?? new RawKvCrud(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
        );
        $this->atomic = $atomic ?? new RawKvAtomic(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
        );
        $this->batch = $batch ?? new RawKvBatch(
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->slowLogConfig,
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
        );
        $this->ingestor = new SstIngestor(
            $grpc,
            $pdClient,
            $regionResolver,
            $timeoutConfig,
            $this->logger,
            $this->maxConcurrency,
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
     * fanned-out range operations. Must be >= 1 (issue #295).
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

    private function createRetryExecutor(): RetryExecutor
    {
        $regionResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            $this->metrics,
            $this->allowedStoreHosts,
            $this->storeHostPolicy,
            $this->logger,
            $this->pdEndpoints,
            $this->allowedStorePorts,
        );

        return new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $regionResolver,
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
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws RegionException
     * @throws GrpcException
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
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws RegionException
     * @throws GrpcException
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
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<string, ?string>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException
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
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
     * @throws BatchPartialFailureException
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
     * @throws BatchPartialFailureException
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
     */
    public function scanIterator(
        string $startKey,
        string $endKey,
        int $batchSize = 256,
        bool $keyOnly = false,
    ): ScanIterator {
        $this->ensureOpen();

        return $this->scanner->scanIterator($startKey, $endKey, $batchSize, $keyOnly, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     */
    public function scanPrefixIterator(string $prefix, int $batchSize = 256, bool $keyOnly = false): ScanIterator
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefixIterator($prefix, $batchSize, $keyOnly, $this->columnFamily);
    }

    /**
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
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
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws RegionException
     * @throws GrpcException
     */
    public function scanPrefix(string $prefix, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefix($prefix, $limit, $keyOnly, $this->columnFamily);
    }

    /**
     * @return array<array{key: string, value: ?string}>
     *
     * @throws ClientClosedException
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
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
     * @throws InvalidArgumentException
     * @throws RegionException
     * @throws GrpcException
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
     * @throws RegionException
     * @throws GrpcException
     */
    public function deleteRange(string $startKey, string $endKey): void
    {
        $this->ensureOpen();

        $this->rangeOps->deleteRange($startKey, $endKey, $this->columnFamily);
    }

    /**
     * @throws ClientClosedException
     * @throws InvalidArgumentException
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
     * @throws RegionException
     * @throws GrpcException
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
     * normal mode on completion (even on failure).
     *
     * @param array<string, string> $keyValuePairs Key-value pairs (sorted or unsorted)
     * @param int|null $ttl Time-to-live in seconds (null = no TTL)
     *
     * @throws ClientClosedException
     * @throws GrpcException
     * @throws RegionException
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
     * but the response header did not carry a cluster ID (callers may
     * treat null as "reachable but identity unknown").
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
}
