<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
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

    public const OPT_TIMEOUT = 'timeout';

    private bool $closed = false;

    private bool $atomicForCAS = false;

    private string $columnFamily = '';

    private readonly RawKvCrud $crud;
    private readonly RawKvAtomic $atomic;
    private readonly RawKvBatch $batch;
    private readonly RawKvScanner $scanner;
    private readonly RawKvRangeOps $rangeOps;

    /**
     * @param string[] $pdEndpoints
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException if PD endpoints array is empty
     */
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self
    {
        if ($pdEndpoints === []) {
            throw new InvalidArgumentException('PD endpoints array must not be empty');
        }

        $resolvedLogger = $logger ?? new NullLogger();

        $tlsConfig = null;
        if (isset($options['tls']) && is_array($options['tls'])) {
            $tlsOptions = $options['tls'];
            $builder = new TlsConfigBuilder();

            // Explicit file-path options take priority
            if (isset($tlsOptions['caCertFile']) && is_string($tlsOptions['caCertFile'])) {
                $baseDir = isset($tlsOptions['caCertBaseDir']) && is_string($tlsOptions['caCertBaseDir'])
                    ? $tlsOptions['caCertBaseDir']
                    : null;
                $builder->withCaCertFile($tlsOptions['caCertFile'], $baseDir);
            } elseif (isset($tlsOptions['caCertPem']) && is_string($tlsOptions['caCertPem'])) {
                $builder->withCaCertPem($tlsOptions['caCertPem']);
            } elseif (isset($tlsOptions['caCert']) && is_string($tlsOptions['caCert'])) {
                // Backward compatibility: guess file path vs inline content
                $builder->withCaCert($tlsOptions['caCert']);
            }

            $hasClientCertFile = isset($tlsOptions['clientCertFile']) && is_string($tlsOptions['clientCertFile']);
            $hasClientKeyFile = isset($tlsOptions['clientKeyFile']) && is_string($tlsOptions['clientKeyFile']);
            $hasClientCertPem = isset($tlsOptions['clientCertPem']) && is_string($tlsOptions['clientCertPem']);
            $hasClientKeyPem = isset($tlsOptions['clientKeyPem']) && is_string($tlsOptions['clientKeyPem']);

            if ($hasClientCertFile && $hasClientKeyFile) {
                $baseDir = isset($tlsOptions['clientCertBaseDir']) && is_string($tlsOptions['clientCertBaseDir'])
                    ? $tlsOptions['clientCertBaseDir']
                    : null;
                $builder->withClientCertFile(
                    $tlsOptions['clientCertFile'],
                    $tlsOptions['clientKeyFile'],
                    $baseDir,
                );
            } elseif ($hasClientCertPem && $hasClientKeyPem) {
                $builder->withClientCertPem($tlsOptions['clientCertPem'], $tlsOptions['clientKeyPem']);
            } elseif (
                isset($tlsOptions['clientCert']) && is_string($tlsOptions['clientCert']) &&
                isset($tlsOptions['clientKey']) && is_string($tlsOptions['clientKey'])
            ) {
                // Backward compatibility: guess file path vs inline content
                $builder->withClientCert($tlsOptions['clientCert'], $tlsOptions['clientKey']);
            }

            $tlsConfig = $builder->build();
        }

        $grpc = new GrpcClient($resolvedLogger, $tlsConfig);
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $pdClient = new PdClient($grpc, $pdEndpoints[0], $resolvedLogger, $storeCache);

        $timeoutConfig = new TimeoutConfig();

        if (isset($options[self::OPT_TIMEOUT]) && is_array($options[self::OPT_TIMEOUT])) {
            $t = $options[self::OPT_TIMEOUT];
            $timeoutConfig = new TimeoutConfig(
                readTimeoutMs: isset($t['readTimeoutMs']) && is_int($t['readTimeoutMs'])
                    ? $t['readTimeoutMs'] : $timeoutConfig->readTimeoutMs,
                writeTimeoutMs: isset($t['writeTimeoutMs']) && is_int($t['writeTimeoutMs'])
                    ? $t['writeTimeoutMs'] : $timeoutConfig->writeTimeoutMs,
                batchReadTimeoutMs: isset($t['batchReadTimeoutMs']) && is_int($t['batchReadTimeoutMs'])
                    ? $t['batchReadTimeoutMs'] : $timeoutConfig->batchReadTimeoutMs,
                batchWriteTimeoutMs: isset($t['batchWriteTimeoutMs']) && is_int($t['batchWriteTimeoutMs'])
                    ? $t['batchWriteTimeoutMs'] : $timeoutConfig->batchWriteTimeoutMs,
                scanTimeoutMs: isset($t['scanTimeoutMs']) && is_int($t['scanTimeoutMs'])
                    ? $t['scanTimeoutMs'] : $timeoutConfig->scanTimeoutMs,
                deleteRangeTimeoutMs: isset($t['deleteRangeTimeoutMs']) && is_int($t['deleteRangeTimeoutMs'])
                    ? $t['deleteRangeTimeoutMs'] : $timeoutConfig->deleteRangeTimeoutMs,
            );
        }

        return new self(
            $pdClient,
            $grpc,
            new RegionCache(logger: $resolvedLogger),
            logger: $resolvedLogger,
            timeoutConfig: $timeoutConfig,
        );
    }

    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly int $serverBusyBudgetMs = 600000,
        TimeoutConfig $timeoutConfig = new TimeoutConfig(),
        ?RegionResolver $regionResolver = null,
        ?RawKvCrud $crud = null,
        ?RawKvAtomic $atomic = null,
        ?RawKvBatch $batch = null,
        ?RawKvScanner $scanner = null,
        ?RawKvRangeOps $rangeOps = null,
    ) {
        $regionResolver ??= new RegionResolver($pdClient, $regionCache);
        $this->crud = $crud ?? new RawKvCrud($grpc, $regionResolver, $timeoutConfig);
        $this->atomic = $atomic ?? new RawKvAtomic($grpc, $regionResolver, $timeoutConfig);
        $this->batch = $batch ?? new RawKvBatch($grpc, $regionResolver, $timeoutConfig, $this->logger);
        $this->scanner = $scanner ?? new RawKvScanner(
            $pdClient,
            $grpc,
            $regionResolver,
            $timeoutConfig,
            $maxBackoffMs,
            $serverBusyBudgetMs,
            $regionCache,
            $this->logger,
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

    private function createRetryExecutor(): RetryExecutor
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new RetryExecutor(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $regionResolver,
            $this->logger,
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
     * @param string[] $keys
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
     * @param string[] $keys
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
    // Lifecycle
    // ========================================================================

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

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
