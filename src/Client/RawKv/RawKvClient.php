<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
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

    private readonly RawKvCrud $crud;
    private readonly RawKvAtomic $atomic;
    private readonly RawKvBatch $batch;
    private readonly RawKvScanner $scanner;
    private readonly RawKvRangeOps $rangeOps;

    /**
     * @param string[] $pdEndpoints
     * @param array<string, mixed> $options
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

            if (isset($tlsOptions['caCert']) && is_string($tlsOptions['caCert'])) {
                $builder->withCaCert($tlsOptions['caCert']);
            }

            if (
                isset($tlsOptions['clientCert']) && is_string($tlsOptions['clientCert']) &&
                isset($tlsOptions['clientKey']) && is_string($tlsOptions['clientKey'])
            ) {
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

    public function get(string $key): ?string
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'get');
        $this->validateKeySize($key, 'get');

        return $this->crud->get($key, $this->createRetryExecutor());
    }

    public function put(string $key, string $value, int $ttl = 0): void
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'put');
        $this->validateKeySize($key, 'put');
        $this->validateValueSize($value, 'put');

        $this->crud->put($key, $value, $ttl, $this->createRetryExecutor());
    }

    public function delete(string $key): void
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'delete');
        $this->validateKeySize($key, 'delete');

        $this->crud->delete($key, $this->createRetryExecutor());
    }

    public function getKeyTTL(string $key): ?int
    {
        $this->ensureOpen();

        return $this->crud->getKeyTTL($key, $this->createRetryExecutor());
    }

    // ========================================================================
    // Atomic operations
    // ========================================================================

    public function compareAndSwap(string $key, ?string $expectedValue, string $newValue, int $ttl = 0): CasResult
    {
        $this->ensureOpen();
        $this->validateKeyNotEmpty($key, 'compareAndSwap');
        $this->validateKeySize($key, 'compareAndSwap');
        $this->validateValueSize($newValue, 'compareAndSwap');

        return $this->atomic->compareAndSwap($key, $expectedValue, $newValue, $ttl, $this->createRetryExecutor());
    }

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

        return $this->batch->batchGet($keys, $this->createRetryExecutor());
    }

    /**
     * @param array<string, string> $keyValuePairs
     * @param int|array<array-key, int> $ttl
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

        $this->batch->batchPut($keyValuePairs, $ttl, $this->createRetryExecutor());
    }

    /**
     * @param string[] $keys
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

        $this->batch->batchDelete($keys, $this->createRetryExecutor());
    }

    // ========================================================================
    // Scan operations
    // ========================================================================

    public function scanIterator(
        string $startKey,
        string $endKey,
        int $batchSize = 256,
        bool $keyOnly = false,
    ): ScanIterator {
        $this->ensureOpen();

        return $this->scanner->scanIterator($startKey, $endKey, $batchSize, $keyOnly);
    }

    public function scanPrefixIterator(string $prefix, int $batchSize = 256, bool $keyOnly = false): ScanIterator
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefixIterator($prefix, $batchSize, $keyOnly);
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function scan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $this->validateKeySize($startKey, 'scan');
        if ($endKey !== '') {
            $this->validateKeySize($endKey, 'scan');
        }

        return $this->scanner->scan($startKey, $endKey, $limit, $keyOnly);
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function scanPrefix(string $prefix, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        return $this->scanner->scanPrefix($prefix, $limit, $keyOnly);
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function reverseScan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array
    {
        $this->ensureOpen();
        $this->validateKeySize($startKey, 'reverseScan');
        if ($endKey !== '') {
            $this->validateKeySize($endKey, 'reverseScan');
        }

        return $this->scanner->reverseScan($startKey, $endKey, $limit, $keyOnly);
    }

    /**
     * @param array<array{0: string, 1: string}> $ranges
     * @return array<array<array{key: string, value: ?string}>>
     */
    public function batchScan(array $ranges, int $eachLimit, bool $keyOnly = false): array
    {
        $this->ensureOpen();

        if ($ranges === []) {
            return [];
        }

        return $this->scanner->batchScan($ranges, $eachLimit, $keyOnly);
    }

    // ========================================================================
    // Range operations
    // ========================================================================

    public function deleteRange(string $startKey, string $endKey): void
    {
        $this->ensureOpen();

        $this->rangeOps->deleteRange($startKey, $endKey);
    }

    public function deletePrefix(string $prefix): void
    {
        $this->ensureOpen();

        if ($prefix === '') {
            throw new InvalidArgumentException('Prefix must not be empty -- refusing to delete all keys');
        }

        $this->rangeOps->deletePrefix($prefix);
    }

    public function checksum(string $startKey, string $endKey): ChecksumResult
    {
        $this->ensureOpen();

        return $this->rangeOps->checksum($startKey, $endKey);
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    public function close(): void
    {
        if (!$this->closed) {
            $this->grpc->close();
            $this->pdClient->close();
            $this->closed = true;
        }
    }
}
