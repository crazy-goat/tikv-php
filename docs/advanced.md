# Advanced Features

Production-ready patterns and advanced usage of the TiKV PHP Client.

## Table of Contents

1. [Production Patterns](#production-patterns)
2. [Performance Optimization](#performance-optimization)
3. [Error Handling Strategies](#error-handling-strategies)
4. [Monitoring and Observability](#monitoring-and-observability)
5. [Security](#security)
6. [Multi-Region Considerations](#multi-region-considerations)

## Production Patterns

### Connection Management

#### Long-Running Processes

For daemons, workers, and long-running scripts:

```php
class TiKvWorker
{
    private RawKvClient $client;
    private int $operationsCount = 0;
    private const RECONNECT_AFTER = 10000;
    
    public function __construct(array $pdEndpoints)
    {
        $this->connect($pdEndpoints);
    }
    
    private function connect(array $pdEndpoints): void
    {
        $this->client = RawKvClient::create($pdEndpoints);
        $this->operationsCount = 0;
    }
    
    public function processJob($job): void
    {
        // Periodic reconnection to prevent stale connections
        if ($this->operationsCount >= self::RECONNECT_AFTER) {
            $this->client->close();
            $this->connect(['127.0.0.1:2379']);
        }
        
        try {
            // Process job
            $this->client->put($job['key'], $job['value']);
            $this->operationsCount++;
        } catch (TiKvException $e) {
            // Reconnect on error
            $this->client->close();
            $this->connect(['127.0.0.1:2379']);
            throw $e;
        }
    }
    
    public function shutdown(): void
    {
        $this->client->close();
    }
}
```

#### Connection Pooling (Simulated)

While the client doesn't have explicit connection pooling, gRPC channels are reused:

```php
class TiKvPool
{
    private array $clients = [];
    private array $config;
    
    public function __construct(array $config)
    {
        $this->config = $config;
    }
    
    public function acquire(): RawKvClient
    {
        // Return existing client or create new one
        foreach ($this->clients as $client) {
            // Simple round-robin
            return $client;
        }
        
        $client = RawKvClient::create($this->config['endpoints']);
        $this->clients[] = $client;
        return $client;
    }
    
    public function release(RawKvClient $client): void
    {
        // In PHP, we typically don't release - just reuse
    }
    
    public function closeAll(): void
    {
        foreach ($this->clients as $client) {
            $client->close();
        }
        $this->clients = [];
    }
}
```

### Caching Strategies

#### Application-Level Caching

Combine TiKV with local cache for hot data:

```php
class CachedTiKvClient
{
    private RawKvClient $client;
    private array $localCache = [];
    private int $cacheTtl;
    
    public function __construct(RawKvClient $client, int $cacheTtl = 60)
    {
        $this->client = $client;
        $this->cacheTtl = $cacheTtl;
    }
    
    public function get(string $key): ?string
    {
        // Check local cache
        if (isset($this->localCache[$key])) {
            $entry = $this->localCache[$key];
            if ($entry['expires'] > time()) {
                return $entry['value'];
            }
            unset($this->localCache[$key]);
        }
        
        // Fetch from TiKV
        $value = $this->client->get($key);
        
        // Cache if found
        if ($value !== null) {
            $this->localCache[$key] = [
                'value' => $value,
                'expires' => time() + $this->cacheTtl,
            ];
        }
        
        return $value;
    }
    
    public function put(string $key, string $value, int $ttl = 0): void
    {
        $this->client->put($key, $value, $ttl);
        
        // Invalidate local cache
        unset($this->localCache[$key]);
    }
    
    public function invalidate(string $pattern): void
    {
        foreach ($this->localCache as $key => $entry) {
            if (str_starts_with($key, $pattern)) {
                unset($this->localCache[$key]);
            }
        }
    }
}
```

#### Cache Warming

Pre-populate cache before high-traffic periods:

```php
function warmCache(RawKvClient $client, array $hotKeys): void
{
    // Batch fetch all hot keys
    $values = $client->batchGet($hotKeys);
    
    // Store in application cache (Redis, Memcached, etc.)
    foreach ($values as $key => $value) {
        if ($value !== null) {
            redis()->setex("tikv:$key", 300, $value);
        }
    }
}

// Usage
$hotKeys = ['config:app', 'user:1', 'product:top:10'];
warmCache($client, $hotKeys);
```

### Distributed Patterns

#### Leader Election

```php
class LeaderElection
{
    private RawKvClient $client;
    private string $nodeId;
    private string $lockKey;
    private int $ttl;
    
    public function __construct(
        RawKvClient $client,
        string $nodeId,
        string $resource,
        int $ttl = 30
    ) {
        $this->client = $client;
        $this->nodeId = $nodeId;
        $this->lockKey = "leader:$resource";
        $this->ttl = $ttl;
    }
    
    public function tryBecomeLeader(): bool
    {
        $existing = $this->client->putIfAbsent($this->lockKey, $this->nodeId, $this->ttl);
        return $existing === null;
    }
    
    public function renewLeadership(): bool
    {
        $current = $this->client->get($this->lockKey);
        
        if ($current === $this->nodeId) {
            // Still leader, renew
            $this->client->put($this->lockKey, $this->nodeId, $this->ttl);
            return true;
        }
        
        return false;
    }
    
    public function stepDown(): void
    {
        $current = $this->client->get($this->lockKey);
        if ($current === $this->nodeId) {
            $this->client->delete($this->lockKey);
        }
    }
    
    public function isLeader(): bool
    {
        return $this->client->get($this->lockKey) === $this->nodeId;
    }
}

// Usage
$election = new LeaderElection($client, 'node-1', 'scheduler');

if ($election->tryBecomeLeader()) {
    echo "Became leader\n";
    
    // Start heartbeat
    while (true) {
        sleep(10);
        if (!$election->renewLeadership()) {
            echo "Lost leadership\n";
            break;
        }
        // Do leader work
    }
}
```

#### Distributed Counter

```php
class DistributedCounter
{
    private RawKvClient $client;
    private string $key;
    
    public function __construct(RawKvClient $client, string $name)
    {
        $this->client = $client;
        $this->key = "counter:$name";
    }
    
    public function increment(int $delta = 1): int
    {
        $maxRetries = 10;
        
        for ($i = 0; $i < $maxRetries; $i++) {
            $current = $this->client->get($this->key) ?? '0';
            $next = (int)$current + $delta;
            
            $result = $this->client->compareAndSwap(
                $this->key,
                $current,
                (string)$next
            );
            
            if ($result->swapped) {
                return $next;
            }
            
            // Exponential backoff
            usleep(1000 * (2 ** $i));  // 1ms, 2ms, 4ms, 8ms...
        }
        
        throw new RuntimeException("Failed to increment counter after $maxRetries retries");
    }
    
    public function get(): int
    {
        return (int)($this->client->get($this->key) ?? '0');
    }
    
    public function reset(): void
    {
        $this->client->put($this->key, '0');
    }
}

// Usage
$counter = new DistributedCounter($client, 'page_views');
$newCount = $counter->increment();
echo "Page views: $newCount\n";
```

## Performance Optimization

### Batch Optimization

#### Optimal Batch Sizes

```php
class BatchOptimizer
{
    private const MAX_BATCH_SIZE = 1000;
    private const TARGET_REGION_SIZE = 100;  // Keys per region
    
    public static function optimizeBatches(array $keys): array
    {
        // Split large batches into optimal chunks
        if (count($keys) <= self::MAX_BATCH_SIZE) {
            return [$keys];
        }
        
        return array_chunk($keys, self::MAX_BATCH_SIZE);
    }
    
    public static function parallelBatchGet(
        RawKvClient $client,
        array $keys,
        int $concurrency = 5
    ): array {
        $batches = self::optimizeBatches($keys);
        $results = [];
        
        // Process batches with limited concurrency
        $queue = new SplQueue();
        foreach ($batches as $batch) {
            $queue->enqueue($batch);
        }
        
        while (!$queue->isEmpty()) {
            $batch = $queue->dequeue();
            $batchResults = $client->batchGet($batch);
            $results = array_merge($results, $batchResults);
        }
        
        return $results;
    }
}
```

#### Write Batching

```php
class WriteBuffer
{
    private RawKvClient $client;
    private array $buffer = [];
    private int $maxSize;
    private float $maxWaitMs;
    private float $lastFlush;
    
    public function __construct(
        RawKvClient $client,
        int $maxSize = 100,
        float $maxWaitMs = 1000
    ) {
        $this->client = $client;
        $this->maxSize = $maxSize;
        $this->maxWaitMs = $maxWaitMs;
        $this->lastFlush = microtime(true) * 1000;
    }
    
    public function put(string $key, string $value, int $ttl = 0): void
    {
        $this->buffer[$key] = ['value' => $value, 'ttl' => $ttl];
        
        if (count($this->buffer) >= $this->maxSize) {
            $this->flush();
        }
    }
    
    public function shouldFlush(): bool
    {
        $elapsed = (microtime(true) * 1000) - $this->lastFlush;
        return $elapsed >= $this->maxWaitMs || count($this->buffer) >= $this->maxSize;
    }
    
    public function flush(): void
    {
        if (empty($this->buffer)) {
            return;
        }
        
        // Group by TTL for efficient batching
        $byTtl = [];
        foreach ($this->buffer as $key => $data) {
            $ttl = $data['ttl'];
            $byTtl[$ttl][$key] = $data['value'];
        }
        
        // Batch put each TTL group
        foreach ($byTtl as $ttl => $pairs) {
            $this->client->batchPut($pairs, $ttl);
        }
        
        $this->buffer = [];
        $this->lastFlush = microtime(true) * 1000;
    }
    
    public function __destruct()
    {
        $this->flush();
    }
}

// Usage
$buffer = new WriteBuffer($client, maxSize: 500, maxWaitMs: 500);

foreach ($data as $key => $value) {
    $buffer->put($key, $value);
}

$buffer->flush();  // Ensure all writes are persisted
```

### Scan Optimization

#### Pagination

```php
class ScanPaginator
{
    private RawKvClient $client;
    private int $pageSize;
    
    public function __construct(RawKvClient $client, int $pageSize = 100)
    {
        $this->client = $client;
        $this->pageSize = $pageSize;
    }
    
    public function paginate(string $prefix): Generator
    {
        $startKey = $prefix;
        $endKey = $this->incrementKey($prefix);
        
        while (true) {
            $page = $this->client->scan($startKey, $endKey, limit: $this->pageSize);
            
            if (empty($page)) {
                break;
            }
            
            yield $page;
            
            // Next page starts after the last key
            $lastKey = $page[count($page) - 1]['key'];
            $startKey = $lastKey . "\x00";
            
            if (count($page) < $this->pageSize) {
                break;  // Last page
            }
        }
    }
    
    private function incrementKey(string $key): string
    {
        // Calculate next possible key after prefix
        $lastByte = ord($key[strlen($key) - 1]);
        if ($lastByte === 255) {
            return substr($key, 0, -1) . chr(0);
        }
        return substr($key, 0, -1) . chr($lastByte + 1);
    }
}

// Usage
$paginator = new ScanPaginator($client, pageSize: 50);

foreach ($paginator->paginate('user:') as $page) {
    echo "Processing page with " . count($page) . " users\n";
    foreach ($page as $item) {
        processUser($item['key'], $item['value']);
    }
}
```

#### Parallel Scanning

```php
function parallelScan(
    RawKvClient $client,
    string $prefix,
    int $workers = 4
): array {
    // Divide key space into ranges
    $ranges = [];
    $chars = '0123456789abcdef';
    $step = strlen($chars) / $workers;
    
    for ($i = 0; $i < $workers; $i++) {
        $start = $prefix . $chars[$i * $step];
        $end = ($i === $workers - 1) 
            ? $prefix . ";"
            : $prefix . $chars[($i + 1) * $step];
        $ranges[] = [$start, $end];
    }
    
    // Scan ranges in parallel (using batchScan)
    $results = $client->batchScan($ranges, eachLimit: 10000);
    
    // Merge results
    return array_merge(...$results);
}
```

## Error Handling Strategies

### Retry Strategies

#### Exponential Backoff

```php
function withRetry(callable $operation, int $maxRetries = 3): mixed
{
    $lastException = null;
    
    for ($i = 0; $i < $maxRetries; $i++) {
        try {
            return $operation();
        } catch (TiKvException $e) {
            $lastException = $e;
            
            // Don't retry non-retryable errors
            if (!isRetryable($e)) {
                throw $e;
            }
            
            // Exponential backoff with jitter
            $delay = min(1000 * (2 ** $i), 30000);  // Max 30s
            $jitter = random_int(0, 100);
            usleep(($delay + $jitter) * 1000);
        }
    }
    
    throw $lastException;
}

function isRetryable(TiKvException $e): bool
{
    $message = $e->getMessage();
    $retryable = [
        'EpochNotMatch',
        'NotLeader',
        'ServerIsBusy',
        'StaleCommand',
        'RegionNotFound',
    ];
    
    foreach ($retryable as $pattern) {
        if (str_contains($message, $pattern)) {
            return true;
        }
    }
    
    return false;
}

// Usage
$value = withRetry(function() use ($client, $key) {
    return $client->get($key);
});
```

#### Circuit Breaker

```php
class CircuitBreaker
{
    private int $failureThreshold;
    private int $recoveryTimeout;
    private int $failures = 0;
    private ?int $lastFailureTime = null;
    private string $state = 'CLOSED';  // CLOSED, OPEN, HALF_OPEN
    
    public function __construct(
        int $failureThreshold = 5,
        int $recoveryTimeout = 60
    ) {
        $this->failureThreshold = $failureThreshold;
        $this->recoveryTimeout = $recoveryTimeout;
    }
    
    public function call(callable $operation): mixed
    {
        if ($this->state === 'OPEN') {
            if ($this->shouldAttemptReset()) {
                $this->state = 'HALF_OPEN';
            } else {
                throw new RuntimeException("Circuit breaker is OPEN");
            }
        }
        
        try {
            $result = $operation();
            $this->onSuccess();
            return $result;
        } catch (Exception $e) {
            $this->onFailure();
            throw $e;
        }
    }
    
    private function onSuccess(): void
    {
        $this->failures = 0;
        $this->state = 'CLOSED';
    }
    
    private function onFailure(): void
    {
        $this->failures++;
        $this->lastFailureTime = time();
        
        if ($this->failures >= $this->failureThreshold) {
            $this->state = 'OPEN';
        }
    }
    
    private function shouldAttemptReset(): bool
    {
        return (time() - $this->lastFailureTime) >= $this->recoveryTimeout;
    }
}

// Usage
$breaker = new CircuitBreaker(failureThreshold: 3);

$value = $breaker->call(function() use ($client, $key) {
    return $client->get($key);
});
```

### Graceful Degradation

```php
class ResilientCache
{
    private RawKvClient $client;
    private array $fallbackCache = [];
    private bool $tiKvAvailable = true;
    
    public function get(string $key): ?string
    {
        if (!$this->tiKvAvailable) {
            return $this->fallbackCache[$key] ?? null;
        }
        
        try {
            $value = $this->client->get($key);
            
            // Update fallback cache
            if ($value !== null) {
                $this->fallbackCache[$key] = $value;
            }
            
            return $value;
        } catch (TiKvException $e) {
            $this->tiKvAvailable = false;
            
            // Schedule recovery check
            $this->scheduleRecovery();
            
            // Return from fallback
            return $this->fallbackCache[$key] ?? null;
        }
    }
    
    private function scheduleRecovery(): void
    {
        // In real implementation, use a timer or background process
        // For now, just mark for retry on next request
        $this->tiKvAvailable = true;  // Will retry on next call
    }
}
```

## Monitoring and Observability

### Metrics Collection

The client ships a built-in, opt-in metrics hook: implement
[`MetricsInterface`](../src/Client/Observability/MetricsInterface.php) and
pass it via the `metrics` option. There is no need to wrap client methods in
a decorator to time operations — the library already reports RPC counts,
per-RPC latency, retries and region-cache behaviour from inside the client,
which an external wrapper cannot see.

The interface has six methods (full reference with emitted tag examples:
[Configuration → Metrics and Observability](configuration.md#metrics-and-observability)):

| Method                 | Reported when                                            |
|------------------------|----------------------------------------------------------|
| `rpcStarted()`         | An outbound gRPC call is dispatched                       |
| `rpcCompleted()`       | The call returns — carries duration in ms + success flag  |
| `retryAttempted()`     | A retryable error triggers another attempt                |
| `regionCacheHit()`     | A region was served from the client's region cache        |
| `regionCacheMiss()`    | The cache missed and PD had to be queried                 |
| `regionInvalidated()`  | A region was dropped from the cache (e.g. `retry_region_error`) |

Implementations must never throw; the default is a zero-cost no-op
(`NoOpMetrics`). A ready-made in-memory recorder for tests and benchmarks
ships as [`InMemoryMetrics`](../src/Client/Observability/InMemoryMetrics.php)
with `getRpcStarted(string $operation)`, `getRpcSucceeded(string $operation)`,
`getRetries(string $operation)`, `getCacheHits(string $operation)`,
`getMeanLatencyMs(string $operation)`, `getInvalidations(string $reason)`
(this one keys on the invalidation *reason*, not an operation tag) and
`reset()`.

Production-shaped example — exporting the built-in counters to Prometheus:

```php
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;

final class PrometheusMetrics implements MetricsInterface
{
    public function __construct(
        private readonly \Prometheus\CollectorRegistry $registry,
        private readonly string $namespace = 'tikv_client',
    ) {
    }

    public function rpcStarted(string $operation): void
    {
        $this->counter('rpc_started_total', 'Outbound gRPC calls', ['operation'])
            ->inc(['operation' => $operation]);
    }

    public function rpcCompleted(string $operation, float $durationMs, bool $success): void
    {
        // Histograms give latency distribution, not just averages.
        $this->histogram('rpc_duration_ms', 'gRPC call duration', [1, 5, 10, 50, 100, 500, 1000, 5000])
            ->observe($durationMs, ['operation' => $operation, 'success' => $success ? '1' : '0']);
    }

    public function retryAttempted(string $operation): void
    {
        $this->counter('retries_total', 'Retryable errors retried', ['operation'])
            ->inc(['operation' => $operation]);
    }

    public function regionCacheHit(string $operation): void
    {
        $this->counter('region_cache_hits_total', 'Region cache hits', ['operation'])
            ->inc(['operation' => $operation]);
    }

    public function regionCacheMiss(string $operation): void
    {
        $this->counter('region_cache_misses_total', 'Region cache misses', ['operation'])
            ->inc(['operation' => $operation]);
    }

    public function regionInvalidated(string $reason): void
    {
        $this->counter('region_invalidations_total', 'Regions dropped from cache', ['reason'])
            ->inc(['reason' => $reason]);
    }

    private function counter(string $name, string $help, array $labels): \Prometheus\Counter
    {
        return $this->registry->getOrRegisterCounter($this->namespace, $name, $help, $labels);
    }

    private function histogram(string $name, string $help, array $buckets): \Prometheus\Histogram
    {
        // Label names must be declared at registration time or ->observe() throws.
        return $this->registry->getOrRegisterHistogram($this->namespace, $name, $help, ['operation', 'success'], $buckets);
    }
}

// Wire it up:
$metrics = new PrometheusMetrics(new \Prometheus\CollectorRegistry(new \Prometheus\Storage\InMemory()));
$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: ['metrics' => $metrics],
);

// Counters are also reachable afterwards:
$injected = $client->getMetrics();   // returns whatever was passed via 'metrics'
```

For tests and quick diagnostics, skip the adapter entirely:

```php
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;

$metrics = new InMemoryMetrics();
$client = RawKvClient::create(pdEndpoints: ['127.0.0.1:2379'], options: ['metrics' => $metrics]);
// ... run operations ...

echo $metrics->getMeanLatencyMs('get');   // mean latency of completed get() RPCs
echo $metrics->getRetries('scan');        // retries observed for scan operations
```

### Health Checks

```php
class TiKvHealthChecker
{
    private RawKvClient $client;
    private string $healthKey;
    
    public function __construct(RawKvClient $client)
    {
        $this->client = $client;
        $this->healthKey = 'health:check:' . uniqid();
    }
    
    public function check(): array
    {
        $start = microtime(true);
        $checks = [
            'write' => false,
            'read' => false,
            'delete' => false,
            'latency_ms' => 0,
        ];
        
        try {
            // Test write
            $this->client->put($this->healthKey, 'ok');
            $checks['write'] = true;
            
            // Test read
            $value = $this->client->get($this->healthKey);
            $checks['read'] = $value === 'ok';
            
            // Test delete
            $this->client->delete($this->healthKey);
            $value = $this->client->get($this->healthKey);
            $checks['delete'] = $value === null;
            
            $checks['latency_ms'] = (microtime(true) - $start) * 1000;
            $checks['healthy'] = $checks['write'] && $checks['read'] && $checks['delete'];
        } catch (Exception $e) {
            $checks['error'] = $e->getMessage();
            $checks['healthy'] = false;
        }
        
        return $checks;
    }
}

// Usage in monitoring endpoint
$health = new TiKvHealthChecker($client);
$status = $health->check();

if (!$status['healthy']) {
    // Alert on-call, log to monitoring system
    error_log("TiKV health check failed: " . json_encode($status));
}
```

## Security

### TLS Best Practices

```php
// Production TLS configuration
$options = [
    'tls' => [
        // Always verify server certificate
        'caCertFile' => '/etc/ssl/certs/tikv-ca.crt',

        // Use client certificates for mutual TLS
        'clientCertFile' => '/etc/ssl/certs/tikv-client.crt',
        'clientKeyFile' => '/etc/ssl/private/tikv-client.key',
    ],
];

// Certificate rotation (reload without restart)
class RotatingTlsClient
{
    private RawKvClient $client;
    private array $config;
    private int $lastReload;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->reload();
    }

    public function reload(): void
    {
        if (isset($this->client)) {
            $this->client->close();
        }

        // Reload certificates from disk (inline PEM content)
        $options = [
            'tls' => [
                'caCertPem' => file_get_contents($this->config['caCertFile']),
                'clientCertPem' => file_get_contents($this->config['clientCertFile']),
                'clientKeyPem' => file_get_contents($this->config['clientKeyFile']),
            ],
        ];
        
        $this->client = RawKvClient::create(
            $this->config['endpoints'],
            options: $options
        );
        
        $this->lastReload = time();
    }
    
    public function getClient(): RawKvClient
    {
        // Reload every hour
        if (time() - $this->lastReload > 3600) {
            $this->reload();
        }
        
        return $this->client;
    }
}
```

### Data Encryption

For sensitive data, encrypt before storing:

```php
class EncryptedTiKvClient
{
    private RawKvClient $client;
    private string $encryptionKey;
    
    public function put(string $key, string $value): void
    {
        $encrypted = $this->encrypt($value);
        $this->client->put($key, $encrypted);
    }
    
    public function get(string $key): ?string
    {
        $encrypted = $this->client->get($key);
        return $encrypted !== null ? $this->decrypt($encrypted) : null;
    }
    
    private function encrypt(string $data): string
    {
        $iv = random_bytes(16);
        $encrypted = openssl_encrypt(
            $data,
            'AES-256-GCM',
            $this->encryptionKey,
            OPENSSL_RAW_DATA,
            $iv,
            $tag
        );
        return base64_encode($iv . $tag . $encrypted);
    }
    
    private function decrypt(string $data): string
    {
        $decoded = base64_decode($data);
        $iv = substr($decoded, 0, 16);
        $tag = substr($decoded, 16, 16);
        $ciphertext = substr($decoded, 32);
        
        return openssl_decrypt(
            $ciphertext,
            'AES-256-GCM',
            $this->encryptionKey,
            OPENSSL_RAW_DATA,
            $iv,
            $tag
        );
    }
}
```

## Multi-Region Considerations

### Cross-Region Latency

When TiKV spans multiple regions:

```php
class MultiRegionClient
{
    private array $regionClients = [];
    
    public function __construct(array $regionConfigs)
    {
        foreach ($regionConfigs as $region => $config) {
            $this->regionClients[$region] = RawKvClient::create($config['endpoints']);
        }
    }
    
    public function get(string $key, string $preferredRegion = 'local'): ?string
    {
        // Try preferred region first
        $client = $this->regionClients[$preferredRegion];
        $value = $client->get($key);
        
        if ($value !== null) {
            return $value;
        }
        
        // Fallback to other regions
        foreach ($this->regionClients as $region => $client) {
            if ($region === $preferredRegion) {
                continue;
            }
            
            $value = $client->get($key);
            if ($value !== null) {
                return $value;
            }
        }
        
        return null;
    }
}
```

### Data Locality

Route requests to nearest TiKV:

```php
class LocalityAwareRouter
{
    private string $localRegion;
    private array $regionClients;
    
    public function getClientForKey(string $key): RawKvClient
    {
        // Determine which region owns this key
        $region = $this->determineRegion($key);
        
        // If local region has replica, use it
        if ($this->hasLocalReplica($region)) {
            return $this->regionClients[$this->localRegion];
        }
        
        // Otherwise use the owning region
        return $this->regionClients[$region];
    }
    
    private function determineRegion(string $key): string
    {
        // Use key prefix or hash to determine region
        if (str_starts_with($key, 'us:')) return 'us';
        if (str_starts_with($key, 'eu:')) return 'eu';
        if (str_starts_with($key, 'ap:')) return 'ap';
        return 'default';
    }
}
```

## See Also

- [Configuration](configuration.md) - Basic configuration options
- [Operations](operations.md) - All available operations
- [Troubleshooting](troubleshooting.md) - Solving common issues
- [Architecture](architecture.md) - Understanding the internals
