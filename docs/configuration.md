# Configuration Guide

Complete guide to configuring the TiKV PHP Client for development and production environments.

## Table of Contents

1. [Basic Configuration](#basic-configuration)
2. [Connection Settings](#connection-settings)
3. [TLS/SSL Configuration](#tlsssl-configuration)
4. [Logging](#logging)
5. [Retry and Backoff](#retry-and-backoff)
6. [Caching](#caching)
7. [Timeouts](#timeouts)
8. [Production Configuration](#production-configuration)

## Basic Configuration

### Creating a Client

The simplest configuration:

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$client = RawKvClient::create(['127.0.0.1:2379']);
```

### Full Configuration Options

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a PSR-3 logger
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::INFO));

// Client options
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',
        'clientCert' => '/path/to/client.crt',
        'clientKey' => '/path/to/client.key',
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,
    options: $options
);
```

## Connection Settings

### PD Endpoints

The client connects to PD (Placement Driver) to discover the cluster topology:

```php
// Single PD node
$client = RawKvClient::create(['192.168.1.100:2379']);

// Multiple PD nodes (for HA)
$client = RawKvClient::create([
    '192.168.1.100:2379',
    '192.168.1.101:2379',
    '192.168.1.102:2379',
]);
```

**Note**: Currently only the first endpoint is used. Future versions will support failover.

### Environment Variables

For flexibility, use environment variables:

```php
$pdEndpoints = getenv('PD_ENDPOINTS') 
    ? explode(',', getenv('PD_ENDPOINTS')) 
    : ['127.0.0.1:2379'];

$client = RawKvClient::create($pdEndpoints);
```

Set in your environment:

```bash
export PD_ENDPOINTS="192.168.1.100:2379,192.168.1.101:2379"
```

> **Note**: The library itself does not read any environment variables. The example above shows how your application can read `PD_ENDPOINTS` and pass it to the client. All configuration must be passed explicitly via constructor arguments or the `$options` array.

## TLS/SSL Configuration

> **Warning:** By default, when no TLS configuration is provided, the client connects
> in plaintext (unencrypted). A warning is logged on every insecure channel creation.
> To ensure TLS is always used, set `allowInsecure: false` (see below).

### Server Verification Only

Verify the server's certificate:

```php
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',  // CA certificate file path or content
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

### Mutual TLS (mTLS)

Client certificate authentication:

```php
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',
        'clientCert' => '/path/to/client.crt',
        'clientKey' => '/path/to/client.key',
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

> **Important:** When providing a client certificate and key, a CA certificate is
> **required**. The configuration will throw `InvalidArgumentException` if
> clientCert/clientKey are provided without caCert — this prevents accidental
> downgrade to plaintext.

### Using Certificate Content

You can pass certificate content directly instead of file paths:

```php
$caCert = file_get_contents('/path/to/ca.crt');
$clientCert = file_get_contents('/path/to/client.crt');
$clientKey = file_get_contents('/path/to/client.key');

$options = [
    'tls' => [
        'caCert' => $caCert,
        'clientCert' => $clientCert,
        'clientKey' => $clientKey,
    ],
];
```

### TLS Configuration Builder

For advanced TLS configuration:

```php
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;

$builder = new TlsConfigBuilder();
$builder->withCaCert('/path/to/ca.crt')
    ->withClientCert('/path/to/client.crt', '/path/to/client.key');

$tlsConfig = $builder->build();

// Use with custom client construction
$grpc = new GrpcClient($logger, $tlsConfig);
$pdClient = new PdClient($grpc, 'tikv.example.com:2379', $logger);
$client = new RawKvClient($pdClient, $grpc);
```

### Fail-Closed Mode (Require TLS)

By default, if no TLS configuration is provided, the client connects in plaintext
and logs a warning. To fail-closed — reject any connection that isn't using TLS —
set `allowInsecure: false` on the `GrpcClient`:

```php
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;

// Insecure connections will throw InvalidStateException
$grpc = new GrpcClient($logger, tlsConfig: $tlsConfig, allowInsecure: false);
```

> **Note:** When using `RawKvClient::create()` or `TxnKvClient::create()`, the
> underlying `GrpcClient` is constructed internally with `allowInsecure: true`.
> To enforce TLS, use the manual construction path shown above.
```

## Logging

### PSR-3 Logger Integration

The client supports any PSR-3 compatible logger:

```php
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);
```

### Log Levels

Choose appropriate log levels for your environment:

```php
// Development - verbose logging
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

// Production - warnings and errors only
$logger->pushHandler(new StreamHandler('/var/log/tikv.log', Logger::WARNING));
```

### What Gets Logged

The client logs:

- **DEBUG**: Connection attempts, cache operations, request details
- **INFO**: Successful operations, region cache hits/misses
- **WARNING**: Retry attempts, region invalidations
- **ERROR**: Failed operations, exhausted retries, fatal errors

### Structured Logging

Use JSON format for log aggregation:

```php
use Monolog\Formatter\JsonFormatter;

$handler = new StreamHandler('/var/log/tikv.log', Logger::INFO);
$handler->setFormatter(new JsonFormatter());
$logger->pushHandler($handler);
```

### Multiple Handlers

Log to multiple destinations:

```php
// Errors to stderr
$logger->pushHandler(new StreamHandler('php://stderr', Logger::ERROR));

// All messages to file
$logger->pushHandler(new StreamHandler('/var/log/tikv.log', Logger::DEBUG));

// Critical alerts to Slack/Email (using Monolog handlers)
$logger->pushHandler(new SlackWebhookHandler(...));
```

## Retry and Backoff

### Automatic Retry

The client automatically retries failed operations:

```php
// Default retry configuration (built-in)
$client = RawKvClient::create(['127.0.0.1:2379']);
// - Max backoff: 20 seconds
// - Server busy budget: 10 minutes
```

### Retry Behavior

The client retries on these errors:

| Error Type | Backoff Strategy | Description |
|------------|------------------|-------------|
| `EpochNotMatch` | No delay | Region epoch mismatch, immediate retry |
| `NotLeader` | Fast | Leader changed, quick retry |
| `ServerIsBusy` | Progressive | Server overloaded, increasing delays |
| `RegionNotFound` | Medium | Region not found, moderate delay |
| `StaleCommand` | Fast | Stale command, quick retry |
| gRPC errors | Progressive | Network issues, increasing delays |

### Non-Retryable Errors

These errors are not retried:

- `KeyNotInRegion` - Key outside region range
- `RaftEntryTooLarge` - Value too large
- `FlashbackInProgress` - Region in flashback mode

### Retry Bounds

Every retry loop is bounded by **two independent safety nets** to prevent
infinite busy loops when the underlying error has zero backoff delay
(e.g. `EpochNotMatch` is classified as `BackoffType::None` with `sleepMs=0`):

| Bound | Default | Description |
|-------|---------|-------------|
| `maxAttempts` | `30` | Maximum number of times the operation is invoked before the executor gives up. |
| `deadlineMs` | `0` (disabled) | Optional wall-clock deadline from the start of the call. When set, the executor terminates if the deadline is reached, regardless of `sleepMs`. |

When either bound is reached the executor throws
`RetryBudgetExhaustedException` (extends `TiKvException`). This exception
exposes `attempts()` and `elapsedOrBackoffMs()` for diagnostics.

### Custom Retry (Advanced)

For custom retry logic, extend the client:

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Retry\BackoffType;

class CustomRawKvClient extends RawKvClient
{
    protected function classifyError(TiKvException $e): ?BackoffType
    {
        // Add custom error classification
        if (str_contains($e->getMessage(), 'CustomError')) {
            return BackoffType::Custom;
        }
        return parent::classifyError($e);
    }
}
```

## Caching

### Region Cache

The client caches region metadata to avoid repeated PD queries:

```php
// Default: In-memory region cache (enabled automatically)
$client = RawKvClient::create(['127.0.0.1:2379']);
```

**Cache behavior**:
- Cache entries expire automatically on region errors
- `NotLeader` errors update the cache with new leader info
- `EpochNotMatch` errors invalidate affected regions

### Store Cache

Store addresses are also cached:

- Store information cached to avoid repeated PD queries
- Automatically refreshed on cache misses

### Cache Statistics

Monitor cache performance via logs:

```
[INFO] Region cache hit: region_id=123
[INFO] Region cache miss: key="user:123"
[INFO] Invalidated region: region_id=123, reason=EpochNotMatch
```

## Timeouts

### Per-Operation Timeouts

The client supports configurable per-operation gRPC timeouts via `TimeoutConfig` / `options['timeout']`:

```php
$options = [
    'timeout' => [
        'readTimeoutMs' => 5000,         // default: 5000
        'writeTimeoutMs' => 5000,        // default: 5000
        'batchReadTimeoutMs' => 10000,   // default: 10000
        'batchWriteTimeoutMs' => 10000,  // default: 10000
        'scanTimeoutMs' => 20000,        // default: 20000
        'deleteRangeTimeoutMs' => 30000, // default: 30000
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: $options
);
```

When a timeout is exceeded, the gRPC call throws `GrpcException` which is caught and retried by the retry executor unless the budget is exhausted.

### Default Timeouts

By default all timeouts are set to sensible values (5s for reads/writes, 10s for batch, 20s for scans, 30s for delete-range). Set any value to `0` to disable it (not recommended in production).

### Handling Slow Operations

For additional application-level safeguards:

```php
// Using PHP's pcntl_alarm (CLI only)
pcntl_alarm(30);  // 30 second timeout

try {
    $value = $client->get('key');
} catch (Exception $e) {
    // Handle timeout
} finally {
    pcntl_alarm(0);
}
```

Or use async patterns:

```php
// For batch operations, process in chunks
$chunks = array_chunk($keys, 100);
foreach ($chunks as $chunk) {
    $results = $client->batchGet($chunk);
    // Process results
}
```

## Production Configuration

### Recommended Production Setup

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\JsonFormatter;

// Production logger
$logger = new Logger('tikv');

// Structured JSON logging for aggregation
$handler = new StreamHandler('/var/log/tikv.log', Logger::INFO);
$handler->setFormatter(new JsonFormatter());
$logger->pushHandler($handler);

// Error output for monitoring
$logger->pushHandler(new StreamHandler('php://stderr', Logger::ERROR));

// TLS configuration (recommended for production)
$options = [
    'tls' => [
        'caCert' => '/etc/ssl/certs/tikv-ca.crt',
        'clientCert' => '/etc/ssl/certs/tikv-client.crt',
        'clientKey' => '/etc/ssl/private/tikv-client.key',
    ],
];

// Create client
$client = RawKvClient::create(
    pdEndpoints: ['tikv-pd-1:2379', 'tikv-pd-2:2379', 'tikv-pd-3:2379'],
    logger: $logger,
    options: $options
);
```

### Environment-Based Configuration

```php
// config/tikv.php
return [
    'endpoints' => explode(',', getenv('TIKV_PD_ENDPOINTS') ?: '127.0.0.1:2379'),
    'tls' => [
        'enabled' => getenv('TIKV_TLS_ENABLED') === 'true',
        'caCert' => getenv('TIKV_TLS_CA_CERT'),
        'clientCert' => getenv('TIKV_TLS_CLIENT_CERT'),
        'clientKey' => getenv('TIKV_TLS_CLIENT_KEY'),
    ],
    'logging' => [
        'level' => getenv('TIKV_LOG_LEVEL') ?: 'warning',
        'path' => getenv('TIKV_LOG_PATH') ?: '/var/log/tikv.log',
    ],
];
```

> **Note**: The `TIKV_*` environment variables shown are **application-level conventions** — the library does not read any environment variables directly. Your application is responsible for reading env vars and passing the values to the client constructor.

### Health Checks

Monitor client health:

```php
// Simple health check
try {
    $client->put('health:check', 'ok');
    $value = $client->get('health:check');
    $client->delete('health:check');
    echo "Healthy\n";
} catch (Exception $e) {
    echo "Unhealthy: " . $e->getMessage() . "\n";
}
```

### Connection Pooling

The client maintains persistent gRPC channels:

- Channels are reused across requests
- Automatic connection management
- No explicit connection pool configuration needed

### Resource Limits

Consider PHP's resource limits:

```ini
; php.ini
memory_limit = 256M
max_execution_time = 300
```

For long-running processes (workers, daemons):

```php
// Periodic cleanup in long-running processes
while (true) {
    // Process work
    
    // Optional: Force reconnection periodically
    if ($iteration % 1000 === 0) {
        $client->close();
        $client = RawKvClient::create($pdEndpoints, logger: $logger);
    }
}
```

## Configuration Checklist

Before deploying to production:

- [ ] PD endpoints are correct and accessible
- [ ] TLS certificates are configured (if required)
- [ ] Logging is configured with appropriate level
- [ ] Log files have correct permissions
- [ ] Health checks are implemented
- [ ] Error handling is in place
- [ ] Resource limits are configured
- [ ] TiKV cluster has `enable-ttl=true` (if using TTL)

## See Also

- [Operations Guide](operations.md) - Using the configured client
- [Advanced Features](advanced.md) - Production patterns
- [Troubleshooting](troubleshooting.md) - Solving configuration issues
