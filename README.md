# TiKV PHP Client

[![Tests](https://github.com/crazy-goat/tikv-php/actions/workflows/tests.yml/badge.svg)](https://github.com/crazy-goat/tikv-php/actions/workflows/tests.yml)

PHP client for TiKV RawKV API using gRPC extension.

## Requirements

- PHP >= 8.2
- gRPC extension
- TiKV cluster with RawKV enabled

## Quick Start

```bash
# Start TiKV cluster and run example
make up
make example
```

## Installation

```bash
composer require crazy-goat/tikv-client
```

## Usage

### Basic CRUD Operations

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$client = RawKvClient::create(['127.0.0.1:2379']);

// Basic CRUD
$client->put('key', 'value');
$value = $client->get('key');       // 'value'
$client->delete('key');

// Batch operations
$client->batchPut(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);
$values = $client->batchGet(['k1', 'k2', 'k3']);
$client->batchDelete(['k1', 'k2', 'k3']);

$client->close();
```

### Scanning

```php
// Range scan [startKey, endKey)
$results = $client->scan('start', 'end', limit: 100);
// Returns: [['key' => 'k1', 'value' => 'v1'], ['key' => 'k2', 'value' => 'v2'], ...]

// Prefix scanning
$results = $client->scanPrefix('user:');

// Reverse scan (descending order)
// Note: startKey = upper bound (exclusive), endKey = lower bound (inclusive)
$results = $client->reverseScan('end', 'start', limit: 100);

// Scan multiple non-contiguous ranges
$results = $client->batchScan([['a:', 'a;'], ['b:', 'b;']], eachLimit: 50);
```

### Range Operations

```php
// Delete all keys in range [startKey, endKey)
$client->deleteRange('temp:', 'temp;');

// Delete all keys with a given prefix
$client->deletePrefix('cache:');
```

### TTL (Time-To-Live)

Requires `enable-ttl=true` in tikv.toml configuration.

```php
// Store with expiration (TTL in seconds)
$client->put('session', 'data', ttl: 3600);        // expires in 1 hour

// Get remaining TTL
$remaining = $client->getKeyTTL('session');          // seconds remaining, or null if not found/no TTL
```

### Atomic Operations

> **Note:** Compare-And-Swap (CAS) and Put-If-Absent require atomic mode to be
> enabled via `setAtomicForCAS(true)`. In atomic mode, the underlying `RawPut`
> RPC sends the `for_cas` flag, which tells TiKV to use the atomic code path.
> Atomic mode is disabled by default for performance — the non-atomic code path
> is faster for regular writes. Enable it only when you need CAS semantics.

```php
use CrazyGoat\TiKV\Client\RawKv\CasResult;

// Enable atomic mode for CAS operations
$client->setAtomicForCAS(true);

// Compare-And-Swap (CAS)
$result = $client->compareAndSwap('counter', '1', '2');
if ($result->swapped) {
    echo "Value was swapped from '1' to '2'";
    echo "Previous value: " . ($result->previousValue ?? 'null');
}

// Put if key does not exist (distributed lock pattern)
$existing = $client->putIfAbsent('lock', 'owner-1');
if ($existing === null) {
    echo "Lock acquired!";
} else {
    echo "Lock already held by: $existing";
}
```

### Data Integrity

```php
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;

// Compute CRC64-XOR checksum over key range
$checksum = $client->checksum('data:', 'data;');
echo "Checksum: {$checksum->checksum}";
echo "Keys: {$checksum->totalKvs}, Bytes: {$checksum->totalBytes}";
```

### TLS/SSL Configuration

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;

// Configure TLS with CA certificate only (server verification)
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',  // or certificate content as string
    ],
];

// Or with mutual TLS (mTLS) - client certificate authentication
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',
        'clientCert' => '/path/to/client.crt',
        'clientKey' => '/path/to/client.key',
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

### PSR-3 Logging

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a PSR-3 compatible logger
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Pass logger to client
$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);

// The client will log:
// - Connection attempts and failures
// - Retry attempts with backoff information
// - Region cache hits/misses
// - Error conditions
```

### Complete Example

```php
<?php
require 'vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Setup logging
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::INFO));

// Connect to TiKV
$pdEndpoints = ['127.0.0.1:2379'];
$client = RawKvClient::create($pdEndpoints, logger: $logger);

try {
    // Store user data with TTL
    $client->put('user:123', json_encode(['name' => 'Alice', 'age' => 30]), ttl: 3600);
    $client->put('user:456', json_encode(['name' => 'Bob', 'age' => 25]), ttl: 3600);
    
    // Batch retrieve
    $users = $client->batchGet(['user:123', 'user:456']);
    foreach ($users as $key => $value) {
        if ($value !== null) {
            $data = json_decode($value, true);
            echo "$key: {$data['name']}\n";
        }
    }
    
    // Scan all users
    $allUsers = $client->scanPrefix('user:');
    echo "Total users: " . count($allUsers) . "\n";
    
    // Check TTL
    $ttl = $client->getKeyTTL('user:123');
    echo "TTL remaining: $ttl seconds\n";
    
    // Enable atomic mode for CAS operations
    $client->setAtomicForCAS(true);
    
    // Atomic counter update
    $client->put('counter', '0');
    $result = $client->compareAndSwap('counter', '0', '1');
    if ($result->swapped) {
        echo "Counter incremented!\n";
    }
    
} finally {
    $client->close();
}
```

## Implemented Operations

### Core CRUD
- ✅ **Get** / **Put** / **Delete** — Single key operations
- ✅ **BatchGet** / **BatchPut** / **BatchDelete** — Batch operations with parallel execution

### Scanning
- ✅ **Scan** — Range scan `[startKey, endKey)` with limit and keyOnly options
- ✅ **ReverseScan** — Reverse range scan (native `reverse=true`)
- ✅ **ScanPrefix** — Prefix-based scanning
- ✅ **BatchScan** — Multiple non-contiguous range scanning

### Range Operations
- ✅ **DeleteRange** — Delete all keys in `[startKey, endKey)`
- ✅ **DeletePrefix** — Delete all keys with a given prefix

### TTL
- ✅ **PutWithTTL** — Store with expiration (seconds)
- ✅ **GetKeyTTL** — Get remaining TTL of a key

### Atomic Operations
- ✅ **CompareAndSwap** — Atomic CAS with `CasResult` (swapped + previousValue)
- ✅ **PutIfAbsent** — Conditional insert (returns existing value or null)

### Data Integrity
- ✅ **Checksum** — CRC64-XOR checksum over key range with `ChecksumResult`

### Infrastructure
- ✅ **PD Region Discovery** — With RegionEpoch support
- ✅ **Region Routing** — Direct to correct TiKV node
- ✅ **Region Cache** — In-memory caching of region metadata
- ✅ **Store Cache** — In-memory caching of store addresses
- ✅ **Retry Logic** — Automatic retry with exponential backoff
- ✅ **NotLeader Handling** — Automatic leader redirection
- ✅ **Batch Async Execution** — Parallel execution across regions
- ✅ **TLS/SSL Support** — Server and mutual TLS authentication
- ✅ **PSR-3 Logging** — Structured logging with any PSR-3 logger

## Project Structure

```
src/
├── Client/
│   ├── Batch/
│   │   ├── BatchAsyncExecutor.php   # Parallel batch execution
│   │   └── GrpcFuture.php           # Async gRPC operations
│   ├── Cache/
│   │   ├── RegionCache.php          # Region metadata cache
│   │   └── StoreCache.php           # Store address cache
│   ├── Connection/
│   │   └── PdClient.php             # PD discovery & region routing
│   ├── Grpc/
│   │   └── GrpcClient.php           # Low-level gRPC wrapper
│   ├── RawKv/
│   │   ├── RawKvClient.php          # Main client (20+ operations)
│   │   ├── CasResult.php            # CompareAndSwap result
│   │   ├── ChecksumResult.php       # Checksum result
│   │   ├── ScanIterator.php         # Lazy auto-paginating scan iterator
│   │   └── Dto/                     # Shared DTOs (KeyValue, RegionInfo, PeerInfo)
│   ├── Retry/
│   │   └── BackoffType.php          # Retry backoff strategies
│   └── Tls/
│       ├── TlsConfig.php            # TLS configuration
│       └── TlsConfigBuilder.php     # TLS builder
└── Proto/                           # Generated protobuf classes
    ├── Kvrpcpb/                     # TiKV request/response
    ├── Pdpb/                        # PD request/response
    └── Tikvpb/                      # gRPC service stubs

tests/
├── Unit/                            # Unit tests
└── E2E/                             # End-to-end tests

examples/
├── basic.php                        # Basic CRUD example
├── batch.php                        # Batch operations example
├── scan.php                         # Scanning examples
├── ttl.php                          # TTL operations example
├── atomic.php                       # Atomic operations example
├── tls.php                          # TLS configuration example
└── logging.php                      # PSR-3 logging example
```

## Available Commands (Makefile)

```bash
make install          # Install PHP dependencies
make test             # Run all tests (unit + e2e)
make test-unit        # Run unit tests only
make test-e2e         # Run E2E tests with TiKV cluster
make proto-generate   # Generate PHP classes from proto files
make proto-clean      # Remove generated proto classes
make build            # Build Docker images
make up               # Start TiKV cluster
make down             # Stop TiKV cluster
make logs             # Show TiKV cluster logs
make clean            # Clean everything (containers + volumes)
make example          # Run basic example
make shell            # Open development shell
```

## Examples

See the `examples/` directory for complete working examples:

- **basic.php** — Basic CRUD operations
- **batch.php** — Batch operations with parallel execution
- **scan.php** — Range scanning and prefix scanning
- **ttl.php** — Time-to-live operations
- **atomic.php** — Compare-and-swap and put-if-absent
- **tls.php** — TLS/SSL configuration
- **logging.php** — PSR-3 logging integration

Run any example:
```bash
make up  # Start TiKV cluster first
php examples/basic.php
```

## Configuration

### Client Options

```php
$options = [
    'tls' => [
        'caCert' => '/path/to/ca.crt',      // Server CA certificate
        'clientCert' => '/path/to/client.crt', // Client certificate (for mTLS)
        'clientKey' => '/path/to/client.key',  // Client private key (for mTLS)
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,           // PSR-3 logger (optional)
    options: $options          // Additional options (optional)
);
```

### TiKV Configuration

For TTL support, enable it in tikv.toml:

```toml
[storage]
enable-ttl = true
```

## Roadmap

### Recently Completed
- ✅ TLS/SSL Support
- ✅ PSR-3 Logging
- ✅ Region & Store Caching
- ✅ Connection Pooling
- ✅ Batch Async Execution
- ✅ Retry with Exponential Backoff
- ✅ Per-key TTL in BatchPut
- ✅ Scan limit enforcement (MAX 10240)
- ✅ Batch auto-splitting by size/count

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass: `make test`
5. Submit a pull request

## License

MIT
