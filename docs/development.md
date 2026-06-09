# Development Guide

Technical guide for developers working on the TiKV PHP Client internals.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Key Components](#key-components)
3. [Adding New Features](#adding-new-features)
4. [Protocol Buffer Changes](#protocol-buffer-changes)
5. [Testing Strategies](#testing-strategies)
6. [Performance Optimization](#performance-optimization)
7. [Debugging](#debugging)

## Architecture Overview

### High-Level Flow

```
User Code
    ↓
RawKvClient (High-level API)
    ↓
Region Cache / Store Cache
    ↓
PdClient (PD communication)
    ↓
GrpcClient (gRPC wrapper)
    ↓
gRPC Extension
    ↓
TiKV Cluster (PD + TiKV nodes)
```

### Request Flow Example

**Get Operation:**

1. User calls `$client->get('key')`
2. Check RegionCache for key's region
3. If miss: Query PD for region info
4. Cache region info
5. Resolve TiKV node address from StoreCache
6. Send gRPC request to TiKV node
7. Handle response (retry if needed)
8. Return value to user

## Key Components

### RawKvClient

Main entry point. Located at `src/Client/RawKv/RawKvClient.php`.

**Responsibilities:**
- Public API for all RawKV operations
- Retry logic coordination
- Request routing

**Key Methods:**
- `executeWithRetry()` - Core retry loop
- `groupKeysByRegion()` - Batch operation routing
- `calculatePrefixEndKey()` - Prefix scan helper

### PdClient

PD (Placement Driver) communication. Located at `src/Client/Connection/PdClient.php`.

**Responsibilities:**
- Cluster topology discovery
- Region information queries
- Store information queries

**Key Methods:**
- `getRegion($key)` - Find region for key
- `getStore($storeId)` - Get store address
- `scanRegions($start, $end)` - Find regions in range

### GrpcClient

gRPC communication wrapper. Located at `src/Client/Grpc/GrpcClient.php`.

**Responsibilities:**
- gRPC channel management
- Request/response serialization
- TLS configuration

**Key Methods:**
- `call($address, $service, $method, $request, $responseClass)` - Sync call
- `getChannel($address)` - Channel pooling

### RegionCache

In-memory region metadata cache. Located at `src/Client/Cache/RegionCache.php`.

**Responsibilities:**
- Cache region information
- Handle region epoch changes
- Leader tracking

**Key Methods:**
- `getByKey($key)` - Lookup region for key
- `put($region)` - Cache region
- `invalidate($regionId)` - Remove from cache
- `switchLeader($regionId, $newLeader)` - Update leader

### Retry System

Located in `src/Client/Retry/`.

**BackoffType** (`BackoffType.php`):
- Defines retry strategies
- `None`, `Fast`, `Medium`, `Slow`
- `ServerBusy` (separate budget)

**Error Classification** (in `RawKvClient.php`):
```php
private function classifyError(TiKvException $e): ?BackoffType
```

## Adding New Features

### Step-by-Step Guide

Let's say you want to add a new operation `RawMyOperation`.

#### 1. Check Proto Definitions

First, check if the operation exists in TiKV proto files:

```bash
# Look in proto/kvproto/proto/kvrpcpb.proto
grep -i "myoperation" proto/kvproto/proto/kvrpcpb.proto
```

#### 2. Generate Proto Classes (if needed)

If proto files changed:

```bash
make proto-generate
```

#### 3. Add Request/Response Classes

Proto generation creates these automatically in `src/Proto/Kvrpcpb/`.

#### 4. Implement in RawKvClient

Add public method:

```php
/**
 * My new operation.
 *
 * @param string $key The key
 * @return MyResult The result
 */
public function myOperation(string $key): MyResult
{
    $this->ensureOpen();
    
    return $this->executeWithRetry($key, function () use ($key): MyResult {
        $region = $this->getRegionInfo($key);
        $address = $this->resolveStoreAddress($region->leaderStoreId);
        
        $request = new RawMyOperationRequest();
        $request->setContext(RegionContext::fromRegionInfo($region));
        $request->setKey($key);
        
        /** @var RawMyOperationResponse $response */
        $response = $this->grpc->call(
            $address,
            'tikvpb.Tikv',
            'RawMyOperation',
            $request,
            RawMyOperationResponse::class
        );
        
        RegionErrorHandler::check($response);
        
        return new MyResult(
            data: $response->getData(),
            // ... map response fields
        );
    });
}
```

#### 5. Create Result DTO (if needed)

```php
<?php
declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

final readonly class MyResult
{
    public function __construct(
        public string $data,
        public int $count,
    ) {
    }
}
```

#### 6. Add Unit Tests

```php
<?php
namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use PHPUnit\Framework\TestCase;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

class MyOperationTest extends TestCase
{
    public function testMyOperation(): void
    {
        // Create mocks
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);
        
        // Setup expectations
        $pdClient->method('getRegion')
            ->willReturn($this->createMockRegion());
        
        $grpc->method('call')
            ->willReturn($this->createMockResponse());
        
        // Test
        $client = new RawKvClient($pdClient, $grpc);
        $result = $client->myOperation('test-key');
        
        // Assert
        $this->assertInstanceOf(MyResult::class, $result);
        $this->assertEquals('expected', $result->data);
    }
}
```

#### 7. Add E2E Tests

```php
<?php
namespace CrazyGoat\TiKV\Tests\E2E;

class MyOperationE2ETest extends TestCase
{
    use TiKvTestTrait;
    
    public function testMyOperation(): void
    {
        $key = 'test:my-op:' . uniqid();
        $this->trackKey($key);
        
        // Setup
        self::$client->put($key, 'value');
        
        // Execute
        $result = self::$client->myOperation($key);
        
        // Verify
        $this->assertNotNull($result);
    }
}
```

#### 8. Update Documentation

- Add to `docs/operations.md`
- Update `README.md` usage section
- Add example to `examples/` (if applicable)

#### 9. Update Implementation Plans

If this was a planned feature:

```markdown
# docs/superpowers/plans/XX-my-operation.md

# My Operation Implementation

## Status: ✅ COMPLETED

## Implementation
- [x] Proto message handling
- [x] RawKvClient method
- [x] Unit tests
- [x] E2E tests
- [x] Documentation
- [x] Example

## Notes
Any special considerations...
```

### Pattern: Batch Operations

For batch operations, implement parallel execution:

```php
public function batchMyOperation(array $keys): array
{
    $this->ensureOpen();
    
    if ($keys === []) {
        return [];
    }
    
    // Group by region
    $keysByRegion = $this->groupKeysByRegion($keys);
    
    // Create async calls for each region
    $regionCalls = [];
    foreach ($keysByRegion as $regionId => $regionData) {
        $regionCalls[$regionId] = fn(): GrpcFuture => 
            $this->executeMyOperationForRegionAsync($regionData['region'], $regionData['keys']);
    }
    
    // Execute in parallel
    $executor = new BatchAsyncExecutor($this->logger);
    $regionResults = $executor->executeParallel($regionCalls);
    
    // Merge results
    $results = [];
    foreach ($regionResults as $response) {
        // Extract data from response
        $results[] = ...;
    }
    
    return $results;
}
```

### Pattern: Scan Operations

For scan operations, handle multi-region scans:

```php
public function scanMyOperation(string $startKey, string $endKey): array
{
    $this->ensureOpen();
    
    // Get all regions in range
    $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
    $results = [];
    
    foreach ($regions as $region) {
        // Calculate intersection with scan range
        $scanStart = max($startKey, $region->startKey);
        $scanEnd = min($endKey, $region->endKey);
        
        if ($scanStart >= $scanEnd) {
            continue;
        }
        
        // Execute scan for this region
        $regionResults = $this->executeMyScanForRegion($region, $scanStart, $scanEnd);
        $results = array_merge($results, $regionResults);
    }
    
    return $results;
}
```

## Protocol Buffer Changes

### When to Regenerate

Regenerate proto classes when:
- TiKV proto files are updated
- New operations are added to TiKV
- Proto definitions change

### Regeneration Process

```bash
# 1. Update proto submodule (if applicable)
git submodule update --remote proto/kvproto

# 2. Clean old generated files
make proto-clean

# 3. Regenerate
make proto-generate

# 4. Verify generation
ls src/Proto/Kvrpcpb/ | head -20

# 5. Run tests to ensure nothing broke
make test
```

### Proto Structure

Key proto files:

```
proto/kvproto/proto/
├── kvrpcpb.proto    # RawKV requests/responses
├── pdpb.proto       # PD requests/responses
├── tikvpb.proto     # TiKV services
└── metapb.proto     # Metadata (Region, Store, etc.)
```

### Adding Custom Proto

If you need custom proto (rare):

1. Add `.proto` file to `proto/custom/`
2. Update `scripts/generate-proto.sh`
3. Run `make proto-generate`

## Testing Strategies

### Unit Test Patterns

**Mocking gRPC:**

```php
$grpc = $this->createMock(GrpcClientInterface::class);
$grpc->method('call')
    ->with(
        $this->equalTo('127.0.0.1:20160'),
        $this->equalTo('tikvpb.Tikv'),
        $this->equalTo('RawGet'),
        $this->isInstanceOf(RawGetRequest::class),
        $this->equalTo(RawGetResponse::class)
    )
    ->willReturn($mockResponse);
```

**Mocking Region Info:**

```php
private function createMockRegion(): RegionInfo
{
    return new RegionInfo(
        regionId: 1,
        startKey: '',
        endKey: '',
        leaderStoreId: 1,
        regionEpoch: new RegionEpoch(1, 1)
    );
}
```

**Testing Retry Logic:**

```php
public function testRetryOnEpochNotMatch(): void
{
    $grpc = $this->createMock(GrpcClientInterface::class);
    
    // First call fails
    $grpc->method('call')
        ->willReturnOnConsecutiveCalls(
            $this->throwException(new RegionException('EpochNotMatch')),
            $mockSuccessResponse  // Second call succeeds
        );
    
    $client = new RawKvClient($mockPd, $grpc);
    $result = $client->get('key');
    
    // Should succeed after retry
    $this->assertEquals('value', $result);
}
```

### gRPC Extension Test Lane

Run the gRPC-dependent unit tests with the PHP gRPC extension loaded:

```bash
composer test:grpc
```

CI runs the same suite on PHP 8.4 with `extensions: grpc` and `coverage: pcov`, using `--fail-on-skipped` so a missing extension fails the job instead of producing skipped tests.

### E2E Test Patterns

**Test Isolation:**

```php
protected function setUp(): void
{
    $this->keysToCleanup = [];
}

protected function tearDown(): void
{
    foreach ($this->keysToCleanup as $key) {
        try {
            self::$client->delete($key);
        } catch (\Exception) {
            // Ignore
        }
    }
}

private function trackKey(string $key): void
{
    $this->keysToCleanup[] = $key;
}
```

**Testing TTL:**

```php
public function testTtlExpiration(): void
{
    $key = 'test:ttl:' . uniqid();
    $this->trackKey($key);
    
    // Put with 2 second TTL
    self::$client->put($key, 'value', ttl: 2);
    
    // Should exist immediately
    $this->assertEquals('value', self::$client->get($key));
    
    // Wait for expiration
    sleep(3);
    
    // Should be gone
    $this->assertNull(self::$client->get($key));
}
```

**Testing Concurrent Operations:**

```php
public function testConcurrentBatchPuts(): void
{
    $keys = [];
    for ($i = 0; $i < 100; $i++) {
        $key = "test:concurrent:$i";
        $keys[$key] = "value-$i";
        $this->trackKey($key);
    }
    
    // Multiple concurrent batch puts
    self::$client->batchPut($keys);
    
    // Verify all exist
    $values = self::$client->batchGet(array_keys($keys));
    $this->assertCount(100, array_filter($values));
}
```

### Test Data Generators

```php
trait TestDataGenerator
{
    protected function generateRandomKey(string $prefix = 'test'): string
    {
        return "$prefix:" . uniqid() . ':' . random_int(1000, 9999);
    }
    
    protected function generateRandomData(int $size = 100): string
    {
        return bin2hex(random_bytes($size / 2));
    }
    
    protected function generateKeyRange(int $count, string $prefix = 'test'): array
    {
        $keys = [];
        for ($i = 0; $i < $count; $i++) {
            $keys[] = sprintf("$prefix:%08d", $i);
        }
        return $keys;
    }
}
```

## Performance Optimization

### Profiling Tools

**Using Blackfire:**

```bash
# Install
composer require --dev blackfire/php-sdk

# Profile
blackfire run php examples/batch.php
```

**Using XHProf:**

```php
// In your test script
xhprof_enable(XHPROF_FLAGS_CPU + XHPROF_FLAGS_MEMORY);

// ... code to profile ...

$data = xhprof_disable();
// Save or analyze $data
```

**Manual Timing:**

```php
$start = hrtime(true);
$client->batchPut($largeBatch);
$elapsed = (hrtime(true) - $start) / 1e6; // Convert to ms
echo "Batch put took: {$elapsed}ms\n";
```

### Benchmarking

Create benchmarks in `tests/Benchmark/`:

```php
<?php
namespace CrazyGoat\TiKV\Tests\Benchmark;

class BatchPerformanceTest
{
    private RawKvClient $client;
    
    public function benchmarkBatchPut(int $size): float
    {
        $data = [];
        for ($i = 0; $i < $size; $i++) {
            $data["bench:$i"] = "value-$i";
        }
        
        $start = microtime(true);
        $this->client->batchPut($data);
        return (microtime(true) - $start) * 1000;
    }
}
```

### Memory Optimization

**Streaming Large Scans:**

```php
public function streamScan(string $prefix, callable $callback): void
{
    $startKey = $prefix;
    $endKey = $this->calculatePrefixEndKey($prefix);
    
    while (true) {
        $batch = $this->client->scan($startKey, $endKey, limit: 100);
        
        if (empty($batch)) {
            break;
        }
        
        foreach ($batch as $item) {
            $callback($item['key'], $item['value']);
        }
        
        // Move to next batch
        $lastKey = $batch[count($batch) - 1]['key'];
        $startKey = $lastKey . "\x00";
        
        // Free memory
        unset($batch);
        gc_collect_cycles();
    }
}
```

## Debugging

### Enable Verbose Logging

```php
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

$logger = new Logger('debug');
$handler = new StreamHandler('php://stderr', Logger::DEBUG);
$handler->setFormatter(new LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message% %context%\n",
    'Y-m-d H:i:s.u'
));
$logger->pushHandler($handler);

$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);
```

### gRPC Debugging

Enable gRPC tracing:

```bash
export GRPC_VERBOSITY=DEBUG
export GRPC_TRACE=all
php your-script.php
```

### Wireshark Analysis

Capture and analyze gRPC traffic:

```bash
# Capture on loopback
sudo tcpdump -i lo -w tikv.pcap port 20160

# Analyze in Wireshark with gRPC/proto plugins
```

### Common Debug Scenarios

**Region Not Found:**

```php
// Check region cache
$logger->debug('Region lookup', ['key' => $key]);

// Clear cache and retry
$client->clearRegionCache();  // If you add this method
```

**Connection Issues:**

```php
try {
    $client->put('test', 'value');
} catch (GrpcException $e) {
    error_log("gRPC Error: " . $e->getMessage());
    error_log("Code: " . $e->getCode());
    
    // Check if TiKV is reachable
    $socket = @fsockopen('127.0.0.1', 20160, $errno, $errstr, 5);
    if (!$socket) {
        error_log("TiKV node not reachable: $errstr ($errno)");
    }
}
```

**Performance Issues:**

```php
// Profile specific operations
$start = microtime(true);
$client->scanPrefix('user:', limit: 10000);
$scanTime = microtime(true) - $start;

if ($scanTime > 1.0) {
    error_log("Slow scan detected: {$scanTime}s");
    // Check region count, network latency, etc.
}
```

### IDE Debugging

**PHPStorm Setup:**

1. Install Xdebug: `pecl install xdebug`
2. Configure `php.ini`:
   ```ini
   zend_extension=xdebug.so
   xdebug.mode=debug
   xdebug.client_host=127.0.0.1
   xdebug.client_port=9003
   ```
3. Set breakpoints in IDE
4. Run with "Start Listening for PHP Debug Connections"

**VS Code Setup:**

1. Install "PHP Debug" extension
2. Create `.vscode/launch.json`:
   ```json
   {
       "version": "0.2.0",
       "configurations": [
           {
               "name": "Listen for Xdebug",
               "type": "php",
               "request": "launch",
               "port": 9003
           }
       ]
   }
   ```

## See Also

- [Contributing Guide](contributing.md) - General contribution guidelines
- [Architecture](architecture.md) - System architecture details
- [Testing](testing.md) - Testing documentation (if exists)
