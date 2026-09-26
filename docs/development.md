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
- `get()` / `put()` / `delete()` / … - Public operations, each building a `RetryExecutor` via `createRetryExecutor()` and delegating to a `RawKv*` collaborator

The retry loop lives in `RetryExecutor::execute()` (`src/Client/Retry/RetryExecutor.php`). Batch routing and prefix scans are delegated to dedicated collaborators:
- `RegionGrouper::groupKeysByRegion()` - Batch operation routing
- `RawKvSplitter::calculatePrefixEndKey()` - Prefix scan helper

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
- Defines retry strategies — see `docs/architecture.md`
  for the full list with base/cap backoff values): `None`, `ServerBusy`,
  `StaleCmd`, `RegionMiss`, `TiKvRpc`, `NotLeader`, `DiskFull`,
  `RegionNotInitialized`, `ReadIndexNotReady`, `ProposalInMergingMode`,
  `RecoveryInProgress`, `IsWitness`, `MaxTimestampNotSynced`, `TxnLock`
- `ServerBusy` (separate budget, 60 s)

**Error Classification** — `ErrorClassifier::classifyByKind()`
(`src/Client/Retry/ErrorClassifier.php`) is the single source of truth for
the error-to-backoff mapping; message-based fallbacks live in
`ErrorClassifier` itself:
```php
public static function classifyByKind(ErrorKind $kind): ?BackoffType
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

    return $this->createRetryExecutor()->execute(
        $key,
        function () use ($key): MyResult {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawMyOperationRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);

            /** @var RawMyOperationResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawMyOperation',
                $request,
                RawMyOperationResponse::class,
                $this->timeoutConfig->readTimeoutMs,
            );

            RegionErrorHandler::check($response);

            return new MyResult(
                data: $response->getData(),
                // ... map response fields
            );
        }
    );
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
├── kvrpcpb.proto    # RawKV/TxnKV requests/responses and API context
├── pdpb.proto       # PD requests/responses
├── keyspacepb.proto # Keyspace metadata and LoadKeyspace
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

### Region boundaries: the #188 split point by hand

`RawKvE2ETest::testBatchRoundTripResolvesEveryKeyWhenTheLargestOneIsARegionStartKey()`
pins issue #188's end-to-end vector — every key of a batch resolves, including
the **largest** one when it is a region start key, and including it again as a
one-key batch — against whatever layout the cluster happens to have. Since
#288 it reads the batch back through a **freshly created client with a cold
region cache, one per read**: its own `batchPut()` now warms the cache for
every key of the batch, and whichever of the two reads ran first would warm it
for the other one, so a warm client computes no scan window at all (reverting
the window to `[minKey, maxKey)` is invisible on a warm client; on a cold one
it throws `TiKvException: PD could not resolve the region for key …`). The
issue's literal case, "a boundary *I* choose with `SplitRegion`, then write to",
is a manual procedure instead, and the reason is a property of this client
worth knowing before you try it yourself:

> **PD stores the default keyspace's region boundaries memory-comparable
> encoded, while a `Mode::Raw` bundle asks PD in the unencoded key space.**
> `CodecV1::encodeRegionKey()` returns the key unchanged in `Mode::Raw`
> (`src/Client/Codec/CodecV1.php:37`), and `SplitRegion` records
> `MemComparableCodec::encode($key)`. So a Raw-mode bundle cannot observe the
> boundary its own split created, while a `Mode::Txn` bundle can —
> `RegionInfoMapper::fromProto()` decodes the stored bytes back to user keys for
> it. That is why `TxnKvE2ETest::splitTxnKeyspaceIntoRegions()` is the working
> precedent, and why its sibling `testTxnWorkloadAcrossPreSplitRegions()` is
> the automated version of the split-point case.

Print the same cluster's boundaries through both bundles and the asymmetry is
one line each — this cluster's boundaries all came from a `SplitRegion` RPC at
the user key `rawkv-188-txnsplit`:

```bash
docker-compose run --rm -T -e PD_ENDPOINTS=pd:2379 php-client php -r '
require "/app/vendor/autoload.php";
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
foreach ([Mode::Raw, Mode::Txn] as $mode) {
    $bundle = ConnectionFactory::create(["pd:2379"], null, [], new CodecV1($mode));
    printf("--- Mode::%s\n", $mode->name);
    foreach ($bundle->pdClient->scanRegions("", "") as $r) {
        printf("  region %-4d start=%s\n", $r->regionId, bin2hex($r->startKey));
    }
}'
```

```text
--- Mode::Raw
  region 32    start=7261776b762d3138ff382d74786e73706cff6974000000000000f9   ← MCE bytes
--- Mode::Txn
  region 32    start=7261776b762d3138382d74786e73706c6974                    ← the user key
```

**1. Start a cluster** (this is the one `make test-e2e` uses, and no client
build is needed):

```bash
docker-compose up -d pd tikv1 tikv2 tikv3
```

**2. Split the keyspace at a key of your choice.** `pd-ctl` takes keys
hex-encoded, and `--keys` is the key **as PD stores it**, so the bytes you pass
are the bytes the new region starts at:

```bash
KEY='rawkv-188-split-point'
HEX=$(printf '%s' "$KEY" | od -An -tx1 | tr -d ' \n')
docker-compose exec -T pd /pd-ctl region key "$HEX"          # JSON; take its "id"
docker-compose exec -T pd /pd-ctl operator add split-region <id> \
    --keys "$HEX" --policy usekey
```

The split is asynchronous (about 9 s here). Poll until the boundary is there —
`docker-compose exec -T pd /pd-ctl operator show` prints `[]` again and
`http://localhost:2379/pd/api/v1/regions` lists one more region whose
`start_key` is your hex — before you write anything. Do not re-run the command
for a key that already has a boundary: PD accepts it, leaves an operator that
never completes, and then refuses every later operator with `failed to add
operator, maybe already have one` until that operator's 1 min timeout expires
(or `pd-ctl operator remove <region-id>` clears it).

**3. Run the batch against the boundary key.** `$boundary` is now a region start
key in the client's own key space, so this is issue #188's vector verbatim:

```bash
docker-compose run --rm -T -e PD_ENDPOINTS=pd:2379 php-client php -r '
require "/app/vendor/autoload.php";
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
$boundary = "rawkv-188-split-point";   // the key the region now starts at
$below = "rawkv-188-a";                // "a" < "s", so it sorts below it
$client = RawKvClient::create(["pd:2379"]);
$client->batchPut([$below => "below", $boundary => "at-the-boundary"]);
print_r($client->batchGet([$below, $boundary]));   // whole batch
print_r($client->batchGet([$boundary]));           // degenerate [k, k . "\x00")
var_dump($client->get($boundary));                 // single-key path
$client->batchDelete([$below, $boundary]);
$client->close();'
```

Both `batchGet()` calls print the values, not `null`. Narrowing the batch's
window from `[minKey, successor(maxKey))` back to the pre-#244
`[minKey, maxKey)` turns this into `TiKvException: PD could not resolve the
region for key …; refusing to silently drop it from the batch` — the reported
symptom.

If the batch fails with `Retry attempt cap (30) exhausted` while the keys
themselves are fine, that is the store-side region information lagging, not the
window: `docker-compose run` recreates the `tikv` containers it depends on (see
`docs/helpers/faq.md`), and a request that reaches a store which has not applied
the new split yet burns the whole retry budget. Wait a few seconds and re-run
before suspecting the client.

**4. Take the boundary away again.** Region boundaries live as long as the
cluster, and *this* one is not MCE-shaped, which costs you two things:

```bash
docker-compose exec -T pd /pd-ctl operator add merge-region <new-region> <previous-region>
```

- Every `Mode::Txn` bundle — the whole TxnKV path, `tests/E2E/TxnKvE2ETest.php` —
  throws `InvalidArgumentException: Invalid MCE marker 0x38 at byte 8` out of
  `MemComparableCodec::decode()` on its next `scanRegions()`/`getRegion()`,
  because a stored boundary that is not a valid MCE group sequence cannot be
  decoded. Merge it away before running the TxnKV lane.
- RawKV routing for keys between this boundary and its MCE-encoded neighbour
  becomes unreliable: reads and writes there fail with `Retry attempt cap (30)
  exhausted`, because PD's region lookup and this client's `RegionResolver`
  disagree about which region owns them. Put the boundary at a key prefix of
  your own, and expect to clean it up.

**The safe variant**: pass the MCE-encoded bytes instead — `--keys` is the output
of `MemComparableCodec::encode($key)`, e.g.
`7261776b762d3138ff382d74786e73706cff6974000000000000f9` for
`rawkv-188-txnsplit`. That is the boundary shape TiKV itself records, so neither
problem above happens — but the RawKV client still cannot use it, because it
asks PD in the unencoded key space. Only the transactional client sees the user
key as a boundary, and that case is already automated
(`testTxnWorkloadAcrossPreSplitRegions()`).

### API V2 E2E lane

The normal E2E lane uses the V1 cluster. Run the API V2 smoke suite against
`tikv-apiv2.toml` with:

```bash
make test-e2e-apiv2
```

The lane starts the override compose file, creates two PD keyspaces, and runs
`E2E-ApiV2` against both of them — one as the client's keyspace, the other to
prove that identical user keys stay isolated per keyspace. It is intentionally
a separate suite because API V1 and API V2 cannot share one TiKV server mode.

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

Keep repeatable performance measurements outside the PHPUnit assertions. The
RegionCache benchmark exercises the ID-map/treap write path directly:

```bash
php benchmarks/RegionCacheBenchmark.php
```

It reports cold sequential inserts, warm replacements, and the resulting
entry count. For new cache data structures, extend this standalone benchmark
with the relevant random, overlap, eviction, and range-enumeration scenarios;
do not turn machine-dependent timings into flaky unit-test failures.

The batch region-resolution benchmark measures one
`RegionResolver::batchResolveRegions()` call for a 100-region batch against a
warm 10 000-entry cache, next to the pre-#288 algorithm and next to the
`put()`-skip micro-measurement (issue #288):

```bash
php benchmarks/BatchResolveRegionsBenchmark.php
```

Its `PD scanRegions() calls` line is the half of the pre-#288 cost a stubbed
scan cannot show: the round trip itself, and the unbounded answer it used to
ask for. Two other lines are worth reading together rather than alone: the
`put() skip` block prints the unconditional `put()`, the `getByKey()`-based
identity check and the `getById()`-based one side by side, which is the only
way to see that the skip criterion is a *win* at all (an optimisation that
costs more than the write it removes is a regression wearing a test's
clothes); and the batch-size sweep below it shows the read-through's cost is
**per key** and linear (~12 µs/key, so ~120 ms for a 10 000-key batch),
which "zero PD calls" does not convey. Read that file's header note before
quoting its CPU numbers — the issue's original 33.85 ms figure was taken
against the pre-#289 `RegionCache` and no longer reproduces.

For other operations, create focused benchmark classes under
`tests/Benchmark/`:

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
        
        $start = hrtime(true);
        $this->client->batchPut($data);
        return (hrtime(true) - $start) / 1e6;
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

> The `$lastKey . "\x00"` continuation is `KeyOrder::successor($lastKey)` in library code
> (`CrazyGoat\TiKV\Client\Util\KeyOrder`, issue #186) — the same byte string, but
> documented and unit-tested as the immediate successor of a key.

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
