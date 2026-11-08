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

The retry loop lives in `RetryExecutor::execute()` (`src/Client/Retry/RetryExecutor.php`). Batch routing, batch splitting and prefix scans are delegated to dedicated collaborators:
- `RegionGrouper::groupKeysByRegionBatch()` - Batch operation routing
- `RawKvSplitter::splitIntoBatches()` / `splitPairsIntoBatches()` - Split a region's keys or key-value pairs into count- and byte-bounded requests; the pair variant keeps the TTL list aligned and throws `InvalidArgumentException` when a non-empty TTL array's length differs from the pair count (`TTL array count (N) must match pair count (M)`). Both are `@internal`; `calculatePrefixEndKey()` is the helper the user-facing documentation points callers at
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
  for the full list with base/cap backoff values): `None`, `EpochNotMatch`,
  `ServerBusy`, `StaleCmd`, `RegionMiss`, `TiKvRpc`, `NotLeader`, `DiskFull`,
  `RegionNotInitialized`, `ReadIndexNotReady`, `ProposalInMergingMode`,
  `RecoveryInProgress`, `IsWitness`, `MaxTimestampNotSynced`, `TxnLock`,
  `TxnLockFast`, `TxnNotFound`, `CommitTSLag`, `PDRpc`
- `ServerBusy` (separate budget, 60 s)

**Error Classification** — `ErrorClassifier::classifyByKind()`
(`src/Client/Retry/ErrorClassifier.php`) is the single source of truth for
the error-to-backoff mapping; message-based fallbacks live in
`ErrorClassifier` itself:
```php
public static function classifyByKind(ErrorKind $kind): ?BackoffType
```

`ErrorClassifier::classify()` returns a `RetryVerdict`
(`src/Client/Retry/RetryVerdict.php`) instead of `?BackoffType`, so a custom
classifier can distinguish "no opinion" (`RetryVerdict::pass()`) from an
explicit fatal verdict (`RetryVerdict::fatal()`); the mapping helpers above
keep returning `?BackoffType` (issue #534).

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

#### 4. Implement the Operation

`RawKvClient` validates the input and delegates to a collaborator in
`src/Client/RawKv/` — the client itself holds no region resolver. Add the
public method to `RawKvClient`:

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
    $this->validateKeyNotEmpty($key, 'myOperation');

    return $this->crud->myOperation($key, $this->createRetryExecutor(), $this->columnFamily);
}
```

Then implement it in `RawKvCrud` (`src/Client/RawKv/RawKvCrud.php`), which
holds the region resolver, the gRPC transport and the timeout configuration:

```php
public function myOperation(string $key, RetryExecutor $retryExecutor, string $columnFamily = ''): MyResult
{
    return $retryExecutor->execute(
        $key,
        function () use ($key, $columnFamily): MyResult {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawMyOperationRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo(
                $region,
                requestContext: $this->requestContext,
                resourceControl: $this->resourceControl,
            ));
            $request->setKey($key);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            /** @var RawMyOperationResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawMyOperation',
                $request,
                RawMyOperationResponse::class,
                $this->timeoutMs('read'),
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

The E2E suite has no shared trait: each class creates a client in
`setUpBeforeClass()` from `PD_ENDPOINTS` and cleans up the keys it created.
`ClusterProbe::assertPdReachable()` (used by the existing E2E classes) fails
with an actionable message when no cluster answers at those endpoints.

```php
<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use PHPUnit\Framework\TestCase;

class MyOperationE2ETest extends TestCase
{
    private static RawKvClient $client;
    private array $keysToCleanup = [];

    public static function setUpBeforeClass(): void
    {
        $endpoints = getenv('PD_ENDPOINTS');
        $endpoints = $endpoints === false ? ['pd:2379'] : explode(',', $endpoints);

        ClusterProbe::assertPdReachable($endpoints);
        self::$client = RawKvClient::create($endpoints);
    }

    public static function tearDownAfterClass(): void
    {
        if (isset(self::$client)) {
            self::$client->close();
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->keysToCleanup as $key) {
            try {
                self::$client->delete($key);
            } catch (\Exception) {
                // Ignore cleanup errors.
            }
        }
    }

    public function testMyOperation(): void
    {
        $key = 'test:my-op:' . uniqid();
        $this->keysToCleanup[] = $key;

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

#### 9. Update the Changelog

Add an entry under `## [Unreleased]` in [`CHANGELOG.md`](../CHANGELOG.md), with
the issue number in parentheses. Implementation plans live in
[GitHub Issues](https://github.com/crazy-goat/tikv-php/issues), not in this
repository.

### Pattern: Batch Operations

For batch operations, implement parallel execution in the batch collaborator
(`RawKvBatch`), which owns the region resolver and the logger:

```php
public function batchMyOperation(array $keys): array
{
    if ($keys === []) {
        return [];
    }

    // Group by region through the same helper the library uses.
    $keysByRegion = RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);

    // Create async calls for each region
    $regionCalls = [];
    foreach ($keysByRegion as $regionId => $regionData) {
        $regionCalls[$regionId] = fn(): GrpcFuture
            => $this->executeMyOperationForRegionAsync($regionData['region'], $regionData['keys']);
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

For scan operations, handle multi-region scans. Ask PD for a **bounded**
number of regions per call and load the next batch only when the current
one is consumed — `scanRegions($startKey, $endKey, 0)` asks for *every*
region covering the range, which is a multi-megabyte answer for a query
that may read a single region (issue #282, see
[helpers/decisions.md](helpers/decisions.md)).

```php
public function scanMyOperation(string $startKey, string $endKey): array
{
    $this->ensureOpen();

    $results = [];
    $cursor = $startKey;

    while (true) {
        // At most RegionScanBatch::MAX_REGIONS (32) regions per PD call.
        $regions = $this->pdClient->scanRegions($cursor, $endKey, RegionScanBatch::MAX_REGIONS);
        if ($regions === []) {
            break;
        }

        foreach ($regions as $region) {
            // Calculate intersection with scan range — byte-wise, through
            // KeyOrder, never with PHP's relational operators (issue #186).
            // An empty end key means +infinity, on both bounds.
            $scanStart = KeyOrder::gte($cursor, $region->startKey) ? $cursor : $region->startKey;
            $scanEnd = $endKey;
            if ($region->endKey !== '' && ($endKey === '' || KeyOrder::lt($region->endKey, $endKey))) {
                $scanEnd = $region->endKey;
            }

            if ($scanEnd !== '' && !KeyOrder::lt($scanStart, $scanEnd)) {
                continue;
            }

            // Execute scan for this region
            $regionResults = $this->executeMyScanForRegion($region, $scanStart, $scanEnd);
            foreach ($regionResults as $row) {
                $results[] = $row;
            }
        }

        // The next batch continues at the last region's end key. An empty
        // end key is the end of the keyspace, so the loop stops there.
        $cursor = $regions[count($regions) - 1]->endKey;
        if ($cursor === '' || ($endKey !== '' && KeyOrder::gte($cursor, $endKey))) {
            break;
        }
    }

    return $results;
}
```

The library's own scanners do exactly this — `RawKvScanner::regionBatches()`
yields the batches lazily, so a scan whose row limit is satisfied by the
first region never triggers a second PD call. Prefer it over a hand-rolled
loop: it also handles the merge case (a region that a merge produced
between two batches starts *below* the cursor and must be trimmed to it,
so the rows below it are not read twice).

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

`RegionInfo` is a `readonly` DTO, so build it through its constructor. The
epoch is flattened into `epochConfVer` / `epochVersion`; there is no
`regionEpoch` argument:

```php
private function createMockRegion(): RegionInfo
{
    return new RegionInfo(
        regionId: 1,
        leaderPeerId: 1,
        leaderStoreId: 1,
        epochConfVer: 1,
        epochVersion: 1,
        startKey: '',
        endKey: '',
    );
}
```

**Testing Retry Logic:**

The client resolves the region and the store through PD before the first
gRPC call, so a retry test mocks both. `RegionException` takes the operation
name first, then the message:

```php
public function testRetryOnEpochNotMatch(): void
{
    $pdClient = $this->createMock(PdClientInterface::class);
    $pdClient->method('getRegion')->willReturn($this->createMockRegion());

    $store = new Store();
    $store->setId(1);
    $store->setAddress('127.0.0.1:20160');
    $pdClient->method('getStore')->willReturn($store);

    $mockSuccessResponse = new RawGetResponse();
    $mockSuccessResponse->setValue('value');

    $grpc = $this->createMock(GrpcClientInterface::class);

    // First call fails, second succeeds.
    $grpc->method('call')
        ->willReturnOnConsecutiveCalls(
            $this->throwException(new RegionException('RawGet', 'EpochNotMatch')),
            $mockSuccessResponse
        );

    $client = new RawKvClient($pdClient, $grpc);
    $result = $client->get('key');

    // Should succeed after retry
    $this->assertSame('value', $result);
}
```

### gRPC Extension Test Lane

Run the gRPC-dependent unit tests with the PHP gRPC extension loaded:

```bash
composer test:grpc
```

CI runs the same suite on PHP 8.4 with `extensions: grpc` and `coverage: pcov`, using `--fail-on-skipped` so a missing extension fails the job instead of producing skipped tests. The job also sets `REQUIRE_GRPC_EXTENSION=1`, so `GrpcExtensionGate` reports "The gRPC extension is required in this environment but is not loaded" rather than a bare skip if the setup step ever loses the extension.

Locally, a bare `vendor/bin/phpunit` still skips the transport tests when the extension is absent, but `phpunit.xml` enables `displayDetailsOnSkippedTests` and `displayDetailsOnTestsThatTriggerWarnings`, and the gate emits one warning per skipped class. The run therefore ends with a list that names every skipped transport class and its reason — the warning covers the TestDox output, which ignores the skipped-details setting — and the message names the class, the `ext-grpc >= 1.57` requirement and `pecl install grpc` (issue #284, GRPC-25).

### Mutation Testing

Mutation testing answers a question coverage cannot: *would the test suite
notice if the production logic were inverted?* [Infection](https://infection.github.io/)
rewrites the code under test (one mutant per mutation operator) and re-runs the
covering tests; a mutant that survives is a hole in the test suite.

```bash
composer mutation        # run the analysis and enforce the MSI floor
composer mutation:show   # same, but print the surviving mutants
```

The configuration lives in `infection.json`:

- **Scope**: `src/Client/TxnKv/` and `src/Client/Region/` — the two areas
  where an inverted production decision is the most expensive. Widen
  `source.directories` as the score is ratcheted up.
- **Test suite**: the `Unit` suite only (`--testsuite=Unit`). It needs no
  `ext-grpc`, so the mutation run works everywhere; the `Grpc` and `E2E`
  suites are out of scope.
- **Floor**: `minMsi` / `minCoveredMsi` are the *Mutation Score Index* the
  run must reach. The floor is the score of the **first full run** — **72%**
  (1854 mutants: 1326 killed, 10 errors, 10 timeouts, 508 escaped, 100%
  mutation code coverage) — and is ratcheted up from there. It is never
  lowered to make a failing run pass. The measurement was taken on macOS
  with PHP 8.5; the nightly job runs Linux with PHP 8.4 and PCOV, and the
  killed/escaped split moves by a handful of mutants between environments
  (a mutant that times out on one machine is killed on another). The same
  scope measures 73% on 1868 mutants after the last update, so the floor
  sits one point below the current score. If the first nightly run reports
  a lower score, re-measure on the runner and set the floor to that value
  **once** — from then on it only goes up.
- **Timing**: Infection needs a coverage driver (PCOV or Xdebug) and re-runs
  the covering test file per mutant, so a full run takes about ten minutes,
  not seconds.

Two details of the configuration are not obvious:

1. `--do-not-fail-on-warning --do-not-fail-on-risky` pin the mutation run's
   exit-code semantics independently of `phpunit.xml`, which sets
   `failOnRisky="true"` (issue #665): Infection reads a non-zero exit code as
   a killed mutant, so a risky test must not abort the run for a reason
   unrelated to the mutant. Warnings do not fail the project's runs either.
2. The `--filter` expression keeps `TlsConfigBuilderTest` out of the initial
   run. It deliberately reads an unreadable file, and the resulting PHP
   warning is a *defect* for PHPUnit — Infection forces
   `stopOnDefect="true"` into the PHPUnit configuration it generates, so the
   initial run would stop at that warning and the coverage report — and with
   it the measured score — would depend on where the random test order
   happened to stop. The class covers `src/Client/Tls/`, which is outside the
   mutation scope, so excluding it does not change the measured score.

CI runs the analysis nightly and on demand (`.github/workflows/mutation.yml`),
not on every pull request: the run is slow and the score is a ratchet, so it
matters when it *drops*, not on every commit. The required `ci-ok` check is
unchanged.

The mutation floor and the coverage floor ([#362]) are two independent CI
quality gates: coverage answers "is the code exercised?", mutation answers
"does the exercise notice a wrong result?". They configure different tools
and neither reads the other's configuration.

[#362]: https://github.com/crazy-goat/tikv-php/issues/362

### Coverage Lane

The `Unit` and `Grpc` suites are also run together under a coverage driver, and the result is
checked against a coverage floor:

```bash
composer test:coverage
```

The script needs `ext-grpc` and a coverage driver (PCOV, or Xdebug with `xdebug.mode=coverage` —
the script passes that mode itself). It writes the Clover report to `coverage.xml` and then runs
`bin/check-coverage.php`, which fails when the line coverage is below
`DEFAULT_COVERAGE_FLOOR` in that script. CI runs the same two commands in the `grpc-unit-tests`
job and uploads the Clover report as the `coverage-clover` artifact.

#### Installing a coverage driver

Neither driver is a project dependency, and PHPUnit 11 does not support `phpdbg`, so a fresh
environment has none of them and the script stops with "No code coverage driver available".
Install PCOV — the driver CI uses and the faster of the two:

```bash
# Debian/Ubuntu (the package tracks the default PHP version; use php8.x-pcov
# if you run a different one)
sudo apt-get install php-pcov

# macOS/Homebrew, or any platform with phpize and a compiler
pecl install pcov
```

`pecl install` prints the `extension=pcov.so` line to add to `php.ini`. Verify with:

```bash
php -m | grep pcov
```

Xdebug works too: load it with `zend_extension=xdebug.so` and leave `xdebug.mode` alone —
`composer test:coverage` passes `-d xdebug.mode=coverage` itself. A bare
`vendor/bin/phpunit --coverage-*` run needs `XDEBUG_MODE=coverage` in the environment instead.

`composer test:coverage` also passes `-d memory_limit=-1`: the combined run loads ~2,900 tests
with generated mock classes and PCOV's own structures, and the common 128M `php.ini` default is
not enough for it.

Coverage is opt-in on the command line: `phpunit.xml` configures no coverage report, so an
ordinary `vendor/bin/phpunit` run — and every composer test script that does not request a
report — neither asks for a coverage driver nor writes `coverage.xml` / `coverage-html/`. The
report is requested only by the commands above (issue #556).

The floor is a ratchet: it is set below the measured coverage and only moves up. The generated
protobuf classes in `src/Proto/` are excluded from the coverage source set (see `phpunit.xml`),
so the number describes the hand-written client code.
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
        return bin2hex(random_bytes(intdiv($size, 2)));
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

It reports cold sequential inserts, warm replacements, the resulting entry
count, and the cost of a `put()` that has to evict the LRU entry, swept over
capacities 1 000 / 10 000 / 50 000 (issue #255). That last curve must stay
logarithmic in the capacity — the treap walk is O(log n) and the LRU victim is
the first key of a recency-ordered hash — rather than grow with the number of
cached entries, which is what a reintroduced linear scan would do. For new
cache data structures, extend this standalone benchmark with the relevant
random, overlap, eviction, and range-enumeration scenarios; do not turn
machine-dependent timings into flaky unit-test failures.

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

The transaction commit benchmark measures the commit latency of a
multi-region transaction, with the two-phase-commit RPCs fanned out across
regions and with one RPC at a time (issue #431):

```bash
php benchmarks/TxnCommitLatencyBenchmark.php
```

Both columns run the same client code path against a fake store with a fixed
per-RPC latency, so they differ only in *when* the requests are sent: the
fan-out keeps the waiting time at one round trip per phase, while the
one-at-a-time shape pays one round trip per region and per phase. The `RPCs`
column is the check that the fan-out changes the timing and not the traffic —
it must stay `fan-out/sequential` and equal. The benchmark also sweeps the
region count (1, 2, 5, 10, 20), because the whole point is how the two curves
diverge.

For other operations, create a focused standalone benchmark under
`benchmarks/` (the existing scripts are plain PHP files, not PHPUnit test
classes):

```php
<?php

declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

final class BatchPerformanceBenchmark
{
    public function __construct(private RawKvClient $client)
    {
    }

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
    $endKey = RawKvSplitter::calculatePrefixEndKey($prefix);
    
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

// Force a refresh of the region cache and retry
$client->refreshRegionCache(force: true);
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
