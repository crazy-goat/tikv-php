# Architecture

System architecture and design of the TiKV PHP Client.

## Table of Contents

1. [Overview](#overview)
2. [Component Architecture](#component-architecture)
3. [Data Flow](#data-flow)
4. [Key Design Decisions](#key-design-decisions)
5. [Performance Considerations](#performance-considerations)
6. [Error Handling Architecture](#error-handling-architecture)
7. [Security Architecture](#security-architecture)

## Overview

The TiKV PHP Client is a high-performance client library for TiKV's RawKV and TxnKV APIs. It provides:

- **Synchronous API**: Simple, blocking operations
- **Region-aware routing**: Automatic routing to correct TiKV nodes
- **Intelligent caching**: Region and store metadata caching
- **Automatic retries**: Exponential backoff with error classification
- **ACID Transactions**: Full TxnKV support with 2-phase commit
- **Pessimistic & Optimistic**: Both transaction modes supported
- **Production features**: TLS, PSR-3 logging, batch optimization

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application Layer                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   User Code  │  │   Examples   │  │    Tests     │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼─────────────────┼─────────────────┼──────────────┘
          │                 │                 │
          └─────────────────┴─────────────────┘
                            │
┌───────────────────────────┼───────────────────────────────┐
│                    Client Layer                             │
│  ┌───────────────────┐  ┌──────────────────────────────┐  │
│  │  RawKvClient      │  │  TxnKvClient                │  │
│  │  • get/put/delete │  │  • begin() → Transaction     │  │
│  │  • batch ops      │  │  • 2PC commit/rollback       │  │
│  │  • scan           │  │  • pessimistic locks          │  │
│  │  • CAS, TTL       │  │  • heartbeat, lock resolve   │  │
│  └────────┬──────────┘  └──────────┬───────────────────┘  │
└───────────┼─────────────────────────┼────────────────────┘
            │                         │
┌───────────▼─────────────────────────▼────────────────────┐
│                   Retry & Routing Layer                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────┐ │
│  │  Retry Logic    │  │ Region Routing  │  │LockResolve│ │
│  │  • BackoffType  │  │  • RegionCache   │  │ • Status  │ │
│  │  • Classify     │  │  • StoreCache    │  │ • Resolve │ │
│  └─────────────────┘  └─────────────────┘  └──────────┘ │
└─────────────────────────┬────────────────────────────────┘
                          │
                      │
┌─────────────────────▼──────────────────────────────────────┐
│                  Communication Layer                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐  │
│  │    PdClient     │  │   GrpcClient    │  │   Proto    │  │
│  │  • Discovery    │  │  • Channels     │  │  • Request │  │
│  │  • Regions      │  │  • TLS          │  │  • Response│  │
│  │  • Stores       │  │  • Calls        │  │  • Serialize│  │
│  └─────────────────┘  └─────────────────┘  └──────────────┘  │
└─────────────────────┼──────────────────────────────────────┘
                      │
┌─────────────────────▼──────────────────────────────────────┐
│                    gRPC Extension                          │
│              (PHP gRPC C Extension)                        │
└─────────────────────┬──────────────────────────────────────┘
                      │
┌─────────────────────▼──────────────────────────────────────┐
│                    TiKV Cluster                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │      PD      │  │    TiKV      │  │    TiKV      │     │
│  │  (Metadata)  │  │   (Node 1)   │  │   (Node 2)   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────────────────────────────────────────┘
```

## Component Architecture

### 1. RawKvClient

**Location**: `src/Client/RawKv/RawKvClient.php`

**Responsibilities**:
- Public API for all RawKV operations
- Request routing and coordination
- Retry loop orchestration
- Resource lifecycle management

**Key Design Patterns**:
- **Factory Pattern**: `RawKvClient::create()` for easy instantiation
- **Template Method**: `executeWithRetry()` for consistent retry logic
- **Strategy Pattern**: Error classification for different backoff strategies

**Public Interface**:
```php
final class RawKvClient
{
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self;
    
    // Single-key operations
    public function get(string $key): ?string;
    public function put(string $key, string $value, int $ttl = 0): void;
    public function delete(string $key): void;
    
    // Batch operations
    public function batchGet(array $keys): array;
    public function batchPut(array $pairs, int|array $ttl = 0): void;
    public function batchDelete(array $keys): void;
    
    // Scan operations
    public function scan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array;
    public function scanPrefix(string $prefix, int $limit = 0, bool $keyOnly = false): array;
    public function reverseScan(string $startKey, string $endKey, int $limit = 0, bool $keyOnly = false): array;
    
    // Atomic operations
    public function compareAndSwap(string $key, ?string $expectedValue, string $newValue, int $ttl = 0): CasResult;
    public function putIfAbsent(string $key, string $value, int $ttl = 0): ?string;
    
    // Lifecycle
    public function close(): void;
}
```

### 2. TxnKvClient & Transaction

**Location**: `src/Client/TxnKv/TxnKvClient.php`, `src/Client/TxnKv/Transaction.php`

**Responsibilities**:
- ACID transaction management via two-phase commit (2PC)
- MVCC reads at transaction start timestamp
- Pessimistic and optimistic transaction modes
- Lock resolution for concurrent conflicts
- Heartbeat to extend lock TTL for long-running transactions

**Key Design Patterns**:
- **Unit of Work**: Transaction tracks write set and read set
- **Two-Phase Commit**: Prewrite → Commit protocol
- **Factory Pattern**: `TxnKvClient::begin()` creates Transaction instances
- **Strategy Pattern**: Optimistic vs pessimistic mode selection

**Public Interface**:
```php
final class TxnKvClient
{
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self;
    
    // Transaction lifecycle
    public function begin(array $options = []): Transaction;  // options: pessimistic (bool), priority (int)
    public function close(): void;
}

final class Transaction
{
    // Read operations (MVCC snapshot at startTs)
    public function get(string $key): ?string;
    public function batchGet(array $keys): array;
    public function scan(string $startKey, string $endKey, int $limit = 0): array;
    
    // Write operations (buffered until commit)
    public function set(string $key, string $value): void;
    public function delete(string $key): void;
    
    // Transaction control
    public function commit(): void;           // 2PC: prewrite + commit
    public function rollback(): void;
    public function heartbeat(int $adviseLockTtlMs = 10000): int;
    
    // Inspection
    public function getTxnId(): string;
    public function getStartTs(): int;
    public function getCommitTs(): ?int;
    public function getStatus(): TransactionStatus;
    public function isPessimistic(): bool;
    public function getWriteSet(): array;
    public function getReadSet(): array;
}
```

**Transaction Flow**:
```
Optimistic Transaction                   Pessimistic Transaction
┌──────────────────────┐                ┌──────────────────────┐
│  begin()              │                │  begin(pessimistic)  │
│  startTs = TSO        │                │  startTs = TSO        │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
┌──────────▼───────────┐                ┌──────────▼───────────┐
│  set(k, v)           │                │  set(k, v)            │
│  Buffered in writeset│                │  → pessimisticLock(k) │
│                      │                │  → buffer in writeset │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
┌──────────▼───────────┐                ┌──────────▼───────────┐
│  commit()             │                │  commit()              │
│  1. Prewrite all keys │                │  1. Prewrite all keys  │
│     (lock + data)     │                │     (data only)       │
│  2. commitTs = TSO   │                │  2. commitTs = TSO    │
│  3. Commit all keys   │                │  3. Commit all keys   │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
┌──────────▼───────────┐                ┌──────────▼───────────┐
│  Committed            │                │  Committed            │
│  or rollback()        │                │  or rollback()         │
│  (pessimisticRollback)│                └──────────────────────┘
└──────────────────────┘
```

### 3. TimestampOracle

**Location**: `src/Client/Connection/TimestampOracle.php`

**Responsibilities**:
- Provides monotonically increasing timestamps (TSO) from PD
- Critical for MVCC: every transaction needs a unique startTs and commitTs
- Uses unary gRPC call (not bidirectional streaming, since PHP processes are short-lived)

**Key Methods**:
```php
class TimestampOracle
{
    public function getTimestamp(): int;
    // Returns a 64-bit timestamp: (physical << 18) | logical
}
```

### 4. PdClient

**Location**: `src/Client/Connection/PdClient.php`

**Responsibilities**:
- PD (Placement Driver) communication
- Cluster topology discovery
- Region information queries
- Store information queries
- Timestamp Oracle integration

**Design Patterns**:
- **Singleton-like**: Single PD connection per client
- **Cache-aside**: Caches region/store info with invalidation

**Key Methods**:
```php
interface PdClientInterface
{
    public function getRegion(string $key): RegionInfo;
    public function scanRegions(string $startKey, string $endKey, int $limit): array;
    public function getStore(int $storeId): ?Store;
    public function getTimestamp(): int;
    public function getClusterId(): int;
    public function close(): void;
}
```

**Region Discovery Flow**:
```
User Request (key="user:123")
    ↓
Check RegionCache
    ↓
Cache Miss?
    ↓ Yes
Query PD: GetRegion("user:123")
    ↓
PD returns RegionInfo
    ↓
Cache in RegionCache
    ↓
Return RegionInfo
```

### 5. GrpcClient

**Location**: `src/Client/Grpc/GrpcClient.php`

**Responsibilities**:
- gRPC channel management
- Request/response serialization
- TLS configuration
- Connection pooling (via persistent channels)

**Design Patterns**:
- **Connection Pool**: Reuses gRPC channels by address
- **Decorator**: Wraps raw gRPC calls with logging/error handling

**Channel Management**:
```php
class GrpcClient
{
    private array $channels = [];  // Address → Channel map
    
    public function getChannel(string $address): Channel
    {
        if (!isset($this->channels[$address])) {
            $this->channels[$address] = $this->createChannel($address);
        }
        return $this->channels[$address];
    }
}
```

### 6. RegionCache

**Location**: `src/Client/Cache/RegionCache.php`

**Responsibilities**:
- Cache region metadata
- Track region epochs
- Handle leader changes
- Invalidate on errors

**Cache Structure**:
```php
class RegionCache
{
    private array $cache = [];  // regionId → RegionEntry
    
    private array $keyIndex = [];  // key → regionId (for quick lookup)
}
```

**Invalidation Strategy**:
- **EpochNotMatch**: Invalidate specific region
- **NotLeader**: Update leader info or invalidate
- **RegionNotFound**: Invalidate and retry

### 7. LockResolver

**Location**: `src/Client/TxnKv/LockResolver.php`

**Responsibilities**:
- Resolve locks encountered during MVCC reads
- Check transaction status (committed, rolled back, active)
- Resolve locks by committing or rolling back stale transactions
- Handle both optimistic and pessimistic lock conflicts

**Key Methods**:
```php
class LockResolver
{
    public function resolveLock(string $key, int $lockTs, int $callerStartTs): void;
}
```

**Lock Resolution Flow**:
```
Transaction encounters lock on key
       │
       ▼
┌─────────────────────┐
│ CheckTxnStatus      │  Check the lock owner's transaction
│ (primary key,       │  status at PD timestamp
│  lockTs,            │
│  callerStartTs)     │
└────────┬───────────┘
         │
    ┌────┴────┐
    │ Status? │
    ├─────────┤
    │ Locked  │──Wait and retry
    │ Committed│──ResolveLock with commitTs
    │ RolledBack│──ResolveLock (cleanup)
    └─────────┘
```

### 8. Retry System

**Location**: `src/Client/Retry/BackoffType.php`, `RawKvClient::executeWithRetry()`

**Responsibilities**:
- Classify errors
- Apply appropriate backoff
- Track retry budgets
- Handle special cases (ServerBusy)

**Backoff Types**:
```php
enum BackoffType
{
    case None;        // Immediate retry (e.g., EpochNotMatch)
    case Fast;        // ~10ms (e.g., NotLeader)
    case Medium;      // ~100ms (e.g., RegionNotFound)
    case Slow;        // ~1s (e.g., general errors)
    case ServerBusy;  // Progressive, separate budget
}
```

**Retry Budgets**:
- **General**: 20 seconds total backoff
- **ServerBusy**: 10 minutes separate budget

## Data Flow

### Single Key Operation (Get)

```
┌─────────────┐
│  $client->  │
│   get(key)  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│   ensureOpen()      │  Check client not closed
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  executeWithRetry() │  Start retry loop
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   getRegionInfo()   │  Get region for key
│   • Check cache     │
│   • Query PD if miss│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ resolveStoreAddress()│ Get TiKV node address
│   • Check store cache │
│   • Query PD if miss  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   grpc->call()      │  Send gRPC request
│   • Get channel     │
│   • Serialize       │
│   • Send/Receive    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ RegionErrorHandler  │  Check for region errors
│   • EpochNotMatch?  │
│   • NotLeader?      │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│   Return value      │  Success!
└─────────────────────┘
          │
          ▼
    [Error?] ──Yes──► Classify error
                          │
                          ▼
                    ┌─────────────┐
                    │   Backoff   │
                    │   & Retry   │
                    └─────────────┘
```

### Batch Operation (BatchGet)

```
┌─────────────────────┐
│  $client->batchGet()│
│   ([k1, k2, k3])    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  groupKeysByRegion()│  Split keys by region
│   • k1 → Region 1   │
│   • k2 → Region 2   │
│   • k3 → Region 1   │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Create async calls  │  One per region
│   • Region 1: [k1,k3]
│   • Region 2: [k2]  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ BatchAsyncExecutor  │  Execute in parallel
│   • Start all calls │
│   • Wait for all    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Merge responses    │  Combine region results
│   • Region 1 results│
│   + Region 2 results│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Order by input     │  Maintain key order
│   • k1, k2, k3      │
└─────────────────────┘
```

### Scan Operation

```
┌─────────────────────┐
│  $client->scan()    │
│  (start, end)       │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  scanRegions()      │  Get all regions in range
│   • Query PD        │
│   • Returns [R1,R2] │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ For each region:    │
│ ┌─────────────────┐ │
│ │ Calculate       │ │
│ │ intersection    │ │
│ │ with scan range │ │
│ └────────┬────────┘ │
│          │          │
│          ▼          │
│ ┌─────────────────┐ │
│ │ Execute scan    │ │
│ │ for region      │ │
│ └────────┬────────┘ │
│          │          │
│          ▼          │
│ ┌─────────────────┐ │
│ │ Collect results │ │
│ └─────────────────┘ │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Merge all results  │
│  (preserve order)   │
└─────────────────────┘
```

## Key Design Decisions

### 1. Synchronous API

**Decision**: All operations are synchronous (blocking).

**Rationale**:
- PHP is primarily synchronous
- Simpler mental model for users
- Easier error handling
- gRPC extension handles async I/O internally

**Trade-offs**:
- ✅ Simplicity
- ✅ Easier debugging
- ❌ Can't do other work while waiting

### 2. Region-aware Routing

**Decision**: Client routes requests to correct TiKV nodes based on region info.

**Rationale**:
- TiKV is distributed by design
- Direct routing avoids PD bottleneck
- Enables parallel batch operations

**Implementation**:
- Cache region info from PD
- Calculate region for each key
- Route to region leader

### 3. Intelligent Caching

**Decision**: Cache region and store metadata with smart invalidation.

**Rationale**:
- PD queries are expensive
- Region topology changes infrequently
- Errors provide cache invalidation hints

**Strategy**:
- Cache on first access
- Invalidate on EpochNotMatch
- Update on NotLeader hint

### 4. Automatic Retries

**Decision**: Built-in retry with exponential backoff.

**Rationale**:
- Distributed systems have transient failures
- Users shouldn't handle all retry logic
- Different errors need different strategies

**Implementation**:
- Classify errors by type
- Apply appropriate backoff
- Track budgets to prevent infinite loops

### 5. PSR-3 Logging

**Decision**: Use PSR-3 logging interface.

**Rationale**:
- Framework agnostic
- Users can plug in any logger
- Structured logging support

### 6. No External Dependencies (except gRPC)

**Decision**: Minimal runtime dependencies.

**Rationale**:
- Easier deployment
- Lower conflict risk
- Smaller attack surface

**Dependencies**:
- `grpc/grpc` - Required for gRPC
- `google/protobuf` - Required for protobuf
- `psr/log` - PSR-3 interface (optional to implement)

## Performance Considerations

### 1. Connection Pooling

**Strategy**: Persistent gRPC channels per address.

```php
// Channels are reused
$channel1 = $grpc->getChannel('127.0.0.1:20160');
$channel2 = $grpc->getChannel('127.0.0.1:20160');
// $channel1 === $channel2
```

**Benefits**:
- Avoid connection setup overhead
- HTTP/2 multiplexing
- Keep-alive handling

### 2. Batch Parallelization

**Strategy**: Execute batch operations across regions in parallel.

```php
// BatchGet to 3 regions happens concurrently
$keys = [...];  // Keys in 3 different regions
$client->batchGet($keys);  // 3 parallel requests
```

**Implementation**:
- Group keys by region
- Create async gRPC calls
- Wait for all with timeout
- Merge results

### 3. Region Cache

**Strategy**: In-memory caching with TTL-based invalidation.

**Benefits**:
- Avoid PD queries (network round-trip)
- Faster key-to-region resolution
- Reduces PD load

**Trade-offs**:
- Stale cache on region split/merge
- Mitigated by error-driven invalidation

### 4. Key Design Impact

**Hot Spots**: All keys in same region = single TiKV node bottleneck

**Solution**: Good key design spreads load:

```php
// Bad: Sequential keys hit same region
for ($i = 0; $i < 1000000; $i++) {
    $client->put("user:$i", $data);  // Hot spot!
}

// Good: Prefixed with hash spreads load
for ($i = 0; $i < 1000000; $i++) {
    $hash = md5($i)[0:2];
    $client->put("user:$hash:$i", $data);  // Distributed
}
```

### 5. Memory Management

**Large Scans**: Can consume significant memory

**Mitigation**:
- Use `limit` parameter
- Paginate large scans
- Process streaming results

```php
// Paginate large dataset
$start = 'user:';
while (true) {
    $batch = $client->scan($start, 'user;', limit: 1000);
    if (empty($batch)) break;
    
    processBatch($batch);
    
    $start = $batch[count($batch) - 1]['key'] . "\x00";
    unset($batch);  // Free memory
}
```

## Error Handling Architecture

### Error Classification

```
TiKvException
├── RegionException
│   ├── EpochNotMatch → Retry immediately
│   ├── NotLeader → Retry with fast backoff
│   ├── RegionNotFound → Retry with medium backoff
│   └── ServerIsBusy → Retry with slow backoff (separate budget)
├── GrpcException
│   ├── Unavailable → Retry with progressive backoff
│   ├── DeadlineExceeded → Retry with progressive backoff
│   └── Cancelled → Don't retry
└── ClientClosedException → Don't retry
```

### Retry Logic Flow

```
Operation Failed
      │
      ▼
┌─────────────┐
│ Classify    │
│ Error       │
└──────┬──────┘
       │
       ▼
   ┌─────────┐
   │ Fatal?  │──Yes──► Throw to user
   └────┬────┘
        │ No
        ▼
   ┌─────────┐
   │ Backoff │
   │ Budget  │──Exceeded──► Throw to user
   └────┬────┘
        │ Available
        ▼
   ┌─────────┐
   │ Sleep   │
   │ (backoff)│
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │ Retry   │
   │ Operation│
   └─────────┘
```

### Error Recovery Examples

**EpochNotMatch** (Region split/merge):
```php
// 1. Invalidate cache
$regionCache->invalidate($regionId);

// 2. Retry immediately (no backoff)
// New region info fetched from PD
```

**NotLeader** (Leader changed):
```php
// 1. Update leader in cache
$regionCache->switchLeader($regionId, $newLeaderId);

// 2. Retry with fast backoff
```

**ServerIsBusy** (TiKV overloaded):
```php
// 1. Use separate budget (10 minutes)
// 2. Progressive backoff (100ms, 200ms, 400ms...)
// 3. Continue until budget exhausted
```

## Security Architecture

### TLS/mTLS

**Server Verification**:
```
Client ──TLS──► TiKV
  │              │
  │  CA Cert     │  Server Cert
  │  (verify)    │  (present)
```

**Mutual TLS**:
```
Client ──mTLS──► TiKV
  │               │
  │  Client Cert  │  Server Cert
  │  (present)    │  (verify)
  │               │
  │  CA Cert      │  CA Cert
  │  (verify)     │  (verify)
```

### Certificate Handling

**Options**:
1. File paths (loaded at connection time)
2. Certificate content (embedded or loaded)

**Security Considerations**:
- Private keys should have restricted permissions (0600)
- Certificates should be rotated regularly
- Use environment variables for paths, not hardcoded

### Data Encryption

**At Rest**: Handled by TiKV (if configured)

**In Transit**: TLS encrypts gRPC traffic

**Application-Level**: Users can encrypt sensitive values:

```php
// Encrypt before storing
$encrypted = encrypt($sensitiveData, $key);
$client->put('secret', $encrypted);

// Decrypt after retrieval
$encrypted = $client->get('secret');
$sensitiveData = decrypt($encrypted, $key);
```

## Scalability Considerations

### Horizontal Scaling

**Client Side**:
- Create multiple clients for different workloads
- Share PD connection (it's lightweight)
- Use separate gRPC channels per TiKV node (handled automatically)

**TiKV Side**:
- Client automatically discovers new TiKV nodes
- Region rebalancing is transparent
- No client changes needed for TiKV scaling

### Load Distribution

**Natural Distribution**: Good key design spreads load across regions

**Hot Spot Detection**: Monitor via TiKV metrics or client logs

**Mitigation**:
- Use hash prefixes
- Implement client-side sharding
- Use TiKV's `split-region` feature

## Future Architecture Improvements

### Planned Enhancements

1. **Connection Pooling**: Explicit pool with size limits
2. **Circuit Breaker**: Fail fast on persistent errors
3. **Metrics**: Built-in Prometheus/OpenTelemetry metrics
4. **Async API**: Optional async/await support (PHP 8.1+)
5. **Streaming**: Streaming scan for very large datasets
6. **TxnHeartBeat**: Automatic heartbeat for long-running transactions

### Research Areas

1. **Smart Routing**: Route based on node load
2. **Read Replicas**: Read from followers for load distribution
3. **Compression**: Compress large values
4. **Batching**: Automatic request batching
5. **API V2 & Keyspace**: TiKV API V2 and keyspace support (issue #25)

## See Also

- [Development Guide](development.md) - Implementation details
- [Contributing Guide](contributing.md) - How to contribute
- [Advanced Features](advanced.md) - Production patterns
- [TiKV Architecture](https://tikv.org/docs/) - TiKV's design
