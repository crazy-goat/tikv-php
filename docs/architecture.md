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
- **Composition**: `RawKvClient` builds a `RetryExecutor` per operation (`createRetryExecutor()`) and delegates the retry loop to it
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
    public function setAtomicForCAS(bool $enabled): self;
    public function isAtomicForCAS(): bool;
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
- Heartbeat to extend lock TTL for long-running transactions (opportunistic for pessimistic transactions, explicit `heartbeat()` otherwise; see [Configuration](configuration.md#lock-ttls-and-the-primary-lock-keepalive-txnkv))
  - The prewrite heartbeat is checked only after the primary region's prewrite
    and after the parallel secondary fan-out (in a multi-region async commit
    the primary is prewritten after the fan-out, so those two checks coincide).
    A single `KvPrewrite` or `KvCommit` RPC that outlives the lock TTL cannot
    be heartbeated mid-flight, so keep `TimeoutConfig::$writeTimeoutMs` below
    the lock TTL or raise the TTL (see [A single prewrite or commit RPC can outlive the lock TTL](configuration.md#a-single-prewrite-or-commit-rpc-can-outlive-the-lock-ttl))

**Key Design Patterns**:
- **Unit of Work**: Transaction tracks the write set, the pessimistic per-key read timestamps, and an optional read set (`trackReadSet`)
- **Two-Phase Commit**: Prewrite → Commit protocol
- **Factory Pattern**: `TxnKvClient::begin()`, `beginOptimistic()` and `beginPessimistic()` create Transaction instances
- **Strategy Pattern**: Optimistic vs pessimistic mode selection

**Public Interface**:
```php
final class TxnKvClient
{
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self;
    
    // Transaction lifecycle
    public function begin(array $options = []): Transaction;  // options: pessimistic (bool, default true), priority (int), enable1Pc (bool), enableAsyncCommit (bool), eagerPessimisticLocks (bool), lockWaitTimeMs (int), optimisticLockTtlMs (int), pessimisticLockTtlMs (int), requestContext (?RequestContextOptions)
    public function beginOptimistic(array $options = []): Transaction;   // begin() with pessimistic = false
    public function beginPessimistic(array $options = []): Transaction;  // begin() with pessimistic = true
    public function close(): void;
}

final class Transaction
{
    // Read operations (MVCC snapshot at startTs)
    public function get(string $key): ?string;
    public function batchGet(array $keys): array;
    public function scan(string $startKey, string $endKey, int $limit = 0): array;
    
    // Write operations (values buffered; pessimistic locks may be eager)
    public function set(string $key, string $value): void;
    public function delete(string $key): void;
    
    // Explicit pessimistic locking (pessimistic transactions only)
    public function lockKeys(string ...$keys): void;  // lock without writing
    public function getForUpdate(string $key): ?string; // lock + read in one RPC
    
    // Transaction control
    public function commit(): void;           // 2PC: prewrite + commit
    public function rollback(): void;
    public function heartbeat(?int $adviseLockTtlMs = null): int;
    
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
│  beginOptimistic()   │                │  beginPessimistic()  │
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
│  1. Prewrite all keys │                │  1. Locks already held│
│     (lock + data)     │                │  2. Prewrite all keys  │
│  2. commitTs = TSO    │                │  3. commitTs = TSO    │
│  3. Commit all keys   │                │  4. Commit all keys   │
└──────────┬───────────┘                └──────────┬───────────┘
           │                                       │
┌──────────▼───────────┐                ┌──────────▼───────────┐
│  Committed            │                │  Committed            │
│  or rollback()        │                │  or rollback()         │
│  (pessimisticRollback)│                └──────────────────────┘
└──────────────────────┘
```

Pessimistic-lock requests are split by the same `maxTxnBatchSize` /
`maxTxnBatchCount` limits as prewrite and commit (issue #430), so one region
can require several lock requests. `PessimisticLockRequest.is_first_lock` is
carried only by the batch whose mutations contain the transaction's primary
key (issue #225); it is not tied to the first region or the first request, and
a retry of that batch keeps the flag. TiKV uses it to decide whether the
transaction can be skipped by the deadlock detector, so flagging a secondary
batch would hide a real lock-wait cycle.
**Rollback semantics**: `rollback()` is a best-effort cleanup that fails
closed. `KvBatchRollback` and `KVPessimisticRollback` responses are inspected
variant by variant, and structured variants take precedence over TiKV's
compatibility `retryable` string: `locked` is resolved and the rollback is
retried, `abort` is fatal, `txn_not_found` / `txn_lock_not_found` confirm the
rollback because the transaction or its lock is already gone (even when
`retryable` is co-set), and a structured `conflict` is a fatal
`TransactionConflictException` rather than a retry. A bare `retryable` string
is retried; every other `KeyError` (including an empty payload) throws the
base `TiKvException`. Whatever the outcome, the attempted rollback is
terminal for the client (issue #231, TXN-26): a `finally` block clears the
write and read sets, sets the status to `RolledBack` and closes the state
before the exception propagates, so `getStatus()` never reports a dead
transaction as `Active` and neither the caller nor the destructor repeats
the failing rollback. Locks the failed RPC left behind expire with their TTL
and are the lock resolvers' responsibility.

**Memory bounds**: the write set grows in the PHP worker for the whole
transaction and is opt-in bounded on `begin()` (`0` = unlimited) by
`maxTxnEntryCount` / `maxTxnEntrySize` / `maxTxnTotalSize`. Exceeding a limit
throws `TransactionTooLargeException` before the write is buffered. The
prewrite and commit requests are independently split per region by
`maxTxnBatchSize` and `maxTxnBatchCount` (issue #430).

Read-set tracking is **off by default** (`trackReadSet`): only `get()` and
`batchGet()` would populate it, `scan()` never does, and nothing in the
commit path consumes it. Opting in enables the public `getReadSet()` map and
the `maxReadSetEntries` / `maxReadSetTotalSize` bounds; an oversized read is
rejected before the fetched value is retained, but the RPC has already
returned, and a whole `KvBatchGet` response is materialised before the check.
A long-lived tracked transaction can call `Transaction::clearReadSet()`.
`RawKvClient::ingest()`
streams its SST payload instead of buffering a second full copy: it copies
only the keys, groups them by region, and builds each `import_sstpb.Pair`
chunk on demand from a generator consumed by the gRPC transport. Its opt-in
`maxIngestSize` cap bounds the caller's payload before the cluster enters
import mode and before any RPC (issue #297).

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
    public function getKeyspaceId(string $name): int;
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

### Key encoding and API versions

**Current support:** the client supports TiKV API V1 (the default) and API V2 with keyspaces.
Set `options['apiVersion']` to `2` to use API V2. The value `0` or `1` selects V1.
Any other value throws `InvalidArgumentException`.
See [API V2 and Keyspaces](configuration.md#api-v2-and-keyspaces) for the options and the cluster settings.

The `Codec\` namespace (`src/Client/Codec/`) hides the key encoding behind `CodecInterface`.
The namespace is internal. Do not depend on it. See the stability policy in the README.

| Method | Purpose |
|---|---|
| `encodeKey()`, `decodeKey()` | Convert a user key to the stored key and back |
| `encodeRegionKey()`, `decodeRegionKey()` | Convert a key for PD region lookups and back |
| `encodeRange()`, `decodeRange()` | Convert a range `[start, end)` for PD region scans |
| `getApiVersion()`, `getKeyspaceId()`, `getKeyspaceName()` | Report the API version and the keyspace |

The classes in the namespace:

| Class | Behavior |
|---|---|
| `CodecV1` | API V1. User keys pass through unchanged. It reports `APIVersion::V1` (protocol value 0), keyspace ID 0 and an empty keyspace name. |
| `CodecV2` | API V2. It adds a 4-byte prefix to each user key: the mode byte and a 3-byte keyspace ID in network byte order. |
| `MemComparableCodec` | Memory-comparable encoding (TiDB `EncodeBytes`) for region keys. It keeps the byte order of the keys. |
| `Mode` | Enum `Raw` (prefix byte `0x72`) and `Txn` (prefix byte `0x78`). `CodecV2` uses the prefix byte. `CodecV1` uses the mode only to choose the region-key encoding. |

Region keys depend on the API version and on the mode:

| API version | Mode | User key | Region key (PD lookup) |
|---|---|---|---|
| V1 | `Raw` | unchanged | unchanged |
| V1 | `Txn` | unchanged | memory-comparable encoded |
| V2 | `Raw` or `Txn` | mode byte + keyspace ID + key | the prefixed key, memory-comparable encoded |

`RawKvClient` builds its client in `Mode::Raw`. `TxnKvClient` builds its client in `Mode::Txn`.
For API V1, both clients start with `CodecV1`.
For API V2, `ConnectionFactory` asks PD for the ID of the keyspace and replaces the codec with a `CodecV2`.
In `Mode::Txn`, `CodecV1::decodeRegionKey()` throws `\InvalidArgumentException` for a malformed encoded key.
`CodecV2` throws `KeyOutOfBoundsException` for a key outside its keyspace.

### API V2 keyspace boundary

For `options['apiVersion'] === 2`, `ConnectionFactory` resolves the configured
keyspace name through PD's `keyspacepb.Keyspace/LoadKeyspace` service and
builds a mode-specific `CodecV2`. The codec is used twice: `PdClient` applies
its memory-comparable region-key encoding for region discovery, while
`ApiV2GrpcClient` decorates the shared transport to add the V2 context and
translate request/response keys for unary, async, streaming, and
BatchCommands calls. RawKV responses are forced to the `default` column family.
This keeps application-facing components in user-key space and makes the
keyspace contract a transport concern rather than a per-operation special
case.

### Resource control on the request Context

Every region-addressed request carries a `kvrpcpb.Context`. Two optional
resource-control fields live on it (issue #427, GAP-13):

- `Context.resource_control_context.resource_group_name` — the TiKV resource
  group the request belongs to, enforced by PD's resource manager;
- `Context.resource_group_tag` — opaque bytes used for query attribution.

`Region\ResourceControl` is the single value object holding the pair;
`RegionContextFactory::fromRegionInfo()` and `resolveTarget()` apply it while
building the context. The raw and transactional components receive the object
from their client (`RawKvClient`, `TxnKvClient` → `Transaction` → `TxnReader`,
`TwoPhaseCommitter`, `LockResolver`), so the same values reach every RPC
built by those components. `TxnKvClient::begin()` layers per-transaction
overrides over the client-wide defaults, field by field.

Both fields are proto3 defaults when unset and the object is only applied
when a value is configured: an unconfigured client never writes them, which
keeps the serialized request bytes unchanged. The client does not parse
`ExecDetailsV2`/`RUV2` responses, so Request Unit accounting remains a
server-side concern (see [Configuration](configuration.md#resource-control-resource-groups)).

### 5. GrpcClient

**Location**: `src/Client/Grpc/GrpcClient.php`

**Responsibilities**:
- gRPC channel management
- Request/response serialization
- TLS configuration
- Connection pooling (via persistent channels)
- BatchCommands stream multiplexing (experimental, issue #418 — see below)

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

**BatchCommands Multiplexing (experimental, off by default)**: the RawKV
batch fan-outs can multiplex their per-region sub-requests over one
bidirectional `BatchCommands` stream per store address
(`/tikvpb.Tikv/BatchCommands`). The pieces, under
`src/Client/Batch/BatchCommands/` and `src/Client/Grpc/`:

```
RawKvBatch (flag on)
    │  builds BatchCommandsEntry per sub-batch (address + unary request)
    ▼
BatchCommandsMultiplexer ── assigns request_ids, groups by store address,
    │                        filters ops absent from the Request oneof →
    │                        fallback entries
    ▼
GrpcBatchCommandsTransport ── per-address stream pool
    │
    ▼
BatchCommandsConnection ── ext-grpc Call::startBatch: SEND_MESSAGE, then
                            RECV batches until BatchCommandsCorrelator has
                            every request_id (out-of-order tolerated)
```

Because the client is synchronous, the multiplexing window is a single
fan-out (one `BatchCommandsRequest` per store per window) — not a general
event-loop multiplexer. Region errors and stream failures fall back to the
unary fan-out whose `RetryExecutor` semantics are preserved. Full details
and limitations: `docs/configuration.md` → "Batch Commands Multiplexing".

### 6. RegionCache

**Location**: `src/Client/Cache/RegionCache.php`

**Responsibilities**:
- Cache region metadata
- Track region epochs
- Handle leader changes
- Invalidate on errors
- Support opt-in refresh and preload (`RegionCacheWarmer`, issue #425)

**Cache Structure**:
```php
class RegionCache
{
    private array $entriesById = [];       // regionId → RegionEntry
    private ?RegionBoundaryNode $boundaryRoot = null; // start-key treap
    private array $lruOrder = [];           // oldest insertion order → true
    private array $expiryHeap = [];          // min-heap by expiresAt
}
```

`getByKey()` uses the treap's predecessor lookup rather than an exact-key
hash, because the query key lies inside a half-open region range. Overlap
replacement walks only the affected ordered range; TTL cleanup pops only due
heap records.

**Invalidation Strategy**:
- **EpochNotMatch**: Refresh the cache from TiKV's `current_regions` payload
  when it is usable (the returned regions cover the key, decode cleanly, and
  the old leader is still a peer); otherwise invalidate the specific region
  and let the next attempt resolve it through PD (issue #246)
- **NotLeader**: Update leader info when the hinted peer may lead — a voter
  that is not a witness; a hint naming a learner or a witness invalidates the
  region and re-resolves it through PD (issue #258, REG-27)
- **RegionNotFound**: Invalidate and retry
- **ServerIsBusy / DiskFull / IsWitness / RecoveryInProgress**: Keep the
  cached region — the request reached the right region, so only the retry
  backoff applies (issue #245)

**Opt-in refresh and preload (issue #425)**:

PHP has no background threads, so `RegionCacheWarmer` owns no timer and starts
no loop. A long-running worker calls `refreshRegionCache()` from its own tick
(the `reapIdleChannels()` pattern), throttled by
`options['regionCacheRefreshMs']` (default `0`, disabled), or warms the cache
at boot with `preloadRegions([['start', 'end'], ...])`. Refresh re-queries PD
for a bounded slice of the cached regions (128 per tick, round-robin) on each
region's start key and folds the answer back through `put()`; a stale epoch is
updated, a split or merge supersedes the entry silently, and an answer that
does not cover the start key drops it. The first PD failure aborts the tick
and the cached entries are kept; neither method throws on a PD failure, and
both are no-ops after `close()`. Scan-path range resolution is a separate
mechanism (`RegionScanSource`, see the Scan Operation flow), not part of the
refresh tick.

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
    public function resolveLock(
        string $primaryLock,
        LockInfo $lock,
        int $remainingDeadlineMs = 0,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): void;
}
```

The last flag is a property of the call site, not of the resolver: a
`resolveLock()` running inside a `RetryExecutor::execute()` closure leaves
NotLeader handling to `RetryExecutor::handleNotLeader()` (valid-hint leader
switching), while a direct call site such as the pessimistic-lock loop passes
`false` so `RegionErrorHandler::check()` self-invalidates with `'not_leader'`.
The resolver threads the flag through every region-error check it performs —
`KvCheckTxnStatus`, `KvCheckSecondaryLocks`, the batch `KvResolveLock`, and the
async-commit finalization (`resolveAsyncCommitLocks()`, issue #568).

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
    │ Locked  │──Throw TxnRetryableException (the caller's
    │         │  RetryExecutor owns the backoff within the deadline)
    │ Committed│──ResolveLock with commitTs
    │ RolledBack│──ResolveLock (cleanup)
    └─────────┘
```

`resolveLock()` never sleeps for a lock TTL (issue #206): a live lock raises a
retryable `TxnRetryableException` and the caller's `RetryExecutor` owns the
bounded backoff. When the call runs inside a retry loop, the operation's
`RetryBudget` (shared by every `execute()` of a `beginOperation()` executor)
exposes the live remaining wall-clock budget through
`RetryBudget::remainingDeadlineMs()`. `resolveLock()` converts that value once
into an absolute deadline and caps every nested lock-resolution RPC (and the
TSO fetch) by the live time left until it, so the RPCs share one budget and
none is given a deadline beyond the operation's `retryDeadlineMs` (issue
#569). A live lock is still retryable; the executor's own deadline check
raises `RetryBudgetExhaustedException` when the budget is spent. A deadline of
`0` keeps the configured `lockResolveTimeoutMs`/`tsoTimeoutMs` and the
historical behaviour.

`TxnKvClient` keeps one cache, shared by every transaction, with the final outcomes (committed or rolled back) in a
bounded FIFO cache keyed by lock start ts (`ResolvedTxnStatusCache`, 2048
entries). A lock of a known transaction skips `CheckTxnStatus`. A live or
undetermined status is never cached. `resolveLocks()` takes several locks,
checks each transaction once, and sends one `ResolveLock` per region and
transaction with all keys of that transaction in the region.

An errored status check is not a status. A region error, a gRPC failure or a
`KeyError` in the `CheckTxnStatus` response is propagated as a retryable
failure (`TxnRetryableException` with `BackoffType::TxnLock` for a `KeyError`)
through the caller's retry executor, so the response's zeroed status fields can
never be read as "not committed" and drive a rollback. The async-commit
secondary check follows the same rule: a `KeyError` in the
`CheckSecondaryLocks` response is retryable, and a region error while grouping
the secondaries propagates instead of falling back to the TTL/rollback path.
Only a successful response decides commit versus rollback (issue #224,
TXN-19).

### 8. Retry System

**Location**: `src/Client/Retry/` — `RetryExecutor` runs the loop; `RawKvClient::createRetryExecutor()` builds one per operation

**Responsibilities**:
- Classify errors
- Apply appropriate backoff
- Track retry budgets
- Handle special cases (ServerBusy)

**Backoff Types** — `src/Client/Retry/BackoffType.php` declares the
authoritative case list (no fabricated `Fast`/`Medium`/`Slow` buckets); base and cap below
come from `BackoffType::baseMs()` / `BackoffType::capMs()`:

```php
enum BackoffType
{
    case None;                    // 0ms sleep — immediate retry (DataIsNotReady replica lag)
    case EpochNotMatch;           // 2ms–500ms, equal jitter (client-go BoEpochNotMatch, issue #241)
    case ServerBusy;              // 2000ms–10000ms, progressive, separate budget
    case StaleCmd;                // 2ms–1000ms
    case RegionMiss;              // 2ms–500ms (e.g., RegionNotFound)
    case TiKvRpc;                 // 100ms–2000ms (transport/gRPC errors)
    case NotLeader;               // 2ms–500ms (its own case, own cap)
    case DiskFull;                // 500ms–5000ms
    case RegionNotInitialized;    // 2ms–1000ms
    case ReadIndexNotReady;       // 2ms–500ms
    case ProposalInMergingMode;   // 2ms–500ms
    case RecoveryInProgress;      // 100ms–10000ms
    case IsWitness;               // 1000ms–10000ms
    case MaxTimestampNotSynced;   // 2ms–500ms
    case TxnLock;                 // 200ms–3000ms (transactional)
    case TxnLockFast;             // 10ms–3000ms (client-go BoTxnLockFast, DefBackoffLockFast base)
    case TxnNotFound;             // 2ms–500ms (client-go BoTxnNotFound)
    case CommitTSLag;             // 2ms–500ms (client-go BoCommitTSLag)
    case PDRpc;                   // 500ms–3000ms (client-go BoPDRPC)
}
```

PHP currently applies equal jitter to every case; client-go uses `NoJitter` for
`BoTxnNotFound` and `BoCommitTSLag`.

**Retry Budgets**:
- **Scope**: one budget per logical operation (issue #223). `RetryExecutor::beginOperation()` returns an executor whose `execute()` calls share the same `maxBackoffMs`, `serverBusyBudgetMs` and wall-clock deadline, and every client fan-out entry point (`scan()`/`reverseScan()`/`batchScan()`, the batch methods, transactional reads, `commit()`, `rollback()`, `deleteRange()`, `checksum()`) uses one for all its regions and pages. A later operation starts from zero, so a read-heavy transaction cannot starve its commit. The attempt cap stays per `execute()` call (one retry loop), so a wide fan-out does not exhaust it on one retry per region.
- **General**: 20 seconds total backoff
- **ServerBusy**: 60 seconds separate budget
- **Wall-clock deadline**: 30 seconds per operation (configurable via `options['retryDeadlineMs']`; issues #294, #237). Checked before each attempt and before each backoff sleep (the sleep is clamped to the remaining budget), so the worst case is ≈ deadline + the duration of the last in-flight RPC (bounded only by the gRPC timeout option, not by the retry deadline). With the defaults this deadline binds before the 60 s ServerBusy budget; the deadline is guaranteed to bind first only while the budget is at least one maximum ServerBusy backoff (~10 s) above it, and when the budget is within ~10 s of the deadline or below it either can bind (see [ServerBusy Budget vs Retry Deadline](configuration.md#serverbusy-budget-vs-retry-deadline)).

**Fan-out and response-borne errors**: a fanned-out operation dispatches one
request per region before awaiting any of them (`BatchAsyncExecutor`). When a
retryable region error comes back *inside* a response rather than as a thrown
gRPC error, `CheckedGrpcFuture::fromRetryableDispatch()` keeps the eager first
dispatch and runs the remaining dispatch+await cycle under `RetryExecutor`
during the wait phase, so the error is retried per region with cache
invalidation / leader switching and backoff. The batch wrappers use it since
#183 and `RawKvRangeOps` (`deleteRange()` / `checksum()`) since #566.

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
│  RawKvCrud::get()   │  Builds the operation closure
│  RetryExecutor::    │  and starts the retry loop
│    execute()        │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ RegionResolver::    │  Get region for key
│  getRegionInfo()    │
│   • Check cache     │
│   • Query PD if miss│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ RegionResolver::    │  Get TiKV node address
│ resolveStoreAddress │
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
│  RegionScanSource   │  Cache first, PD only for gaps
│   • Cached run      │  (RawKV: 'scan' metrics,
│   • Gap → PD scan   │   TxnKV: 'txn_scan' metrics)
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

Both scanners enumerate regions through `RegionScanSource` (issue #425): a
contiguous cached run is served without PD, and at the first gap one bounded
`ScanRegions` call starts at the gap's key and its answer is cached. This is
the same source for RawKV and TxnKV, so the two paths cannot drift apart, and
it emits the scan-path `regionCacheHit()` / `regionCacheMiss()` counters
(`scan` and `txn_scan`).

An unbounded (`limit: 0`) `scan()` / `scanPrefix()` / `reverseScan()`
executes the per-region sub-scans concurrently, in windows of at most
`options['maxConcurrency']` regions (default 16): every send in a window is
issued before the first wait, and the responses are merged in key order
(descending for a reverse scan). A bounded scan (`limit > 0`) walks the
regions sequentially. The reverse path resolves its regions from the upper
bound downwards in bounded batches (issue #198):
`RegionCache::getRegionBelow()` serves the warm case and a miss asks PD for
the region containing the cursor or, when the cursor sits on a region
boundary, for the adjacent region below it through `GetPrevRegion`. It no
longer collects the whole chain before reading the top region. A region or
transport error, or a region that split after enumeration, falls back to
the sequential retrying path.

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
- Refresh on EpochNotMatch from its `current_regions` payload; invalidate
  when the payload is absent or unusable
- Update on a NotLeader hint only when the hinted peer is a voter and not a
  witness; a learner or witness hint invalidates the entry so the next attempt
  re-resolves it through PD (issue #258)
- Offer an opt-in tick (`refreshRegionCache()`) and an explicit boot-time
  preload (`preloadRegions()`) for long-lived workers (issue #425)
- Drop the cached store address on a `StoreNotMatch` region error or a
  transport failure to that store, so the next attempt asks PD for a fresh
  `GetStore` instead of retrying a stale address (issue #247)
- Close the channel of the store the failed attempt actually targeted, not
  the cached leader's: every RPC path reports its target store to the retry
  executor, so a replica-read follower or a store whose cached leader changed
  during the attempt is the one torn down (issue #248). Only `UNAVAILABLE`
  closes a channel; a `DEADLINE_EXCEEDED` on a slow but healthy connection is
  retried without a re-handshake. The exception carries the address of the
  failed call, so a nested lock-resolution RPC against another region also
  closes its own channel
- Track per-store health as a circuit breaker (issue #424): after three
  consecutive `UNAVAILABLE` or `DEADLINE_EXCEEDED` transport failures within a
  60-second window a store is unavailable for a one-second cooldown, its
  cached leader regions are invalidated with the `store_unhealthy` reason, and
  `resolveStoreAddress()` throws the retryable `StoreUnavailableException`
  before any RPC. After the cooldown the next request is a half-open probe: a
  success recovers the store, a failure restarts the cooldown. Failures are
  attributed by the exception's `targetAddress`, so a PD outage or a nested
  RPC against another store does not demote the outer store; a
  `CasIndeterminateException` is unwrapped to its transport cause. Replica
  reads skip stores in cooldown. Fatal statuses such as `UNAUTHENTICATED` and
  server-busy responses do not demote a store. The state is reported through
  `MetricsInterface::storeHealthChanged()`

**Trade-off**: refresh costs one PD `GetRegion` per cached region, which is
why it is opt-in and throttled by `regionCacheRefreshMs` rather than running
by default.

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
- `ext-grpc` - The gRPC C extension (1.57 or newer), a hard requirement
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

### 2. Batch Concurrent Fan-Out

**Strategy**: Issue every per-region gRPC send RPC before any response is awaited, so server-side latencies for distinct regions overlap on their respective channels. Response collection then proceeds in declaration order.

```php
// BatchGet to 3 regions dispatches all 3 sends before any wait begins
$keys = [...];  // Keys in 3 different regions
$client->batchGet($keys);  // 3 concurrent sends, responses collected in order
```

**Implementation**:
- Group keys by region (sub-batched under the per-region RawKV caps)
- Issue sends via `executeBatch*ForRegionAsync()`, returning un-waited `GrpcFuture` objects wrapped in `CheckedGrpcFuture`
- `BatchAsyncExecutor::executeParallel()` invokes every per-region callable in the dispatch phase, then awaits responses in the wait phase
- A `$deadlineMs` argument (finite by default, `TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS`; `0` disables it) caps the wall-clock time for dispatch + wait; on overrun the executor raises `BatchDeadlineExceededException` and cancels any still-unfinished futures. One budget covers every window of the fan-out, and it is checked before each dispatch and before each wait, never inside one blocking wait
- On the first wait-phase failure, remaining futures are cancelled to prevent gRPC channel/completion-queue leaks
- The rare split/merge slow path re-dispatches every sub-region's send during the dispatch phase and merges responses in the wait phase

### 3. Region Cache

**Strategy**: In-memory caching with TTL-based invalidation. Entries are
stored by region ID, while a start-key treap provides ordered predecessor and
range operations; LRU recency uses insertion order and expiry uses a min-heap.
This avoids packed-array splices and full identity/LRU scans during writes.

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

> The `$lastKey . "\x00"` continuation is `KeyOrder::successor($lastKey)` in library code
> (`CrazyGoat\TiKV\Client\Util\KeyOrder`, issue #186) — the same byte string, but
> documented and unit-tested as the immediate successor of a key.

## Error Handling Architecture

### Error Classification

```
TiKvException
├── RegionException
│   ├── EpochNotMatch → Retry with small jittered backoff (2–500 ms)
│   ├── NotLeader → Retry with fast backoff
│   ├── RegionNotFound → Retry with medium backoff
│   └── ServerIsBusy → Retry with slow backoff (separate budget)
├── GrpcException
│   ├── Unavailable → Retry with progressive backoff
│   ├── DeadlineExceeded → Retry with progressive backoff
│   └── Cancelled → Don't retry
├── PdException → Don't retry (a PD-level error in the response header —
│   the PD client already rotated the endpoint once per configured member
│   for the not-leader case; see issue #234)
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
// 1. If TiKV sent current_regions, map them and put them into the cache.
//    The new regions supersede the old entry, and their leader is seeded
//    from the stale cached entry's leader store when that store is still
//    one of their peers. No PD round trip.
//    If the payload is empty, undecodable, leaderless, older-epoch or
//    does not cover the key, invalidate the entry instead.
// 2. Retry with a small jittered backoff (2–500 ms, issue #241).
//    The retry re-resolves the key: from the refreshed cache on the
//    payload path, or from PD on the fallback path (issue #246).
```

**NotLeader** (Leader changed):
```php
// 1. Update leader in cache
$regionCache->switchLeader($regionId, $newLeaderId);

// 2. Retry with fast backoff
```

**ServerIsBusy** (TiKV overloaded):
```php
// 1. Use separate budget (60 seconds) within the 30s wall-clock deadline
// 2. Progressive backoff (100ms, 200ms, 400ms...)
// 3. Continue until budget or deadline exhausted
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

## See Also

- [Development Guide](development.md) - Implementation details
- [Contributing Guide](contributing.md) - How to contribute
- [Advanced Features](advanced.md) - Production patterns
- [TiKV Architecture](https://tikv.org/docs/) - TiKV's design
