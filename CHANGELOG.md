# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Transaction SRP decomposition** — broke up the 944-line `Transaction` god object into three focused collaborators: `TransactionState` (mutable state), `TxnReader` (get/batchGet/scan), and `TwoPhaseCommitter` (commit/rollback/pessimistic lock/heartbeat). The public `Transaction` API is preserved as a thin façade delegating to these classes, following the same decomposition pattern already established in the RawKv module (`RawKvCrud`, `RawKvBatch`, etc.). (#83)

### Added
- `ConnectionFactory` and `ConnectionBundle` in `src/Client/Connection/` — extract the shared TLS parsing, gRPC/PD wiring and timeout-config construction that was duplicated between `RawKvClient::create()` and `TxnKvClient::create()`. Both `create()` methods now delegate to `ConnectionFactory::create()`, eliminating ~100 lines of copy-paste bootstrap code. (#94)
- `RegionGrouper::groupItemsByRegion()` — a generic region grouping method that accepts any item array with a key-extractor callable, allowing `Transaction::groupMutationsByRegion()` to delegate to the shared grouper instead of reimplementing the same batch-resolution loop. (#94)
- `RegionErrorHandler::check()` now accepts optional `RegionCacheInterface $cache` and `?int $regionId` parameters: when provided, the region is invalidated from the cache before the `RegionException` is thrown. `Transaction::handleRegionError()` and `LockResolver::checkTxnStatus()` now delegate to `RegionErrorHandler::check()`, eliminating a latent correctness divergence where the shared handler did not invalidate the cache but the inline copies did. (#94)
- `MetricsInterface` in `src/Client/Observability/` — optional zero-cost observability hook for emitting RPC counts (`rpcStarted`/`rpcCompleted`), retry counts (`retryAttempted`), region-cache hit/miss (`regionCacheHit`/`regionCacheMiss`) and region-invalidation events (`regionInvalidated`). Counters are tagged by operation (e.g. `'tikvpb.Tikv/KvGet'`) or backoff type (`'NotLeader'`, `'ServerBusy'`, …) and the implementation is free to bucket tags however it wishes. The default `NoOpMetrics` is on the hot path of every gRPC call, region lookup and retry, so callers that do not opt in pay a single empty-method dispatch per call site. Pass an implementation via `RawKvClient::create()` and `TxnKvClient::create()` options key `'metrics'`, or inject it directly through the constructor. (#116)
- `InMemoryMetrics` — a thread-unsafe reference implementation useful for tests and benchmarks; counters are stored as `[tag => count]` maps and RPC latency is retained so tests can assert on mean latency or counts. (#116)
- `RawKvClient::healthCheck()` / `TxnKvClient::healthCheck()` — issues the lightweight `GetMembers` RPC (no user data, no "no region" failure) and returns the learned cluster ID, or null if the PD response carried no cluster ID header. Suitable as a load-balancer / Kubernetes readiness probe. (#116)
- `RawKvClient::getMetrics()` / `TxnKvClient::getMetrics()` — accessor for the metrics implementation in use, allowing in-process inspection of counters. (#116)
- `PdClientInterface::ping()` — dedicated `GetMembers` RPC concretely implemented in `PdClient::ping()`; uses the existing cluster-id mismatch retry path. (#116)
- `HealthCheckException` in `src/Client/Exception/` — thrown by `healthCheck()` on transport / PD failure; extends `TiKvException` so it composes with the existing exception hierarchy. (#116)
- Unit tests for `MetricsInterface`/`NoOpMetrics`/`InMemoryMetrics` covering counter independence, mean-latency computation, region-cache and retry tagging, and `reset()` semantics. (#116)
- Unit tests for `RawKvClient::healthCheck()` (cluster ID returned, null when PD has no cluster ID, transport failure becomes `HealthCheckException`, throws `ClientClosedException` after `close()`). (#116)
- Unit tests for `RawKvClient::getMetrics()` (default is `NoOpMetrics`, injected instance round-trip, factory option accepted, factory rejects non-`MetricsInterface` values). (#116)
- Unit tests for `RegionResolver` cache-hit / cache-miss metric emission. (#116)
- Unit tests for `RetryExecutor` metric emission (no-retry yields no metrics, retryable error increments retry counter, attempts under attempt-cap all emit). (#116)
- Unit tests for `PdClient::ping()` (cluster ID from response header, null when missing, propagates gRPC errors). (#116)
- E2E tests `testHealthCheckReturnsLearnedClusterId` / `testHealthCheckAfterClientCloseThrows` / `testMetricsBackendReceivesRpcCountersOnRealOperation` — exercise the new APIs end-to-end against a real PD cluster. (#116)

### Security
- **Plaintext gRPC is no longer the silent default** — `GrpcClient` now logs a warning whenever an insecure (plaintext) channel is opened. A new `allowInsecure` constructor parameter (defaults to `true` for backward compatibility) controls whether insecure channels are permitted; set `allowInsecure: false` to fail-closed when no TLS configuration is provided. (#80)
- **Partial TLS configuration now throws `InvalidArgumentException`** — `TlsConfig` constructor rejects configurations with client certificate/key without a CA certificate. Previously, a partial mTLS configuration (client cert+key without CA) was reported as "not enabled", causing the connection to silently downgrade to plaintext, sending credentials over an unencrypted channel. (#80)
- **`TlsConfig::isEnabled()` now detects any TLS material** — returns `true` when either a CA certificate or a client certificate+key pair is present, preventing partial configurations from being misinterpreted as "disabled". Added `TlsConfig::isComplete()` to check for a fully valid configuration (CA certificate present). (#80)

### Fixed
- **Batch operations now issue all per-region gRPC sends before any wait begins** — `RawKvBatch`'s `batchGetWithRetry`/`batchPutWithRetry`/`batchDeleteWithRetry` previously called `$future->wait()` inside the retry closure, which meant the public `BatchAsyncExecutor::executeParallel` always received already-resolved values and the genuine fan-out path (`executeBatch*ForRegionAsync()` returning un-waited `GrpcFuture`s) was dead code. Each call now returns an un-waited `GrpcFuture` wrapped in `CheckedGrpcFuture`, so every region's gRPC send RPC is dispatched in the executor's dispatch phase (true client-side fan-out at the wire layer) before the executor's wait phase collects responses in declaration order. Region-error checking moves to `CheckedGrpcFuture::waitForExecutor()` and is enforced uniformly for the single-region fast path and the multi-region split/merge slow path. The executor's deadline safeguard (`BatchDeadlineExceededException`) now cancels any in-flight futures when the wall-clock budget is exhausted, preventing gRPC channel/completion-queue leaks. The `slowLog` threshold is still honoured — measurement now wraps the inner wait callback so the logged duration reflects the actual round-trip latency. (#81)
- **TxnKv pessimistic locking & lock resolution are now safe** — `LockResolver::resolveLock()` now accepts a `LockInfo` proto object and extracts the lock's actual `primary_lock` for `CheckTxnStatus`, preventing status checks against the wrong key that could roll back a live transaction. `LockResolver::checkTxnStatus()` now uses the `Action` enum constants (`Action::NoAction`, `Action::MinCommitTSPushed`) instead of the dead `(string) $action === 'Lock'` comparison. `Transaction::pessimisticLockBatch()` no longer swallows `GrpcException` (transport errors now propagate), and on a locked-key conflict the lock is retried with exponential backoff capped by `maxBackoffMs`. `for_update_ts` now uses a fresh PD timestamp (not `startTs`) and the maximum is carried into prewrite. All `resolveLock()` callers (`get()`, `scan()`, `prewrite`, `rollback`) now pass the `LockInfo` object with the correct primary key. (#73)

### Added
- `CheckedGrpcFuture` — lazy region-error-checking wrapper for un-waited `GrpcFuture`s and synthetic message producers (`fromGrpcFuture()` / `fromCallable()`). Used by `RawKvBatch` to defer region-error checking from the dispatch phase to the executor's wait phase while preserving batch fan-out. (#81)
- `BatchDeadlineExceededException` — thrown by `BatchAsyncExecutor::executeParallel($regionCalls, $deadlineMs)` when dispatch + wait exceeds the wall-clock budget. The exception carries the configured deadline, the elapsed time, and diagnostic context (`dispatchedRegions` / `pendingRegions`) describing where in the lifecycle the budget ran out. (#81)
- `BatchAsyncExecutor::executeParallel()` accepts an optional `$deadlineMs` parameter; pass `0` (the default) to disable the deadline entirely. (#81)
- Unit tests for `BatchAsyncExecutor` deadline semantics: dispatch-phase cancellation, wait-phase cancellation, zero-deadline passthrough, `CheckedGrpcFuture` fan-out, region-error-to-partial-failure mapping. (#81)
- Unit tests for `CheckedGrpcFuture`: lazy inner accessor, region-error surface at wait boundary, cancel/no-op for synthetic wrappers. (#81)
- E2E test `testBatchFanOutAcrossMultipleRegions` — writes 600 keys (well above any region split boundary) and reads them back in a single `batchGet`, asserting that the multi-region fan-out returns every value in the original key order. (#81)

### Fixed
- **`Transaction::commitKeys()` now enforces primary-region-first ordering and retries secondary-region commits** — previously, 2PC commit dispatched `KvCommit` RPCs to all regions in `keysByRegion` order with no retry wrapper. A transient region error on any secondary region threw after the primary was already committed, leaving a **half-committed transaction** whose status was never set to Committed. TiKV also rejects secondary commits that arrive before the primary's commit. `commitKeys()` now resolves the primary's region and commits it first (no retry, because re-trying an errored primary commit would invalidate the captured `commitTs`), then sets `TransactionStatus::Committed` immediately on primary success; secondary regions are committed under the existing retry executor (commits are idempotent given a fixed `start_version` + `commit_version`, so transient region/grpc errors are safely retryable). `commitForRegion()` now asserts `commitTs` is non-null before sending `commit_version`. The dead `$firstRegionKeys`/`$isPrimaryRegion` variables that hinted at intended-but-missing primary-first logic have been replaced with explicit primary-region resolution. (#76)
- **Transaction::scan() now enforces a `MAX_SCAN_LIMIT` (10240), applies the limit after write-set merge, and inserts in-range write-set keys not returned by TiKV** — previously, limit=0 sent uint32 max (memory exhaustion risk), and the limit was not re-applied after merging results with the write set. Additionally, newly-written keys inside the scan range that TiKV had not yet committed were silently missing from the result, violating read-your-writes semantics for scans. (#86)
- **RawKvBatch retry now re-groups keys after region split/merge** — `resolveRegion()` no longer returns a stale `RegionInfo` when the region ID changes; the retry wrappers (`batchGetWithRetry`, `batchPutWithRetry`, `batchDeleteWithRetry`) now verify every key still falls within the resolved region's range and, when a split/merge scatters keys across multiple regions, re-resolve and re-dispatch each sub-group to its own region. `RegionErrorHandler::check()` now surfaces per-pair `KeyError`s from `RawBatchGetResponse` and top-level error strings from `RawBatchPutResponse`/`RawBatchDeleteResponse`, eliminating silent partial writes/deletes/reads. (#140)
- **`__destruct()` on `RawKvClient`, `TxnKvClient`, and `Transaction`** — clients now close deterministically when dropped without an explicit `close()` call; `Transaction` rolls back if still active, preventing leaked server-side locks. Destructors never throw. (#91)
- **`RegionCache` now bounded with LRU eviction and incremental index updates** — `RegionCache` grew without bound and had O(n²) warm-up cost from rebuilding the `idToIndex` map on every insertion/removal. Added configurable `maxEntries` (default 10 000) with LRU eviction; `idToIndex` is now updated incrementally instead of rebuilt from scratch; periodic expired-entry sweep runs every N puts (configurable `sweepInterval`, default 100). `clear()` is now called in `RawKvClient::close()` and `TxnKvClient::close()`. (#82)
- **`PdClient ↔ TimestampOracle` reference cycle broken** — `TimestampOracle` now receives `getClusterId`/`setClusterId` closures instead of the full `PdClient` reference, allowing the cycle collector to reclaim memory promptly without relying on GC. (#91)

### Added
- `SlowLogConfig` — optional, injectable configuration for slow operation logging in `RawKvClient`. Accepts per-operation-type thresholds (read, write, batch read/write, scan, delete range, checksum) in milliseconds. Pass via `RawKvClient::create()` options key `'slowLog'`. When a gRPC call exceeds its threshold, a PSR-3 warning is logged with the redacted key, duration, and threshold. (#31)
- `TimeoutConfig` now has a `checksumTimeoutMs` property (default 30s) for the `RawChecksum` RPC, separate from `deleteRangeTimeoutMs`. Configured via `'timeout'` option key `checksumTimeoutMs`. (#31)
- Unit tests for `SlowLogConfig`: default values, custom values, per-operation threshold lookups, unknown operation returns zero, zero threshold disables logging. (#31)
- `StoreCache` now has a `maxEntries` cap (default 128) with LRU eviction — when at capacity, the least recently used entry is evicted before inserting a new one, preventing unbounded memory growth over time. (#108)
- `GrpcClient` `closed` guard — `close()` sets an internal flag and `call()` throws `InvalidStateException` if invoked after close, making the closed state explicit and preventing latent use-after-close bugs. (#108)
- Unit tests for `StoreCache` LRU eviction: eviction at capacity, correct LRU ordering with multiple entries, overwrite does not count towards capacity, clear resets and allows re-adding. (#108)
- Unit tests for `PdClient::close()`: verifies store cache is cleared, cluster ID is reset, and TSO is nulled; verifies the shared `GrpcClient` is NOT closed by `PdClient`. (#108)
- Unit test for `GrpcClient::call()` throwing `InvalidStateException` after `close()`. (#108)
- Channel eviction in `GrpcClient`: idle channels are closed after a configurable TTL (default 10 min); at most `maxChannels` (default 64) are cached, evicting the least recently used channel when at capacity; channels in `TRANSIENT_FAILURE` or `SHUTDOWN` states are now reaped on next access (previously only `FATAL_FAILURE` was reaped). (#89)
- `__destruct()` on `GrpcClient` — closes all cached channels when the reference is dropped without an explicit `close()` call. (#89)
- `GrpcResponseParser::setMaxMessageSize()` — configurable maximum protobuf message size guard before `mergeFromString()`, preventing potential DoS from oversized messages (defense in depth for CVE-2026-6409). Default is 0 (unlimited). (#75)
- Unit tests for `GrpcResponseParser` max message size: within limit, exceeds limit, null message, zero disables limit. (#75)
- `TlsConfigBuilder::withCaCertFile()`, `withCaCertPem()`, `withClientCertFile()`, `withClientCertPem()` — explicit API to distinguish file paths from inline PEM content, replacing the ambiguous `file_exists()` guessing in legacy methods. The new methods support an optional `$baseDir` parameter to restrict allowed directories. The legacy `withCaCert()` and `withClientCert()` are deprecated. (#87)
- `RegionRangeClipper` in `src/Client/Region/` — centralises the region range-clipping logic that was duplicated across `RawKvScanner`, `RawKvRangeOps` and `Transaction`. All three call sites now delegate to the shared clipper, ensuring consistent half-open `[start, end)` semantics and empty-end-key = +infinity treatment. (#84)
- Unit tests for `RegionRangeClipper` covering forward/reverse clipping across multiple adjacent regions, empty end key (+infinity), keys at region split points, range outside region, and empty regions array. (#84)
- Unit tests for `RawKvScanner` multi-region scan boundary clipping, empty-end-key unbounded scan, three-region limit spanning, reverse-scan limit across regions, all-0xFF prefix to empty-end-key conversion, and non-aligned key-range clipping at region split points (#85)
- Exposed cluster ID from PD via `RawKvClient::getClusterId()` and `TxnKvClient::getClusterId()`, delegating to `PdClientInterface::getClusterId()` (#27)
- Atomic mode for Compare-And-Swap (CAS): CAS and Put-If-Absent now require `setAtomicForCAS(true)` to be called first, enabling the TiKV `for_cas` atomic code path. Disabled by default for performance. (#103)
- Dedicated CI lane for gRPC-dependent unit tests with the PHP gRPC extension loaded and skipped tests failing (#117)
- E2E tests for client lifecycle: Close then get/put/delete/scan/... throws ClientClosedException, double close is idempotent, transaction before close remains usable (#55)
- Removed `__construct()` from `GrpcClientInterface` — interfaces should not enforce constructor signatures; all consumers use DI or concrete class instantiation (#46)
- `RegionInfoMapper`, a shared proto→DTO mapper used by `PdClient::getRegion()` and `scanRegions()` so both RPCs share identical, explicit null-handling for a missing leader (#104)
- Unit tests for `RetryExecutor` budget exhaustion (total backoff, ServerBusy, max-attempts cap, wall-clock deadline), `BatchAsyncExecutor` dispatch-phase vs wait-phase failure aggregation, `TlsConfigBuilder` disallowed-extension rejection, and `TlsConfig::close()` key-zeroing (#100)

### Changed
- **`PdClient::close()` no longer closes the shared `GrpcClient`** — ownership of the gRPC connection pool belongs to the high-level client (`RawKvClient` / `TxnKvClient`), which already closes it. `PdClient::close()` now only clears its own resources: store cache, cluster ID, and TSO. This prevents double-close on the shared `GrpcClient`. (#108)
- **Upgraded `google/protobuf` from `^3.25` to `^4.33.6`** — fixes CVE-2026-6409 (HIGH), a DoS vulnerability through malicious protobuf messages containing negative varints or deep recursion. (#75)
- `composer.json` `config.audit.block-insecure` set to `true` — vulnerable dependencies now fail `composer audit` and block CI. (#75)
- **Moved shared region DTOs and helpers from `RawKv` to `Region` namespace**: `RegionInfo` and `PeerInfo` DTOs moved to `CrazyGoat\TiKV\Client\Region\Dto`, `RegionGrouper` and `RegionErrorHandler` moved to `CrazyGoat\TiKV\Client\Region`. All imports updated across the codebase. The old `RawKv\Dto\RegionInfo`, `RawKv\Dto\PeerInfo`, `RawKv\RegionGrouper`, and `RawKv\RegionErrorHandler` aliases are removed. (#111)
- Merged `commitOptimistic()` and `commitPessimistic()` into a single `doCommit()` method; removed dead `$firstRegionKeys`/`$isPrimaryRegion` that were never used (#113)
- **Error classification now uses typed `ErrorKind` enum instead of message-string matching.** Added `ErrorKind` enum covering all 21 `Errorpb\Error` oneof variants; `RegionException` auto-detects the kind from the proto `Error` message; `ErrorClassifier::classifyByKind()` is the single source of truth for the error→backoff mapping; `Transaction::classifyError()` uses `TxnRetryableException` carrying `BackoffType` directly; `PdClient::extractClusterIdFromError()` uses pure regex without `str_contains()` (#93)

### Fixed
- **Security**: TLS file path inputs are now canonicalized with `realpath()` and validated against an optional allowed base directory, preventing path traversal via `../` and symlink attacks. Exception messages no longer leak the resolved filesystem path. The legacy `withCaCert()` and `withClientCert()` methods that guess between file path and inline PEM via `file_exists()` are deprecated in favor of the new explicit `*File()` and `*Pem()` methods. (#87)
- Removed `instanceof PdClient` concrete-class checks from `TimestampOracle`; added `setClusterId()` to `PdClientInterface` so any implementation of the interface can propagate cluster-ID discovery (DIP/LSP compliance) (#110)
- Replaced bare `\RuntimeException` and `\LogicException` with `InvalidStateException` (extends `TiKvException`) in `RawKvClient::compareAndSwap()`, `Transaction::ensureActive()`, `Transaction::getPrimaryKey()`, and `GrpcClient::createTlsCredentials()`, so all library failures derive from `TiKvException` (#102)
- TxnKv RPCs now have per-operation deadlines via `TimeoutConfig`, threaded from `TxnKvClient::create()` `options['timeout']` into `Transaction`, preventing indefinite blocking on hung TiKV nodes (#79)
- Added `@throws` annotations to all public methods in `RawKvClient` and `Transaction`, documenting the typed exceptions they raise (`InvalidArgumentException`, `ClientClosedException`, `InvalidStateException`, `RegionException`, `GrpcException`, `BatchPartialFailureException`, `TransactionConflictException`, `DeadlockException`) (#102)
- `PdClient::getRegion()` no longer fabricates `regionId=0`/`leaderStoreId=1` when PD returns no region/leader — it now throws a `TiKvException` on a missing region and reports leader store id `0` ("unknown") for a missing leader, so `resolveStoreAddress()` raises a visible `StoreNotFoundException` instead of silently routing requests to a guessed store on split/merge (#104)
- `GrpcFuture` now exposes `cancel()` and `__destruct()` so abandoned pending gRPC calls are cancelled instead of leaking completion-queue/channel resources; `BatchAsyncExecutor` cancels remaining un-waited futures on the first wait-phase failure (#90)
- `splitPairsIntoBatches()` now normalizes `$pairs` and `$ttls` keys to sequential 0-indexed arrays, preventing silent TTL misalignment when pairs have non-sequential keys (#106)
- Removed dead runtime guard in `batchScan()` that was suppressed with `@phpstan-ignore`; the PHPDoc type `array<array{0: string, 1: string}>` already guarantees the contract, and malformed input now produces a `TypeError` instead of `InvalidArgumentException` (#112)
- `getKeyTTL()` now validates key (empty/oversized), consistent with `get()`, `put()`, `delete()` (#109)
- `deletePrefix()` now rejects prefixes consisting entirely of 0xFF bytes, which would previously delete to the end of the keyspace instead of just the intended range (#105)
- Replaced `array_merge` in scan loops with `array_push` to avoid O(n²) copying across regions (#97)
- `getPrimaryKey()` now uses `array_key_first()` instead of `array_keys()`, avoiding O(n) memory allocation on every call (#114)
- PHPStan analysis now sets an explicit memory limit of 1G, preventing crashes with the default 128M limit (#115)
- Raw keys are now automatically redacted in log context across `RegionCache`, `RetryExecutor`, `Transaction`, and `LockResolver` — added `KeyRedactor` utility that replaces raw keys with a hex prefix + length, preventing sensitive-data leakage into log aggregation. A custom redaction callable can be injected via `KeyRedactor::setRedactor()`. (#88)
- `get()`, `batchGet()`, and `Transaction::get()` now correctly distinguish empty-string values from missing keys by checking the response's `not_found` flag instead of treating empty values as null (#77)
- `batchPut()` with a scalar TTL now expands the TTL to one element per pair instead of sending a 1-element array for an N-key batch, ensuring every key receives the intended expiry (#78)
- `LockResolverTest` now executes (all 11 tests) instead of erroring on every method — replaced `createMock()` of generated protobuf messages with real `CheckTxnStatusResponse`/`ResolveLockResponse` instances constructed via setters, restoring coverage of the commit/rollback/wait/region-error decision matrix (#99)
- `close()` is now exception-safe across `GrpcClient`, `RawKvClient`, and `TxnKvClient` — a throw from one channel/sub-client no longer leaks the rest or leaves the client in a non-idempotent state (#107)
- `RetryExecutor::execute()` is now bounded by a configurable attempt cap (`maxAttempts`, default 30) and an optional wall-clock deadline (`deadlineMs`), independent of accumulated backoff. Previously, errors classified as `BackoffType::None` (e.g. `EpochNotMatch`) returned `sleepMs=0`, so `totalBackoffMs` never grew and the `while (true)` loop drove an infinite zero-sleep 100%-CPU busy loop. New exhaustion throws `RetryBudgetExhaustedException` (extends `TiKvException`) (#72)
- Corrected `CHANGELOG.md`, `README.md`, and `docs/configuration.md` to reference actual APIs, constants, and file paths instead of nonexistent ones (`GrpcClient::setTimeout()` → `TimeoutConfig`/`options['timeout']`, `MAX_RAW_SCAN_LIMIT` → `MAX_SCAN_LIMIT`, removed dead `RegionContext.php` and `docs/superpowers/` link, moved shipped "In Progress" features to "Recently Completed", clarified env var usage as application-level wiring) (#101)

## [0.2.0] - 2026-05-11

### Added
- TxnKV client with full ACID transaction support (begin/commit/rollback, snapshot reads, pessimistic locking)
- Lazy auto-paginating `ScanIterator` for RawKV scans — fetches next pages transparently on iteration
- Auto-split large batches by key count and total byte size — prevents oversized gRPC requests
- Configurable per-operation gRPC timeouts via `TimeoutConfig` / `options['timeout']`
- Scan limit guard (`MAX_SCAN_LIMIT = 10240`) — prevents accidental unbounded scans
- Per-key TTL support in `batchPut()` — accepts `int|array $ttl` for individual key expiration times
- E2E tests running against a real TiKV cluster in CI via GitHub Actions

### Removed
- `docs/superpowers/` — all implementation plans migrated to GitHub issues

## [0.1.0] - 2026-03-30

### Added
- Initial release of TiKV PHP Client
- Complete RawKV operations support:
  - Single-key operations: Get, Put, Delete
  - Batch operations: BatchGet, BatchPut, BatchDelete with parallel execution
  - Scanning: Scan, ScanPrefix, ReverseScan, BatchScan
  - Range operations: DeleteRange, DeletePrefix
  - TTL support: PutWithTTL, GetKeyTTL
  - Atomic operations: CompareAndSwap, PutIfAbsent
  - Data integrity: Checksum
- Production features:
  - TLS/SSL support (server verification and mTLS)
  - PSR-3 logging integration
  - Automatic retry with exponential backoff
  - Region and store caching
  - Connection pooling via persistent gRPC channels
- Comprehensive documentation:
  - Getting Started guide
  - Configuration reference
  - Operations guide
  - Advanced patterns
  - Troubleshooting guide
  - Contributing guide
  - Development guide
  - Architecture documentation
- Working examples for all major features
- Full test suite: 148 unit tests + 141 E2E tests
- CI/CD pipeline with GitHub Actions

### Infrastructure
- Branch protection rules requiring CI checks and approvals
- PHP 8.2, 8.3, 8.4 support

[0.2.0]: https://github.com/crazy-goat/tikv-php/releases/tag/v0.2.0
[0.1.0]: https://github.com/crazy-goat/tikv-php/releases/tag/v0.1.0
