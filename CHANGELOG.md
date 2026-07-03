# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
- **Moved shared region DTOs and helpers from `RawKv` to `Region` namespace**: `RegionInfo` and `PeerInfo` DTOs moved to `CrazyGoat\TiKV\Client\Region\Dto`, `RegionGrouper` and `RegionErrorHandler` moved to `CrazyGoat\TiKV\Client\Region`. All imports updated across the codebase. The old `RawKv\Dto\RegionInfo`, `RawKv\Dto\PeerInfo`, `RawKv\RegionGrouper`, and `RawKv\RegionErrorHandler` aliases are removed. (#111)
- Merged `commitOptimistic()` and `commitPessimistic()` into a single `doCommit()` method; removed dead `$firstRegionKeys`/`$isPrimaryRegion` that were never used (#113)
- **Error classification now uses typed `ErrorKind` enum instead of message-string matching.** Added `ErrorKind` enum covering all 21 `Errorpb\Error` oneof variants; `RegionException` auto-detects the kind from the proto `Error` message; `ErrorClassifier::classifyByKind()` is the single source of truth for the error→backoff mapping; `Transaction::classifyError()` uses `TxnRetryableException` carrying `BackoffType` directly; `PdClient::extractClusterIdFromError()` uses pure regex without `str_contains()` (#93)
- **Security**: Plaintext gRPC is now fail-closed by default. Added explicit `insecure` option (boolean, default `false`). The client throws `InvalidStateException` when neither TLS nor `insecure => true` is configured. `TlsConfig::isEnabled()` now returns `true` if any TLS material is present (not just CA cert). `TlsConfigBuilder::build()` validates that client cert/key require a CA certificate. (#80)

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
