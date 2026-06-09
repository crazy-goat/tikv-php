# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Dedicated CI lane for gRPC-dependent unit tests with the PHP gRPC extension loaded and skipped tests failing (#117)
- E2E tests for client lifecycle: Close then get/put/delete/scan/... throws ClientClosedException, double close is idempotent, transaction before close remains usable (#55)
- Removed `__construct()` from `GrpcClientInterface` — interfaces should not enforce constructor signatures; all consumers use DI or concrete class instantiation (#46)

### Changed
- Merged `commitOptimistic()` and `commitPessimistic()` into a single `doCommit()` method; removed dead `$firstRegionKeys`/`$isPrimaryRegion` that were never used (#113)

### Fixed
- `splitPairsIntoBatches()` now normalizes `$pairs` and `$ttls` keys to sequential 0-indexed arrays, preventing silent TTL misalignment when pairs have non-sequential keys (#106)
- Removed dead runtime guard in `batchScan()` that was suppressed with `@phpstan-ignore`; the PHPDoc type `array<array{0: string, 1: string}>` already guarantees the contract, and malformed input now produces a `TypeError` instead of `InvalidArgumentException` (#112)
- `getKeyTTL()` now validates key (empty/oversized), consistent with `get()`, `put()`, `delete()` (#109)
- `deletePrefix()` now rejects prefixes consisting entirely of 0xFF bytes, which would previously delete to the end of the keyspace instead of just the intended range (#105)
- Replaced `array_merge` in scan loops with `array_push` to avoid O(n²) copying across regions (#97)
- `getPrimaryKey()` now uses `array_key_first()` instead of `array_keys()`, avoiding O(n) memory allocation on every call (#114)
- PHPStan analysis now sets an explicit memory limit of 1G, preventing crashes with the default 128M limit (#115)
- `get()`, `batchGet()`, and `Transaction::get()` now correctly distinguish empty-string values from missing keys by checking the response's `not_found` flag instead of treating empty values as null (#77)

## [0.2.0] - 2026-05-11

### Added
- TxnKV client with full ACID transaction support (begin/commit/rollback, snapshot reads, pessimistic locking)
- Lazy auto-paginating `ScanIterator` for RawKV scans — fetches next pages transparently on iteration
- Auto-split large batches by key count and total byte size — prevents oversized gRPC requests
- Configurable per-operation gRPC timeouts via `GrpcClient::setTimeout()`
- Scan limit guard (`MAX_RAW_SCAN_LIMIT = 10240`) — prevents accidental unbounded scans
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
