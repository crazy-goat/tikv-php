# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Per-key TTL support in `batchPut()` — accepts `int|array $ttl` for individual key expiration times

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

[0.1.0]: https://github.com/crazy-goat/tikv-php/releases/tag/v0.1.0
