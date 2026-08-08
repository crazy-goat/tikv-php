# TiKV PHP Client Documentation

Complete documentation for the TiKV PHP Client library.

## Quick Navigation

### For Users

New to TiKV PHP Client? Start here:

- **[Getting Started](getting-started.md)** - Installation, setup, and your first TiKV operations
- **[Configuration](configuration.md)** - Client configuration options, TLS, timeouts, logging
- **[Operations](operations.md)** - Complete guide to all RawKV operations
- **[Advanced Features](advanced.md)** - Production-ready patterns and optimization

### For Developers

Contributing to the project or need deep technical details?

- **[Contributing Guide](contributing.md)** - How to contribute, development workflow, before your first commit
- **[Development Guide](development.md)** - Technical implementation details, adding features, testing strategies
- **[Architecture](architecture.md)** - System architecture, design decisions, component details
- **[Workflow](../workflow.md)** - Full issue→branch→implementation→review→PR→CI→merge workflow

### Reference

Quick reference and troubleshooting:

- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
- **[Implementation Plans](superpowers/plans/)** - Roadmap and feature plans
- **[Examples](../examples/)** - Working code examples (in repository)

## Overview

The TiKV PHP Client provides a high-performance, production-ready interface to TiKV's RawKV API. It supports:

- **All RawKV Operations**: Get, Put, Delete, Batch operations, Scanning, TTL, CAS
- **Production Features**: TLS, PSR-3 logging, automatic retries, region caching
- **High Performance**: Parallel batch execution, connection pooling, efficient region routing
- **Type Safety**: Full PHP 8.2+ type support with generics

## Quick Example

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

// Connect to TiKV
$client = RawKvClient::create(['127.0.0.1:2379']);

// Basic operations
$client->put('key', 'value');
$value = $client->get('key');

// Batch operations
$client->batchPut(['k1' => 'v1', 'k2' => 'v2']);
$values = $client->batchGet(['k1', 'k2']);

// Scanning
$results = $client->scanPrefix('user:');

// Cleanup
$client->close();
```

## Requirements

- PHP >= 8.2
- gRPC PHP extension
- TiKV cluster with RawKV enabled

## Installation

```bash
composer require crazy-goat/tikv-client
```

## Support

- **Issues**: [GitHub Issues](https://github.com/crazy-goat/tikv-php/issues)
- **Documentation**: Browse this documentation
- **Examples**: See `examples/` directory in the repository

## Contributing

We welcome contributions! See:
- [Contributing Guide](contributing.md) - Getting started as a contributor
- [Development Guide](development.md) - Technical details for developers
- [Architecture](architecture.md) - Understanding the system

## License

MIT License - see [LICENSE](../LICENSE) file for details.
