# Getting Started with TiKV PHP Client

This guide will help you get up and running with the TiKV PHP Client in minutes.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Setting Up TiKV](#setting-up-tikv)
4. [Your First TiKV Application](#your-first-tikv-application)
5. [Next Steps](#next-steps)

## Prerequisites

Before you begin, ensure you have:

- **PHP >= 8.2** installed
- **gRPC PHP extension** enabled
- **Composer** for dependency management
- **Docker** (optional, for running TiKV locally)

### Checking PHP Version

```bash
php --version
# Should show PHP 8.2 or higher
```

### Checking gRPC Extension

```bash
php -m | grep grpc
# Should output: grpc
```

If gRPC is not installed, install it:

```bash
# Ubuntu/Debian
sudo apt-get install php-grpc

# Or using PECL
pecl install grpc
```

Then enable it in your `php.ini`:

```ini
extension=grpc.so
```

## Installation

Install the TiKV PHP Client via Composer:

```bash
composer require crazy-goat/tikv-client
```

This will install the client and its dependencies:
- `grpc/grpc` - gRPC client library
- `google/protobuf` - Protocol Buffers
- `psr/log` - PSR-3 logging interface

## Setting Up TiKV

### Option 1: Using Docker (Recommended for Development)

The repository includes a Docker Compose setup for easy local development:

```bash
# Clone the repository
git clone https://github.com/crazy-goat/tikv-php.git
cd tikv-php

# Start TiKV cluster
make up

# Verify cluster is running
make logs
```

This starts a 3-node TiKV cluster with PD (Placement Driver).

### Option 2: Using Existing TiKV Cluster

If you already have a TiKV cluster, note down the PD endpoints (usually port 2379):

```php
$pdEndpoints = ['192.168.1.100:2379', '192.168.1.101:2379'];
```

### Option 3: Manual TiKV Setup

See the [TiKV documentation](https://tikv.org/docs/) for manual installation instructions.

## Your First TiKV Application

Create a file named `first-app.php`:

```php
<?php
require 'vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

// Connect to TiKV
$pdEndpoints = ['127.0.0.1:2379'];  // Adjust if using different address
$client = RawKvClient::create($pdEndpoints);

try {
    echo "Connected to TiKV!\n";
    
    // Store some data
    $client->put('hello', 'world');
    echo "Stored: hello => world\n";
    
    // Retrieve the data
    $value = $client->get('hello');
    echo "Retrieved: hello => $value\n";
    
    // Store multiple values
    $client->batchPut([
        'user:1' => 'Alice',
        'user:2' => 'Bob',
        'user:3' => 'Charlie',
    ]);
    echo "Stored 3 users\n";
    
    // Retrieve multiple values
    $users = $client->batchGet(['user:1', 'user:2', 'user:3']);
    echo "Retrieved users:\n";
    foreach ($users as $key => $value) {
        echo "  $key => $value\n";
    }
    
    // Scan all users
    $allUsers = $client->scanPrefix('user:');
    echo "Found " . count($allUsers) . " users by scanning\n";
    
    // Cleanup
    $client->delete('hello');
    $client->batchDelete(['user:1', 'user:2', 'user:3']);
    echo "Cleanup complete\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    // Always close the client
    $client->close();
    echo "Disconnected\n";
}
```

Run your application:

```bash
php first-app.php
```

Expected output:

```
Connected to TiKV!
Stored: hello => world
Retrieved: hello => world
Stored 3 users
Retrieved users:
  user:1 => Alice
  user:2 => Bob
  user:3 => Charlie
Found 3 users by scanning
Cleanup complete
Disconnected
```

## Understanding the Basics

### Connection

```php
$client = RawKvClient::create(['127.0.0.1:2379']);
```

- The client connects to PD (Placement Driver), not directly to TiKV nodes
- PD provides cluster topology and region routing information
- The client automatically discovers TiKV nodes from PD

### Key-Value Operations

```php
// Single key operations
$client->put('key', 'value');           // Store
$value = $client->get('key');           // Retrieve (returns null if not found)
$client->delete('key');                  // Delete

// Batch operations (more efficient for multiple keys)
$client->batchPut(['k1' => 'v1', 'k2' => 'v2']);
$values = $client->batchGet(['k1', 'k2']);  // Returns array: ['k1' => 'v1', 'k2' => 'v2']
$client->batchDelete(['k1', 'k2']);
```

### Scanning

```php
// Range scan [startKey, endKey)
$results = $client->scan('user:a', 'user:z');

// Prefix scan (convenience method)
$results = $client->scanPrefix('user:');

// Results format:
// [
//   ['key' => 'user:1', 'value' => 'Alice'],
//   ['key' => 'user:2', 'value' => 'Bob'],
//   ...
// ]
```

### Resource Cleanup

Always close the client when done:

```php
$client->close();
```

This closes gRPC connections and releases resources.

## Transactional Operations (TxnKV)

The client also supports ACID transactions via the TxnKV API. This is useful when you need strong consistency guarantees.

### Creating a Transaction

```php
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

$txnClient = TxnKvClient::create(['127.0.0.1:2379']);

try {
    // Pessimistic transaction (default) — acquires locks on write
    $txn = $txnClient->begin(['pessimistic' => true]);
    
    // Optimistic transaction — locks only on commit
    // $txn = $txnClient->begin(['pessimistic' => false]);
    
    // ... use the transaction ...
    
} finally {
    $txnClient->close();
}
```

### Basic Transaction Operations

```php
// Start a transaction
$txn = $txnClient->begin();

// Read (snapshot at startTs)
$value = $txn->get('account:1');

// Write (buffered until commit)
$txn->set('account:1', '1000');
$txn->delete('account:2');

// Batch read
$values = $txn->batchGet(['account:1', 'account:2']);

// Scan within transaction
$results = $txn->scan('account:', 'account;');

// Commit (2-phase: prewrite + commit)
$txn->commit();

// Or rollback if something went wrong
// $txn->rollback();
```

### Transfer Example (ACID Guarantee)

```php
$txn = $txnClient->begin(['pessimistic' => true]);

try {
    $fromBalance = (int) $txn->get('account:alice');
    $toBalance = (int) $txn->get('account:bob');
    
    $amount = 100;
    if ($fromBalance < $amount) {
        throw new RuntimeException('Insufficient funds');
    }
    
    $txn->set('account:alice', (string) ($fromBalance - $amount));
    $txn->set('account:bob', (string) ($toBalance + $amount));
    
    $txn->commit();
    echo "Transfer committed!\n";
    
} catch (Exception $e) {
    $txn->rollback();
    echo "Transfer rolled back: " . $e->getMessage() . "\n";
}
```

### Pessimistic vs Optimistic

| Feature | Pessimistic | Optimistic |
|---------|-----------|------------|
| Lock timing | On write (`set`/`delete`) | On commit (prewrite) |
| Conflicts | Detected early | Detected at commit |
| Best for | High contention | Low contention |
| Default lock TTL | 30 seconds | 3 seconds |
| Use case | Financial transfers | Caching, config |

## Next Steps

Now that you have the basics working, explore more features:

### Learn More Operations

- **[Operations Guide](operations.md)** - Complete guide to all RawKV operations
  - TTL (Time-To-Live) for automatic expiration
  - Atomic operations (Compare-And-Swap)
  - Range deletions
  - Reverse scanning

### Production Configuration

- **[Configuration Guide](configuration.md)** - Configure for production
  - TLS/SSL encryption
  - PSR-3 logging
  - Timeout settings
  - Retry policies

### Advanced Topics

- **[Advanced Features](advanced.md)** - Production-ready patterns
  - Connection pooling
  - Region caching
  - Batch optimization
  - Error handling

### Understanding the Architecture

- **[Architecture](architecture.md)** - How the client works
  - PD discovery and region routing
  - gRPC communication
  - Retry mechanisms
  - Performance considerations

## Common Issues

### Connection Refused

If you get "Connection refused" errors:

1. Check if TiKV is running: `docker-compose ps`
2. Verify PD endpoint is correct
3. Check firewall rules

### gRPC Extension Not Found

If you see "Class 'Grpc\Channel' not found":

1. Install gRPC extension: `pecl install grpc`
2. Enable in php.ini: `extension=grpc.so`
3. Restart web server/PHP-FPM

### TTL Not Working

If TTL operations fail:

1. Check TiKV configuration: `enable-ttl` must be `true` in tikv.toml
2. Restart TiKV after configuration change

See [Troubleshooting](troubleshooting.md) for more solutions.

## Examples

The repository includes complete examples in the `examples/` directory:

```bash
# Basic operations
php examples/basic.php

# Batch operations
php examples/batch.php

# Scanning
php examples/scan.php

# TTL
php examples/ttl.php

# Atomic operations
php examples/atomic.php

# TLS
php examples/tls.php

# Logging
php examples/logging.php
```

## Getting Help

- **Documentation**: Browse the [docs/](.) directory
- **Issues**: [GitHub Issues](https://github.com/crazy-goat/tikv-php/issues)
- **Examples**: Check the `examples/` directory

## Summary

You've learned:
- ✅ How to install the TiKV PHP Client
- ✅ How to connect to a TiKV cluster
- ✅ Basic CRUD operations (RawKV)
- ✅ Batch operations for efficiency
- ✅ Scanning for data retrieval
- ✅ ACID transactions (TxnKV)
- ✅ Pessimistic vs optimistic transaction modes
- ✅ Proper resource cleanup

Ready to dive deeper? Continue with the [Operations Guide](operations.md)!
