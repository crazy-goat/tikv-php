# Troubleshooting Guide

Common issues and solutions for the TiKV PHP Client.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Connection Issues](#connection-issues)
3. [Runtime Errors](#runtime-errors)
4. [Performance Issues](#performance-issues)
5. [Data Issues](#data-issues)
6. [Debugging Techniques](#debugging-techniques)

## Installation Issues

### gRPC Extension Not Found

**Error:**
```
Fatal error: Uncaught Error: Class 'Grpc\Channel' not found
```

**Solution:**

1. Check if gRPC is installed:
   ```bash
   php -m | grep grpc
   ```

2. Install gRPC extension:
   ```bash
   # Using PECL
   pecl install grpc
   
   # Ubuntu/Debian
   sudo apt-get install php-grpc
   
   # macOS with Homebrew
   brew install php@8.2-grpc
   ```

3. Enable in php.ini:
   ```ini
   extension=grpc.so
   # or
   extension=grpc.dll  # Windows
   ```

4. Restart web server/PHP-FPM

**Verification:**
```bash
php -r "echo class_exists('Grpc\Channel') ? 'OK' : 'FAIL';"
```

### Composer Dependencies Fail

**Error:**
```
Your requirements could not be resolved to an installable set of packages.
```

**Solutions:**

1. Update Composer:
   ```bash
   composer self-update
   ```

2. Clear Composer cache:
   ```bash
   composer clear-cache
   rm -rf vendor/ composer.lock
   composer install
   ```

3. Check PHP version:
   ```bash
   php --version  # Must be >= 8.2
   ```

4. Check platform requirements:
   ```bash
   composer check-platform-reqs
   ```

### Protobuf Extension Issues

**Error:**
```
Class 'Google\Protobuf\Internal\Message' not found
```

**Solution:**

```bash
# Install protobuf extension
pecl install protobuf

# Add to php.ini
extension=protobuf.so
```

Or use the pure-PHP implementation (slower but no extension needed):
```bash
composer require google/protobuf --prefer-dist
```

## Connection Issues

### Connection Refused

**Error:**
```
GrpcException: Connection refused
```

**Causes & Solutions:**

1. **TiKV not running:**
   ```bash
   # Check if TiKV is up
   make up
   
   # Or check Docker
   docker-compose ps
   ```

2. **Wrong endpoint:**
   ```php
   // Wrong - connecting to TiKV directly
   $client = RawKvClient::create(['127.0.0.1:20160']);
   
   // Correct - connect to PD
   $client = RawKvClient::create(['127.0.0.1:2379']);
   ```

3. **Firewall blocking:**
   ```bash
   # Check if port is open
   telnet 127.0.0.1 2379
   
   # Or using nc
   nc -zv 127.0.0.1 2379
   ```

4. **TiKV not ready yet:**
   ```bash
   # Wait for cluster to be healthy
   sleep 10
   
   # Check logs
   make logs
   ```

### TLS Connection Failures

**Error:**
```
GrpcException: Handshake failed
```

**Solutions:**

1. **Certificate not found:**
   ```php
   // Check if paths are correct
   $options = [
       'tls' => [
           'caCertFile' => '/absolute/path/to/ca.crt',
       ],
   ];
   ```

2. **Certificate permissions:**
   ```bash
   # Fix permissions
   chmod 600 /path/to/client.key
   chmod 644 /path/to/client.crt
   ```

3. **Wrong certificate format:**
   ```bash
   # Verify certificate
   openssl x509 -in ca.crt -text -noout
   ```

4. **Hostname mismatch:**
   ```php
   // Use IP if certificate doesn't have hostname
   $client = RawKvClient::create(['192.168.1.100:2379'], options: $options);
   ```

### Timeout Issues

**Error:**
```
GrpcException: Deadline exceeded
```

**Solutions:**

1. **Network latency:**
   ```bash
   # Check latency
   ping tikv-host
   ```

2. **TiKV overloaded:**
   ```bash
   # Check TiKV metrics
   curl http://tikv-host:20180/metrics
   ```

3. **Large requests:**
   ```php
   // Split large batches
   $chunks = array_chunk($keys, 100);
   foreach ($chunks as $chunk) {
       $client->batchGet($chunk);
   }
   ```

## Runtime Errors

### Region Errors

#### EpochNotMatch

**Error:**
```
RegionException: EpochNotMatch
```

**What it means:** Region metadata is stale (region was split/merged).

**Solution:**
- Client automatically retries
- If persistent, check TiKV cluster health:
  ```bash
  # Check PD logs
  docker-compose logs pd
  ```

#### NotLeader

**Error:**
```
RegionException: NotLeader
```

**What it means:** The TiKV node we tried is not the leader for this region.

**Solution:**
- Client automatically retries with new leader info
- If persistent, check region health:
  ```bash
  # Check TiKV logs
  docker-compose logs tikv1
  ```

#### RegionNotFound

**Error:**
```
RegionException: RegionNotFound
```

**What it means:** The region doesn't exist (possibly dropped).

**Solution:**
- Client retries with cache invalidation
- If persistent, cluster may be unhealthy

### Key Errors

#### KeyNotInRegion

**Error:**
```
RegionException: KeyNotInRegion
```

**What it means:** Key is outside the region's range (shouldn't happen with normal use).

**Solution:**
- Check if key is valid
- Clear region cache:
  ```php
  // Force cache refresh by creating new client
  $client->close();
  $client = RawKvClient::create($pdEndpoints);
  ```

### TTL Errors

#### TTL Not Supported

**Error:**
```
RegionException: TTL not enabled
```

**What it means:** TiKV wasn't started with `enable-ttl=true`.

**Solution:**

1. Update tikv.toml:
   ```toml
   [storage]
   enable-ttl = true
   ```

2. Restart TiKV:
   ```bash
   make down
   make up
   ```

3. Verify:
   ```php
   $client->put('test', 'value', ttl: 60);
   $ttl = $client->getKeyTTL('test');
   echo "TTL: $ttl";  // Should show seconds
   ```

### Client Errors

#### Client Closed

**Error:**
```
ClientClosedException: Client is closed
```

**What it means:** Trying to use client after calling `close()`.

**Solution:**
```php
// Don't use after close
$client->close();
// $client->get('key');  // ERROR!

// Create new client if needed
$client = RawKvClient::create($pdEndpoints);
```

#### Invalid Arguments

**Error:**
```
InvalidArgumentException: ...
```

**Common causes:**

1. **Empty prefix in deletePrefix:**
   ```php
   // Wrong
   $client->deletePrefix('');
   
   // Correct
   $client->deletePrefix('temp:');
   ```

2. **Invalid batch limit:**
   ```php
   // Wrong
   $client->batchScan($ranges, eachLimit: 0);
   
   // Correct
   $client->batchScan($ranges, eachLimit: 100);
   ```

## Performance Issues

### Slow Operations

**Symptom:** Operations taking >1 second

**Diagnosis:**

```php
// Add timing
$start = microtime(true);
$result = $client->batchGet($keys);
$elapsed = microtime(true) - $start;
echo "Took: {$elapsed}s\n";
```

**Solutions:**

1. **Enable logging to see retries:**
   ```php
   use Monolog\Logger;
   
   $logger = new Logger('debug');
   $logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));
   $client = RawKvClient::create($pdEndpoints, logger: $logger);
   ```

2. **Check for retries:**
   - If seeing many retries, TiKV may be overloaded
   - Check TiKV metrics: `curl http://tikv:20180/metrics`

3. **Batch size too large:**
   ```php
   // Reduce batch size
   $chunks = array_chunk($keys, 100);
   ```

4. **Hot spot (all keys in one region):**
   ```php
   // Bad: Sequential keys
   for ($i = 0; $i < 10000; $i++) {
       $client->put("key:$i", $value);  // Same region!
   }
   
   // Good: Distributed keys
   for ($i = 0; $i < 10000; $i++) {
       $hash = md5($i)[0:2];
       $client->put("key:$hash:$i", $value);  // Distributed
   }
   ```

### High Memory Usage

**Symptom:** PHP memory limit exceeded

**Solutions:**

1. **Large scan results:**
   ```php
   // Paginate instead of loading all
   $start = 'user:';
   while (true) {
       $batch = $client->scan($start, 'user;', limit: 1000);
       if (empty($batch)) break;
       
       processBatch($batch);
       
       $start = $batch[count($batch) - 1]['key'] . "\x00";
       unset($batch);
       gc_collect_cycles();
   }
   ```

2. **Large values:**
   ```php
   // Check value sizes
   $value = $client->get('large-key');
   if (strlen($value) > 10 * 1024 * 1024) {
       // Value > 10MB, consider chunking
   }
   ```

### Connection Pool Exhaustion

**Symptom:** "Too many open files" or connection errors

**Solutions:**

1. **Close clients properly:**
   ```php
   try {
       $client = RawKvClient::create($pdEndpoints);
       // ... use client ...
   } finally {
       $client->close();  // Always close!
   }
   ```

2. **Limit concurrent clients:**
   ```php
   // Don't create many clients
   $client = RawKvClient::create($pdEndpoints);
   
   foreach ($workloads as $work) {
       // Reuse same client
       process($client, $work);
   }
   
   $client->close();
   ```

## Data Issues

### Data Not Found

**Symptom:** `get()` returns null when data should exist

**Checklist:**

1. **Key mismatch:**
   ```php
   // Check exact key
   $key = 'user:123';
   echo "Looking for: '$key'\n";
   
   // Scan to see what exists
   $results = $client->scanPrefix('user:');
   print_r($results);
   ```

2. **TTL expiration:**
   ```php
   $ttl = $client->getKeyTTL('key');
   if ($ttl === null) {
       echo "Key expired or has no TTL\n";
   }
   ```

3. **Wrong cluster:**
   ```php
   // Verify you're connecting to right cluster
   echo "PD: " . implode(', ', $pdEndpoints) . "\n";
   ```

### Data Corruption

**Symptom:** Values don't match what was stored

**Solutions:**

1. **Encoding issues:**
   ```php
   // Always use consistent encoding
   $data = ['name' => 'Alice'];
   $client->put('key', json_encode($data));
   
   $value = $client->get('key');
   $data = json_decode($value, true);  // Not unserialize!
   ```

2. **Verify with checksum:**
   ```php
   $checksum = $client->checksum('data:', 'data;');
   echo "Keys: {$checksum->totalKvs}, Bytes: {$checksum->totalBytes}\n";
   ```

### Concurrent Modification

**Symptom:** CAS operations failing frequently

**Solutions:**

1. **Add backoff:**
   ```php
   $maxRetries = 10;
   for ($i = 0; $i < $maxRetries; $i++) {
       $current = $client->get('counter') ?? '0';
       $result = $client->compareAndSwap('counter', $current, (string)((int)$current + 1));
       
       if ($result->swapped) {
           break;
       }
       
       usleep(1000 * (2 ** $i));  // Exponential backoff
   }
   ```

2. **Use PutIfAbsent for locks:**
   ```php
   $existing = $client->putIfAbsent('lock:resource', 'owner-123', ttl: 30);
   if ($existing === null) {
       // Acquired lock
   }
   ```

## Debugging Techniques

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

$client = RawKvClient::create($pdEndpoints, logger: $logger);
```

### Check Cluster Health

```php
// Simple health check
function checkHealth($client): bool
{
    try {
        $testKey = 'health:' . uniqid();
        $client->put($testKey, 'ok');
        $value = $client->get($testKey);
        $client->delete($testKey);
        return $value === 'ok';
    } catch (Exception $e) {
        return false;
    }
}

if (!checkHealth($client)) {
    echo "Cluster unhealthy!\n";
}
```

### Monitor Retries

```php
// Wrap operations to count retries
class RetryMonitor
{
    private int $retryCount = 0;
    
    public function execute(callable $operation)
    {
        $start = microtime(true);
        
        try {
            return $operation();
        } catch (TiKvException $e) {
            $this->retryCount++;
            throw $e;
        } finally {
            $elapsed = microtime(true) - $start;
            if ($elapsed > 1.0) {
                echo "Slow operation: {$elapsed}s\n";
            }
        }
    }
}
```

### gRPC Debugging

```bash
# Enable gRPC tracing
export GRPC_VERBOSITY=DEBUG
export GRPC_TRACE=all
php your-script.php 2>&1 | head -100
```

### Network Analysis

```bash
# Capture traffic
sudo tcpdump -i lo -w tikv.pcap port 2379 or port 20160

# Analyze with Wireshark
# Filter: grpc or protobuf
```

### PHP Debugging

```php
// Check loaded extensions
var_dump(get_loaded_extensions());

// Check gRPC version
echo Grpc\VERSION;

// Memory usage
echo "Memory: " . memory_get_usage(true) / 1024 / 1024 . " MB\n";

// Peak memory
echo "Peak: " . memory_get_peak_usage(true) / 1024 / 1024 . " MB\n";
```

### Common Debug Script

```php
<?php
require 'vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Setup
$logger = new Logger('debug');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

$pdEndpoints = ['127.0.0.1:2379'];

echo "=== TiKV Debug Script ===\n\n";

// 1. Connection test
echo "1. Testing connection...\n";
try {
    $client = RawKvClient::create($pdEndpoints, logger: $logger);
    echo "   ✓ Connected\n";
} catch (Exception $e) {
    echo "   ✗ Failed: {$e->getMessage()}\n";
    exit(1);
}

// 2. Basic operations
echo "\n2. Testing basic operations...\n";
$testKey = 'debug:test:' . uniqid();
try {
    $client->put($testKey, 'value1');
    echo "   ✓ Put\n";
    
    $value = $client->get($testKey);
    echo "   ✓ Get: $value\n";
    
    $client->delete($testKey);
    echo "   ✓ Delete\n";
} catch (Exception $e) {
    echo "   ✗ Error: {$e->getMessage()}\n";
}

// 3. TTL test
echo "\n3. Testing TTL...\n";
$ttlKey = 'debug:ttl:' . uniqid();
try {
    $client->put($ttlKey, 'value', ttl: 60);
    $ttl = $client->getKeyTTL($ttlKey);
    echo "   ✓ TTL: $ttl seconds\n";
    $client->delete($ttlKey);
} catch (Exception $e) {
    echo "   ✗ TTL not supported or error: {$e->getMessage()}\n";
}

// 4. Batch operations
echo "\n4. Testing batch operations...\n";
try {
    $keys = [];
    for ($i = 0; $i < 10; $i++) {
        $keys["debug:batch:$i"] = "value-$i";
    }
    
    $client->batchPut($keys);
    echo "   ✓ BatchPut (10 keys)\n";
    
    $values = $client->batchGet(array_keys($keys));
    echo "   ✓ BatchGet: " . count(array_filter($values)) . " values\n";
    
    $client->batchDelete(array_keys($keys));
    echo "   ✓ BatchDelete\n";
} catch (Exception $e) {
    echo "   ✗ Error: {$e->getMessage()}\n";
}

// 5. Scan
echo "\n5. Testing scan...\n";
$scanKey = 'debug:scan:' . uniqid();
try {
    $client->put($scanKey, 'scan-value');
    $results = $client->scanPrefix('debug:scan:');
    echo "   ✓ Scan found " . count($results) . " keys\n";
    $client->delete($scanKey);
} catch (Exception $e) {
    echo "   ✗ Error: {$e->getMessage()}\n";
}

// Cleanup
$client->close();

echo "\n=== Debug Complete ===\n";
```

## Getting More Help

If issues persist:

1. **Check documentation:**
   - [Getting Started](getting-started.md)
   - [Configuration](configuration.md)
   - [Operations](operations.md)

2. **Check examples:**
   ```bash
   php examples/basic.php
   ```

3. **Run test suite:**
   ```bash
   make test
   ```

4. **Enable debug logging** (see above)

5. **Create an issue** with:
   - Error message
   - PHP version
   - TiKV version
   - Minimal reproduction code
   - Debug logs

## Quick Reference

| Issue | Quick Fix |
|-------|-----------|
| Connection refused | `make up` to start TiKV |
| gRPC not found | `pecl install grpc` |
| TLS errors | Check certificate paths |
| Slow operations | Enable logging, check retries |
| Memory issues | Paginate scans, reduce batch size |
| TTL not working | Enable `enable-ttl` in TiKV config |
| Data not found | Check key format, TTL, cluster |

## See Also

- [Getting Started](getting-started.md) - Basic setup
- [Configuration](configuration.md) - Configuration options
- [Advanced Features](advanced.md) - Production patterns
- [Architecture](architecture.md) - System design
