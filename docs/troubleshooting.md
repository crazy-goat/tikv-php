# Troubleshooting Guide

Common issues and solutions for the TiKV PHP Client.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Connection Issues](#connection-issues)
3. [Runtime Errors](#runtime-errors)
    - [Region Errors](#region-errors)
    - [TTL Errors](#ttl-errors)
    - [Transaction Failures](#transaction-failures)
    - [Client Errors](#client-errors)
4. [Performance Issues](#performance-issues)
5. [Data Issues](#data-issues)
6. [Debugging Techniques](#debugging-techniques)

For the full exception hierarchy, per-operation exception lists and the
recommended catch order, see the [Error Handling Reference](error-handling.md).

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

#### ServerIsBusy

**Error:**
```
RegionException: ServerIsBusy
```

**What it means:** The TiKV node asked to throttle this request — it is
overloaded (raft too busy, scheduler congested).

**Solution:** The client retries internally with backoff, charging a
separate ServerBusy budget (60000 ms default). If you see
`RetryBudgetExhaustedException` wrapping this repeatedly, shed load or
scale the cluster. See
[Retry Budgets Behind Every Operation](error-handling.md#retry-budgets-behind-every-operation).

#### Other RegionException Messages

The message template is `<operation> failed: <server error>`, e.g.
`RawGetKeyTTL failed: ...`, `RegionError failed: ...`. The server-side part
is passed through verbatim; there are more kinds than listed above.

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

### gRPC Transport Errors

#### GrpcException

**Error:**
```
GrpcException: gRPC error: <details>
```

**What it means:** A gRPC status other than `OK` came back from the call
(connection refused, deadline exceeded, unavailable, …); `$details` is the
transport-layer string and `$e->grpcStatusCode` carries the numeric status.

**Likely cause:** TiKV/PD down or restarting, network partition, per-call
deadline too small.

**Solution:** Same as [Connection Refused](#connection-refused) /
[Timeout Issues](#timeout-issues). Transport errors were already retried
internally with backoff — retry yourself only if your operation is
idempotent.

#### Invalid Store Address

**Error:**
```
InvalidStoreAddressException: PD returned malformed store address "<address>" for store <storeId> (expected host:port, port 1-65535)
```
(two more variants exist for reserved gRPC/URI scheme hosts and addresses
outside the configured allowed set)

**What it means:** PD returned a store address that failed validation
before being used as a gRPC channel target — not a bare `host:port`, a
reserved scheme name (`unix:`/`dns:`/…), an out-of-range port, or outside
`options['allowedStoreHosts']` / the host policy (issue #306).

**Solution:** This is classified fatal *before any retry* — retrying cannot
succeed without a configuration change. Check what PD is advertising
(`curl http://<pd-host>:2379/pd/api/v1/stores`) and your
`allowedStoreHosts`/`allowedStorePorts`/`storeHostPolicy` options.

### Base Exception

#### TiKvException

**Error:**
```
TiKvException: PD GetRegion returned no region for key
TiKvException: TSO request failed: gRPC error: ...
TiKvException: TSO response missing timestamp
TiKvException: Heartbeat failed: key error
```

**What it means:** Failures raised directly as the base class instead of a
subclass: PD answered a region lookup with no region (fail-closed routing),
the timestamp oracle could not allocate a PD timestamp, or a transactional
heartbeat hit a key error.

**Solution:** For TSO failures check PD health first — nothing in the
client can proceed without timestamps. These surface only when the specific
subclass does not apply; catch them via the general arm of your catch
chain ([Recommended Catch Order](error-handling.md#recommended-catch-order)).

### TTL Errors

#### TTL Not Enabled

**Error:**
```
RegionException: BatchRequest failed: ttl is not enabled, but get put request with ttl
```
(`getKeyTTL()` fails differently: the TiKV raw store refuses the call outright —
`GrpcException: gRPC error: get ttl on non-ttl store`)

**What it means:** TiKV was started without `enable-ttl = true`. A
`put(..., ttl: N)` with `N > 0` is rejected by the server with the literal
string `ttl is not enabled, but get put request with ttl`; the client
wraps every top-level `error` field as `<operation> failed: <message>`
(`RegionErrorHandler::check()`), hence the `BatchRequest failed:` prefix.
`getKeyTTL()` never works against a non-TTL cluster. Writes *without* a
TTL keep succeeding, which is why this often shows up as "my keys never
expire" instead of an exception.

**Solution:**

1. Enable TTL in tikv.toml:
   ```toml
   [storage]
   enable-ttl = true
   ```
   Note: switching `enable-ttl` requires wiping existing data — TiKV
   refuses to start a TTL-enabled node on data written without it.

2. Restart TiKV with a wiped data directory:
   ```bash
   make down
   make up   # repo docker-compose mounts tikv.toml with enable-ttl = true
   ```

3. Verify:
   ```php
   $client->put('test', 'value', ttl: 60);
   $ttl = $client->getKeyTTL('test');
   echo "TTL: $ttl";  // Should show seconds
   ```

> **Note:** The repo ships both configs: `tikv.toml` (with
> `enable-ttl = true`) and `tikv-v1.toml` (V1 mode, no TTL — used for TxnKV
> E2E tests via the `docker-compose.txnkv.yml` override). If you booted the
> V1 override, TTL operations behave exactly as described above.

### Transaction Failures

These are raised by the transactional (`TxnKv`) API during prewrite, commit
and pessimistic locking — the errors a transactional workload hits first and
most often. All of them mean **the transaction did not apply its writes**;
retry with a *new* transaction (fresh begin, re-read all values) rather than
replaying into the stale snapshot. See
[Transaction Operations](error-handling.md#transaction-operations) and
[Retrying transactions safely](error-handling.md#retrying-transactions-safely).

#### Write Conflict

**Error:**
```
TransactionConflictException: Write conflict during prewrite
```
Also thrown from the commit and pessimistic-lock paths (`Write conflict during pessimistic lock`); when TiKV returns its own retryable/abort strings they are passed through verbatim.

**What it means:** Another transaction committed a write to one of this
transaction's keys between our read (snapshot start ts) and our write.

**Likely cause:** Two transactions writing the same key concurrently —
hot-key contention.

**Solution:** Retry the business operation in a new transaction with
jittered backoff. If conflicts are frequent, shard the hot key or use
pessimistic transactions. Do NOT retry the same transaction object.

#### Deadlock Detected

**Error:**
```
DeadlockException: Deadlock detected during pessimistic lock
```

**What it means:** Pessimistic locking detected a cycle: transaction A waits
for a lock held by B while B waits for a lock held by A.

**Likely cause:** Transactions acquiring the same locks in different orders.

**Solution:** Access keys in a consistent order across your codebase.
Retry in a new transaction after jittered backoff. Inspect
`getDeadlockKey()` / `getLockTs()` to identify the contended key.

#### Lock Wait Timeout

**Error:**
```
LockWaitTimeoutException: Lock wait timeout for key: <redacted> (<maxBackoffMs>ms)
```

**What it means:** A pessimistic lock wait exhausted its wait budget
(`maxBackoffMs`, default 20000 ms) without acquiring the lock (issue #219).

**Likely cause:** A competing transaction holds the lock longer than your
budget — long-running transaction, or a genuinely stuck lock.

**Solution:** Increase `maxBackoffMs` only if the holder is legitimately
slow; otherwise retry in a new transaction. The competing key is redacted in
the message; call `getKey()` for the actual key if your log pipeline is
allowed to see it.

#### Txn Retryable (Lock Resolved Mid-Operation)

**Error:**
```
TxnRetryableException: Lock encountered, resolved - retry
```
Other real messages follow the same pattern: `Lock conflict during prewrite, resolved - retry`, `Lock encountered during scan, resolved - retry`, `Lock encountered during rollback, resolved - retry`, `Retryable error during rollback: <server string>`.

**What it means:** The operation hit another transaction's lock, resolved it
(checking expiry / pushing forward), and expects the caller's retry loop to
take it from here. Normally consumed internally; it escapes only when the
surrounding executor gives up.

**Solution:** Treat like other transaction errors: retry in a new
transaction unless the operation is idempotent. The carried
`$e->backoffType` says how the internal loop would have backed off.

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

**What it means:** Input validation failed *before* any RPC was sent —
empty/oversized keys or values, negative or too-large scan limits, bad
option values. Note this class extends `\InvalidArgumentException`
directly and is **not** a `TiKvException`, so `catch (TiKvException)` will
not catch it.

**Solution:** Fix the calling code; never retry (retrying reproduces the
same failure deterministically). Real messages include:

- `Key must not be empty in <method>`
- `Key size (<len>) exceeds maximum allowed size (<max>) in <method>`
- `Value size (<len>) exceeds maximum allowed size (<max>) in <method>`
- `Scan limit must be 0 or greater` / `Scan limit (<n>) exceeds maximum allowed scan limit of <max>` (max 10240)
- `eachLimit must be greater than 0`
- `Prefix must not be empty -- refusing to delete all keys`
- `PD endpoints array must not be empty`

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

#### Invalid State

**Error:**
```
InvalidStateException: gRPC client is closed
```

**What it means:** A raw-gRPC operation was attempted after the underlying
gRPC client was shut down (`GrpcClient` throws this from `call()`,
`callStreaming()`, `callAsync()` and `getChannel()`). Distinct trap from the
`ClientClosedException` above: closing the *client wrapper* and then using a
reference that still reaches the gRPC layer fails with a *different class
and message* than the documented one — search logs for both.

**Solution:** Same as for `ClientClosedException`: do not use the client
after `close()`; create a fresh client if needed.

Other real messages thrown as `InvalidStateException`:

- `CompareAndSwap requires atomic mode (enable via setAtomicForCAS(true))` — call `$client->setAtomicForCAS(true)` before `compareAndSwap()`/`putIfAbsent()`
- `Transaction is not active` — transaction already committed/rolled back; start a new one
- `Write set is empty, no primary key` — `commit()` on a transaction with no buffered writes
- `commitTs must be set before committing; commit() must run first.`
- `TLS config is required for TLS credentials`

Never retry any of these: they are lifecycle/programming errors, not
cluster failures.

#### Batch Deadline Exceeded

**Error:**
```
BatchDeadlineExceededException: Batch operation exceeded its <deadlineMs> ms deadline (elapsed <elapsedMs> ms)
```

**What it means:** A fanned-out batch operation (batchScan/deleteRange/
checksum/batchGet/batchPut/batchDelete) hit its wall-clock deadline before
all regions answered; some regions were never dispatched or their results
were cancelled.

**Likely cause:** Deadline too small for region count, or slow/unreachable
TiKV nodes.

**Solution:** Do not retry blindly — the batch is **partially applied**.
Inspect `getContext()['pendingRegions']` / `['dispatchedRegions']`, re-drive
only the affected keys/ranges, or raise the deadline
(`options['retryDeadlineMs']`). See
[BatchDeadlineExceededException](error-handling.md#exception-hierarchy).

#### Batch Partial Failure

**Error:**
```
BatchPartialFailureException: Batch operation partially failed: <n> of <total> regions failed. First error: <first region error>
```

**What it means:** In a fanned-out batch operation some regions succeeded,
others failed; per-region errors are aggregated instead of aborting at the
first failure.

**Likely cause:** Region-level problems on a subset of TiKV nodes
(overload, restarts, network partition).

**Solution:** Do not retry the whole batch blindly. Inspect
`getRegionErrors()` (regionId → exception) and re-drive only the failing
keys/ranges — deleteRange/checksum are idempotent and safe to retry whole;
batchPut/batchDelete are idempotent per key too, but re-check which regions
actually failed first.

#### Store Not Found

**Error:**
```
StoreNotFoundException: Store <storeId> not found in PD
```

**What it means:** The store backing a region's leader is missing from PD
(or answers with an empty address), so no RPC target exists for the region.

**Likely cause:** A TiKV node is down/restarting, or was removed from the
cluster while the region cache still points at it.

**Solution:** Check cluster health (`docker-compose ps`, TiKV metrics).
Do not retry blindly: transient only after PD restores the store or the
stale cache entry expires — inspect `getStoreId()` first. See
[Caller Retryability](error-handling.md#caller-retryability).

#### Health Check Failed

**Error:**
```
HealthCheckException: PD health check failed: <underlying error message>
```

**What it means:** `healthCheck()` could not reach PD; the wrapped
underlying transport/proto error is available via `getPrevious()`.

**Likely cause:** PD down, wrong endpoint, TLS or network problem.

**Solution:** Verify PD endpoints and connectivity (`telnet <pd-host>
2379`, `docker-compose logs pd`). This is a probe result — no user data was
touched; re-probe after an interval rather than tight-looping.

#### Retry Budget Exhausted

**Error:**
```
RetryBudgetExhaustedException: Retry attempt cap (30) exhausted for key "<key>"
RetryBudgetExhaustedException: Retry deadline (30000 ms) exhausted for key "<key>"
```
(the two messages come from the same class)

**What it means:** The internal retry loop gave up: either the attempt cap
(30 by default) or the wall-clock deadline (30000 ms by default,
`options['retryDeadlineMs']`) was reached before the operation succeeded.
The original error is preserved — check `getPrevious()`. Note the other two
budgets (`maxBackoffMs`, `serverBusyBudgetMs`) do **not** throw this class:
they rethrow the original `TiKvException`.

**Solution:** Retry only if your operation is idempotent — the client
already retried internally until its budget ran out. For non-idempotent
work, fail the request. See
[Retry Budgets Behind Every Operation](error-handling.md#retry-budgets-behind-every-operation).

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
   - [Error Handling Reference](error-handling.md)

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
| Write conflict / deadlock | New transaction, jittered backoff ([Transaction Failures](#transaction-failures)) |
| Batch partial failure | Re-drive only failed regions ([Batch Partial Failure](#batch-partial-failure)) |

## See Also

- [Getting Started](getting-started.md) - Basic setup
- [Configuration](configuration.md) - Configuration options
- [Advanced Features](advanced.md) - Production patterns
- [Architecture](architecture.md) - System design
