# RawKV Operations Guide

Complete reference for all TiKV RawKV operations supported by the PHP client.

## Table of Contents

1. [Basic CRUD Operations](#basic-crud-operations)
2. [Batch Operations](#batch-operations)
3. [Scanning Operations](#scanning-operations)
4. [Range Operations](#range-operations)
5. [TTL Operations](#ttl-operations)
6. [Atomic Operations](#atomic-operations)
7. [Data Integrity](#data-integrity)
8. [Bulk Import (SST Ingest)](#bulk-import-sst-ingest)

## Basic CRUD Operations

### Get

Retrieve a value by key:

```php
$value = $client->get('mykey');
// Returns: string value or null if not found
```

**Parameters:**
- `key` (string): The key to retrieve

**Returns:** `?string` - Value or null if key doesn't exist

**Example:**

```php
$value = $client->get('user:123');
if ($value !== null) {
    $user = json_decode($value, true);
    echo "User: {$user['name']}";
} else {
    echo "User not found";
}
```

### Put

Store a key-value pair:

```php
$client->put('mykey', 'myvalue');
```

**Parameters:**
- `key` (string): The key to store
- `value` (string): The value to store
- `ttl` (int, optional): Time-to-live in seconds (0 = no expiration)

**Returns:** `void`

**Example:**

```php
// Simple put
$client->put('config:theme', 'dark');

// Put with TTL (expires in 1 hour)
$client->put('session:abc123', json_encode(['user_id' => 42]), ttl: 3600);

// Store serialized data
$user = ['id' => 1, 'name' => 'Alice'];
$client->put('user:1', json_encode($user));
```

### Delete

Remove a key:

```php
$client->delete('mykey');
```

**Parameters:**
- `key` (string): The key to delete

**Returns:** `void`

**Example:**

```php
// Delete single key
$client->delete('temp:file:123');

// Delete with existence check
if ($client->get('lock:resource') !== null) {
    $client->delete('lock:resource');
}
```

## Batch Operations

> **Batch operations are not atomic.** Keys are grouped by region and each
> region's keys are split further into sub-batches (at most 512 keys or 16 KB
> per RPC), then all sub-batches are dispatched concurrently. If any
> sub-batch fails — after the client's own per-sub-batch retry budget is
> exhausted — the client throws
> [`BatchPartialFailureException`](error-handling.md): an **unknown subset**
> of the batch has already been applied. Do not assume the exception means
> "nothing happened", and do not assume it means "everything happened".
> See [Partial Failure and Recovery](#partial-failure-and-recovery) below.

### Batch Get

Retrieve multiple keys efficiently:

```php
$values = $client->batchGet(['key1', 'key2', 'key3']);
// Returns: ['key1' => 'value1', 'key2' => null, 'key3' => 'value3']
```

**Parameters:**
- `keys` (string[]): Array of keys to retrieve

**Returns:** `array<string, ?string>` - Associative array of key => value (null for missing keys)

**Throws:** [`BatchPartialFailureException`](error-handling.md) if any region's sub-batch fails. The call is read-only, so a full retry is always safe. Note that **no results are returned** on partial failure — keys served by regions that succeeded are discarded and must be fetched again.

**Example:**

```php
$userIds = ['user:1', 'user:2', 'user:3'];
$users = $client->batchGet($userIds);

foreach ($users as $userId => $userData) {
    if ($userData !== null) {
        $user = json_decode($userData, true);
        echo "$userId: {$user['name']}\n";
    } else {
        echo "$userId: Not found\n";
    }
}
```

**Performance Note:** BatchGet executes requests to multiple regions in parallel, making it much faster than individual Get calls.

### Batch Put

Store multiple key-value pairs:

```php
$client->batchPut(['key1' => 'value1', 'key2' => 'value2']);
```

**Parameters:**
- `keyValuePairs` (array<string, string>): Associative array of key => value
- `ttl` (int, optional): TTL applied to all keys (0 = no expiration)

**Returns:** `void`

**Throws:** [`BatchPartialFailureException`](error-handling.md) if any region's sub-batch fails.

> **Non-atomic write.** Like `deleteRange`, `batchPut` is not atomic across
> regions: when the exception is thrown an unknown subset of the key-value
> pairs has already been written. For a *fixed* key/value set a full retry
> is safe — puts are idempotent — but if your application layers
> non-idempotent logic on top (counters, appends), a blind whole-batch
> retry may double-apply work. In that case the failed sub-batch's keys
> are not identifiable through the exception — recompute or issue keys
> individually (see [Partial Failure and Recovery](#partial-failure-and-recovery)).

**Example:**

```php
// Store multiple users
$users = [
    'user:1' => json_encode(['name' => 'Alice', 'age' => 30]),
    'user:2' => json_encode(['name' => 'Bob', 'age' => 25]),
    'user:3' => json_encode(['name' => 'Charlie', 'age' => 35]),
];

$client->batchPut($users);

// Store with TTL (all keys expire in 10 minutes)
$cacheData = [
    'cache:page:home' => '<html>...</html>',
    'cache:page:about' => '<html>...</html>',
];
$client->batchPut($cacheData, ttl: 600);
```

**Performance Note:** Like BatchGet, BatchPut executes parallel requests across regions.

### Batch Delete

Delete multiple keys:

```php
$client->batchDelete(['key1', 'key2', 'key3']);
```

**Parameters:**
- `keys` (string[]): Array of keys to delete

**Returns:** `void`

**Throws:** [`BatchPartialFailureException`](error-handling.md) if any region's sub-batch fails.

> **Non-atomic write.** As with `batchPut`, an unknown subset of the keys may
> already be deleted when the exception is thrown. Deletes are idempotent
> (deleting a missing key is not an error), so a full retry of the same key
> list is safe.

**Example:**

```php
// Cleanup old sessions
$oldSessions = ['session:abc', 'session:def', 'session:ghi'];
$client->batchDelete($oldSessions);

// Delete by pattern (using scan + batch delete)
$results = $client->scanPrefix('temp:');
$keysToDelete = array_column($results, 'key');
if (!empty($keysToDelete)) {
    $client->batchDelete($keysToDelete);
}
```

### Partial Failure and Recovery

All three batch operations share the same execution model
(`BatchAsyncExecutor`):

1. Keys are grouped by TiKV region; each region's keys are split into
   sub-batches of at most 512 keys / 16 KB (one gRPC RPC each).
2. Each sub-batch is wrapped in the client's retry executor — transient
   region/transport errors are retried automatically within the configured
   `options['retryDeadlineMs']` budget (default 30 s) before it is counted
   as a failure.
3. All sub-batches are dispatched concurrently and awaited in order. Errors
   are accumulated rather than aborting the loop; on the first wait-phase
   failure the remaining in-flight futures are cancelled and are **not**
   awaited.
4. If any sub-batch failed, `BatchPartialFailureException` is thrown — even
   if most regions succeeded.

**The exception's diagnostics:** `getRegionErrors()` returns
`array<int, TiKvException>` (failed sub-batch index → exception) and
`getTotalRegions()` returns the number of sub-batch units in the batch. The
message summarises both: `Batch operation partially failed: <n> of <total>
regions failed. First error: ...`.

> **Note:** the array keys in `getRegionErrors()` are internal sub-batch
> indices, *not* TiKV region IDs and not your keys — the client does not
> expose which user keys belonged to a failed sub-batch. In practice, treat
> the batch as "partially applied with unknown affected keys".

**Recovery patterns:**

```php
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;

try {
    $client->batchPut($pairs);
} catch (BatchPartialFailureException $e) {
    printf(
        "%d of %d sub-batches failed\n",
        count($e->getRegionErrors()),
        $e->getTotalRegions(),
    );

    // Idempotent work (fixed key/value set): a full retry is safe and
    // simplest — already-written keys are simply written again.
    $client->batchPut($pairs);
}
```

If your batch carries non-idempotent logic (counters, appends), you cannot
retry blind — the failed sub-batch's keys are not identifiable through the
exception. Either:

- issue each key as an individual `put()` (each with its own read-decide-write
  or CAS guard), or
- design the operation as one that can be re-derived: e.g. re-read the state
  and recompute the whole batch, or wrap the update in a transaction
  (`$tikv->transaction()`) which *is* atomic.

`batchGet` is read-only: after a partial failure simply re-run the whole
call. `batchDelete` with a fixed key list is idempotent (missing keys are
not an error) and may be retried whole.

**Deadline variant:** `BatchDeadlineExceededException` is thrown by
`BatchAsyncExecutor` when an explicit wall-clock deadline (the
`$deadlineMs` argument) expires; in-flight futures are cancelled, so the
applied subset is likewise unknown. No shipped client option currently
wires a batch deadline — built-in batch operations run without one — so you
will only see this exception if you drive `BatchAsyncExecutor` directly.

## Scanning Operations

### Scan

Range scan over keys:

```php
$results = $client->scan('startKey', 'endKey', limit: 100, keyOnly: false);
// Returns: [['key' => 'k1', 'value' => 'v1'], ['key' => 'k2', 'value' => 'v2'], ...]
```

**Parameters:**
- `startKey` (string): Start of range (inclusive)
- `endKey` (string): End of range (exclusive)
- `limit` (int, optional): Maximum results (0 = unlimited)
- `keyOnly` (bool, optional): Return only keys, no values

**Returns:** `array<array{key: string, value: ?string}>`

**Example:**

```php
// Get all users (assuming user: prefix)
$users = $client->scan('user:', 'user;', limit: 100);
foreach ($users as $user) {
    echo "{$user['key']}: {$user['value']}\n";
}

// Get only keys (faster, no value transfer)
$keys = $client->scan('log:2024-01-', 'log:2024-02-', keyOnly: true);
echo "Found " . count($keys) . " log entries\n";

// Need to read more than one page? Don't hand-roll a pagination loop —
// use the built-in lazy scan iterator (see
// [Iterating Large Ranges](#iterating-large-ranges) below):
$allResults = [];
foreach ($client->scanIterator('user:', 'user;') as $row) {
    $allResults[] = $row;
}
```

### Scan Prefix

Scan all keys with a given prefix:

```php
$results = $client->scanPrefix('user:', limit: 100, keyOnly: false);
```

**Parameters:**
- `prefix` (string): Key prefix to scan
- `limit` (int, optional): Maximum results
- `keyOnly` (bool, optional): Return only keys

**Returns:** `array<array{key: string, value: ?string}>`

**Example:**

```php
// Get all users
$users = $client->scanPrefix('user:');

// Get all products in a category
$products = $client->scanPrefix('product:electronics:');

// Count keys (keyOnly for efficiency)
$keys = $client->scanPrefix('session:', keyOnly: true);
$activeSessions = count($keys);
```

**Implementation Note:** ScanPrefix is a convenience method that calculates the end key automatically by incrementing the last byte of the prefix.

### Iterating Large Ranges

A single `scan()` call can return at most **10240** keys (the client-side scan
limit, `RawKvClient::MAX_SCAN_LIMIT`), and even that amount is held in a PHP
array at once. To read a range larger than that — or to keep memory flat at all
— use the built-in **lazy scan iterator** instead of paging manually:

```php
// scanIterator(string $startKey, string $endKey, int $batchSize = 1024, bool $keyOnly = false): ScanIterator
// Constant memory: one page of $batchSize rows is held at a time.
foreach ($client->scanPrefixIterator('user:', batchSize: 500) as $key => $value) {
    process($key, $value);
}

foreach ($client->scanIterator('a', 'b', batchSize: 256, keyOnly: true) as $key => $_) {
    // $value is null when keyOnly is true
}
```

```php
// scanPrefixIterator(string $prefix, int $batchSize = 1024, bool $keyOnly = false): ScanIterator
// End key computed automatically (same prefix-end logic as scanPrefix()).
foreach ($client->scanPrefixIterator('session:') as $key => $value) {
    expire($key, $value);
}
```

Both methods return a `CrazyGoat\TiKV\Client\RawKv\ScanIterator` that
implements PHP's `Iterator` (so it works directly in `foreach`) and behaves as
follows:

- **Signatures** — `scanIterator($startKey, $endKey, $batchSize = 1024, $keyOnly = false)`
  and `scanPrefixIterator($prefix, $batchSize = 1024, $keyOnly = false)`. The
  range is `[startKey, endKey)` (empty `endKey` = unbounded); the prefix
  variant derives the end key via the same prefix-end calculation as
  `scanPrefix()`.
- **Prefetching** — each page fetches up to `batchSize` rows (the underlying
  `scan()` call). `batchSize` must satisfy `1 <= batchSize <= 10240`; out of
  range it throws `InvalidArgumentException` immediately when the iterator is
  created. Larger pages mean fewer RPCs; only one page is ever held in memory.
  Region routing is served from the region cache, so after the first page a
  page does not issue an extra PD `ScanRegions` request; PD is only consulted
  when the cached region chain is incomplete.
- **Continuation** — after consuming a page, the next fetch starts after the
  page's last key (`lastKey . "\x00"`), the same continuation rule as manual
  pagination. The scan is exhausted when a page returns fewer than `batchSize`
  rows.
- **keyOnly** — with `keyOnly: true` values are not transferred; `current()`
  returns `null` for every entry.
- **Rewindable** — calling `rewind()` (e.g. running the same `foreach` twice)
  restarts the iteration from the original start key and re-scans from the
  beginning; it does not cache results.
- **Exceptions** — `batchSize` is validated synchronously in the factory call
  (plus `ClientClosedException` if the client is closed), but the actual scan
  RPCs happen lazily during iteration: `RegionException` and `GrpcException`
  can be thrown mid-`foreach`, and the underlying scans use the same automatic
  retry logic as `scan()`.
- **No reverse iterator** — there is no descending counterpart; for reverse
  reads use `reverseScan()` with a limit (see [Reverse Scan](#reverse-scan)).

**See also:** [Scan Optimization](advanced.md#scan-optimization) in Advanced
Features, and [docs/error-handling.md](error-handling.md) for the full
exception table.

### Reverse Scan

Scan in descending order:

```php
$results = $client->reverseScan('startKey', 'endKey', limit: 100, keyOnly: false);
```

**Parameters:**
- `startKey` (string): Upper bound (exclusive) - scan starts below this
- `endKey` (string): Lower bound (inclusive) - scan stops at or above this
- `limit` (int, optional): Maximum results
- `keyOnly` (bool, optional): Return only keys

**Returns:** `array<array{key: string, value: ?string}>`

**Example:**

```php
// Get 10 most recent log entries
// (assuming log keys are timestamp-based like "log:2024-01-15T10:30:00")
$logs = $client->reverseScan('log:', 'log:0', limit: 10);

// Get last 5 messages for a user
$messages = $client->reverseScan(
    'msg:user:123:', 
    'msg:user:123:0', 
    limit: 5
);
```

**Important:** Reverse scan semantics differ from forward scan:
- `startKey` = upper bound (exclusive)
- `endKey` = lower bound (inclusive)
- Results are in descending order

### Batch Scan

Scan multiple non-contiguous ranges:

```php
$ranges = [
    ['user:a', 'user:f'],      // Users A-F
    ['user:p', 'user:t'],      // Users P-T
];
$results = $client->batchScan($ranges, eachLimit: 50, keyOnly: false);
// Returns: [[results for range 1], [results for range 2], ...]
```

**Parameters:**
- `ranges` (array<array{0: string, 1: string}>): Array of [startKey, endKey] pairs
- `eachLimit` (int): Maximum results per range (required)
- `keyOnly` (bool, optional): Return only keys

**Returns:** `array<array<array{key: string, value: ?string}>>`

> **Performance Note:** `batchScan` fans out the ranges **concurrently** —
> at most `options['maxConcurrency']` ranges (default **16**; must be `>= 1`)
> are in flight at once, and each range's per-region scan RPCs are all
> dispatched before any response is awaited, so server-side latencies
> overlap. Results are returned in input range order.

**Example:**

```php
// Scan specific time ranges
$timeRanges = [
    ['log:2024-01-01', 'log:2024-01-02'],
    ['log:2024-01-15', 'log:2024-01-16'],
    ['log:2024-01-30', 'log:2024-01-31'],
];
$dailyLogs = $client->batchScan($timeRanges, eachLimit: 1000);

foreach ($dailyLogs as $day => $logs) {
    echo "Day $day: " . count($logs) . " entries\n";
}
```

## Range Operations

### Delete Range

Delete all keys in a range:

```php
$client->deleteRange('startKey', 'endKey');
```

**Parameters:**
- `startKey` (string): Start of range (inclusive)
- `endKey` (string): End of range (exclusive)

**Returns:** `void`

**Example:**

```php
// Delete all temporary files
$client->deleteRange('temp:', 'temp;');

// Delete old logs (be careful!)
$client->deleteRange('log:2023-01-01', 'log:2024-01-01');

// Clear a user's data
$client->deleteRange('data:user:123:', 'data:user:123;');
```

**Warning:** This operation is not atomic across regions. If it fails partway through, some keys may be deleted and others not. The same partial-failure rules as the [batch operations](#partial-failure-and-recovery) apply: `deleteRange` also throws `BatchPartialFailureException`, and because deletes are idempotent the whole range operation may simply be re-issued.

### Delete Prefix

Delete all keys with a prefix:

```php
$client->deletePrefix('cache:');
```

**Parameters:**
- `prefix` (string): Prefix to delete (must not be empty)

**Returns:** `void`

**Throws:** `InvalidArgumentException` if prefix is empty

**Example:**

```php
// Clear all cache entries
$client->deletePrefix('cache:');

// Clear specific cache namespace
$client->deletePrefix('cache:api:v1:');

// Delete all sessions for a user
$client->deletePrefix('session:user:123:');
```

**Safety:** DeletePrefix refuses to delete all keys (empty prefix) to prevent accidents.

## TTL Operations

**Note:** TTL requires TiKV to be configured with `enable-ttl=true` in tikv.toml.

> **Cluster mode is exclusive.** `enable-ttl = true` puts TiKV into V1TTL
> storage mode, which serves RawKV with TTL but **not** transactional
> (TxnKV) requests. A single cluster cannot serve both RawKV-with-TTL and
> TxnKV.
>
> - RawKV **with** TTL → `[storage] enable-ttl = true` (see `tikv.toml`)
> - RawKV without TTL, or TxnKV → leave `enable-ttl` unset (see `tikv-v1.toml`)
>
> This project's own E2E suites reflect the split: RawKV tests run against
> `docker-compose.yml`, TxnKV tests against
> `docker-compose.yml + docker-compose.txnkv.yml`.
>
> Changing this setting on a live cluster is an operational migration —
> plan it before adopting either feature.

### Put with TTL

Store with automatic expiration:

```php
$client->put('session:123', 'data', ttl: 3600);  // Expires in 1 hour
```

**Parameters:**
- `key` (string): Key to store
- `value` (string): Value to store
- `ttl` (int): Time-to-live in seconds

**Returns:** `void`

**Example:**

```php
// Session with 2 hour expiration
$sessionData = json_encode(['user_id' => 42, 'login_time' => time()]);
$client->put('session:abc123', $sessionData, ttl: 7200);

// Temporary cache entry (5 minutes)
$client->put('cache:api:result', $apiResponse, ttl: 300);

// Rate limit counter (1 minute window)
$client->put('ratelimit:ip:192.168.1.1', '100', ttl: 60);
```

### Get Key TTL

Check remaining time-to-live:

```php
$ttl = $client->getKeyTTL('session:123');
// Returns: int (seconds remaining) or null (not found/no TTL)
```

**Parameters:**
- `key` (string): Key to check

**Returns:** `?int` - Seconds remaining, or null if key not found or has no TTL

**Example:**

```php
$ttl = $client->getKeyTTL('session:abc123');

if ($ttl === null) {
    echo "Session not found or expired\n";
} elseif ($ttl < 300) {
    echo "Session expires soon ($ttl seconds left)\n";
    // Refresh session
    $client->put('session:abc123', $data, ttl: 7200);
} else {
    echo "Session valid for $ttl seconds\n";
}
```

### Batch Put with TTL

Apply TTL to batch operations:

```php
$client->batchPut(['k1' => 'v1', 'k2' => 'v2'], ttl: 3600);
```

Per-key TTL is also supported — pass an array of TTLs indexed by key name or positional:

```php
// Per-key TTL (associative array by key name)
$client->batchPut([
    'cache:hot' => 'frequently-accessed',
    'cache:cold' => 'rarely-accessed',
], ttl: ['cache:hot' => 3600, 'cache:cold' => 60]);

// Per-key TTL (positional array — order must match pairs)
$client->batchPut(['k1' => 'v1', 'k2' => 'v2'], ttl: [3600, 60]);

// Mix of TTL=0 (no expiry) and TTL>0 in the same batch
$client->batchPut(['perm' => 'permanent', 'temp' => 'temporary'], ttl: [0, 300]);
```

## Atomic Operations

### Compare And Swap (CAS)

Atomic compare-and-set operation:

```php
use CrazyGoat\TiKV\Client\RawKv\CasResult;

$result = $client->compareAndSwap('counter', '5', '6');
// Returns: CasResult object
```

**Parameters:**
- `key` (string): Key to modify
- `expectedValue` (?string): Expected current value (null = key should not exist)
- `newValue` (string): New value to set
- `ttl` (int, optional): TTL for the new value

**Returns:** `CasResult` with properties:
- `swapped` (bool): True if swap succeeded
- `previousValue` (?string): Previous value (null if key didn't exist)

**Example:**

```php
// Counter increment
$current = $client->get('counter') ?? '0';
$result = $client->compareAndSwap('counter', $current, (string)($current + 1));

if ($result->swapped) {
    echo "Counter incremented to " . ($current + 1);
} else {
    echo "Counter changed by another process, retry needed";
    echo "Current value: {$result->previousValue}";
}
```

**CAS Loop Pattern:**

```php
function incrementCounter(RawKvClient $client, string $key): int
{
    while (true) {
        $current = $client->get($key) ?? '0';
        $next = (int)$current + 1;
        
        $result = $client->compareAndSwap($key, $current, (string)$next);
        
        if ($result->swapped) {
            return $next;
        }
        
        // Retry with new value
        usleep(1000);  // 1ms backoff
    }
}
```

### Put If Absent

Insert only if key doesn't exist:

```php
$existing = $client->putIfAbsent('lock:resource', 'owner-123');
// Returns: null (success) or existing value (failure)
```

**Parameters:**
- `key` (string): Key to insert
- `value` (string): Value to insert
- `ttl` (int, optional): TTL for the value

**Returns:** `?string` - null if inserted, existing value if key already exists

**Example:**

```php
// Distributed lock
$owner = 'process-' . getmypid();
$existing = $client->putIfAbsent('lock:resource:123', $owner, ttl: 30);

if ($existing === null) {
    echo "Lock acquired\n";
    
    // Do work...
    
    // Release lock
    $client->delete('lock:resource:123');
} else {
    echo "Lock held by: $existing\n";
}
```

**Lock with Heartbeat:**

```php
function acquireLock($client, $resource, $owner, $ttl = 30)
{
    $existing = $client->putIfAbsent("lock:$resource", $owner, $ttl);
    
    if ($existing !== null && $existing !== $owner) {
        return false;  // Lock held by someone else
    }
    
    // Start heartbeat in background
    startHeartbeat($client, $resource, $owner, $ttl);
    
    return true;
}

function startHeartbeat($client, $resource, $owner, $ttl)
{
    // In real implementation, use a background process or timer
    // This is a simplified example
    while (hasLock($client, $resource, $owner)) {
        sleep($ttl / 2);
        $client->put("lock:$resource", $owner, $ttl);
    }
}
```

## Data Integrity

### Checksum

Compute CRC64-XOR checksum over a key range:

```php
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;

$checksum = $client->checksum('data:start', 'data:end');
// Returns: ChecksumResult object
```

**Parameters:**
- `startKey` (string): Start of range (inclusive)
- `endKey` (string): End of range (exclusive)

**Returns:** `ChecksumResult` with properties:
- `checksum` (int): CRC64-XOR checksum value
- `totalKvs` (int): Total number of key-value pairs
- `totalBytes` (int): Total bytes of keys and values

**Example:**

```php
// Verify data integrity
$before = $client->checksum('backup:data:', 'backup:data;');

// ... perform backup/restore ...

$after = $client->checksum('backup:data:', 'backup:data;');

if ($before->checksum === $after->checksum && 
    $before->totalKvs === $after->totalKvs) {
    echo "Data integrity verified\n";
} else {
    echo "Data mismatch detected!\n";
    echo "Before: {$before->totalKvs} keys, checksum {$before->checksum}\n";
    echo "After: {$after->totalKvs} keys, checksum {$after->checksum}\n";
}
```

**Use Cases:**
- Data migration verification
- Backup integrity checks
- Data consistency validation

## Error Handling

All operations throw exceptions derived from `TiKvException` (plus the
standalone `InvalidArgumentException`, which is outside that hierarchy).
Batch operations (`batchGet`/`batchPut`/`batchDelete`) additionally throw
`BatchPartialFailureException` — a *partially applied* batch, not a clean
failure; see [Partial Failure and Recovery](#partial-failure-and-recovery).
The full class hierarchy, a per-operation exception table for every
`RawKvClient` and `Transaction` method, caller-retryability guidance and a
recommended catch order live in the dedicated
**[Error Handling Reference](error-handling.md)**.

Quick sketch:

```php
use CrazyGoat\TiKV\Client\Exception\TiKvException;

try {
    $value = $client->get('key');
} catch (TiKvException $e) {
    // Transport, region or retry-budget failure — see the reference for
    // which subclass it is and whether your operation may be retried.
    echo "TiKV error: {$e->getMessage()}\n";
}
```

Note that `InvalidArgumentException` is *not* caught by the arm above — it
does not extend `TiKvException`. See
[Troubleshooting](troubleshooting.md) for common errors and solutions.

## Best Practices

### Key Design

```php
// Good: Hierarchical, sortable keys
$userId = 123;
$timestamp = date('Y-m-d-H-i-s');
$client->put("user:$userId:log:$timestamp", $logData);

// Good: Prefix for grouping
$client->put('cache:page:home', $html);
$client->put('cache:api:users:list', $json);

// Avoid: Keys that don't sort well
$client->put('user_' . uniqid(), $data);  // Random, hard to scan
```

### Value Size

```php
// Good: Small to medium values
$client->put('user:123', json_encode(['name' => 'Alice', 'email' => 'alice@example.com']));

// Avoid: Very large values (TiKV has limits)
// If you need to store large data, consider chunking:
$largeData = str_repeat('x', 10 * 1024 * 1024);  // 10MB
$chunks = str_split($largeData, 1024 * 1024);     // 1MB chunks
foreach ($chunks as $i => $chunk) {
    $client->put("largefile:abc:chunk:$i", $chunk);
}
```

### Batch Size

```php
// Good: Reasonable batch sizes (100-1000 keys)
$batch = array_slice($allKeys, 0, 100);
$client->batchPut($batch);

// Avoid: Extremely large batches
// If you have many keys, process in chunks:
$chunks = array_chunk($allKeys, 500);
foreach ($chunks as $chunk) {
    $client->batchDelete($chunk);
}
```

## Bulk Import (SST Ingest)

### Ingest

Bulk-import key-value pairs into TiKV via SST (Sorted String Table) ingestion:

```php
$client->ingest(['k1' => 'v1', 'k2' => 'v2'], ttl: 3600);
```

**Signature:** `ingest(array $keyValuePairs, ?int $ttl = null): void` — `$ttl`
is in seconds; `null` means no TTL (the TTL is only sent to TiKV when it is a
positive integer).

**How it works** (see `src/Client/RawKv/SstIngestor.php`):

1. Keys are sorted client-side (`ksort(..., SORT_STRING)` — SST data must be
   sorted), converted to `import_sstpb.Pair` messages and grouped by region
   (batch region resolution against PD).
2. For each region the pairs are streamed to the region leader via the TiKV
   ImportSST **`RawWrite`** client-streaming RPC (1024 pairs per chunk, first
   message carries the SST metadata: UUID, key range, region epoch, CRC32),
   and the resulting SST is committed with the **`Ingest`** RPC.

This bypasses the normal Raft write path and writes pre-sorted data directly
into regions, giving far higher throughput than `batchPut()` on large loads.
Keys are validated like every other write (non-empty, key/value size limits);
an empty array is a no-op.

> **Cluster-wide side effect — read before running in production.** For the
> duration of the call **every** TiKV store in the cluster is switched into
> **import mode** (`SwitchMode(Import)` via the ImportSST service) and switched
> back to normal mode on completion. Import mode changes behaviour for every
> client of the cluster, not just the one that set it. The switch-back runs in
> a `finally` block, so exceptions are safe — but a **killed process** (OOM
> killer, deployment restart, `max_execution_time`, fatal error) leaves the
> cluster in import mode with no client-side record and no automatic recovery.
>
> Before running `ingest()` in production:
>
> - Run it in a dedicated, supervised process with no execution-time limit
>   (CLI, `max_execution_time = 0`), never inside a web request.
> - Run batches in a size you can afford to re-run: `ingest()` is **not**
>   retried automatically — any region error or transport error aborts the
>   remaining regions and throws.
> - Verify afterwards that all stores are back in normal mode.
>
> **Recovering a cluster stuck in import mode:** the switch-back is issued to
> every store PD reports, so the simplest recovery is to re-run a successful
> `ingest()` call (any batch, even small) against the same cluster — its
> `finally` block switches all stores back to normal mode. Alternatively, issue
> the ImportSST `SwitchMode(Normal)` RPC to every store directly (see the
> `import_sstpb.ImportSST/SwitchMode` gRPC service); the client's
> `switchStoresMode()` in `SstIngestor` shows the exact request shape. A store
> whose `SwitchMode(Normal)` RPC fails during the switch-back is logged as
> `Failed to switch store to normal mode` and must be restored manually on
> that store. (Stores skipped for a rejected address never entered import
> mode — the import path rejects them too, before any RPC — so they need no
> restore.)

**Operational notes:**

- The SwitchMode fan-out to all stores, the `RawWrite` stream and the `Ingest`
  RPC each use the ingest gRPC deadline, fixed at **60 s**
  (`TimeoutConfig::ingestTimeoutMs`, default `60000`). It is **not**
  configurable through `RawKvClient::create()` `options['timeout']` — that
  array only maps `readTimeoutMs`, `writeTimeoutMs`, `batchReadTimeoutMs`,
  `batchWriteTimeoutMs`, `scanTimeoutMs`, `deleteRangeTimeoutMs` and
  `checksumTimeoutMs` keys.
- Keys that cannot be resolved to a region (region lookup returned nothing for
  them) are **silently dropped** from the import — no error is raised. Verify
  the imported key count if completeness matters.
- The number of simultaneously in-flight SwitchMode requests is bounded by the
  client's `options['maxConcurrency']` (default
  `RawKvClient::DEFAULT_MAX_CONCURRENCY`), so the per-call mode-switch overhead
  does not scale linearly with store count.

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Configuration](configuration.md) - Client configuration
- [Advanced Features](advanced.md) - Production patterns
- [Troubleshooting](troubleshooting.md) - Error handling
