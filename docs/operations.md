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

### Batch Get

Retrieve multiple keys efficiently:

```php
$values = $client->batchGet(['key1', 'key2', 'key3']);
// Returns: ['key1' => 'value1', 'key2' => null, 'key3' => 'value3']
```

**Parameters:**
- `keys` (string[]): Array of keys to retrieve

**Returns:** `array<string, ?string>` - Associative array of key => value (null for missing keys)

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

// Pagination pattern
$pageSize = 50;
$allResults = [];
$startKey = 'user:';

while (true) {
    $page = $client->scan($startKey, 'user;', limit: $pageSize);
    if (empty($page)) {
        break;
    }
    $allResults = array_merge($allResults, $page);
    
    // Next page starts after the last key
    $lastKey = $page[count($page) - 1]['key'];
    $startKey = $lastKey . "\x00";  // Next possible key
    
    if (count($page) < $pageSize) {
        break;  // Last page
    }
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

**Warning:** This operation is not atomic across regions. If it fails partway through, some keys may be deleted and others not.

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

All operations throw exceptions on failure:

```php
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;

try {
    $value = $client->get('key');
} catch (RegionException $e) {
    // Region-related errors (epoch mismatch, not leader, etc.)
    echo "Region error: {$e->getMessage()}\n";
} catch (GrpcException $e) {
    // gRPC communication errors
    echo "Connection error: {$e->getMessage()}\n";
} catch (TiKvException $e) {
    // General TiKV errors
    echo "TiKV error: {$e->getMessage()}\n";
}
```

See [Troubleshooting](troubleshooting.md) for common errors and solutions.

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

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Configuration](configuration.md) - Client configuration
- [Advanced Features](advanced.md) - Production patterns
- [Troubleshooting](troubleshooting.md) - Error handling
