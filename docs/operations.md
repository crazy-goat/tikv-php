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
8. [Error Handling](#error-handling)
9. [Limits](#limits)
10. [Connection lifecycle and process model](#connection-lifecycle-and-process-model)
11. [Cluster and lifecycle](#cluster-and-lifecycle)
12. [Best Practices](#best-practices)
13. [Bulk Import (SST Ingest)](#bulk-import-sst-ingest)

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
> sub-batch fails — after the operation's shared retry budget is
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

**Returns:** `array<array-key, ?string>` - Associative array of key => value (null for missing keys). Keys come back under PHP's array-key semantics: a canonical decimal-integer key (`'1000'`, `'0'`, `'-5'`) is returned as an `int` key, everything else (and a canonical decimal beyond `PHP_INT_MAX`) stays a `string` key. The lookup is lossless — `$values['1000']` finds the entry stored as int 1000 — so cast a `foreach` key with `(string)` before passing it to a `string`-typed parameter. See [`helpers/faq.md`](helpers/faq.md).

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

// Delete by pattern: collect keys through the lazy iterator and delete in
// batches, so memory stays flat. A bare scanPrefix('temp:') would buffer the
// whole prefix and throw ScanLimitExceededException past
// options['maxScanRows'] (default 100000).
$batch = [];
foreach ($client->scanPrefixIterator('temp:') as $key => $_) {
    $batch[] = $key;
    if (count($batch) >= 500) {
        $client->batchDelete($batch);
        $batch = [];
    }
}
if ($batch !== []) {
    $client->batchDelete($batch);
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

For the per-operation verdicts — which operations converge when repeated and
which ones must not be repeated after an ambiguous failure — see
[Idempotency and Retry Safety](error-handling.md#idempotency-and-retry-safety).

**Deadline variant:** `BatchDeadlineExceededException` is thrown by
`BatchAsyncExecutor` when the wall-clock batch deadline expires (the
`batchDeadlineMs` timeout option, 60000 ms by default; `0` disables it);
in-flight futures are cancelled, so the applied subset is likewise unknown.

## Scanning Operations

### Range bounds

Every range operation takes a half-open `[startKey, endKey)` range. An empty
`endKey` means "unbounded": +infinity for `scan()`, `scanPrefix()`,
`batchScan()`, `deleteRange()` and `checksum()`, and the start of the keyspace
for `reverseScan()`, whose arguments are `[upper bound, lower bound)`. An
empty `startKey` is valid for a forward range (it means the start of the
keyspace), but it is **not** a valid reverse-scan upper bound: reverse scan
has no +infinity sentinel. `reverseScan('', $lower)` with a non-empty lower
bound is rejected with `InvalidArgumentException` before any RPC, while
`reverseScan('', '')` is the documented empty range. To read from the top of
a prefix, pass the prefix's end key as the upper bound (for example `log;`
for the prefix `log:`).

A **reversed** range is rejected with `InvalidArgumentException` before any PD
or store RPC: `startKey > endKey` for the forward operations (`scan()`,
`scanIterator()`, `batchScan()`, `deleteRange()`, `checksum()`) and
`startKey < endKey` for `reverseScan()`, whose first argument is the upper
bound. `startKey == endKey` with a non-empty `endKey` is a valid empty range:
it returns no rows and deletes nothing. Because an empty `endKey` is the
unbounded sentinel, `scan('', '')` and `batchScan([['', '']])` cover the whole
keyspace, while `deleteRange('', '')` keeps its explicit no-op guard and
`reverseScan('', '')` is empty (both bounds are the keyspace minimum). This
turns the most common reverse-scan mistake into an error instead of a silent
empty result. The comparison is bytewise, so `scan('20', '100')` is rejected
even though PHP would compare the numeric strings in the other order.

### Scan

Range scan over keys:

```php
$results = $client->scan('startKey', 'stopKey', limit: 100, keyOnly: false);
// Returns: [['key' => 'k1', 'value' => 'v1'], ['key' => 'k2', 'value' => 'v2'], ...]
```

**Parameters:**
- `startKey` (string): Start of range (inclusive)
- `endKey` (string): End of range (exclusive)
- `limit` (int, optional): Maximum results; `0` (default) returns the **whole
  range** — the client pages internally (`RawKvClient::MAX_SCAN_LIMIT` rows per
  RPC) and buffers every row, up to the configurable `options['maxScanRows']`
  guard (checked per fetched page, not per row). See
  [Iterating Large Ranges](#iterating-large-ranges).
- `keyOnly` (bool, optional): Return only keys, no values

**Returns:** `array<array{key: string, value: ?string}>`

**Truncation is never silent.** An unbounded scan that hits the
`options['maxScanRows']` guard throws `ScanLimitExceededException`, so a
returned result is the whole range. A bounded scan (`limit > 0`) returns at
most `limit` rows, so a result whose count equals the limit may be truncated —
compare the count with the limit to find out.

**Example:**

```php
// Get the first 100 users (assuming user: prefix)
$users = $client->scan('user:', 'user;', limit: 100);
foreach ($users as $user) {
    echo "{$user['key']}: {$user['value']}\n";
}

// Get only keys (faster, no value transfer)
$keys = $client->scan('log:2024-01-', 'log:2024-02-', keyOnly: true);
echo "Found " . count($keys) . " log entries\n";

// limit: 0 returns everything, but holds it all in memory. For a large
// range prefer the lazy iterator, which keeps only one page in memory:
foreach ($client->scanIterator('user:', 'user;') as $row) {
    process($row);
}
```

### Scan Prefix

Scan all keys with a given prefix:

```php
$results = $client->scanPrefix('user:', limit: 100, keyOnly: false);
```

**Parameters:**
- `prefix` (string): Key prefix to scan
- `limit` (int, optional): Maximum results; `0` (default) returns the whole
  prefix (paginated internally, guarded by `options['maxScanRows']`, checked
  per fetched page)
- `keyOnly` (bool, optional): Return only keys

**Returns:** `array<array{key: string, value: ?string}>`

**Truncation is never silent.** An unbounded prefix scan that hits the
`options['maxScanRows']` guard throws `ScanLimitExceededException`; a bounded
scan (`limit > 0`) returns at most `limit` rows, so a result whose count
equals the limit may be truncated.

**Example:**

```php
// Get the whole prefix into memory (limit: 0 is the default; the result is
// buffered and capped by options['maxScanRows'], default 100000)
$users = $client->scanPrefix('user:');

// Get all products in a category
$products = $client->scanPrefix('product:electronics:');

// Count keys (keyOnly for efficiency)
$keys = $client->scanPrefix('session:', keyOnly: true);
$activeSessions = count($keys);

// For a large prefix prefer the constant-memory lazy iterator:
foreach ($client->scanPrefixIterator('user:') as $key => $value) {
    process($key, $value);
}
```

**Implementation Note:** ScanPrefix is a convenience method that calculates the end key automatically by incrementing the last byte of the prefix.

### Iterating Large Ranges

A bounded `scan()` / `scanPrefix()` / `reverseScan()` call accepts at most
`RawKvClient::MAX_SCAN_LIMIT` (10240) rows per RPC. A negative limit is
rejected with `InvalidArgumentException` before any RPC is sent. Passing
`limit: 0` does **not** cap the result at 10240: the client pages internally
(10240 rows per RPC) and returns the complete range. That is convenient, but
the whole result is still buffered in one PHP array, so an unbounded scan is
guarded by
`options['maxScanRows']` (default `RawKvClient::DEFAULT_MAX_SCAN_ROWS` =
100000) — exceeding it throws `ScanLimitExceededException` rather than
silently truncating. The guard is evaluated per internally fetched page
(after that page is buffered), so it is page-granular, not a strict per-row
bound: a guard below the page size still reads a whole page before throwing,
and `getScannedRows()` can exceed `getMaxRows()`.

A **transactional** scan (`Transaction::scan()`) has a different, harder cap:
it reads the range at the transaction's snapshot and does not page internally,
so `limit: 0` returns the whole range up to
`Transaction::MAX_SCAN_LIMIT` (10240) rows. A range with more rows than that
throws `ScanLimitExceededException` as well — the result is never truncated
silently — so page the range explicitly (an explicit `limit` plus the last key
you received) when it is larger. The unbounded alternative is the RawKV
iterator above, with the trade-off that it is a raw read: it does not observe
the transaction's own buffered writes.

To read a range larger than `maxScanRows`, or to keep memory flat at all,
use the built-in **lazy scan iterator** instead of paging manually:

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
  pagination. The iteration ends only when a page returns no rows, or when the
  resume key reaches `endKey`; a page that returns fewer than `batchSize` rows
  is **not** treated as the end, because `scan()` may return a short page while
  data remains (a skipped region, a server-side per-region cap, a
  region-boundary effect). A short page therefore costs at most one extra RPC
  and can no longer silently truncate the result (issue #195). A page whose
  last key does not move the cursor forward (or whose first key lies before it)
  throws `TiKvException`: it is a protocol violation, not a transient error
  (#312).
- **keyOnly** — with `keyOnly: true` values are not transferred; `current()`
  returns `null` for every entry.
- **Rewindable** — calling `rewind()` (e.g. running the same `foreach` twice)
  restarts the iteration from the original start key and re-scans from the
  beginning; it does not cache results.
- **Exceptions** — `batchSize` and the range order are validated synchronously
  in the factory call (plus `ClientClosedException` if the client is closed),
  but the actual scan RPCs happen lazily during iteration: `RegionException`
  and `GrpcException` can be thrown mid-`foreach`, and the underlying scans use
  the same automatic retry logic as `scan()`.
- **No reverse iterator** — there is no descending counterpart; for reverse
  reads use `reverseScan()`. `limit: 0` returns the whole range by paging
  internally (guarded by `options['maxScanRows']`, checked per fetched page);
  for a very large reverse range either raise that guard deliberately or page
  in the caller.

**See also:** [Scan Optimization](advanced.md#scan-optimization) in Advanced
Features, and [docs/error-handling.md](error-handling.md) for the full
exception table.

### Reverse Scan

Scan in descending order:

```php
// Note the order: the UPPER bound comes first, the LOWER bound second.
$results = $client->reverseScan($upperBound, $lowerBound, limit: 100, keyOnly: false);
```

**Parameters:**
- `startKey` (string): Upper bound (exclusive) - scan starts below this
- `endKey` (string): Lower bound (inclusive) - scan stops at or above this
- `limit` (int, optional): Maximum results; `0` (default) returns the whole
  range (paginated internally and guarded by `options['maxScanRows']`, checked
  per fetched page)
- `keyOnly` (bool, optional): Return only keys

**Returns:** `array<array{key: string, value: ?string}>`

**Truncation is never silent.** An unbounded reverse scan that hits the
`options['maxScanRows']` guard throws `ScanLimitExceededException`; a bounded
scan (`limit > 0`) returns at most `limit` rows, so a result whose count
equals the limit may be truncated. There is no reverse iterator — for a large
reverse range raise the guard deliberately or page the range in the caller.

An unbounded reverse scan whose page spans several regions issues the
per-region reads **concurrently**, in windows of at most
`options['maxConcurrency']` regions (default 16), and concatenates them in
descending key order — the same fan-out as an unbounded forward scan. A
region or transport error, or a region that split after enumeration, falls
back to the sequential retrying path. A bounded reverse scan
(`limit > 0`) stays sequential.

Regions are resolved from the upper bound downwards in bounded batches
(issue #198): the region cache answers the common case and a miss asks PD for
the region containing the cursor or the adjacent region below it, so a narrow
reverse scan does not enumerate the rest of the range.

**Example:**

```php
// Get the 10 most recent log entries
// (assuming log keys are timestamp-based like "log:2024-01-15T10:30:00")
//
// reverseScan($startKey, $endKey) scans [$endKey, $startKey) descending, so the
// FIRST argument is the UPPER bound and must sort AFTER the second one. To
// cover every key with a prefix, bump the prefix's last byte: 'log:' -> 'log;'
// (':' is 0x3A, the next byte ';' is 0x3B).
$logs = $client->reverseScan('log;', 'log:', limit: 10);

// Get the last 5 messages for a user, newest first.
// To make the upper bound INCLUSIVE of one specific key, append "\x00" to it:
// that key sorts below its own "\x00"-suffixed form, and nothing else sorts in
// between, so it is the last row returned.
$newest = 'msg:user:123:2024-01-15T10:31:12';
$messages = $client->reverseScan($newest . "\x00", 'msg:user:123:', limit: 5);
```

**Important:** Reverse scan semantics differ from forward scan:
- `startKey` = upper bound (exclusive) — the **first** argument
- `endKey` = lower bound (inclusive) — the **second** argument
- Results are in descending order
- The upper bound must be explicit: an empty `startKey` is not +infinity.
  With a non-empty `endKey` it is rejected with `InvalidArgumentException`
  before any RPC, and `reverseScan('', '')` is the empty range.

Passing the bounds the other way round is the most common mistake here. It is
rejected with `InvalidArgumentException` before any RPC instead of returning a
silent empty array: the first argument must sort after the second. An empty
`endKey` (the second argument) means "down to the start of the keyspace".

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
- `ranges` (array<array{0: string, 1: string}>): Array of `[startKey, endKey]`
  pairs; an empty `endKey` means +infinity. A malformed range (not a
  two-element pair of strings) or a reversed range rejects the whole call with
  `InvalidArgumentException` before any RPC.
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
$client->deleteRange('startKey', 'stopKey');
```

**Parameters:**
- `startKey` (string): Start of range (inclusive)
- `endKey` (string): End of range (exclusive); an empty `endKey` deletes to the
  end of the keyspace. A reversed range throws `InvalidArgumentException`
  before any RPC.

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

**Warning:** This operation is not atomic across regions. If it fails partway through, some keys may be deleted and others not. The same partial-failure rules as the [batch operations](#partial-failure-and-recovery) apply: `deleteRange` also throws `BatchPartialFailureException`, and because deletes are idempotent the whole range operation may simply be re-issued. Retryable region errors carried in a region's response (`NotLeader`, `EpochNotMatch`, `ServerIsBusy`, …) are retried per region before the operation gives up: a leader transfer, or a split whose fresh region still covers the sub-range, is retried transparently; a split that leaves the fresh region smaller than the sub-range still fails closed (via `assertRegionCoversRange()`, reported as a `BatchPartialFailureException`).

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

Check remaining time-to-live. The lookup has three possible outcomes, and
`getKeyTTLResult()` reports all of them separately:

```php
$result = $client->getKeyTTLResult('session:123');

$result->exists;      // bool: whether the key exists
$result->ttlSeconds;  // ?int: seconds remaining, or null when it has no expiry
```

| Key state | `exists` | `ttlSeconds` |
|---|---|---|
| Missing (never written, deleted or expired) | `false` | `null` |
| Present, no expiry | `true` | `null` |
| Present, expires | `true` | `int > 0` |

The convenience method `getKeyTTL()` returns only `ttlSeconds`, so its
`null` means either "key missing" or "key present without an expiry". Use
`getKeyTTLResult()` whenever the two states lead to different actions, such
as an expiry monitor that must not report a deleted key as permanent.

**Parameters:**
- `key` (string): Key to check

**Returns:** `getKeyTTLResult()` returns a `KeyTtl` with `exists: bool` and
`ttlSeconds: ?int`; `getKeyTTL()` returns `?int` and collapses both `null`
cases.

**Example:**

```php
$result = $client->getKeyTTLResult('session:abc123');

if (!$result->exists) {
    echo "Session not found or expired\n";
} elseif ($result->ttlSeconds === null) {
    echo "Session has no expiry\n";
} elseif ($result->ttlSeconds < 300) {
    echo "Session expires soon ({$result->ttlSeconds} seconds left)\n";
    // Refresh session
    $client->put('session:abc123', $data, ttl: 7200);
} else {
    echo "Session valid for {$result->ttlSeconds} seconds\n";
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

### Atomic Mode (`setAtomicForCAS` / `isAtomicForCAS`)

Both operations in this section are gated on a client-wide flag that is **off by
default**.

**Signatures:**

```php
public function setAtomicForCAS(bool $enabled): self;  // fluent, returns the client
public function isAtomicForCAS(): bool;                 // current state of the flag
```

**What the flag does:** with atomic mode enabled the client sets the `for_cas`
flag on every `RawPut`/`RawDelete` request it sends (single-key and batch),
which is what selects TiKV's atomic code path on the store.

**Scope:** the flag is a switch of the client instance. It affects all put and
delete writes of that instance, not only the CAS calls. `compareAndSwap()` and
`putIfAbsent()` require the flag. These methods send `for_cas` while the flag
is on:

- `put()`
- `delete()`
- `batchPut()`
- `batchDelete()`

`deleteRange()`, `deletePrefix()` and `ingest()` do not send the flag. Reads do
not send it either.

**Mixed writers:** a client with the flag on and a client with the flag off send
writes to the same keys through different code paths. TiKV serializes an atomic
write and a `compareAndSwap()` on the same key with latches. TiKV applies a
plain write without these latches. A plain write can land between the read and
the write of a CAS, and the CAS then overwrites it. The client does not check
this. Use the same mode for every writer of the same keys. If only a part of
your workload needs CAS, give that part its own client instance, and let this
client be the only writer of its keys.

**Why it defaults to `false`:** the non-atomic path is the faster one for plain
writes, which is what most calls are. The flag is per client and not per call,
so a client that mixes a handful of CAS operations with bulk plain writes pays
the atomic-path cost on *every* write it sends: atomic mode cannot be paid for
only where CAS is actually needed. When just a few operations need CAS, give
those their own client and leave the bulk client in the fast mode.

**When it is required:** `compareAndSwap()` and `putIfAbsent()` — and therefore
every lock, leader-election and counter pattern built on them — throw
`InvalidStateException` while the flag is off:

```
CompareAndSwap requires atomic mode (enable via setAtomicForCAS(true))
```

**How to enable it:** once, right after the client is created. It is a
client-wide switch, not a per-call argument:

```php
$client = RawKvClient::create(['127.0.0.1:2379']);
$client->setAtomicForCAS(true);  // required by compareAndSwap() / putIfAbsent()
```

The flag lives on the client *instance*: a process that calls `close()` and
`RawKvClient::create()` again (reconnect, failover) has to re-apply it. Read it
back with `isAtomicForCAS()` when a library hands a client to code that may not
have enabled it.

### Compare And Swap (CAS)

Atomic compare-and-set operation:

> **Prerequisite:** `compareAndSwap()` and `putIfAbsent()` require atomic mode.
> Call `$client->setAtomicForCAS(true)` once after creating the client, otherwise
> both methods throw `InvalidStateException`. Atomic mode makes the client send the
> `for_cas` flag on every `RawPut`/`RawDelete` (`put()`, `delete()`, `batchPut()`
> and `batchDelete()` too), which selects TiKV's atomic code path. See
> [Atomic Mode](#atomic-mode-setatomicforcas--isatomicforcas) for the scope.
> It is disabled by default because the non-atomic path is faster for plain writes.

```php
use CrazyGoat\TiKV\Client\RawKv\CasResult;

// Required once per client: compareAndSwap() throws without atomic mode
$client->setAtomicForCAS(true);

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

**Indeterminate outcome:** a transport failure on the CAS request itself
(deadline exceeded, connection reset, `UNAVAILABLE`) throws
`CasIndeterminateException` instead of a bare `GrpcException`: the swap may
already have been applied and the client deliberately does not retry a
non-idempotent write. The original `GrpcException` is preserved as
`getPrevious()`. Re-read the key and decide from its current value; never
treat this exception as "not swapped". Region errors are still retried
automatically, and a failure that happens before the request is sent (for
example PD unreachable during region resolution) stays a plain
`GrpcException` and means the swap definitively did not happen. An
application-level rejection of the request itself (`UNAUTHENTICATED`,
`PERMISSION_DENIED`, `INVALID_ARGUMENT`, …) also stays a plain
`GrpcException`: the server refused it, so no swap happened.

**Example:**

```php
// Counter increment
$client->setAtomicForCAS(true);  // required by compareAndSwap()
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
    // Requires atomic mode: the caller must have called
    // $client->setAtomicForCAS(true) on this client.
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
// Same prerequisite as compareAndSwap(): atomic mode must be on
$client->setAtomicForCAS(true);

$existing = $client->putIfAbsent('lock:resource', 'owner-123');
// Returns: null (success) or existing value (failure)
```

**Parameters:**
- `key` (string): Key to insert
- `value` (string): Value to insert
- `ttl` (int, optional): TTL for the value

**Returns:** `?string` - null if inserted, existing value if key already exists

An inserted value is confirmed only by a `null` return. A thrown
`CasIndeterminateException` means the outcome is unknown — the value may have
been stored — so a distributed-lock caller must not interpret it as "lock not
acquired"; re-read the key first (see [Compare And Swap](#compare-and-swap-cas)).

**Example:**

```php
// Distributed lock
$client->setAtomicForCAS(true);  // required by putIfAbsent()
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
    // Requires atomic mode: the caller must have called
    // $client->setAtomicForCAS(true) on this client.
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

$checksum = $client->checksum('data:a', 'data:z');
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

Retryable region errors carried in a per-region response are retried per
sub-range with backoff and region-cache invalidation (issue #566). Only a
fatal region error or an exhausted retry budget aborts the call — as a
`BatchPartialFailureException`, never as a partial checksum.

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

## Limits

The client enforces these limits. The constants are public on `RawKvClient`.

| Limit | Value | Constant | Enforced |
|---|---|---|---|
| Maximum key size | 8 MiB (8388608 bytes) | `MAX_KEY_SIZE` | Client, before any RPC. Throws `InvalidArgumentException` |
| Maximum value size | 4 MiB (4194304 bytes) | `MAX_VALUE_SIZE` | Client, before any RPC. Throws `InvalidArgumentException` |
| Minimum key size | 1 byte | `MIN_KEY_SIZE` | An empty key throws `InvalidArgumentException` |
| Maximum scan limit | 10240 rows per RPC | `MAX_SCAN_LIMIT` | Client, before any RPC. A negative limit throws `InvalidArgumentException`; `limit: 0` means the whole range, paged internally. See [Scan](#scan) |
| Keys per batch RPC | 512 | `MAX_BATCH_LIMIT` | Automatic split. No error |
| Bytes per batch RPC | 16384 bytes (16 KiB) | `MAX_BATCH_GET_SIZE`, `MAX_BATCH_PUT_SIZE`, `MAX_BATCH_DELETE_SIZE` | Automatic split. No error |
| Batch fan-out deadline | 60000 ms | `TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS` | Client, `batchDeadlineMs`. Throws `BatchDeadlineExceededException`. `0` disables it |

The batch deadline is checked at dispatch boundaries and at wait boundaries. It is not checked during one blocking wait. One wait on a retryable call runs that region's share of the operation's retry budget; all regions of a batch share one `RetryExecutor` budget (issue #223), so `retryDeadlineMs` bounds the fan-out as a whole (up to 30000 ms, plus one per-call timeout). The worst case is `batchDeadlineMs` plus the operation's remaining retry deadline. The experimental BatchCommands transport does not use the batch deadline.

`InvalidArgumentException` is `CrazyGoat\TiKV\Client\Exception\InvalidArgumentException`.
Its message names the size and the method, for example `Value size (5242880) exceeds maximum allowed size (4194304) in put`.
The key and value checks apply to `get`, `put`, `delete`, `getKeyTTL`, `getKeyTTLResult`, `compareAndSwap`, `putIfAbsent`, `batchGet`, `batchPut`, `batchDelete` and `ingest`.
`putIfAbsent` calls `compareAndSwap`, so its error message names `compareAndSwap`.
A batch method checks every key and value before it sends the first RPC.
`scan` and `reverseScan` check the size of the start key and of the end key. `scan` accepts an empty start key (the keyspace start); `reverseScan` accepts it only together with an empty end key (`reverseScan('', '')`, the empty range), because it has no +infinity upper-bound sentinel.

### Batch auto-split

You do not need to split a batch yourself. `batchGet`, `batchPut` and `batchDelete` group the keys by region.
Then they split the keys of each region into sub-batches of at most 512 keys and 16384 bytes.
For `batchGet` and `batchDelete`, the byte count is the total size of the keys.
For `batchPut`, the byte count is the total size of the keys and the values.
A single pair that is larger than 16384 bytes is not rejected. It goes alone in its own sub-batch.
The client sends the sub-batches in parallel. A batch is not atomic. See [Batch Operations](#batch-operations).

### TxnKV

The TxnKV client has no key or value size check of its own. The `MAX_KEY_SIZE` and `MAX_VALUE_SIZE` constants apply to RawKV only.
A transactional scan has its own cap of 10240 rows. See [Scan](#scan).

## Connection lifecycle and process model

The client is **not thread-safe**: its caches are plain mutable state with no
locking. Use one instance at a time per PHP process, build a fresh one in each
forked child, and never share an instance between threads or processes.

### What a client holds

| State | Default | Behaviour |
|---|---|---|
| gRPC channel cache | one channel per address, soft cap **64**, idle TTL **600000 ms** (10 minutes) | LRU eviction when a new address needs a slot; idle channels are swept on a cache miss and, rate-limited, on a cache hit |
| Region cache | **10000** entries, TTL **600 s** plus up to **60 s** of jitter | LRU eviction at the cap; an entry is dropped on a region error or when a newer region supersedes its range |
| PD client | store cache (**128** entries), TSO pool (**64** timestamps) | endpoint rotation, cluster-ID cache, timestamp batching |

The channel cap is **soft**: eviction never closes a channel that still
carries in-flight calls, so a batch that fans out over more stores than the
cap holds grows the cache past 64 while the calls are in flight. It stays
above the cap after they settle: it only shrinks on a later cache miss (idle
channels are evicted down to the cap) or when the idle TTL drops them (the
overshoot is logged at warning level). Raise `maxChannels` when that warning
appears.

Both caches only pay off when the client instance is reused. `maxChannels`
and `idleTtlMs` are `create()` options — see
[Channel Pool](configuration.md#channel-pool-maxchannels-idlettlms). The
region-cache sizing is **not** exposed by `create()`: `ttlSeconds`,
`jitterSeconds` and `maxEntries` are constructor arguments of `RegionCache`,
so only a client assembled by hand can change them:

```php
$client = new RawKvClient(
    $pdClient,
    $grpcClient,
    new RegionCache(ttlSeconds: 60),
    ownsGrpcClient: false, // the transport is the application's
);
```

`create()` always builds the default. A hand-assembled client closes the
transport it was given unless you pass `ownsGrpcClient: false` — see
[Sharing One Transport Between Clients](configuration.md#sharing-one-transport-between-clients).
A `RegionCache` passed to the constructor is still cleared by `close()` (the
client has no ownership flag for it), so do not share one between clients
that you close independently. The same applies to the `PdClient`: `close()`
always closes it (there is no `ownsPdClient` flag), resetting its store
cache, TSO pool and leader-discovery state.

### Long-running workers and CLI daemons (recommended)

Create one client at startup, reuse it for the life of the process, and call
`close()` on shutdown. Channels and region entries stay warm, so steady-state
traffic does not pay a PD lookup per key.

```php
$client = RawKvClient::create(['127.0.0.1:2379']);

try {
    while ($job = $queue->next()) {
        $client->put($job->key, $job->value);
    }
} finally {
    $client->close();
}
```

A worker that can be quiet for longer than `idleTtlMs` does not need to
reconnect: call `reapIdleChannels()` from its own tick to drop the cached
handles that have been idle longer than `idleTtlMs`. The next request reopens
a handle if it needs one. Dropping a handle is not a reconnect: ext-grpc's
persistent table keeps the channel warm until the process ends or the
per-target persist bound drops the entry.

The same tick can keep the region cache fresh. `refreshRegionCache()` re-queries
PD for a bounded slice of the cached regions at most once per
`options['regionCacheRefreshMs']` (default `0`, i.e. the non-forced refresh is
disabled — refreshing is opt-in), so a split or leader transfer is learned
before a user request has to fail and retry for it. Call
`preloadRegions([['start', 'end'], ...])` once at boot to resolve the keys a
long-running worker will touch without serving a read. Both are no-ops after
`close()`, never throw on a PD failure (the failure is logged and the old entry
is kept), and start no timer of their own. See
[Region Cache Refresh and Preload](configuration.md#region-cache-refresh-and-preload)
for the cost, the per-tick bound and the interval guidance.

Scans use the same warm cache without extra configuration: RawKV scans and
TxnKV `Transaction::scan()` read contiguous cached regions locally and ask PD
only for the uncovered gaps, so a scan-heavy worker does not put PD on the
path of every request once the cache is warm
([Scan-path Region Cache Use](configuration.md#scan-path-region-cache-use)).

### PHP-FPM and other per-request runtimes

A client created per request starts with a **cold region cache**, so the
first operation that touches a region pays a PD lookup for it
(`GetRegion` / `ScanRegions`). The PHP-side channel handles are rebuilt too,
but the TCP connections need not be: ext-grpc keeps a process-global
persistent channel per target, so a later request served by the same FPM
worker can reuse the connection — the region cache (and the PHP-side handle
cache) cannot survive the request, the connection can
([What the shared channel really is](configuration.md#what-the-shared-channel-really-is)).

Create the client lazily and keep it as a singleton for the request lifetime
(a DI container scoped to the request is fine). Do not create more than one
client per request: each one carries its own region cache, so a second client
repeats every PD lookup and, if it builds its own transport, doubles the
channel handles. A client cannot be persisted between requests — PHP resets
userland state, and a live channel is not serializable.

### `close()` semantics

`close()` releases the client's resources in guarded steps: `TxnKvClient`
first rolls back the transactions it began that are still active
(`RawKvClient` has no transactions), then the client clears the region cache,
closes the gRPC transport if the client owns it, and closes the PD client
(always — there is no ownership flag for it). The transaction rollback runs
while the region cache, transport and PD client are still usable, so an
abandoned transaction does not leave its locks behind until the TTL expires.
A failing step is logged at error level and does not stop the others, so:

- `close()` is **idempotent** — a second call does nothing;
- `close()` **never throws**.

A rollback that fails during `close()` is logged and the transaction stays
active; its destructor keeps the existing best-effort rollback fallback. A
transaction that already committed or rolled back is not rolled back again.
The rollback is a real RPC, so `close()` can block while it retries; its
retry budget is the transaction's usual one. The rollback is attempted for a
client that shares the application's transport (`options['grpcClient']`)
too — the client owns its transactions even when it does not own the
transport. The transport itself is still never closed in that case.

For a client that owns its transport, `close()` is **not graceful**: it
closes the channels that still carry in-flight calls, so an operation that is
running when the client is closed fails — finish outstanding operations
before calling `close()`, or expect them to fail. A client built with
`options['grpcClient']` does not close the shared transport, so its in-flight
calls continue (and become the application's responsibility when it closes
the transport).

`__destruct()` calls `close()`, so a forgotten `close()` is not a leak: the
caches and handles are released when the object is destroyed. In a
long-running process, still call `close()` explicitly so resources are
released at a known point instead of at an arbitrary destructor.

For a client that owns its transport, `close()` releases this client's
channel handles, not necessarily the TCP connection: ext-grpc's persistent
table keeps the channel warm until the process ends or the per-target persist
bound drops the entry, and a channel created beyond that bound is private to
its handle, so closing it can release the underlying channel (see
[What the shared channel really is](configuration.md#what-the-shared-channel-really-is)).
A client built with `options['grpcClient']` closes no transport at all — the
application closes the transport it supplied (see
[Sharing One Transport Between Clients](configuration.md#sharing-one-transport-between-clients)).

After `close()`, the client's own operations that talk to the cluster throw
`ClientClosedException`. The lifecycle methods and client-local getters and
setters remain callable: `close()`, `reapIdleChannels()`,
`refreshRegionCache()` / `preloadRegions()` (both no-ops after close),
`getMetrics()`, `setAtomicForCAS()` / `isAtomicForCAS()`, `setColumnFamily()` /
`getColumnFamily()` and `getClusterId()` (which returns `null` after close
because the PD client resets the cached cluster ID). A `Transaction` obtained
before `close()` holds the transport and the PD client directly and does not
check the client's closed flag. `TxnKvClient::close()` rolls back the
transactions it began while they are still active, so a successfully rolled
back transaction is closed and cannot be used afterwards. If that rollback
fails, the transaction stays active: with an owned transport its RPCs then
fail with `InvalidStateException`; with a shared transport they can still
reach TiKV. Finish open transactions before closing the client (see below).

### Transactions

An active `Transaction` is rolled back by its destructor when it goes out of
scope, and a failed destructor rollback is logged. `TxnKvClient::close()` is a
second safety net: it rolls back the transactions it began that are still
active before it releases the transport. That is still not the contract:

- call `commit()` or `rollback()` explicitly. Destructor and `close()` timing
  are not guaranteed, and either rollback is a real RPC that can block
  shutdown while it retries;
- until the rollback succeeds or the lock TTL expires, the transaction's
  server-side locks stay in place and block other writers;
- a rollback that fails inside `close()` is only logged — `close()` never
  throws — so the locks stay until the transaction's destructor succeeds or
  their TTL expires. Do not rely on `close()` to clean up transactions:
  finish them explicitly before closing the client.

## Cluster and lifecycle

These methods report on the cluster the client is connected to and configure the
client instance. None of them touches user data. `healthCheck()` is the only one
that sends an RPC and the only one that can throw a library exception; the rest
are local accessors and setters that throw nothing. `TxnKvClient` has
the same cluster-level methods (`healthCheck()`, `getClusterId()` and
`getMetrics()`) plus `pdClient()`, which exposes the PD connection it owns; the
column-family and atomic-mode settings are RawKV-only.

### Health and cluster identity

**Signatures:**

```php
public function healthCheck(): ?int;
public function getClusterId(): ?int;
public function getMetrics(): MetricsInterface;
public function pdClient(): PdClientInterface;
```

**`healthCheck()`** issues a PD `GetMembers` RPC through
`PdClientInterface::ping()` and returns the cluster ID PD reports. It touches no
user data and cannot fail with a "no region" error.

- **Returns:** the cluster ID, or `null` when PD answered but the response
  carried no cluster-ID header or reported `0` ("reachable, identity
  unknown"). Both are a successful probe.
- **Throws:** `ClientClosedException` when the client has been closed, and
  `HealthCheckException` for every other failure (PD unreachable, a gRPC
  transport error, or a PD-level error in the response header). The underlying
  exception is available through `getPrevious()`; a `PdException` there names
  the failing RPC in its public `$method` property, carries the raw
  `pdpb.ErrorType` in its public `$errorType` property and its enum name from
  `getErrorTypeName()`.
- **What it proves:** the client is open and can reach a configured PD endpoint
  that accepts `GetMembers`. A successful probe also records the current PD
  leader reported by PD (when its address matches the configured endpoints) and
  caches the cluster ID, so subsequent PD and TSO RPCs target that leader.
- **What it does not prove:** anything about the TiKV data path. It does not
  contact a store, resolve a region, or read/write a key, so it can stay green
  while every data operation fails — a down store, a missing region leader, a
  degraded write path. It is a PD readiness probe, not a TiKV data-path probe.

If a readiness check must also cover the data path, pair the PD probe with one
shallow read of a key your deployment maintains:

```php
use CrazyGoat\TiKV\Client\Exception\TiKvException;

try {
    $client->healthCheck();                            // PD is reachable
    $healthy = $client->get('health:canary') !== null; // region + store are reachable
} catch (TiKvException $e) {
    // HealthCheckException, ClientClosedException, or a data-path failure
    $healthy = false;
}
```

The read exercises region resolution and the read path; it still does not prove
that writes work. Do not use a synthetic write/delete as a probe: at probe
frequency it adds continuous write traffic to the cluster, and it fails whenever
the write path is degraded even though the cluster is reachable. Monitor the
write path through your application's own traffic and the counters exposed by
`getMetrics()`.

The data path has its own per-store failover (issue #424): after three
consecutive `UNAVAILABLE` or `DEADLINE_EXCEEDED` transport failures within a
60-second window the client marks a store unhealthy for a one-second cooldown,
drops the cached regions it led and refuses to route to it until the cooldown
elapses, then probes it again. Failures are attributed to the store the failing
RPC actually targeted, so a PD outage or a nested RPC against another store does
not demote the outer store; replica reads skip a store in cooldown. That is
transparent to `healthCheck()`, which remains a PD-only probe and does not report
individual store health; the state changes are visible through
`MetricsInterface::storeHealthChanged()` when a metrics backend is configured.

**`getClusterId()`** returns the cluster ID the client has already learned, or
`null` when it has not learned one yet. It is a cached accessor: it sends no RPC
and throws nothing. The ID is learned from the first PD response whose header
carries a non-zero cluster ID, and every later response header must report the
same ID (a different one throws `TiKvException` before the response is used), or
explicitly by `healthCheck()`. After `close()` it returns `null` because
closing the PD client resets the cache.

**`getMetrics()`** returns the metrics backend in use: the instance passed via
`options['metrics']`, or the no-op default (`NoOpMetrics`) when none was
configured. It is that same object, not a copy, so an `InMemoryMetrics` instance
can be inspected after operations. It sends no RPC, throws nothing, and remains
callable after `close()`.

**`pdClient()`** returns the `PdClientInterface` the TxnKV client uses for
region discovery and timestamps. It is the same object, not a second
connection: a consumer that also needs PD — for example to register a service
GC safe point or to inspect the cluster — reuses this one instead of calling
`ConnectionFactory::create()` again, which would open another PD connection and
a second TSO stream. The client owns the object and closes it in `close()`, so
do not close it yourself and treat the reference as stale afterwards.
`pdClient()` itself sends no RPC and throws nothing.

### Column family and atomic mode

**Signatures:**

```php
public function setColumnFamily(string $cf): self;    // fluent, returns the client
public function getColumnFamily(): string;
public function setAtomicForCAS(bool $enabled): self; // see Atomic Operations
public function isAtomicForCAS(): bool;
```

**Column family.** `setColumnFamily()` sets the TiKV column family of the
client instance and `getColumnFamily()` reads it back. `''` — the default —
means TiKV's default column family; the client does not send a `cf` field for an
empty string. Any other value is sent verbatim as the `cf` field of every
subsequent key/value request of that client: single-key CRUD, CAS, the batch
operations, scans (including both iterators) and delete-range operations. The
setting is **per client instance**, not per call, so it applies to all later
operations until it is changed. `setColumnFamily()` returns the client and
performs no validation; TiKV rejects an unknown column family on the first
request that uses it. The setting lives on the client instance: a new client
instance starts at `''`, so re-apply it.

Two exceptions to "every operation":

- `checksum()` and `ingest()` have no column-family parameter and always
  operate on the default column family.
- With `options['apiVersion' => 2]` the transport clears the `cf` field on every
  RawKV request (API V2 treats an explicit column family as deprecated and uses
  the keyspace's default), so `setColumnFamily()` has no effect on an API V2
  client.

`scanIterator()` and `scanPrefixIterator()` capture the column family when they
are created; after changing it, create a new iterator. `TxnKvClient` has no
column-family setting: transactions use the default column family.

**Atomic mode.** `setAtomicForCAS()` / `isAtomicForCAS()` are the same kind of
client-instance switch; the flag is off by default and `compareAndSwap()` /
`putIfAbsent()` require it. See
[Atomic Mode](#atomic-mode-setatomicforcas--isatomicforcas) for the full scope.

```php
$client->setColumnFamily('write');   // all later key/value operations
$client->put('user:1', $payload);    // RawPut carries cf = 'write'
$cf = $client->getColumnFamily();    // 'write'; '' is the default CF

$client->setColumnFamily('');        // back to TiKV's default CF
$atomic = $client->isAtomicForCAS(); // false unless setAtomicForCAS(true)
```

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

// Avoid: Values larger than MAX_VALUE_SIZE (4 MiB). put() throws InvalidArgumentException.
// See "Limits". If you need to store large data, split it into chunks:
$largeData = str_repeat('x', 10 * 1024 * 1024);  // 10MB
$chunks = str_split($largeData, 1024 * 1024);     // 1MB chunks
foreach ($chunks as $i => $chunk) {
    $client->put("largefile:abc:chunk:$i", $chunk);
}
```

### Batch Size

```php
// Good: Pass the whole list. The client groups the keys by region and
// splits them into sub-batches of 512 keys and 16 KiB (see "Limits").
$client->batchDelete($allKeys);

// Chunk only to bound memory use or to limit the part of the work
// that a BatchPartialFailureException can leave unknown.
$chunks = array_chunk($allKeys, 10000);
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
   sorted) and grouped by region (batch region resolution against PD). Only
   the keys are copied; the values stay in the caller's array.
2. For each region the data is streamed lazily to the region leader via the
   TiKV ImportSST **`RawWrite`** client-streaming RPC: the first message
   carries the SST metadata (UUID, key range, region epoch, CRC32), then one
   data chunk per 1024 keys or 4 MiB of key-plus-value payload, whichever
   comes first. Each chunk's `import_sstpb.Pair` messages are
   built on demand and released when the next chunk is requested, so the
   payload is not held as a full `Pair[]` array plus a full request array plus
   the serialised messages. The resulting SST is committed with the
   **`Ingest`** RPC.

This bypasses the normal Raft write path and writes pre-sorted data directly
into regions, giving far higher throughput than `batchPut()` on large loads.
Keys are validated like every other write (non-empty, key/value size limits);
an empty array is a no-op. The opt-in
`options['maxIngestSize']` cap (`0` = unlimited, set on
`RawKvClient::create()`) bounds the whole payload in bytes and rejects an
oversized call with `IngestTooLargeException` before the cluster is switched
into import mode or any RPC is sent. Even with a cap, the caller's input array
is held in memory for the whole call, so split very large imports into
batches yourself. The streamed copy is bounded: at most one chunk (1024
pairs or 4 MiB of key-plus-value bytes) is materialised, so a 100 MB ingest
holds the caller's payload plus the key list and roughly one 4 MiB chunk,
not a second full `Pair[]` copy of every value.

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
> - Run batches in a size you can afford to re-run. Retryable region errors
>   (`NotLeader`, `EpochNotMatch`, `ServerIsBusy`, …) and retryable transport
>   errors are retried per region against a freshly resolved region, but the
>   import is **not atomic**: a permanent failure aborts the remaining regions
>   and throws `IngestPartialFailureException`, whose `getAppliedRegionIds()`
>   lists the region groups that were already written. Re-run the whole call
>   to converge (see below).
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

**Retries and partial failure:**

Each region group runs through the client's retry executor and gets its **own**
retry budget (`retryDeadlineMs`, `maxBackoffMs` and the ServerBusy budget), so
a long import is not capped by the per-operation retry deadline. A retryable
region error (`NotLeader`, `EpochNotMatch`, `ServerIsBusy`, `RegionNotFound`,
…) or a retryable transport error is retried against a **freshly resolved
region**. A leader transfer, a merge, or a split/merge that leaves the group
inside the re-resolved region is recovered transparently.

A split that **shrinks the region below the group's last key** is not
re-grouped: the retry keeps rebuilding an SST that spans both regions, so TiKV
keeps rejecting it until the budget is exhausted. The import then fails closed
(no part of that group is applied); split the batch at the split key and
re-run, or re-run the whole call after the region layout settles.

The import remains **non-atomic across regions**. When a region group fails
permanently (a fatal region error, an exhausted retry budget or a missing
store), `ingest()` stops, switches every store back to normal mode and throws:

- `IngestPartialFailureException` when at least one earlier region group was
  already ingested — `getAppliedRegionIds()` lists those regions and
  `getPrevious()` carries the original failure; or
- the original exception (`RegionException`, `RetryBudgetExhaustedException`,
  `StoreNotFoundException`, …) when nothing was applied yet.

Re-running the **whole** call is the recommended recovery: SST ingestion is
idempotent for identical key-value pairs. With a TTL the TTL restarts on every
attempt, exactly as for `put()`.

**Operational notes:**

- The SwitchMode fan-out to all stores, the `RawWrite` stream and the `Ingest`
  RPC each use the ingest gRPC deadline (`TimeoutConfig::ingestTimeoutMs`,
  default `60000`), configurable through `RawKvClient::create()`
  `options['timeout']['ingestTimeoutMs']` (alongside the
  `readTimeoutMs`, `writeTimeoutMs`, `batchReadTimeoutMs`,
  `batchWriteTimeoutMs`, `scanTimeoutMs`, `deleteRangeTimeoutMs` and
  `checksumTimeoutMs` keys).
- Keys that cannot be resolved to a region fail the import with a
  `TiKvException` naming the redacted key — `ingest()` never writes a subset
  of the pairs and reports success (issue #187).
- The number of simultaneously in-flight SwitchMode requests is bounded by the
  client's `options['maxConcurrency']` (default
  `RawKvClient::DEFAULT_MAX_CONCURRENCY`), so the per-call mode-switch overhead
  does not scale linearly with store count.

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Configuration](configuration.md) - Client configuration
- [Advanced Features](advanced.md) - Production patterns
- [Troubleshooting](troubleshooting.md) - Error handling
