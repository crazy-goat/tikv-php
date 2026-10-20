# TiKV PHP Client

[![Tests](https://github.com/crazy-goat/tikv-php/actions/workflows/tests.yml/badge.svg)](https://github.com/crazy-goat/tikv-php/actions/workflows/tests.yml)

PHP client for TiKV's RawKV and TxnKV APIs, using the gRPC extension.

## Requirements

- PHP >= 8.2
- **`ext-grpc`** — the PECL gRPC extension, **1.57 or newer**. It is a hard
  requirement: Composer refuses to install the package without it. See
  [Installation](#installation) for the per-platform instructions.
- TiKV cluster — default (V1) mode for TxnKV, or `[storage] enable-ttl = true`
  for RawKV TTL. The two are mutually exclusive; see
  [Transactions (TxnKV)](#transactions-txnkv).

## Quick Start

```bash
# Start TiKV cluster and run example
make up
make example
```

## Installation

```bash
composer require crazy-goat/tikv-client
```

The client speaks gRPC through the **`ext-grpc`** PHP extension, so the
requirement is declared in `composer.json` (`"ext-grpc": ">=1.57"`). Composer
checks it at install time and refuses to install the package on a machine
without the extension:

```
crazy-goat/tikv-client <version> requires ext-grpc >=1.57 -> it is missing from your system. Install or enable PHP's grpc extension.
```

`<version>` is the version of the package you install.
Contributors working on this repository see the root-package wording of the
same error instead:

```
Root composer.json requires PHP extension ext-grpc >=1.57 but it is missing from your system. Install or enable PHP's grpc extension.
```

The minimum version (1.57) matches the `grpc/grpc` library the client depends
on and covers every extension API it uses (`Grpc\Call::startBatch()`,
`Grpc\Timeval`, `Grpc\Channel`, the `OP_*` call-operation constants).

### Installing the extension

**Debian / Ubuntu**

PECL compiles the extension, so the build tools come first:

```bash
sudo apt-get install php-dev build-essential autoconf
sudo pecl install grpc
PHP_VERSION=$(php -r 'echo PHP_MAJOR_VERSION, ".", PHP_MINOR_VERSION;')
echo "extension=grpc.so" | sudo tee "/etc/php/$PHP_VERSION/mods-available/grpc.ini"
sudo phpenmod grpc
```

The `php-grpc` distribution package comes from the
[sury.org PHP repository](https://packages.sury.org/php/), which is not part
of stock Debian or Ubuntu. When that repository is already configured, the
package works too:

```bash
sudo apt-get install php-grpc
```

**Alpine**

```bash
apk add php83-pecl-grpc
```

**macOS (Homebrew)**

```bash
pecl install grpc
echo "extension=grpc.so" >> "$(brew --prefix)/etc/php.ini"
```

**Verify**

```bash
php -m | grep grpc
php -r 'echo phpversion("grpc"), PHP_EOL;'
```

The reported version must be 1.57 or newer. Restart PHP-FPM (or your web
server) after enabling the extension.

## Usage

### Basic CRUD Operations

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$client = RawKvClient::create(['127.0.0.1:2379']);

// Basic CRUD
$client->put('key', 'value');
$value = $client->get('key');       // 'value'
$client->delete('key');

// Batch operations
$client->batchPut(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3']);
$values = $client->batchGet(['k1', 'k2', 'k3']);
$client->batchDelete(['k1', 'k2', 'k3']);

$client->close();
```

### Scanning

```php
// Range scan [startKey, endKey). limit: 0 (the default) returns the whole
// range: the client pages internally and buffers every row, guarded by
// options['maxScanRows'] (default 100000).
$results = $client->scan('start', 'end', limit: 100);
// Returns: [['key' => 'k1', 'value' => 'v1'], ['key' => 'k2', 'value' => 'v2'], ...]

// Prefix scanning (limit: 0 returns all matching keys, buffered)
$results = $client->scanPrefix('user:');

// Lazy scan iterators — constant memory, auto-paginating (page of
// $batchSize rows at a time; batchSize must be 1..10240, default 1024).
// Use these instead of scanPrefix()/scan() when a range may be large.
foreach ($client->scanPrefixIterator('user:', batchSize: 500) as $key => $value) {
    process($key, $value);
}
// $value is null when keyOnly is true
foreach ($client->scanIterator('a', 'b', batchSize: 256, keyOnly: true) as $key => $_) {
    // ...
}

// For an unbounded scan that matches more than options['maxScanRows'] rows
// (default 100000) the client throws ScanLimitExceededException rather than
// silently truncating; the iterators above have no such limit.

// Reverse scan (descending order)
// Note the order: startKey = UPPER bound (exclusive), endKey = LOWER bound
// (inclusive) — the first argument must sort after the second. Bump the last
// byte of a prefix to cover all of its keys ('log:' -> 'log;'); append "\x00"
// to a key to make the upper bound inclusive of that key.
$results = $client->reverseScan('log;', 'log:', limit: 100);

// Scan multiple non-contiguous ranges
$results = $client->batchScan([['a:', 'a;'], ['b:', 'b;']], eachLimit: 50);
```

### Range Operations

```php
// Delete all keys in range [startKey, endKey)
$client->deleteRange('temp:', 'temp;');

// Delete all keys with a given prefix
$client->deletePrefix('cache:');
```

### TTL (Time-To-Live)

Requires `enable-ttl=true` in tikv.toml configuration.

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

```php
// Store with expiration (TTL in seconds)
$client->put('session', 'data', ttl: 3600);        // expires in 1 hour

// Get remaining TTL
$remaining = $client->getKeyTTL('session');          // seconds remaining, or null if not found/no TTL
```

### Atomic Operations

> **Note:** Compare-And-Swap (CAS) and Put-If-Absent require atomic mode to be
> enabled via `setAtomicForCAS(true)`. In atomic mode, the underlying `RawPut`
> RPC sends the `for_cas` flag, which tells TiKV to use the atomic code path.
> Atomic mode is disabled by default for performance — the non-atomic code path
> is faster for regular writes. Enable it only when you need CAS semantics.

```php
use CrazyGoat\TiKV\Client\RawKv\CasResult;

// Enable atomic mode for CAS operations
$client->setAtomicForCAS(true);

// Compare-And-Swap (CAS)
$result = $client->compareAndSwap('counter', '1', '2');
if ($result->swapped) {
    echo "Value was swapped from '1' to '2'";
    echo "Previous value: " . ($result->previousValue ?? 'null');
}

// Put if key does not exist (distributed lock pattern)
$existing = $client->putIfAbsent('lock', 'owner-1');
if ($existing === null) {
    echo "Lock acquired!";
} else {
    echo "Lock already held by: $existing";
}
```

### Data Integrity

```php
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;

// Compute CRC64-XOR checksum over key range
$checksum = $client->checksum('data:', 'data;');
echo "Checksum: {$checksum->checksum}";
echo "Keys: {$checksum->totalKvs}, Bytes: {$checksum->totalBytes}";
```

### Bulk Import (SST Ingest)

```php
// High-throughput bulk load: bypasses the Raft write path, writes
// pre-sorted SST data directly into regions. Optional TTL in seconds.
$client->ingest(['k1' => 'v1', 'k2' => 'v2'], ttl: 3600);
```

> **Cluster-wide hazard.** For the duration of the call **every** TiKV store is
> switched into import mode and switched back on completion. The switch-back
> runs in a `finally` block, so exceptions are safe — but a killed process
> (OOM killer, deploy restart, `max_execution_time`) leaves the cluster in
> import mode, degrading it for all clients. Run `ingest()` only from a
> dedicated, supervised CLI process, and see
> [Bulk Import (SST Ingest)](docs/operations.md#bulk-import-sst-ingest) in the
> operations guide for the full semantics, the fixed 60 s ingest deadline and
> the recovery procedure for a cluster stuck in import mode.

### Transactions (TxnKV)

`TxnKvClient` speaks TiKV's TxnKV API: snapshot reads and two-phase commit
across an arbitrary set of keys, in one cluster, with no application-level
locking.

**Use TxnKV when** a read-modify-write must be atomic across more than one
key, or when several reads inside one operation must observe a single
consistent snapshot. **Stay on RawKV when** each key is independent — RawKV
has no transaction machinery to pay for, and the per-key atomic operations
(`compareAndSwap`, `putIfAbsent`, which need `setAtomicForCAS(true)`) cover the
single-key case.

> **Cluster mode is exclusive.** TxnKV needs a cluster in default (V1) mode —
> `enable-ttl` must **not** be set, see `tikv-v1.toml`. A cluster started with
> `enable-ttl = true` runs in V1TTL mode, which serves RawKV with TTL but
> **rejects** transactional requests. One cluster serves one of the two.

#### Lifecycle

```php
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

$txnClient = TxnKvClient::create(['127.0.0.1:2379']);   // same options as RawKvClient

$txn = $txnClient->begin([          // pessimistic by default
    'pessimistic' => true,          // false = optimistic (locks at commit)
    'priority'    => 0,             // 0 Normal (default), 1 Low, 2 High
    'lockWaitTimeMs' => 1000,       // how long TiKV may wait for a contended lock
]);

// ... or state the mode explicitly (recommended):
// $txn = $txnClient->beginPessimistic();
// $txn = $txnClient->beginOptimistic();

$txn->set('account:1', '100');
$txn->commit();                     // prewrite + commit; or $txn->rollback()

$txnClient->close();
```

A committed or rolled-back `Transaction` is closed and cannot be reused — a
second `commit()` throws `InvalidStateException`. Start a new one instead.

`TxnKvClient::create()` accepts the same `tls`, `timeout` and `metrics`
options as `RawKvClient`, plus `retryDeadlineMs` (the wall-clock bound on one
transaction's internal retry loop, default
`Transaction::DEFAULT_RETRY_DEADLINE_MS`), `gcSafePointValidation`,
`replicaRead`, `enable1Pc`, `enableAsyncCommit` and `tsoPoolSize`.

#### Reads and writes

```php
$value  = $txn->get('account:1');                        // ?string
$values = $txn->batchGet(['account:1', 'account:2']);    // array<array-key, ?string>
$rows   = $txn->scan('account:', 'account;', limit: 100); // array<array{key, value}>

$txn->set('account:1', '100');
$txn->delete('account:2');
```

Reads are **snapshot reads at `startTs`** and observe your own buffered
writes, so `get()` after `set()` returns the new value without a round trip.
They do *not* see other transactions' uncommitted data.

> `Transaction::scan()` differs from `RawKvClient::scan()`: `limit: 0` means
> "up to 10240 rows", not "the whole range" — a transactional scan does not
> page internally. Pass an explicit `limit` when you need one.

#### Optimistic vs pessimistic

`pessimistic` defaults to **true** — a transaction started with `begin()` is a
**pessimistic** one. That is the opposite of the TiKV documentation and of
client-go and client-rust, where optimistic is the default; the difference is
tracked in issue #439. The default is kept for now (changing it is a breaking
change that belongs in a major release), so state the mode explicitly in new
code:

```php
// pessimistic — the physical lock is taken inside set()/delete()
$txn = $txnClient->beginPessimistic();

// optimistic — the physical lock is taken during the prewrite at commit()
$txn = $txnClient->beginOptimistic();
```

Both are thin wrappers over `begin()`: they accept the same options
(`priority`, `enable1Pc`, `enableAsyncCommit`, `eagerPessimisticLocks`,
`lockWaitTimeMs`) and go through the same option handling. `lockWaitTimeMs`
matters in the pessimistic mode only — an optimistic transaction takes no
pessimistic lock, so it never waits for one.

| | Pessimistic (default) | Optimistic |
|---|---|---|
| Lock timing | physical lock during each `set()` / `delete()` | lock acquired during prewrite at `commit()` |
| Where a conflict surfaces | the write call, or `commit()` for the deferred mode | `commit()` |
| Lock TTL | fixed 30 s | `3000 ms + 10 ms` per write-set mutation, capped at `120000 ms` |
| Fits | contended writes (transfers, counters) | low contention (config, caching) |

The value stays buffered until `commit()` in both modes. In default eager
pessimistic mode the physical lock is taken before `set()` or `delete()`
returns, so a conflict, deadlock or wait timeout raises there. A preceding
pessimistic read's per-key `for_update_ts` is reused by the matching write, and
prewrite keeps `DO_CONSTRAINT_CHECK` as a safety net. The legacy deferred pass
is available as `begin(['eagerPessimisticLocks' => false])`, which moves the
lock pass to `commit()`.

`begin(['lockWaitTimeMs' => N])` bounds how long TiKV waits for a contended
key before it answers with a "locked" error (default 1000 ms, the timeout
TiKV applies by default). The value sent to TiKV is
`min(lockWaitTimeMs, remaining lock-wait budget)`: it never exceeds the budget
and shrinks as the budget is consumed. When the budget runs out the
transaction fails with `LockWaitTimeoutException` instead of committing
without the lock. See [Configuration](docs/configuration.md#pessimistic-lock-wait-timeout-txnkv).

#### Isolation, timestamps and status

Snapshot isolation at `startTs`, a timestamp from PD's TSO. A transaction sees
the database as of `startTs` plus its own writes; it never observes a partial
commit.

```php
$txn->getTxnId();       // string, unique per transaction
$txn->getStartTs();     // int, the snapshot timestamp
$txn->getCommitTs();    // ?int, null until commit() succeeds
$txn->getStatus();      // TransactionStatus::{Active, Committed, RolledBack, Undetermined}
$txn->isPessimistic();  // bool
$txn->getPriority();    // int
$txn->getWriteSet();    // array<array-key, ?string>, buffered writes
$txn->getReadSet();     // array<array-key, ?string>, resolved reads
```

`getWriteSet()`, `getReadSet()` and `batchGet()` return a **map**, and PHP
coerces a canonical decimal-integer key such as `'1000'`, `'0'` or `'-5'` to
its `int` array-key form — so a `foreach` key can be an `int` even though you
passed a `string`. The lookup is lossless (`$values['1000']` finds the entry
stored as int 1000); a key beyond `PHP_INT_MAX` stays a `string`. Cast a
`foreach` key with `(string)` before handing it to a `string`-typed parameter.
See [`docs/helpers/faq.md`](docs/helpers/faq.md).

`Undetermined` means the primary commit RPC failed at the transport level: the
commit may already have been applied. Never roll back such a transaction —
resolve it out of band (for example by re-checking the primary key).

#### Failure and retry

Retrying a transaction means starting a **new** one. A conflict leaves the old
`startTs` already behind, so re-running the same object cannot succeed.

| Exception | What to do |
|---|---|
| `TransactionConflictException` | discard, **new transaction** |
| `DeadlockException` | discard, **new transaction** |
| `LockWaitTimeoutException` | discard, **new transaction** |
| `TxnRetryableException` | escapes only when the internal retry budget ran out — **new transaction** |
| `TxnAbortedByGcException` | the `startTs` is behind the cluster's GC safe point. **New transaction**; for reads that must outlive `gc_life_time`, call `TxnKvClient::holdGcSafePoint()` first |
| `UndeterminedCommitException` | outcome unknown — do **not** roll back, resolve out of band |
| `RegionException`, `GrpcException`, `RetryBudgetExhaustedException` | already retried internally; retry only if the operation is idempotent |
| `InvalidStateException`, `InvalidArgumentException` | programming or lifecycle error — never retry |

The full table, with accessors and the reasoning per row, is in
[Error Handling](docs/error-handling.md#caller-retryability).

#### Long-running transactions

A transaction that stays open between operations keeps its locks only for the
granted TTL, and **heartbeats are not automatic between your own calls** — the
client only heartbeats the primary lock on its own while the prewrite loop
runs. Call `Transaction::heartbeat()` before the previously granted TTL
elapses (10 s is a safe default); it returns the TTL TiKV actually granted.

```php
$txn = $txnClient->begin();
$txn->set('account:1', '100');

// ... a long computation or an external call ...

$grantedTtlMs = $txn->heartbeat(10000);

$txn->set('account:2', '0');
$txn->commit();
```

#### Abandoned transactions

`Transaction::__destruct()` rolls back a transaction that is still `Active`.
That is a safety net, not a mechanism to rely on: destruction order at
`shutdown` is not guaranteed, the rollback is a network RPC, and a failure is
only logged. Always `commit()` or `rollback()` in your own code.

#### Complete example

This is `examples/txn.php` — run it verbatim against a TxnKV cluster:

```bash
docker compose -f docker-compose.yml -f docker-compose.txnkv.yml down -v
docker compose -f docker-compose.yml -f docker-compose.txnkv.yml up -d pd tikv1
php examples/txn.php
```

```php
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

/**
 * Move $amount from one account to another atomically, retrying on conflict.
 *
 * The retry has to build a NEW transaction: the failed one holds a
 * startTs that has already lost the race, so re-running the same object can
 * never succeed.
 */
function transfer(TxnKvClient $client, string $from, string $to, int $amount): void
{
    for ($attempt = 1; ; $attempt++) {
        $txn = $client->begin(); // pessimistic by default
        try {
            $fromBalance = (int) $txn->get($from);
            $toBalance = (int) $txn->get($to);

            if ($fromBalance < $amount) {
                throw new RuntimeException("Insufficient funds in {$from}");
            }

            $txn->set($from, (string) ($fromBalance - $amount));
            $txn->set($to, (string) ($toBalance + $amount));
            $txn->commit();

            return;
        } catch (TransactionConflictException | DeadlockException | LockWaitTimeoutException | TxnRetryableException $e) {
            if ($txn->getStatus() === TransactionStatus::Active) {
                $txn->rollback();
            }
            if ($attempt >= 5) {
                throw $e;
            }
            echo "  Conflict on attempt {$attempt} ({$e->getMessage()}), retrying...\n";
            usleep(50_000 * $attempt);
        } catch (Throwable $e) {
            if ($txn->getStatus() === TransactionStatus::Active) {
                $txn->rollback();
            }
            throw $e;
        }
    }
}
```

The `getStatus() === Active` guard matters: after an undetermined commit the
transaction is closed, and calling `rollback()` on it would throw a second
exception over the first.

### TLS/SSL Configuration

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;

// Configure TLS with CA certificate only (server verification)
$options = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        'caCertBaseDir' => '/path/to', // optional: restrict to base directory
    ],
];

// Or with mutual TLS (mTLS) - client certificate authentication
$options = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        // Optional base-directory restrictions (*BaseDir apply only to their
        // matching file reads; caCertBaseDir → caCertFile, clientCertBaseDir →
        // clientCertFile + clientKeyFile)
        'caCertBaseDir' => '/path/to',
        'clientCertFile' => '/path/to/client.crt',
        'clientKeyFile' => '/path/to/client.key',
        'clientCertBaseDir' => '/path/to',
    ],
];

// For inline PEM content, use the *Pem variants:
$options = [
    'tls' => [
        'caCertPem' => $caPemString,
        'clientCertPem' => $clientCertPemString,
        'clientKeyPem' => $clientKeyPemString,
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

The `*BaseDir` options are optional and restrict **where** the certificate
files may live: the path is canonicalized with `realpath()` before the
comparison, so a `..` segment or a symlink cannot pull in a file from outside
the base directory. `caCertBaseDir` guards `caCertFile`; `clientCertBaseDir`
guards `clientCertFile` and `clientKeyFile`.

> **Deprecated:** the legacy `caCert`, `clientCert` and `clientKey` keys guess
> between a file path and inline PEM content and cannot be restricted to a base
> directory. They still work for backward compatibility — use the `*File` /
> `*Pem` keys above instead.

To make a misconfigured TLS setup fail instead of silently downgrading to
plaintext, set `allowInsecure` to `false` — the client then throws
`InvalidStateException` when it cannot open a TLS channel:

```php
$options = [
    'tls' => ['caCertFile' => '/path/to/ca.crt'],
    'allowInsecure' => false, // recommended for production
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

### Connection Lifetime and Transport Ownership

The gRPC transport behind a client is **long-lived and shared**: it keeps one
open channel per address, and that cache only pays off when the same
transport serves many requests. Create the client once (at the start of a
worker, or in a container) and reuse it — a client per request throws the
cache away and pays a reconnect every time.

Closing is the mirror image of creating:

- a client created by `RawKvClient::create()` / `TxnKvClient::create()` owns
  the transport it built and closes it in `close()`;
- a transport **you** build and pass in (`options['grpcClient']`) belongs to
  your application — the client never closes it, so the same transport can
  serve a RawKV and a transactional client. Close it yourself;
- a client you assemble by hand (`new RawKvClient($pdClient, $grpc)`) closes
  the transport it was **given** — pass `ownsGrpcClient: false` when your
  application keeps the transport and wants to close it itself.

```php
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;

// The transport the application builds and owns.
// $tlsConfig is your TlsConfig (null = plaintext); $logger your PSR-3 logger.
$grpc = new GrpcClient($logger, $tlsConfig);
$options = ['grpcClient' => $grpc];

$raw = RawKvClient::create(['127.0.0.1:2379'], $logger, $options);
$txn = TxnKvClient::create(['127.0.0.1:2379'], $logger, $options);

// …use both clients…

// The application owns $grpc:
$grpc->close();
```
### PSR-3 Logging

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a PSR-3 compatible logger
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Pass logger to client
$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);

// The client will log:
// - Connection attempts and failures
// - Retry attempts with backoff information
// - Region cache hits/misses
// - Error conditions
```

### Complete Example

```php
<?php
require 'vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Setup logging
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::INFO));

// Connect to TiKV
$pdEndpoints = ['127.0.0.1:2379'];
$client = RawKvClient::create($pdEndpoints, logger: $logger);

try {
    // Store user data with TTL
    $client->put('user:123', json_encode(['name' => 'Alice', 'age' => 30]), ttl: 3600);
    $client->put('user:456', json_encode(['name' => 'Bob', 'age' => 25]), ttl: 3600);
    
    // Batch retrieve
    $users = $client->batchGet(['user:123', 'user:456']);
    foreach ($users as $key => $value) {
        if ($value !== null) {
            $data = json_decode($value, true);
            echo "$key: {$data['name']}\n";
        }
    }
    
    // Scan all users with the lazy iterator (constant memory)
    $userCount = 0;
    foreach ($client->scanPrefixIterator('user:') as $_) {
        $userCount++;
    }
    echo "Total users: $userCount\n";
    
    // Check TTL
    $ttl = $client->getKeyTTL('user:123');
    echo "TTL remaining: $ttl seconds\n";
    
    // Enable atomic mode for CAS operations
    $client->setAtomicForCAS(true);
    
    // Atomic counter update
    $client->put('counter', '0');
    $result = $client->compareAndSwap('counter', '0', '1');
    if ($result->swapped) {
        echo "Counter incremented!\n";
    }
    
} finally {
    $client->close();
}
```

## Implemented Operations

### Core CRUD
- ✅ **Get** / **Put** / **Delete** — Single key operations
- ✅ **BatchGet** / **BatchPut** / **BatchDelete** — Batch operations with parallel execution

### Scanning
- ✅ **Scan** — Range scan `[startKey, endKey)` with limit and keyOnly options
- ✅ **ReverseScan** — Reverse range scan (native `reverse=true`)
- ✅ **ScanPrefix** — Prefix-based scanning
- ✅ **ScanIterator / ScanPrefixIterator** — Lazy auto-paginating scan iterators (`ScanIterator`), constant memory for large ranges
- ✅ **BatchScan** — Multiple non-contiguous range scanning

### Range Operations
- ✅ **DeleteRange** — Delete all keys in `[startKey, endKey)`
- ✅ **DeletePrefix** — Delete all keys with a given prefix

### TTL
- ✅ **PutWithTTL** — Store with expiration (seconds)
- ✅ **GetKeyTTL** — Get remaining TTL of a key

### Atomic Operations
- ✅ **CompareAndSwap** — Atomic CAS with `CasResult` (swapped + previousValue)
- ✅ **PutIfAbsent** — Conditional insert (returns existing value or null)

### Data Integrity
- ✅ **Checksum** — CRC64-XOR checksum over key range with `ChecksumResult`

### Transactions
- ✅ **TxnKvClient / Transaction** — ACID transactions with snapshot reads and two-phase commit
- ✅ **Pessimistic & Optimistic** — pessimistic locks by default; lock heartbeat for long transactions
- ✅ **Conflict retry** — typed `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`
- ✅ **GC safe points** — `holdGcSafePoint()` / `releaseGcSafePoint()` for reads that outlive `gc_life_time`
- ✅ **Replica reads** — `options['replicaRead']` for follower reads inside a transaction

### Infrastructure
- ✅ **PD Region Discovery** — With RegionEpoch support
- ✅ **Region Routing** — Direct to correct TiKV node
- ✅ **Region Cache** — In-memory caching of region metadata
- ✅ **Store Cache** — In-memory caching of store addresses
- ✅ **Retry Logic** — Automatic retry with exponential backoff
- ✅ **NotLeader Handling** — Automatic leader redirection
- ✅ **Batch Async Execution** — Parallel execution across regions
- ✅ **TLS/SSL Support** — Server and mutual TLS authentication
- ✅ **PSR-3 Logging** — Structured logging with any PSR-3 logger

## Project Structure

```
src/
├── Client/
│   ├── Batch/
│   │   ├── BatchAsyncExecutor.php   # Concurrent fan-out with deadline-bounded collection
│   │   ├── CheckedGrpcFuture.php    # Lazy region-error check around dispatch futures
│   │   └── GrpcFuture.php           # Async gRPC operations
│   ├── Cache/
│   │   ├── RegionCache.php          # Region metadata cache
│   │   └── StoreCache.php           # Store address cache
│   ├── Connection/
│   │   └── PdClient.php             # PD discovery & region routing
│   ├── Grpc/
│   │   └── GrpcClient.php           # Low-level gRPC wrapper
│   ├── RawKv/
│   │   ├── RawKvClient.php          # Main client (20+ operations)
│   │   ├── CasResult.php            # CompareAndSwap result
│   │   ├── ChecksumResult.php       # Checksum result
│   │   ├── ScanIterator.php         # Lazy auto-paginating scan iterator
│   │   └── Dto/                     # Shared DTOs (KeyValue)
│   ├── Region/
│   │   ├── RegionResolver.php       # Region resolution & caching
│   │   ├── RegionContextFactory.php  # Region context factory
│   │   ├── RegionRangeClipper.php   # Range clipping
│   │   ├── RegionGrouper.php        # Group keys by region
│   │   ├── RegionErrorHandler.php   # Region error checking
│   │   └── Dto/                     # Region DTOs (RegionInfo, PeerInfo)
│   ├── Retry/
│   │   └── BackoffType.php          # Retry backoff strategies
│   ├── TxnKv/
│   │   ├── TxnKvClient.php          # Transactional client (create/begin)
│   │   ├── Transaction.php          # Transaction (get/set/commit/rollback)
│   │   ├── TxnReader.php            # Snapshot reads at startTs
│   │   ├── TwoPhaseCommitter.php    # Prewrite, commit, locks, heartbeat
│   │   ├── LockResolver.php         # Conflict/lock resolution
│   │   ├── TransactionState.php     # Mutable transaction state machine
│   │   ├── TransactionStatus.php    # Active/Committed/RolledBack/Undetermined
│   │   └── Exception/               # Conflict, deadlock, lock-wait, GC, undetermined
│   └── Tls/
│       ├── TlsConfig.php            # TLS configuration
│       └── TlsConfigBuilder.php     # TLS builder
└── Proto/                           # Generated protobuf classes
    ├── Kvrpcpb/                     # TiKV request/response
    ├── Pdpb/                        # PD request/response
    └── Tikvpb/                      # gRPC service stubs

tests/
├── Unit/                            # Unit tests
└── E2E/                             # End-to-end tests

examples/
├── basic.php                        # Basic CRUD example
├── batch.php                        # Batch operations example
├── scan.php                         # Scanning examples
├── txn.php                          # Transactions with conflict retry
├── ttl.php                          # TTL operations example
├── atomic.php                       # Atomic operations example
├── tls.php                          # TLS configuration example
└── logging.php                      # PSR-3 logging example
```

## Available Commands (Makefile)

```bash
make install          # Install PHP dependencies
make test             # Run all tests (unit + e2e)
make test-unit        # Run unit tests only
make test-e2e         # Run E2E tests with TiKV cluster
make proto-generate   # Generate PHP classes from proto files
make proto-clean      # Remove generated proto classes
make build            # Build Docker images
make up               # Start TiKV cluster
make down             # Stop TiKV cluster
make logs             # Show TiKV cluster logs
make clean            # Clean everything (containers + volumes)
make example          # Run basic example
make shell            # Open development shell
```

## Examples

See the `examples/` directory for complete working examples:

- **basic.php** — Basic CRUD operations
- **batch.php** — Batch operations with parallel execution
- **scan.php** — Range scanning and prefix scanning
- **txn.php** — Transactions: transfer, snapshot reads, rollback, conflict retry
- **ttl.php** — Time-to-live operations
- **atomic.php** — Compare-and-swap and put-if-absent
- **tls.php** — TLS/SSL configuration
- **logging.php** — PSR-3 logging integration

Run any example:
```bash
make up  # Start TiKV cluster first
php examples/basic.php
```

> `examples/txn.php` needs a cluster in default (V1) mode, so use
> `docker-compose.txnkv.yml` and start from fresh volumes — TiKV refuses to
> disable TTL on a cluster that was bootstrapped with it.

## Documentation

- **[Getting Started](docs/getting-started.md)** — Installation, setup, and your first TiKV operations
- **[Configuration](docs/configuration.md)** — Client options, TLS, timeouts, retry budgets, logging
- **[Operations](docs/operations.md)** — Complete guide to all RawKV operations
- **[Advanced Features](docs/advanced.md)** — Production-ready patterns and optimization
- **[Error Handling](docs/error-handling.md)** — Exception hierarchy, per-operation exceptions and retryability
- **[Troubleshooting](docs/troubleshooting.md)** — Common issues and solutions

For TxnKV, this README's [Transactions](#transactions-txnkv) section is the
entry point; `docs/error-handling.md` carries the full per-exception
retryability matrix and
`[Transaction Operations](docs/error-handling.md#transaction-operations)`
covers where each transactional exception originates.

## Configuration

### Client Options

```php
$options = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',      // Server CA certificate (file path)
        'clientCertFile' => '/path/to/client.crt', // Client certificate (for mTLS)
        'clientKeyFile' => '/path/to/client.key',  // Client private key (for mTLS)
        // Or inline PEM: caCertPem, clientCertPem, clientKeyPem
    ],
    'allowInsecure' => false,     // Fail closed: refuse a plaintext channel
    'maxChannels' => 64,          // Open gRPC channels kept at the same time
    'idleTtlMs' => 600000,        // Idle time before an unused channel is evicted

    // Slow-operation logging (opt-in): a PSR-3 warning is logged when an
    // operation takes longer than its threshold (milliseconds). Without this
    // key, slow logging is off entirely; a threshold of 0 (or a negative one)
    // disables that one operation type.
    // See docs/configuration.md#slow-operation-logging.
    'slowLog' => [
        'readThresholdMs'        => 100,  // default 100  (get, getKeyTTL)
        'writeThresholdMs'       => 200,  // default 200  (put, delete, CAS)
        'batchReadThresholdMs'   => 500,  // default 500  (batchGet)
        'batchWriteThresholdMs'  => 500,  // default 500  (batchPut, batchDelete)
        'scanThresholdMs'        => 1000, // default 1000 (scan, batchScan)
        'deleteRangeThresholdMs' => 2000, // default 2000 (deleteRange)
        'checksumThresholdMs'    => 3000, // default 3000 (checksum)
    ],];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,           // PSR-3 logger (optional)
    options: $options          // Additional options (optional)
);
```

The full list of options lives in [docs/configuration.md](docs/configuration.md).

The `slowLog` option logs a warning with the message `Slow TiKV operation` and
the operation type, the redacted key, the duration and the threshold — see
[Slow Operation Logging](docs/configuration.md#slow-operation-logging) for the
full reference.
### TiKV Configuration

For TTL support, enable it in tikv.toml:

```toml
[storage]
enable-ttl = true
```

> **Note:** TTL mode is exclusive with TxnKV — see
> [TTL (Time-To-Live)](#ttl-time-to-live).

## Roadmap

### Recently Completed
- ✅ TLS/SSL Support
- ✅ PSR-3 Logging
- ✅ Region & Store Caching
- ✅ Connection Pooling
- ✅ Batch Async Execution
- ✅ Retry with Exponential Backoff
- ✅ Per-key TTL in BatchPut
- ✅ Scan limit enforcement (MAX 10240 per RPC; `limit: 0` auto-paginates)
- ✅ Batch auto-splitting by size/count

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass: `make test`
5. Submit a pull request

## License

MIT
