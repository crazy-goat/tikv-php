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

## Stability and versioning

The library is **pre-1.0**. It follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), but before `1.0.0`
a breaking change bumps the **minor** version — so a `0.x` release may break
your code, and `0.9.0 → 0.10.0` is not a safe upgrade on its own.

[`CHANGELOG.md`](CHANGELOG.md) marks every breaking change of the public API
below with a **BREAKING** prefix and a migration note. A change that breaks a
class outside that surface is announced as **Breaking change:** with a
migration note and needs no version bump of its own.

### Which version to require

The current release is **v0.9.0**. Require the exact version, or allow patch
releases inside the current minor line only:

```json
{
    "require": {
        "crazy-goat/tikv-client": "~0.9.0"
    }
}
```

`~0.9.0` (equivalently `^0.9.0` or `0.9.*`) resolves to `>=0.9.0 <0.10.0`, so
it never pulls `0.10.0` — and therefore never pulls a breaking change. Use
`"0.9.0"` instead when you want a reproducible build.

Avoid the constraints that *do* admit the next minor: `^0`, `0.*`, `*` and
`~0.9` all resolve to a range that includes `0.10.0` and every later release.

The versions above follow the releases: every one of them is updated in the
same pull request as the changelog section of the release, because the guard
test reads the latest released version from `CHANGELOG.md` and fails until
every version of this section names the current release or the one that
follows it. The step is on the
[release checklist](docs/release-workflow.md#checklist).

### Public API

These are covered by the stability promise: they may only change in a way
that keeps existing, correct code working.

| Surface | What it covers |
|---|---|
| `RawKv\RawKvClient` | every public method — the RawKV entry point |
| `TxnKv\TxnKvClient` | every public method — `create()`, `begin()` (with its `pessimistic`, `priority`, `enable1Pc`, `enableAsyncCommit` and `eagerPessimisticLocks` options), `beginOptimistic()`, `beginPessimistic()`, `holdGcSafePoint()`, `releaseGcSafePoint()`, `getClusterId()`, `healthCheck()`, `getMetrics()`, `close()` — the TxnKV entry point |
| `TxnKv\Transaction` | every public method — the transaction handle |
| `RawKv\CasResult`, `RawKv\ChecksumResult` | the result DTOs of `compareAndSwap()` and `checksum()` |
| `RawKv\ScanIterator` | the iterator returned by `scanIterator()` and `scanPrefixIterator()` |
| `TxnKv\TransactionStatus` | the enum returned by `Transaction::getStatus()` |
| `Exception\*` | `TiKvException` and every subclass, plus `InvalidArgumentException` (which extends PHP's `\InvalidArgumentException`, not `TiKvException`) |
| `TxnKv\Exception\*` | the six transaction exceptions: `DeadlockException`, `LockWaitTimeoutException`, `TransactionConflictException`, `TxnAbortedByGcException`, `TxnRetryableException`, `UndeterminedCommitException` |
| `Retry\RetryBudgetExhaustedException` | thrown when the internal retry budget is exhausted |
| `Retry\ErrorKind` | the enum behind `RegionException::$errorKind` — the typed variant of a region error |
| `Observability\MetricsInterface` | the metrics backend you implement and pass as `options['metrics']` |
| `Observability\InMemoryMetrics` | the ready-made in-process backend, for tests and benchmarks |
| `Region\ReplicaReadMode`, `Region\ReplicaReadPolicy` | the values you pass as `options['replicaRead']` |
| the documented `options[]` keys | every key documented in the [configuration guide](docs/configuration.md) — `tls`, `timeout`, `metrics`, `apiVersion`, `keyspace`, `replicaRead`, `batchCommands`, `maxScanRows`, `maxConcurrency`, `tsoPoolSize`, … |

The namespaces above are relative to `CrazyGoat\TiKV\Client\`. One class
outside that namespace is part of the promise as well:
`CrazyGoat\Proto\Errorpb\NotLeader`, the message behind
`RegionException::$notLeader`.

### Internal

Every other class of `src/Client/` is internal: it may change in **any**
release, with no version bump and no `BREAKING` note, because it is not part
of the contract.

- the namespaces `Batch\`, `Cache\`, `Codec\`, `Connection\`, `Grpc\`,
  `Tls\`, `Util\`;
- `Region\` — except `Region\ReplicaReadMode` and `Region\ReplicaReadPolicy`,
  which the table above covers;
- `Observability\NoOpMetrics` — the default backend `getMetrics()` returns
- `Observability\DebugLogging` — the internal probe that decides whether a logger can emit a `debug` record
  when no `options['metrics']` is configured; write against
  `Observability\MetricsInterface`, which is covered;
- `RawKv\Dto\` (the `RegionInfoMapper` helper) and `RawKv\RawKvSplitter`;
- the collaborators behind the entry points: `RawKv\RawKvCrud`,
  `RawKv\RawKvAtomic`, `RawKv\RawKvBatch`, `RawKv\RawKvScanner`,
  `RawKv\RawKvRangeOps`, `RawKv\SstIngestor`, `TxnKv\TxnReader`,
  `TxnKv\TwoPhaseCommitter`, `TxnKv\LockResolver`, `TxnKv\TransactionState`,
  `TxnKv\RollbackRegroupSignal`;
- `Retry\` — except `Retry\RetryBudgetExhaustedException` and
  `Retry\ErrorKind`, which the table above covers: `Retry\Backoff`,
  `Retry\BackoffType`, `Retry\ErrorClassifier` and `Retry\RetryExecutor`;
- the generated protobuf and gRPC classes under `src/Proto/` — except
  `Errorpb\NotLeader`, which the promise covers.

The advanced extension points — `Retry\RetryExecutor` and `Retry\BackoffType`
in [Custom Retry (Advanced)](docs/configuration.md#custom-retry-advanced), and
`Tls\TlsConfigBuilder` — are documented, but they are internal like the
rest: they change whenever the internals they wrap change.
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
//
// A scan never truncates silently: an unbounded scan that hits its cap
// throws, and a bounded scan (limit > 0) returns at most `limit` rows —
// compare the returned count with the limit to see whether the range may
// hold more.

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
`replicaRead` and `tsoPoolSize`. The per-transaction options
(`pessimistic`, `priority`, `enable1Pc`, `enableAsyncCommit`,
`eagerPessimisticLocks` and `lockWaitTimeMs`) belong to
`TxnKvClient::begin()`, not to `create()`.

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
> "the whole range, up to 10240 rows" — a transactional scan does not page
> internally. A range with more rows than that does **not** come back
> truncated: the scan throws `ScanLimitExceededException` instead, so a
> partial range is never mistaken for a complete one. Pass an explicit
> `limit` when you need one.

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

`options['metrics']` stays a client-side option: it backs the client-side
counters (region cache, retries) of every client, injected transport or not.
The transport's own RPC counters follow the transport — pass the same backend
to it (`new GrpcClient(metrics: $metrics)`) to get them everywhere.

#### What closing a transport really does

ext-grpc keeps a **process-global** table of persistent channels, keyed by the
target, a hash of the channel arguments and a hash of the credentials. Two
`Grpc\Channel` objects built from the same key therefore share **one**
underlying `grpc_channel` — as long as the key is in that table. The table is
bounded **per target** (one persisted channel per target by default), so a
channel created beyond that bound is private to its handle: ext-grpc logs "The
number of channel for the target is maxed out bounded" and does not persist
it. A private channel is never shared, so it is never at risk either.

`Channel::close()` is a **reference-count decrement**
(`php_grpc_channel_unref()`), not a destroy: the underlying channel survives
until its last holder releases it, and the persistent table is one of those
holders. So closing one client never invalidates a channel another client
holds — it only drops that client's own reference to it. Verified against the
deployed extension (grpc 1.83.0; the persistent-channel code is unchanged in
the oldest supported release, 1.57).

Two consequences for the API:

- `close()` releases this client's handles; the TCP connection stays warm in
  the process (until the process ends or the per-target persist bound drops
  the entry). It does not tear the connection down.
- `closeChannel()` drops the cached handle but does **not** force a
  reconnect: the persistent entry survives, so the next call reuses the same
  underlying channel and the same TCP connection. Forcing a real reconnect
  needs a new persistent entry.

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

Every public capability of the library, taken from `src/Client/` — the
`RawKvClient`, the `TxnKvClient` (including its explicit
`beginOptimistic()` / `beginPessimistic()` entry points), the `Transaction` and
the client options.
The list is cross-checked against `CHANGELOG.md` at release time, see
[docs/release-workflow.md](docs/release-workflow.md).

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
- ✅ **Atomic mode** — `setAtomicForCAS(true)` / `isAtomicForCAS()` switch the write path to TiKV's atomic code path

### Data Integrity
- ✅ **Checksum** — CRC64-XOR checksum over key range with `ChecksumResult`

### Bulk Import
- ✅ **Ingest** — SST bulk import: the key/value pairs are sorted and written straight into the regions, bypassing the Raft write path (optional TTL)

### Transaction Capabilities
- ✅ **TxnKvClient / Transaction** — ACID transactions with snapshot reads and two-phase commit
- ✅ **Pessimistic & Optimistic** — pessimistic locks by default; lock heartbeat for long transactions
- ✅ **Conflict retry** — typed `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`
- ✅ **GC safe points** — `holdGcSafePoint()` / `releaseGcSafePoint()` for reads that outlive `gc_life_time`
- ✅ **Replica reads** — `options['replicaRead']` picks the replica that serves a read — `ReplicaReadMode::Leader`, `::Follower`, `::Mixed` or `::PreferLeader` — on both clients
- ✅ **One-phase and async commit** — the `begin()` options `enable1Pc` and `enableAsyncCommit`
- ✅ **Transaction state** — `getTxnId()`, `getStartTs()`, `getCommitTs()`, `getStatus()`, `isPessimistic()`, `getPriority()`, `getWriteSet()`, `getReadSet()`

### Observability
- ✅ **Metrics** — `options['metrics']` injects a `MetricsInterface` backend (`InMemoryMetrics` ships with the library for tests and benchmarks); `getMetrics()` returns the backend in use
- ✅ **Health check** — `healthCheck()` probes PD with a lightweight `GetMembers` RPC and returns the cluster ID
- ✅ **Cluster ID** — `getClusterId()` returns the cluster ID learned from PD
- ✅ **Slow log** — `options['slowLog']` logs the RawKV operations that are slower than their per-operation threshold
- ✅ **PSR-3 Logging** — Structured logging with any PSR-3 logger

### API Versions and Codec
- ✅ **API V1 and V2** — `options['apiVersion']` and `options['keyspace']`; V2 prefixes the keys with the mode and the keyspace ID
- ✅ **Memory-comparable encoding** — `MemComparableCodec` encodes the PD region boundaries (TiDB-compatible `EncodeBytes`)

### Configuration Options

Everything in the table is passed in the `options` array of
`RawKvClient::create()` / `TxnKvClient::create()`. The per-transaction
options of `TxnKvClient::begin()` are listed below the table. The full
reference, with the defaults, is in [Configuration](docs/configuration.md).

| Option | What it does |
|---|---|
| `tls` | Server or mutual TLS, from file paths or inline PEM |
| `allowInsecure` | `false` refuses to open a channel without usable TLS credentials |
| `timeout` | A gRPC deadline per operation class — read, write, batch, scan, delete-range, checksum, ingest, PD, TSO, lock resolution |
| `slowLog` | Thresholds above which an operation is logged as slow |
| `metrics` | A `MetricsInterface` backend for the RPC, retry and region-cache counters |
| `retryDeadlineMs` | Wall-clock bound on one retry loop |
| `maxConcurrency` | Maximum number of in-flight requests in the batch and range fan-outs |
| `maxScanRows` | Row cap for an unbounded (`limit: 0`) scan |
| `replicaRead` | A `ReplicaReadPolicy` object — `ReplicaReadMode::Leader`, `::Follower`, `::Mixed` or `::PreferLeader` ([Replica Reads](docs/configuration.md#replica-reads)) |
| `batchCommands` | RawKV experimental RPC multiplexing over the `BatchCommands` stream |
| `apiVersion` / `keyspace` | API V2 with a named keyspace |
| `grpc` | gRPC channel arguments (keepalive, message sizes) |
| `maxChannels` / `idleTtlMs` | The channel pool — how many gRPC channels the transport keeps open and after how long an unused one is evicted ([Channel Pool](docs/configuration.md#channel-pool-maxchannels-idlettlms)) |
| `grpcClient` | A transport the application built — the client uses it as it is and never closes it ([Sharing One Transport](docs/configuration.md#sharing-one-transport-between-clients)) |
| `tsoPoolSize` / `lowResTimestampMaxStalenessMs` | TSO batching and the low-resolution timestamp cache of the TxnKV timestamp oracle |
| `allowedStoreHosts` / `storeHostPolicy` / `allowedStorePorts` | Which store addresses the client is allowed to talk to |
| `gcSafePointValidation` / `gcSafePointRefreshMs` | TxnKV GC safe-point validation and its refresh interval |

The transport options (`tls`, `allowInsecure`, `maxChannels`, `idleTtlMs`,
`grpc`) cannot be combined with `grpcClient` — they configure a transport the
client does not build, so they are rejected instead of being ignored.

`TxnKvClient::begin()` takes its own options per transaction: `pessimistic`
(the default is `true`), `priority` (0 Normal, 1 Low, 2 High), `enable1Pc`
(one-phase commit), `enableAsyncCommit`, `eagerPessimisticLocks` (pessimistic
locking at write time — the default — or at commit time) and `lockWaitTimeMs`
(how long TiKV may wait for a contended lock).

Two settings are methods on the client rather than options:
`setAtomicForCAS()` / `isAtomicForCAS()` (see
[Atomic Operations](#atomic-operations)) and `setColumnFamily()` /
`getColumnFamily()`, which selects the column family of the RawKV requests.

### Error Handling
- ✅ **Typed exceptions** — a dedicated exception class per failure mode; all of them but `InvalidArgumentException` (which extends PHP's `\InvalidArgumentException`) extend `TiKvException`; the most common exceptions and their retryability verdict are listed in [Error Handling](docs/error-handling.md)

### Infrastructure
- ✅ **PD Region Discovery** — With RegionEpoch support
- ✅ **Region Routing** — Direct to correct TiKV node
- ✅ **Region Cache** — In-memory caching of region metadata; a custom `RegionCacheInterface` can be injected through the constructor
- ✅ **Store Cache** — In-memory caching of store addresses
- ✅ **Retry Logic** — Automatic retry with exponential backoff; the wall-clock bound of one retry loop is `options['retryDeadlineMs']` (see [Retry System](docs/architecture.md#8-retry-system))
- ✅ **NotLeader Handling** — Automatic leader redirection
- ✅ **Batch Async Execution** — Parallel execution across regions
- ✅ **TLS/SSL Support** — Server and mutual TLS authentication
- ✅ **Lifecycle** — `close()` clears the region cache and closes the PD connection; the gRPC transport is closed with it only when the client owns it — a transport passed in through `options['grpcClient']` belongs to the application and is never closed by the client (see [Connection Lifetime and Transport Ownership](#connection-lifetime-and-transport-ownership))

## Project Structure

The tree below is verified against the filesystem by
`tests/Unit/Docs/ReadmeProjectStructureTest.php`: every directory under
`src/Client/`, every test suite and every example script has to appear in it,
and every file, directory or class it names has to exist. A new package
therefore fails the unit suite until the tree names it.

The tree is a map, not a file inventory. It names the test suites and not the
directories below `tests/Unit/`, and it names the generated protobuf packages
the library uses most and not all of them — the rest come with the proto
submodule and change with it, so they are not checked for completeness.

```
src/
├── Client/
│   ├── Batch/                         # Concurrent fan-out across regions
│   │   └── BatchCommands/             # BatchCommands multiplexing over a bidi stream
│   ├── Cache/                         # Region and store metadata caches
│   ├── Codec/                         # Key encoding (CodecV1, CodecV2, MemComparableCodec)
│   ├── Connection/                    # PD client, keyspace resolver, TSO oracle
│   ├── Exception/                     # Exception hierarchy (TiKvException, GrpcException, RegionException)
│   ├── Grpc/                          # Low-level gRPC transport, deadlines, timeouts
│   ├── Observability/                 # Metrics collection (MetricsInterface, NoOpMetrics)
│   ├── RawKv/                         # RawKV API (RawKvClient, RawKvCrud, RawKvBatch, RawKvScanner)
│   │   └── Dto/                       # RawKV DTOs (RegionInfoMapper)
│   ├── Region/                        # Region resolution, grouping, range clipping
│   │   └── Dto/                       # Region DTOs (RegionInfo, PeerInfo, ReplicaReadTarget)
│   ├── Retry/                         # Retry executor (RetryExecutor, Backoff, ErrorClassifier)
│   ├── Tls/                           # TLS configuration
│   ├── TxnKv/                         # Transactional API (TxnKvClient, Transaction, TwoPhaseCommitter)
│   │   └── Exception/                 # Transaction exceptions (TransactionConflictException, DeadlockException)
│   └── Util/                          # Key ordering and redaction (KeyOrder, KeyRedactor)
└── Proto/                             # Generated protobuf classes (Kvrpcpb, Pdpb, Tikvpb, …)
    ├── Kvrpcpb/                       # TiKV request/response
    ├── Pdpb/                          # PD request/response
    └── Tikvpb/                        # gRPC service stubs

tests/
├── Unit/                              # Unit tests
├── Integration/                       # Integration tests
├── E2E/                               # End-to-end tests
└── phpstan/                           # PHPStan bootstrap (protobuf class alias)

examples/
├── basic.php                          # Basic CRUD example
├── batch.php                          # Batch operations example
├── scan.php                           # Scanning examples
├── txn.php                            # Transactions with conflict retry
├── ttl.php                            # TTL operations example
├── atomic.php                         # Atomic operations example
├── tls.php                            # TLS configuration example
└── logging.php                        # PSR-3 logging example
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

### Completed

Shipped in the released versions — `CHANGELOG.md` has the details of each one.

- ✅ **RawKV core** — CRUD, batch operations, scans, range operations, TTL and compare-and-swap
- ✅ **Transactions (TxnKV)** — snapshot reads, two-phase commit, pessimistic and optimistic locking, GC safe points
- ✅ **API V2 and keyspaces** — V2 key encoding and PD keyspace resolution
- ✅ **Multi-endpoint PD** — leader discovery and failover
- ✅ **GC safe-point awareness** — service safe points for long-running reads
- ✅ **Replica reads** — stale reads, follower reads and the replica read modes
- ✅ **Fast commit modes** — one-phase commit and async commit
- ✅ **TSO batching** — batched TSO requests and a low-resolution timestamp cache
- ✅ **Memory-comparable region encoding** — the TiDB-compatible `EncodeBytes` scheme for TxnKV region lookups
- ✅ **Experimental RPC multiplexing** — the `BatchCommands` bidirectional stream (opt-in)
- ✅ **TLS/SSL Support**
- ✅ **PSR-3 Logging**
- ✅ **Region & Store Caching**
- ✅ **Connection Pooling**
- ✅ **Batch Async Execution**
- ✅ **Retry with Exponential Backoff**
- ✅ **Per-key TTL in BatchPut**
- ✅ **Scan limit enforcement** — MAX 10240 rows per RPC; `limit: 0` returns the whole range, bounded by `options['maxScanRows']`
- ✅ **Batch auto-splitting by size/count**

### Planned

Not shipped yet. Every item is an open issue in the repository, and the issue
is the source of truth — a feature moves to **Completed** when its issue is
closed. The list holds the open feature-gap issues whose work is still to be
done; the other open issues of the repository (API additions, quality fixes)
are tracked in their own issues and are not part of the roadmap. A
feature-gap issue whose work already shipped stays out of the list even while
the issue is still open.

- ⬜ **The remaining gRPC channel parameters** — compression, the HTTP/2 initial window size and `keepalive_permit_without_calls` are still hard-coded (the message sizes and the keepalive times are configurable already) ([#423](https://github.com/crazy-goat/tikv-php/issues/423))
- ⬜ **Automatic transaction lock keepalive** — the primary lock is kept alive on its own for long-running transactions; today `heartbeat()` has to be called by hand ([#429](https://github.com/crazy-goat/tikv-php/issues/429))
- ⬜ **Prometheus exporter and tracing hooks** — a ready-made metrics backend and request tracing ([#434](https://github.com/crazy-goat/tikv-php/issues/434))
- ⬜ **Resource control and resource groups** — per-request resource-group tags ([#427](https://github.com/crazy-goat/tikv-php/issues/427))
- ⬜ **Background region-cache refresh and region preloading** ([#425](https://github.com/crazy-goat/tikv-php/issues/425))
- ⬜ **Store health checking and failover to healthy replicas** ([#424](https://github.com/crazy-goat/tikv-php/issues/424))
- ⬜ **Transaction reverse scan and range deletion** ([#435](https://github.com/crazy-goat/tikv-php/issues/435))
- ⬜ **`RawBatchScan` RPC for multi-range scans** — `batchScan()` is a client-side loop today ([#433](https://github.com/crazy-goat/tikv-php/issues/433))
- ⬜ **Batch lock resolution and a resolved-transaction-status cache** ([#428](https://github.com/crazy-goat/tikv-php/issues/428))
- ⬜ **Transaction size and memory limits** ([#430](https://github.com/crazy-goat/tikv-php/issues/430))
- ⬜ **Explicit key-locking API for pessimistic transactions** ([#436](https://github.com/crazy-goat/tikv-php/issues/436))
- ⬜ **Read-through locks for snapshot reads** ([#432](https://github.com/crazy-goat/tikv-php/issues/432))
- ⬜ **The remaining `kvrpcpb.Context` control fields** — isolation level, not-fill-cache, request source ([#426](https://github.com/crazy-goat/tikv-php/issues/426))

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass: `make test`
5. Submit a pull request

## License

MIT
