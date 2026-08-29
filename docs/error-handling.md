# Error Handling Reference

Complete reference for every exception thrown by the TiKV PHP Client: the
class hierarchy, which operations throw which exceptions, whether **you**
(the caller) should retry, and the recommended catch order for a production
service.

## Table of Contents

1. [Exception Hierarchy](#exception-hierarchy)
2. [Caller Retryability](#caller-retryability)
3. [RawKvClient Operations](#rawkvclient-operations)
4. [Transaction Operations](#transaction-operations)
5. [Recommended Catch Order](#recommended-catch-order)
6. [Retry Budgets Behind Every Operation](#retry-budgets-behind-every-operation)

## Exception Hierarchy

Every failure raised by this library derives from
[`TiKvException`](../src/Client/Exception/TiKvException.php), which itself
extends PHP's `\RuntimeException`. One deliberate exception lives outside
that hierarchy: [`InvalidArgumentException`](../src/Client/Exception/InvalidArgumentException.php)
extends `\InvalidArgumentException` directly — it is **not** a
`TiKvException` (and not a `RuntimeException` either), so a bare
`catch (TiKvException $e)` will never swallow an argument-validation error.

```
\RuntimeException
└── TiKvException                                  src/Client/Exception/
    ├── BatchDeadlineExceededException             src/Client/Exception/
    ├── BatchPartialFailureException               src/Client/Exception/
    ├── ClientClosedException                      src/Client/Exception/
    ├── GrpcException                              src/Client/Exception/
    ├── HealthCheckException                       src/Client/Exception/
    ├── InvalidStateException                      src/Client/Exception/
    ├── InvalidStoreAddressException               src/Client/Exception/
    ├── RegionException                            src/Client/Exception/
    ├── StoreNotFoundException                     src/Client/Exception/
    ├── RetryBudgetExhaustedException              src/Client/Retry/
    ├── DeadlockException                          src/Client/TxnKv/Exception/
    ├── LockWaitTimeoutException                   src/Client/TxnKv/Exception/
    ├── TransactionConflictException               src/Client/TxnKv/Exception/
    └── TxnRetryableException                      src/Client/TxnKv/Exception/

\InvalidArgumentException  ← outside the TiKvException hierarchy
└── InvalidArgumentException                        src/Client/Exception/
```

All fifteen `TiKvException` subclasses are `final`; `TiKvException` itself is
the only non-final class in the tree (it is the intended base for any custom
project-wide exception work).

> The client never throws a raw `\Exception`, `\RuntimeException` or
> `\InvalidArgumentException`: everything that can reach your code is one of
> the sixteen classes above.

`TiKvException` itself is also thrown **directly** — no more specific subclass —
at four sites, so a `catch` on any single subclass will not match these; only a
bare `catch (TiKvException $e)` does:

| Site | Thrown when | Message (verbatim in `src/`) |
|---|---|---|
| `PdClient::getRegion()` | PD returned no region for the requested key. Fail-closed by design: a fabricated region would be cached and silently misroute requests. Reachable from every region-resolved operation. | `PD GetRegion returned no region for key` |
| `TimestampOracle::getTimestamp()` | The TSO RPC failed. **Re-wraps a `GrpcException`** (preserved as `getPrevious()`, gRPC status code kept) into the base class, so `catch (GrpcException)` around a transaction begin/commit will **not** match. | `TSO request failed: %s` (sprintf'd with the wrapped message) |
| `TimestampOracle` response check (private, same call) | The TSO response carried no timestamp. Fail-closed: no local timestamp is fabricated. | `TSO response missing timestamp` |
| `TwoPhaseCommitter` heartbeat (reachable via `Transaction::heartbeat()`) | `KvTxnHeartBeat` reported a key error. | `Heartbeat failed: key error` |

### What Each Class Means

| Class | Parent | Meaning |
|---|---|---|
| [`BatchDeadlineExceededException`](../src/Client/Exception/BatchDeadlineExceededException.php) | `TiKvException` | A batch operation exceeded its wall-clock deadline; accessors: `getDeadlineMs()`, `getElapsedMs()`, `getContext()` |
| [`BatchPartialFailureException`](../src/Client/Exception/BatchPartialFailureException.php) | `TiKvException` | A fanned-out batch operation failed on some regions; accessors: `getRegionErrors()`, `getTotalRegions()` |
| [`ClientClosedException`](../src/Client/Exception/ClientClosedException.php) | `TiKvException` | The operation was attempted after `$client->close()` |
| [`GrpcException`](../src/Client/Exception/GrpcException.php) | `TiKvException` | gRPC transport/status error; carries `public readonly int $grpcStatusCode` |
| [`HealthCheckException`](../src/Client/Exception/HealthCheckException.php) | `TiKvException` | `healthCheck()` could not reach PD; wraps the underlying cause via `getPrevious()` |
| [`InvalidArgumentException`](../src/Client/Exception/InvalidArgumentException.php) | `\InvalidArgumentException` | Invalid argument (empty key, oversized key/value, negative scan limit, bad option value). **Outside** the `TiKvException` hierarchy |
| [`InvalidStateException`](../src/Client/Exception/InvalidStateException.php) | `TiKvException` | Client/transaction is in a state that cannot serve the call (e.g. CAS without atomic mode, transaction already committed) |
| [`InvalidStoreAddressException`](../src/Client/Exception/InvalidStoreAddressException.php) | `TiKvException` | PD returned a store address that failed validation (not a bare `host:port`, reserved scheme, out-of-range port, or outside the allowed host policy) |
| [`RegionException`](../src/Client/Exception/RegionException.php) | `TiKvException` | Region-level error reported by TiKV (NotLeader, EpochNotMatch, ServerIsBusy, …); carries `public readonly ?NotLeader $notLeader` and `?ErrorKind $errorKind` |
| [`StoreNotFoundException`](../src/Client/Exception/StoreNotFoundException.php) | `TiKvException` | The store backing a region leader is missing from PD; carries `public readonly int $storeId` |
| [`RetryBudgetExhaustedException`](../src/Client/Retry/RetryBudgetExhaustedException.php) | `TiKvException` | The internal retry loop exhausted its attempt cap or wall-clock deadline; accessors: `attempts()`, `elapsedOrBackoffMs()`, `getPrevious()` (original error) |
| [`DeadlockException`](../src/Client/TxnKv/Exception/DeadlockException.php) | `TiKvException` | Pessimistic locking detected a deadlock; accessors: `getDeadlockKey()`, `getDeadlockKeyHash()`, `getLockTs()` |
| [`LockWaitTimeoutException`](../src/Client/TxnKv/Exception/LockWaitTimeoutException.php) | `TiKvException` | A pessimistic lock wait exhausted its budget (`maxBackoffMs`); accessors: `getKey()` (redacted message), `getTimeoutMs()` |
| [`TransactionConflictException`](../src/Client/TxnKv/Exception/TransactionConflictException.php) | `TiKvException` | Write conflict / abort reported during prewrite, commit or pessimistic lock; accessor: `getConflictingKeys()` |
| [`TxnRetryableException`](../src/Client/TxnKv/Exception/TxnRetryableException.php) | `TiKvException` | A lock was encountered and resolved inside a transactional operation — safe to retry with the carried `public readonly BackoffType $backoffType` |
| `TiKvException` | `\RuntimeException` | Base class of every library exception |

## Caller Retryability

The client already retries transient errors internally (`NotLeader`,
`EpochNotMatch`, `ServerIsBusy`, `StaleCommand`, `RegionNotFound`, gRPC
transport errors and more — see
[Retry Budgets](#retry-budgets-behind-every-operation)). Your catch block is
therefore a *last resort*, not another retry loop: wrapping calls in your own
retry multiplies the total wait on top of what the library already spent.

Verdict legend:

- **Never retry** — retrying reproduces the same failure deterministically.
- **Do not retry blindly** — retrying may be correct only after you inspect
  the exception or change something.
- **Retry if idempotent** — the client already retried internally and gave
  up; one more caller-level attempt is safe only when repeating the operation
  cannot corrupt data.
- **New transaction** — retry by building a fresh transaction object; the
  current one must be discarded.

| Exception | Caller verdict |
|---|---|
| `InvalidArgumentException` | **Never retry** — programming or input error |
| `InvalidStateException` | **Never retry** — fix the calling code / lifecycle first |
| `ClientClosedException` | **Never retry** on that client instance — reopen a new client if appropriate |
| `InvalidStoreAddressException` | **Never retry** — classified fatal before any retry backoff (`ErrorClassifier::classify()` returns null), so the client did not retry and will not succeed next time without configuration change |
| `HealthCheckException` | Probe result only — no user data was touched. Re-probe after an interval instead of tight-looping |
| `TransactionConflictException` | **New transaction** — the transaction's writes were not applied |
| `DeadlockException` | **New transaction** |
| `LockWaitTimeoutException` | **New transaction** — the pessimistic locks were not acquired, the transaction cannot proceed |
| `TxnRetryableException` | Escapes only when the surrounding executor gives up; treat like other txn errors: **new transaction** unless the op is idempotent |
| `RetryBudgetExhaustedException` | **Retry if idempotent** — the client already retried internally until its budget ran out (attempt cap or deadline); check `getPrevious()` for the root cause |
| `RegionException` | **Retry if idempotent** — same reasoning; the client already invalidated the region cache and retried |
| `GrpcException` | **Retry if idempotent** — same reasoning; transport-level errors were already retried with backoff |
| `StoreNotFoundException` | **Do not retry blindly** — transient only after the stale cache entry expires (TTL/LRU sweep) or PD restores the store; the retry executor does *not* invalidate the region cache for it (it is not classified as a region error). A short caller-side delay-then-retry is reasonable for idempotent ops, but inspect the exception's public `$storeId` property first |
| `BatchPartialFailureException` | **Do not retry blindly** — the operation is *partially applied*: some regions succeeded, others failed. Inspect `getRegionErrors()` and re-drive only the affected keys/ranges |
| `BatchDeadlineExceededException` | **Do not retry blindly** — also partially applied; check `getContext()` for pending regions |

### Transaction::commit() and double-apply

`commit()` deserves special care because a network failure at exactly the
wrong moment leaves the outcome *unknown*: the commit may have taken effect
even though the RPC returned an error. Two facts about the implementation
matter:

1. On retry-after-failure, `commit()` **reuses** the commit timestamp stored
   on the transaction state instead of allocating a new one (#217) — a second
   `commit()` can never commit the same writes at two different timestamps.
2. Once the commit phase succeeds, the transaction status becomes `Committed`
   and a second `commit()` call is rejected with `InvalidStateException`
   (#217/#83).

Safe pattern:

```php
try {
    $txn->set('k', 'v');
    $txn->commit();
} catch (\CrazyGoat\TiKV\Client\Exception\TiKvException $e) {
    // Outcome unknown → verify before re-applying:
    // start a NEW transaction and re-read your keys. Only re-run the whole
    // business operation if re-reading shows it did NOT apply. Blindly
    // re-committing the SAME transaction object is never correct: if the
    // first attempt actually committed, the state is closed and commit()
    // throws InvalidStateException; if it did not, conflicts must be
    // resolved against fresh reads anyway.
}
```

For RawKV there is no transaction boundary: `put()`/`delete()` are naturally
idempotent (same value rewritten), but `compareAndSwap()`,
`putIfAbsent()` and counter-style read-modify-write patterns are **not** —
after any exception their precondition is unknown, so re-read before
re-applying.

## RawKvClient Operations

One row per public method. "Throws" lists every exception class the method
can raise, verified against both the `@throws` annotations and the method
bodies (several scan methods validate limits even where no annotation says
so). Two routing exceptions are deliberately *not* repeated in every row:
`StoreNotFoundException` and `InvalidStoreAddressException` can escape any
operation that resolves a store address on the RPC path — treat them as an
additional possibility for every row that includes `RegionException`.

| Method | Throws | Notes |
|---|---|---|
| `__construct` | `InvalidArgumentException` | Bad option values (`retryDeadlineMs < 0`, `maxConcurrency < 1`) |
| `create()` | `InvalidArgumentException` | Empty PD endpoints array; invalid `options['tls']` material, `options['timeout']`, `options['slowLog']`, `options['metrics']`, `options['allowedStoreHosts']`, `options['storeHostPolicy']`, `options['allowedStorePorts']`, `options['retryDeadlineMs']`, `options['maxConcurrency']` |
| `setAtomicForCAS(bool)` / `isAtomicForCAS()` | — | Plain setters/getters, nothing thrown |
| `setColumnFamily(string)` / `getColumnFamily()` | — | Plain setters/getters, nothing thrown |
| `get(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Key validated non-empty + size limit before the RPC |
| `put(string, string, int $ttl = 0)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Same validation as `get()` plus value size |
| `delete(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Idempotent at the TiKV level |
| `getKeyTTL(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Returns `null` when the key has no TTL |
| `compareAndSwap(...)` | `ClientClosedException`, `InvalidArgumentException`, `InvalidStateException`, `RegionException`, `GrpcException` | `InvalidStateException` when atomic mode is off |
| `putIfAbsent(...)` | `ClientClosedException`, `InvalidArgumentException`, `InvalidStateException`, `RegionException`, `GrpcException` | Delegates to `compareAndSwap()`; requires atomic mode |
| `batchGet(array)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Empty input returns `[]` without RPC |
| `batchPut(array, int|array $ttl = 0)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Per-key validation happens before any send |
| `batchDelete(array)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | As above |
| `scanIterator(...)` | `ClientClosedException`, `InvalidArgumentException` | `batchSize` validated synchronously in the factory call (`ScanIterator::__construct`, bounds 1..10240); the underlying scan RPCs happen during iteration |
| `scanPrefixIterator(string, int $batchSize = 256, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException` | Same synchronous validation as `scanIterator()` |
| `scan(string, string, int $limit = 0, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Limit validated (`'Scan limit must be 0 or greater'`, max 10240) even though the annotation omits it |
| `scanPrefix(string, int $limit = 0, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Delegates to `scan()`; limit validated |
| `reverseScan(...)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Limit validated |
| `batchScan(array $ranges, int $eachLimit, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Ranges fan out **concurrently**, capped by `options['maxConcurrency']` (default 16); order preserved |
| `deleteRange(string, string)` | `ClientClosedException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Concurrent per-region deletes; idempotent, so a `BatchPartialFailureException` may be retried whole |
| `deletePrefix(string)` | `ClientClosedException`, `InvalidArgumentException` | Rejects empty prefix and all-`0xFF` prefixes; range errors surface from `deleteRange()` semantics |
| `checksum(string, string)` | `ClientClosedException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Concurrent per-region checksums; idempotent |
| `ingest(array, ?int $ttl = null)` | `ClientClosedException`, `GrpcException`, `RegionException`, `InvalidStoreAddressException` | SST import path; store mode switches fan out concurrently |
| `getClusterId(): ?int` | — | Returns `null` when unknown; no exceptions documented |
| `healthCheck(): ?int` | `ClientClosedException`, `HealthCheckException` | Wraps any PD failure into `HealthCheckException` |
| `getMetrics(): MetricsInterface` | — | Pure accessor |
| `close(): void` | — | Swallows and logs shutdown errors |

## Transaction Operations

The same routing exceptions noted in the RawKvClient section
(`StoreNotFoundException`, `InvalidStoreAddressException`) can escape any
method below whose row includes `RegionException` — they resolve store
addresses on the RPC path just the same.

| Method | Throws | Notes |
|---|---|---|
| `__construct` | `InvalidArgumentException` | `retryDeadlineMs < 0` |
| `getTxnId()` / `getStartTs()` / `getCommitTs()` / `getStatus()` / `isPessimistic()` / `getPriority()` / `getWriteSet()` / `getReadSet()` | — | State getters, nothing thrown |
| `get(string)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException` | Locks resolved inline; a lock hit surfaces as `TxnRetryableException` (subclass of `TiKvException`) consumed by the retry executor |
| `batchGet(array)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException` | No retry executor wraps this path — errors propagate directly |
| `scan(string, string, int $limit = 0)` | `InvalidArgumentException`, `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException` | Limit normalized/rejected ('Scan limit must be 0 or greater', max 10240) |
| `set(string, string)` | `InvalidStateException` | Buffers the write locally; no I/O |
| `delete(string)` | `InvalidStateException` | Buffers the delete locally; no I/O |
| `commit()` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`, `RegionException`, `GrpcException` | See [double-apply section](#transactioncommit-and-double-apply); prewrite runs outside the shared retry executor, so conflicts/deadlocks escape rather than being auto-retried |
| `rollback()` | `InvalidStateException`, `TiKvException`, `RegionException`, `GrpcException` | Idempotent-ish: rolling back an already-rolled-back set is a no-op locally |
| `heartbeat(int $adviseLockTtlMs = 10000): int` | `InvalidStateException`, `TiKvException`, `RegionException`, `GrpcException` | Extends the primary lock TTL |

(`TxnRetryableException` is a subclass of `TiKvException`, so rows listing
`TiKvException` cover it implicitly; it is listed explicitly wherever the
code raises it as the *most specific* type.)

## Recommended Catch Order

Catch from the most specific to the most general. Because
`InvalidArgumentException` sits outside the hierarchy, it must have its own
catch arm — placing it after `catch (TiKvException)` would make it dead code.

```php
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;

try {
    $result = $client->get($key);
} catch (InvalidArgumentException $e) {
    // 1. Outside the TiKv hierarchy — programming/input error. Log loudly,
    //    fix the caller. Never retry.
} catch (ClientClosedException | InvalidStateException $e) {
    // 2. Lifecycle misuse. Fix the code path; reopening a client is the
    //    only remedy for ClientClosedException.
} catch (
    DeadlockException
    | LockWaitTimeoutException
    | TransactionConflictException $e
) {
    // 3. Transaction-level conflicts: discard this transaction, start a
    //    NEW one (fresh begin()), optionally with bounded attempts and
    //    jittered delay between rounds.
} catch (BatchPartialFailureException $e) {
    // 4. Partially applied batch. Inspect getRegionErrors(), collect the
    //    affected keys/ranges, and re-drive just those.
} catch (RetryBudgetExhaustedException $e) {
    // 5. The client already retried to its budget. For idempotent work a
    //    single delayed retry is acceptable; otherwise fail the request.
    //    getPrevious() holds the root cause.
} catch (RegionException | GrpcException $e) {
    // 6. Remaining cluster/transport errors the client chose not to retry
    //    (fatal kinds such as RaftEntryTooLarge, KeyNotInRegion,
    //    FlashbackInProgress). Retry only if idempotent.
} catch (TiKvException $e) {
    // 7. Any other library failure (StoreNotFoundException,
    //    InvalidStoreAddressException, HealthCheckException,
    //    TxnRetryableException, ...). Default: do not retry.
}
```

A production service typically does not need all seven arms: arms 1–2 should
be impossible in reviewed code (let them bubble to your global handler), and
arm 7 is the safety net that decides "fail the request".

### Retrying transactions safely

```php
const TXN_ATTEMPTS = 3;

for ($attempt = 1; $attempt <= TXN_ATTEMPTS; $attempt++) {
    $txn = $tikv->begin();
    try {
        $current = $txn->get('counter');
        $txn->set('counter', (string) ((int) $current + 1));
        $txn->commit();
        break;
    } catch (
        TransactionConflictException
        | DeadlockException
        | LockWaitTimeoutException $e
    ) {
        if ($attempt === TXN_ATTEMPTS) {
            throw $e;
        }
        usleep(random_int(10_000, 50_000)); // jittered back-off
        // loop → brand-new transaction, fresh reads
    }
}
```

The new transaction re-reads every value it depends on — never replay writes
into a stale snapshot.

## Retry Budgets Behind Every Operation

Every RawKV operation and transactional read/commit runs through
`RetryExecutor`, whose budgets decide when an error stops being retried
internally and surfaces to your `catch` block:

| Budget | Default | Exhaustion behaviour |
|---|---|---|
| Attempt cap (`maxAttempts`) | 30 | Throws `RetryBudgetExhaustedException` |
| Wall-clock deadline (`retryDeadlineMs`) | 30000 ms | Throws `RetryBudgetExhaustedException`; `0` disables the deadline |
| Cumulative backoff (`maxBackoffMs`) | 20000 ms | **Rethrows the original `TiKvException`** (not `RetryBudgetExhaustedException`) |
| ServerBusy budget (`serverBusyBudgetMs`) | 60000 ms | **Rethrows the original `TiKvException`** |

Notes:

- A budget value of `0` does **not** disable the cumulative-backoff or
  ServerBusy budgets — it exhausts on the first charge. Only
  `retryDeadlineMs=0` disables its budget (restoring the pre-#294 unbounded
  behaviour).
- Deadline checks run *before* each attempt; worst-case worker occupancy is
  therefore approximately the deadline plus one more in-flight attempt
  (bounded by the gRPC timeout).
- `ErrorClassifier` decides which errors are retryable: NotLeader /
  EpochNotMatch / ServerIsBusy / StaleCommand / RegionNotFound and friends
  retry with typed backoff curves, while `RaftEntryTooLarge`,
  `KeyNotInRegion`, `FlashbackInProgress`, `FlashbackNotPrepared` and
  `InvalidStoreAddressException` are fatal immediately.

See also:

- [Operations guide](operations.md) — what each method does
- [Configuration guide](configuration.md) — `retryDeadlineMs`,
  `serverBusyBudgetMs`, `maxBackoffMs`, `maxConcurrency` options
- [Troubleshooting](troubleshooting.md) — symptom-first problem solving
- [Advanced features](advanced.md) — retry-strategy chapter
