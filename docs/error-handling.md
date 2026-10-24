# Error Handling Reference

Complete reference for every exception thrown by the TiKV PHP Client: the
class hierarchy, which operations throw which exceptions, whether **you**
(the caller) should retry, and the recommended catch order for a production
service.

## Table of Contents

1. [Exception Hierarchy](#exception-hierarchy)
2. [Caller Retryability](#caller-retryability)
3. [Idempotency and Retry Safety](#idempotency-and-retry-safety)
4. [RawKvClient Operations](#rawkvclient-operations)
5. [Transaction Operations](#transaction-operations)
6. [Recommended Catch Order](#recommended-catch-order)
7. [Retry Budgets Behind Every Operation](#retry-budgets-behind-every-operation)
8. [GC Safe Points and Long-Running Reads](#gc-safe-points-and-long-running-reads)

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
    ├── KeyErrorException                          src/Client/Exception/
    ├── PdException                                src/Client/Exception/
    ├── RegionException                            src/Client/Exception/
    ├── ScanLimitExceededException                 src/Client/Exception/
    ├── StoreNotFoundException                     src/Client/Exception/
    ├── RetryBudgetExhaustedException              src/Client/Retry/
    ├── DeadlockException                          src/Client/TxnKv/Exception/
    ├── LockWaitTimeoutException                   src/Client/TxnKv/Exception/
    ├── TransactionConflictException               src/Client/TxnKv/Exception/
    ├── TxnAbortedByGcException                    src/Client/TxnKv/Exception/
    ├── TxnRetryableException                      src/Client/TxnKv/Exception/
    └── UndeterminedCommitException                src/Client/TxnKv/Exception/

\InvalidArgumentException  ← outside the TiKvException hierarchy
└── InvalidArgumentException                        src/Client/Exception/
```

All nineteen `TiKvException` subclasses are `final`; `TiKvException` itself is
the only non-final class in the tree (it is the intended base for any custom
project-wide exception work).

> The client never throws a raw `\Exception`, `\RuntimeException` or
> `\InvalidArgumentException`: everything that can reach your code is one of
> the nineteen classes above.

`TiKvException` itself is also thrown **directly** — no more specific subclass —
at sixteen `throw new TiKvException(...)` statements across multiple call
sites (some inside shared private helpers), so a `catch` on any single
subclass will not match these; only a bare `catch (TiKvException $e)` does.
Several are
fail-closed defense-in-depth paths that are unreachable unless a collaborator's
contract changes (noted per row):

| Site | Thrown when | Message (verbatim in `src/`) |
|---|---|---|
| `PdClient::getRegion()` | PD returned no region for the requested key. Fail-closed by design: a fabricated region would be cached and silently misroute requests. A PD *header error* is a separate, earlier failure — see [`PdException`](#what-each-class-means) below. Reachable from every region-resolved operation. | `PD GetRegion returned no region for key` |
| `PdClient::getKeyspaceId()` | PD answered `LoadKeyspace` with no keyspace. Fail-closed: the codec cannot be built without a keyspace ID, and defaulting to `0` would address the wrong keyspace. | `PD LoadKeyspace returned no keyspace` |
| `PdClient::uint64ToInt()` (private) | A PD uint64 scalar (`GC safe point` / `min GC safe point`) was non-numeric, did not round-trip through a PHP int (out of 64-bit range or non-canonical), or was negative. Called from `getGCSafePoint()` and `updateServiceGCSafePoint()`. Three throw statements. | `PD returned a non-numeric %s: %s` / `PD returned an invalid %s: %s` |
| `TimestampOracle::requestTimestampRange()` (private; reached from `getTimestamp()`, `getTimestampBatch()` and the pool refill) | The TSO RPC failed. **Re-wraps a `GrpcException`** (preserved as `getPrevious()`, gRPC status code kept) into the base class, so `catch (GrpcException)` around a transaction begin/commit will **not** match. Reachable from every timestamp mint (`begin()`, `commit()`, …). | `TSO request failed: %s` (sprintf'd with the wrapped message) |
| `TimestampOracle::extractTimestampRange()` (private, same call) | The TSO response carried no timestamp. Fail-closed: no local timestamp is fabricated. | `TSO response missing timestamp` |
| `SafePointCache::get()` | PD returned a negative GC safe point (defensive; PD safe points are uint64). Caught by `TxnKvClient::validateStartTsAgainstGcSafePoint()`, which degrades to a warning (fail-open), so it does not normally reach the caller. | `PD returned invalid GC safe point: %d` |
| `TwoPhaseCommitter` heartbeat (reachable via `Transaction::heartbeat()`) | `KvTxnHeartBeat` reported a `retryable` or `abort` KeyError payload → `TransactionConflictException`; a `locked` payload → `TxnRetryableException` (`BackoffType::TxnLock`, deliberately **not** `LockResolver::resolveLock()`ed — the only lock a heartbeat can report is the calling transaction's own primary lock, and resolving it would roll the transaction back under itself); any other payload variant (`txn_not_found`, `txn_lock_not_found`, …) → the base class with the variant named, via the shared `KeyErrorDescriber` (see [#492](https://github.com/crazy-goat/tikv-php/issues/492)). | `Heartbeat failed: retryable: <server text>` / `Heartbeat failed: abort: <server text>` / `Heartbeat failed: locked key "<redacted primary>"` / `Heartbeat failed: <variant>` |
| `TwoPhaseCommitter` prewrite (reachable via `Transaction::commit()`) | `KvPrewrite` reported a `KeyError`: `deadlock` → `DeadlockException` (key/hash/lockTs); `locked` → resolves the lock then `TxnRetryableException`; `conflict` / `retryable` / `abort` and `already_exist` / `assertion_failed` / `primary_mismatch` / `txn_not_found` / `commit_ts_too_large` → `TransactionConflictException`; any other variant (`commit_ts_expired`, `txn_lock_not_found`, …) → the base class with the variant named, via `KeyErrorDescriber`. Fail-closed (issue #214): before that fix, unhandled variants fell out of the loop and prewrite was treated as successful, so `KvCommit` ran against keys that held no lock. Since issue #213, prewrite runs inside the shared retry executor with the transaction classifier: region errors (`NotLeader`, `EpochNotMatch`, `RegionNotFound`, `ServerIsBusy`) and a resolved `locked` conflict are auto-retried (the region is re-resolved on each attempt), while `DeadlockException`, `TransactionConflictException` and unrecognised variants are fatal and escape `commit()`. | `Deadlock detected during prewrite` / `Lock conflict during prewrite, resolved - retry` / `Write conflict during prewrite` / `Prewrite failed: <variant>` |
| `TwoPhaseCommitter` pessimistic lock (reachable from eager `Transaction::set()`/`delete()` and from deferred `commit()`) | `KvPessimisticLock` reported a `KeyError`: `deadlock` → `DeadlockException`; `locked` → resolves the lock then `TxnRetryableException`; `conflict` / `retryable` / `abort` → `TransactionConflictException`; any other variant → the base class with the variant named, via `KeyErrorDescriber` (fail-closed, issue #454). | `Pessimistic lock failed: <variant>` |
| `TxnReader::handleReadKeyError()` (private; reached by `get()`, `batchGet()`, and `scan()`) | An unrecognised transactional read `KeyError` variant is fail-closed rather than treated as a missing value. Locks are resolved and retried separately; retryable errors become `TransactionConflictException`; GC aborts become `TxnAbortedByGcException`. | `Get failed: <variant>` / `BatchGet failed: <variant>` / `Scan failed: <variant>` |
| `RegionResolver::batchResolveRegions()` | PD returned regions that do not cover one of the requested keys. Fail-closed: a silently dropped key would be lost from the batch. | `PD could not resolve the region for key %s; refusing to silently drop it from the batch` (key redacted) |
| `RegionGrouper::resolvedRegion()` | The single accessor every region-grouping loop reads its resolved map through, so the message and the redacted key are the same everywhere. Thrown when the map lacks the key the loop is grouping; the resolver above already fails closed, so this is defense in depth and unreachable unless that contract changes. **Reached from** `groupKeysByRegionBatch()` (so `batchGet()`, `batchDelete()`, `LockResolver`, `TwoPhaseCommitter`), `groupItemsByRegion()` (`TwoPhaseCommitter`), `TxnReader::batchGetFromTiKV()`, `RawKvBatch::batchPut()` and its three multi-region re-split paths, and `SstIngestor::groupPairsByRegion()`. Skipping the key instead of throwing was silent data loss — `batchPut()`/`batchDelete()`/`ingest()` return `void`, and `batchGet()` cannot tell a skipped key from a missing one (issue #187). | `Region could not be resolved for key %s; refusing to silently drop it from the batch` (key redacted) |
| `RawKvRangeOps::assertRegionCoversRange()` (private; called from `deleteRange()` and `checksum()`) | The resolved region shrank to an upper bound that no longer covers the clipped sub-range (split after enumeration). Deliberately free of every retry-classifier keyword so the retry loop stops without replaying the stale sub-range; a clamped raw read would otherwise be reported as a complete delete/checksum (issue #190). | `Region %d shrank to an upper bound of %s after enumeration and no longer covers the clipped sub-range starting at key %s; refusing a partial operation` (keys redacted) |

### What Each Class Means

| Class | Parent | Meaning |
|---|---|---|
| [`BatchDeadlineExceededException`](../src/Client/Exception/BatchDeadlineExceededException.php) | `TiKvException` | A batch operation exceeded its wall-clock deadline; accessors: `getDeadlineMs()`, `getElapsedMs()`, `getContext()` |
| [`BatchPartialFailureException`](../src/Client/Exception/BatchPartialFailureException.php) | `TiKvException` | A fanned-out batch operation failed on some regions; accessors: `getRegionErrors()`, `getTotalRegions()` |
| [`ClientClosedException`](../src/Client/Exception/ClientClosedException.php) | `TiKvException` | The operation was attempted after `$client->close()` |
| [`GrpcException`](../src/Client/Exception/GrpcException.php) | `TiKvException` | gRPC transport/status error; carries `public readonly int $grpcStatusCode` |
| [`KeyErrorException`](../src/Client/Exception/KeyErrorException.php) | `TiKvException` | A per-pair `KeyError` reported on a response where key errors are not handled by the caller (e.g. a per-pair error on a raw scan); previously reported as `RegionException` and retried as a region miss |
| [`HealthCheckException`](../src/Client/Exception/HealthCheckException.php) | `TiKvException` | `healthCheck()` could not reach PD; wraps the underlying cause via `getPrevious()` |
| [`InvalidArgumentException`](../src/Client/Exception/InvalidArgumentException.php) | `\InvalidArgumentException` | Invalid argument (empty key, oversized key/value, negative scan limit, bad option value). **Outside** the `TiKvException` hierarchy |
| [`InvalidStateException`](../src/Client/Exception/InvalidStateException.php) | `TiKvException` | Client/transaction is in a state that cannot serve the call (e.g. CAS without atomic mode, transaction already committed) |
| [`InvalidStoreAddressException`](../src/Client/Exception/InvalidStoreAddressException.php) | `TiKvException` | PD returned a store address that failed validation (not a bare `host:port`, reserved scheme, out-of-range port, or outside the allowed host policy) |
| [`PdException`](../src/Client/Exception/PdException.php) | `TiKvException` | PD reported an application-level error in a `pdpb.ResponseHeader.error` — `NOT_BOOTSTRAPPED`, `ErrNotLeader`, `INVALID_VALUE`, … — which arrives with a gRPC `OK` status and an **empty payload**, so reading it as a success produced a silently wrong result (issue #234). Raised by the single choke point every PD response passes through, so it covers `getRegion()`, `scanRegions()`, `getStore()`, `getAllStores()`, `getKeyspaceId()`, `ping()`, both GC-safe-point methods and the `GetMembers` discovery path. Accessors: `getMethod()`, `getErrorType()` (raw `pdpb.ErrorType`), `getErrorTypeName()`, `getPdErrorMessage()`, `isNotLeader()`. A typed error with an **empty** message is still an error and renders as `unknown PD error (header error with empty message)` |
| [`RegionException`](../src/Client/Exception/RegionException.php) | `TiKvException` | Region-level error reported by TiKV (NotLeader, EpochNotMatch, ServerIsBusy, …); carries `public readonly ?NotLeader $notLeader` and `?ErrorKind $errorKind` |
| [`ScanLimitExceededException`](../src/Client/Exception/ScanLimitExceededException.php) | `TiKvException` | An unbounded scan (`limit: 0`) hit a row cap: the RawKV path collected more than `options['maxScanRows']` rows (default 100000, issue #191), the transactional path more than `Transaction::MAX_SCAN_LIMIT` rows (10240, issue #440); accessors: `getMaxRows()`, `getScannedRows()`. Thrown instead of silently truncating. On the RawKV path the cap is `options['maxScanRows']` (raise it, or switch to `scanIterator()`/`scanPrefixIterator()`); the transactional path has no option (its cap is `Transaction::MAX_SCAN_LIMIT`) and has to page the range explicitly — the iterators are a **raw** read and do not see the transaction's own writes |
| [`StoreNotFoundException`](../src/Client/Exception/StoreNotFoundException.php) | `TiKvException` | The store backing a region leader is missing from PD; carries `public readonly int $storeId` |
| [`RetryBudgetExhaustedException`](../src/Client/Retry/RetryBudgetExhaustedException.php) | `TiKvException` | The internal retry loop exhausted its attempt cap or wall-clock deadline; accessors: `attempts()`, `elapsedOrBackoffMs()`, `getPrevious()` (original error), `getRawKey()` (the un-redacted key, kept out of the message — see #269) |
| [`DeadlockException`](../src/Client/TxnKv/Exception/DeadlockException.php) | `TiKvException` | Pessimistic locking detected a deadlock; accessors: `getDeadlockKey()`, `getDeadlockKeyHash()`, `getLockTs()` |
| [`LockWaitTimeoutException`](../src/Client/TxnKv/Exception/LockWaitTimeoutException.php) | `TiKvException` | A pessimistic lock wait exhausted its budget (`maxBackoffMs`); the per-request wait is `begin(['lockWaitTimeMs' => …])` (issue #443); accessors: `getKey()` (redacted message), `getTimeoutMs()` |
| [`TransactionConflictException`](../src/Client/TxnKv/Exception/TransactionConflictException.php) | `TiKvException` | Write conflict / abort reported during prewrite, commit or pessimistic lock; accessor: `getConflictingKeys()` |
| [`TxnAbortedByGcException`](../src/Client/TxnKv/Exception/TxnAbortedByGcException.php) | `TiKvException` | The transaction's start timestamp is below the cluster's GC safe point — the data it would read has been garbage collected. Raised by GC safe-point validation at `begin()` (default-on, `options['gcSafePointValidation'] => false` to disable) or when TiKV rejects a read with the "GC life time is shorter than transaction duration" abort |
| [`TxnRetryableException`](../src/Client/TxnKv/Exception/TxnRetryableException.php) | `TiKvException` | A lock was encountered and resolved inside a transactional operation — safe to retry with the carried `public readonly BackoffType $backoffType` |
| [`UndeterminedCommitException`](../src/Client/TxnKv/Exception/UndeterminedCommitException.php) | `TiKvException` | The commit outcome is unknown (client-go's `ErrResultUndetermined`): the primary-key commit RPC failed at the transport level, so the commit may or may not have been applied — the transaction must NOT be rolled back; resolve out of band (e.g. `CheckTxnStatus`) |
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
| `PdException` | **Do not retry blindly** — read `getErrorType()` first. A **not-leader** rejection (`isNotLeader()`) is the one header error another endpoint can satisfy, and the client already rotated the PD endpoint and retried once per remaining endpoint before letting it reach you, so a second failure means no configured endpoint can serve the call. `NOT_BOOTSTRAPPED` and `INCOMPATIBLE_VERSION` resolve on their own (wait for the cluster, upgrade PD) and retrying only multiplies the RPC load; `INVALID_VALUE` means the request itself is wrong. **No user data was touched** — the failure is in the metadata plane — so re-driving the operation is safe for idempotent ops once PD is healthy (issue #234) |
| `ScanLimitExceededException` | **Never retry** — the same query will hit the same guard. Switch to `scanIterator()`/`scanPrefixIterator()` (constant memory), raise `options['maxScanRows']`, or page the range explicitly |
| `HealthCheckException` | Probe result only — no user data was touched. Re-probe after an interval instead of tight-looping |
| `TransactionConflictException` | **New transaction** — the transaction's writes were not applied |
| `DeadlockException` | **New transaction** |
| `TxnAbortedByGcException` | **New transaction** — the start timestamp is permanently behind GC; retrying the same transaction cannot succeed. For reads that must outlive `gc_life_time`, register a service safe point first (`TxnKvClient::holdGcSafePoint()`) |
| `LockWaitTimeoutException` | **New transaction** — the pessimistic locks were not acquired, the transaction cannot proceed |
| `TxnRetryableException` | Escapes only when the surrounding executor gives up; treat like other txn errors: **new transaction** unless the op is idempotent |
| `RetryBudgetExhaustedException` | **Retry if idempotent** — the client already retried internally until its budget ran out (attempt cap or deadline); check `getPrevious()` for the root cause |
| `RegionException` | **Retry if idempotent** — same reasoning; the client already invalidated the region cache and retried |
| `GrpcException` | **Retry if idempotent** — same reasoning; transport-level errors were already retried with backoff |
| `StoreNotFoundException` | **Do not retry blindly** — transient only after the stale cache entry expires (TTL/LRU sweep) or PD restores the store; the retry executor does *not* invalidate the region cache for it (it is not classified as a region error). A short caller-side delay-then-retry is reasonable for idempotent ops, but inspect the exception's public `$storeId` property first |
| `BatchPartialFailureException` | **Do not retry blindly** — the operation is *partially applied*: some sub-batches succeeded, others failed. `getRegionErrors()` is keyed by internal sub-batch indices (not region IDs, not user keys), so the affected keys are unknown; treat the batch as partially applied and re-check per key (see [Batch operations are not atomic](operations.md), and the per-operation verdicts in [Idempotency and Retry Safety](#idempotency-and-retry-safety)) |
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
3. A **transport-level** failure (`GrpcException`) on the primary-key
   `KvCommit` RPC raises
   [`UndeterminedCommitException`](../src/Client/TxnKv/Exception/UndeterminedCommitException.php)
   and leaves the status `Undetermined` (#216): the commit may already be
   applied, so the transaction is closed and never rolled back — resolve the
   ambiguity out of band (e.g. re-check the primary key's status). A
   `KeyError`-based commit failure still propagates as a definite failure.

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
re-applying. The per-operation verdicts are in
[Idempotency and Retry Safety](#idempotency-and-retry-safety).

## Idempotency and Retry Safety

[Caller Retryability](#caller-retryability) answers "may I retry this
*exception*?". This section answers the other half: "may I repeat this
*operation*?". The two are different questions. A `GrpcException` is
retryable in the sense that the client already retried it — but repeating a
`compareAndSwap()` is still wrong, because the operation itself is not
idempotent.

An **ambiguous failure** is one where the client cannot tell whether the
request was applied: a gRPC deadline exceeded, a connection reset, or a
[`RetryBudgetExhaustedException`](../src/Client/Retry/RetryBudgetExhaustedException.php)
(the internal retry loop gave up). After such a failure the caller has to
decide: *repeat the operation*, or *assume it was applied and verify*. The
tables below give that verdict for every public operation of `RawKvClient`,
`TxnKvClient` and `Transaction`, and for the `ScanIterator` `RawKvClient`
hand out. The lower-level helpers behind them (`RawKvScanner`,
`SstIngestor`, the connection layer) carry the verdict of the operation
that uses them.

Two questions are answered per operation:

- **Idempotent** — does repeating the operation with the same arguments
  converge to the same state? For a **write** that means the same result
  ends up in the cluster; for a **read** it means the call has no side
  effect at all — a read answers from the *current* state, so the answer
  can of course differ when the data changed in between. Inside a
  transaction the snapshot is fixed at `startTs`, so a repeated read
  answers the same.
- **Safe to retry after an ambiguous failure** — may the caller repeat the
  operation without first re-reading the state?

Verdict legend:

- **Yes** — repeating the operation is safe; the state converges (a read
  simply answers again).
- **No** — repeating the operation can produce a *different* result than the
  first attempt. Re-read the affected state and decide from that.
- **With care** — repeating converges only under the condition stated in the
  notes; outside it the call fails instead of converging.

The Notes column narrows a verdict: it names the part that does *not*
converge (a TTL that restarts on every attempt, a lock TTL that is
extended) or the condition under which the operation converges at all. A
narrowed verdict is still a verdict — the operation stays safe to repeat,
only the timing it produces differs.

### RawKV operations

| Operation | Idempotent | Safe to retry after an ambiguous failure | Notes |
|---|---|---|---|
| `RawKvClient::get()` | Yes | Yes | Read-only. A repeated read returns the *current* value, which may differ from the first answer when a concurrent write landed in between |
| `RawKvClient::getKeyTTL()` | Yes | Yes | Read-only. The TTL keeps counting down, so the second answer can be smaller |
| `RawKvClient::batchGet()` | Yes | Yes | Read-only. A `BatchPartialFailureException` returns **no** results — re-run the whole call |
| `RawKvClient::scan()`, `RawKvClient::scanPrefix()`, `RawKvClient::reverseScan()`, `RawKvClient::batchScan()` | Yes | Yes | Read-only. The rows are the same set the first call returned unless the data changed in between |
| `RawKvClient::scanIterator()`, `RawKvClient::scanPrefixIterator()` | Yes | Yes | Read-only, but **lazy**: the failure surfaces from the iteration, not from the factory call. A new iterator starts again at the beginning of the range. Exception: the cursor-progress `TiKvException` (#312) is not transient, so do not loop on it |
| `ScanIterator::current()`, `ScanIterator::key()`, `ScanIterator::next()`, `ScanIterator::valid()`, `ScanIterator::rewind()` | Yes | Yes | Read-only. `next()` and `valid()` issue the scan RPCs, so an ambiguous failure surfaces from the iteration, not from the factory call — repeat the iteration from the beginning of the range (`rewind()` restarts the same iterator at the range's first key) |
| `RawKvClient::checksum()` | Yes | Yes | Read-only. The merged CRC64-XOR covers the whole range because a failed region aborts the call with a `BatchPartialFailureException` instead of returning a partial result |
| `RawKvClient::put()` | Yes | Yes | The same key/value is written again, so the final value is the same. With a TTL the **TTL restarts on every attempt** — the TTL travels as a relative number of seconds and TiKV computes the expiry when it applies the write — so a retry extends the key's lifetime. In atomic mode the write also carries `for_cas` and takes TiKV's atomic path, but it stays an unconditional overwrite of the same value |
| `RawKvClient::delete()` | Yes | Yes | Deleting an already-deleted key is not an error |
| `RawKvClient::batchPut()` | Yes | Yes | The same key/value pairs converge, and the TTL restarts on every attempt as for `put()`. A `BatchPartialFailureException` leaves an **unknown subset** written — re-run the whole batch |
| `RawKvClient::batchDelete()` | Yes | Yes | Deleting an already-deleted key is not an error. A `BatchPartialFailureException` leaves an unknown subset deleted — re-run the whole batch |
| `RawKvClient::deleteRange()` | Yes | Yes | The range is deleted region by region; re-running deletes what is left. A `BatchPartialFailureException` leaves an unknown subset deleted |
| `RawKvClient::deletePrefix()` | Yes | Yes | A wrapper around `deleteRange()` |
| `RawKvClient::ingest()` | Yes (same input) | **With care** | Re-run the **whole** call, never a part of it — see [Ingest is never retried for you](#ingest-is-never-retried-for-you). The TTL restarts on every attempt, exactly as for `put()`, so a retry extends the keys' lifetime |
| `RawKvClient::compareAndSwap()` | **No** | **No** | See [The CAS and putIfAbsent retry hazard](#the-cas-and-putifabsent-retry-hazard) |
| `RawKvClient::putIfAbsent()` | **No** | **No** | The same call as `compareAndSwap()` with `previous_not_exist = true` — see [The CAS and putIfAbsent retry hazard](#the-cas-and-putifabsent-retry-hazard) |
| `RawKvClient::setAtomicForCAS()`, `RawKvClient::isAtomicForCAS()` | Yes | Yes | Client-local flag; no cluster state is touched |
| `RawKvClient::setColumnFamily()`, `RawKvClient::getColumnFamily()` | Yes | Yes | Client-local setting; no cluster state is touched |
| `RawKvClient::create()`, `RawKvClient::close()` | Yes | Yes | `create()` validates the options (`InvalidArgumentException` on a bad option) and, with `apiVersion => 2`, reads PD to resolve the keyspace — on the default V1 path it does not contact PD at all. `close()` is idempotent — a second call does nothing |
| `RawKvClient::getClusterId()`, `RawKvClient::healthCheck()` | Yes | Yes | A local accessor and a read-only PD probe; no user data is touched |
| `RawKvClient::getMetrics()` | Yes | Yes | Pure accessor |

### TxnKV operations

| Operation | Idempotent | Safe to retry after an ambiguous failure | Notes |
|---|---|---|---|
| `TxnKvClient::begin()` | Yes | Yes | It mints a fresh start timestamp and returns a fresh transaction, so a retry produces a **new** transaction, never a continuation of the old one. A `TxnAbortedByGcException` means the start timestamp is permanently behind the GC safe point — begin again *later*, not again right now |
| `TxnKvClient::beginOptimistic()` | Yes | Yes | Same as `begin()`: it mints a fresh start timestamp and returns a fresh transaction, so a retry produces a **new** transaction |
| `TxnKvClient::beginPessimistic()` | Yes | Yes | Same as `begin()`: it mints a fresh start timestamp and returns a fresh transaction, so a retry produces a **new** transaction |
| `TxnKvClient::holdGcSafePoint()` | Yes | Yes | Registering the same service safe point twice with the same arguments is the same registration — PD keeps the minimum safe point **across all service ids**, and a re-registration under the same service id replaces that id's entry |
| `TxnKvClient::releaseGcSafePoint()` | Yes | Yes | A removal is idempotent; releasing a service safe point that is not registered changes nothing |
| `TxnKvClient::create()`, `TxnKvClient::close()` | Yes | Yes | `create()` validates the options and, with `apiVersion => 2`, reads PD to resolve the keyspace — on the default V1 path it does not contact PD at all. `close()` is idempotent |
| `TxnKvClient::getClusterId()`, `TxnKvClient::healthCheck()` | Yes | Yes | A local accessor and a read-only PD probe; no user data is touched |
| `TxnKvClient::getMetrics()` | Yes | Yes | Pure accessor |
| `Transaction::get()`, `Transaction::batchGet()`, `Transaction::scan()` | Yes | Yes | Read-only inside the transaction's snapshot at `startTs`, so a repeated read sees the same version of the data |
| `Transaction::set()`, `Transaction::delete()` | Yes (same value) | **With care** | The value is buffered locally, so the same call twice writes the same thing. But in the default eager pessimistic mode the physical lock is acquired *before* the call returns: a failed `set()`/`delete()` may have left a lock behind and leaves the transaction unwritable (`InvalidStateException` on the next write). Repeating therefore converges only while the transaction is still writable. The transaction cannot be continued — discard it and begin a new one |
| `Transaction::commit()` | **No** | **No** | See [Transaction commit and the new-transaction rule](#transaction-commit-and-the-new-transaction-rule) |
| `Transaction::rollback()` | Yes | **With care** | Rolling back an already-rolled-back transaction is a semantic no-op, but the client rejects it: `rollback()` requires an `Active` transaction and raises `InvalidStateException` otherwise. Retry only while `getStatus()` is still `Active` — and never after an `UndeterminedCommitException`, where the transaction was closed on purpose and must not be rolled back |
| `Transaction::heartbeat()` | Yes | Yes | It only extends the primary lock's TTL, so repeating it grants another TTL — the lock outlives a single call, and nothing else changes. It requires a **writable** transaction (`InvalidStateException` otherwise) and a **pessimistic** one: an optimistic transaction holds no primary lock, so TiKV answers with a `KeyError` that surfaces as a retryable `TxnRetryableException` |
| `Transaction::getTxnId()`, `Transaction::getStartTs()`, `Transaction::getCommitTs()`, `Transaction::getStatus()`, `Transaction::isPessimistic()`, `Transaction::getPriority()`, `Transaction::getWriteSet()`, `Transaction::getReadSet()` | Yes | Yes | Local state getters; no cluster state is touched |

### The CAS and putIfAbsent retry hazard

`compareAndSwap()` — and `putIfAbsent()`, which is the same call with
`previous_not_exist = true` — is the one RawKV write that is **not**
idempotent, and the client does not retry it on transport errors on purpose
(#239). `RawKvAtomic::compareAndSwap()` installs a classifier that rethrows
every `GrpcException` from inside the retry executor, so a deadline exceeded
or a connection reset reaches the caller **unretried**. Region errors
(`NotLeader`, `EpochNotMatch`, …) are different: they are rejected before
the write is proposed, so the client still retries them automatically.

The failure mode is the reason. The server may have applied the swap and lost
the response; the client cannot tell, so it does not retry and leaves the
decision to the caller. The caller must not retry blindly either:

- **`compareAndSwap('counter', '5', '6')`.** The first attempt wrote `'6'`
  and its response was lost. The retry compares `'6'` against the expected
  `'5'` and answers `swapped: false` with `previousValue: '6'` — the caller
  reads "somebody else changed the value", although it was its own first
  attempt that changed it. A caller that reports this as a lost update has
  lost a write.
- **`putIfAbsent('lock:resource', 'owner-1')`.** The first attempt wrote
  `'owner-1'`; the retry finds the key present and returns `'owner-1'` — the
  caller concludes "the lock is held by somebody else" and walks away, while
  it in fact holds the lock itself. A distributed-lock user must treat a
  thrown exception as *indeterminate*, never as "not acquired".

The recovery is the same in both cases: **re-read the key and decide from
what you find.** If the value is already the one you wanted to write, the
first attempt succeeded; if it is something else, somebody else won the race
and you have to decide again. The CAS loop in
[Compare And Swap](operations.md#compare-and-swap-cas) does exactly that.

### Ingest is never retried for you

`RawKvClient::ingest()` is the one write the client does **not** run through
the retry executor: `SstIngestor::ingest()` has no `RetryExecutor`, so a
region error or a transport error aborts the remaining regions and
propagates.

Three consequences:

1. **The final content converges.** The same key/value pairs produce the same
   data and every region is overwritten by its SST, so repeating the call is
   not a double-apply. With a TTL the **TTL restarts on every attempt**,
   exactly as for `put()`: the TTL travels as a relative number of seconds
   and TiKV computes the expiry when it applies the write, so a re-run
   extends the keys' lifetime — the values converge, the expiry does not.
2. **The cluster may hold a partial import.** The regions ingested before the
   failure keep their data; the rest was never written. Re-running the whole
   call converges.
3. **A killed process leaves the cluster in import mode.** The switch back to
   normal mode runs in a `finally` block, so an exception is safe — but a
   SIGKILL, an OOM kill or an execution-time abort is not, and every store
   stays in import mode, which changes the behaviour of every client of the
   cluster. See [Ingest](operations.md#ingest) for the recovery procedure.

Practical rule: run `ingest()` in a supervised CLI process, size the batch so
that you can afford to re-run it, and re-run the **whole** call after a
failure — never a part of it.

### Transaction commit and the new-transaction rule

`Transaction::commit()` is not idempotent as a whole, and the client makes an
asymmetry inside it that the caller never sees:

- The **primary** region's `KvCommit` is deliberately **not** retried. A
  transport failure there raises
  [`UndeterminedCommitException`](../src/Client/TxnKv/Exception/UndeterminedCommitException.php)
  (#216): the commit may already be applied, so the transaction is closed and
  is never rolled back. Re-issuing the commit with the same `start_version`
  and `commit_version` would be safe on the wire, but the client cannot know
  the outcome and the transaction object is finished.
- The **secondary** regions' commits **are** retried automatically, because a
  commit is idempotent for a fixed `start_version` + `commit_version`:
  replaying it cannot write the data twice.

**Guidance: after a failed `commit()`, begin a new transaction.** Never
re-invoke `commit()` on the same object:

- if the first attempt reached the commit point, the status is `Committed` and
  a second `commit()` raises `InvalidStateException`;
- if it did not, the conflicts have to be resolved against fresh reads
  anyway;
- if the outcome is undetermined, the write may be applied — re-read the keys
  in a **new** transaction before re-applying, or you double-apply.

```php
use CrazyGoat\TiKV\Client\TxnKv\Exception\UndeterminedCommitException;

const TXN_ATTEMPTS = 3;

for ($attempt = 1; $attempt <= TXN_ATTEMPTS; $attempt++) {
    $txn = $tikv->begin();      // a NEW transaction every round
    try {
        $txn->set('k', 'v');
        $txn->commit();
        break;
    } catch (UndeterminedCommitException $e) {
        // The commit may already be applied. Re-read the key in a NEW
        // transaction and only re-apply it if it is not there.
        if ($attempt === TXN_ATTEMPTS) {
            throw $e;
        }
    }
}
```

Conflicts and deadlocks are retried the same way — with a new transaction and
a jittered delay; see
[Retrying transactions safely](#retrying-transactions-safely).

## RawKvClient Operations

One row per public method. "Throws" lists every exception class the method
can raise, verified against both the `@throws` annotations and the method
bodies (several scan methods validate limits even where no annotation says
so). Three routing exceptions are deliberately *not* repeated in every row:
`StoreNotFoundException` and `InvalidStoreAddressException` can escape any
operation that resolves a store address on the RPC path, and `PdException`
can escape any operation that resolves a region or a store through PD —
treat them as an additional possibility for every row that includes
`RegionException`. (Since issue #234 every PD response header is checked, so
a PD application-level error such as `NOT_BOOTSTRAPPED` is raised as
`PdException` instead of degrading into an empty result — e.g. `scanRegions()`
can no longer return `[]` for a failed `ScanRegions`, so `deleteRange()`
cannot report success having deleted nothing.)

| Method | Throws | Notes |
|---|---|---|
| `__construct` | `InvalidArgumentException` | Bad option values (`retryDeadlineMs < 0`, `maxConcurrency < 1`, `maxScanRows < 1`) |
| `create()` | `InvalidArgumentException` | Empty PD endpoints array; invalid `options['tls']` material, `options['timeout']`, `options['slowLog']`, `options['metrics']`, `options['allowedStoreHosts']`, `options['storeHostPolicy']`, `options['allowedStorePorts']`, `options['retryDeadlineMs']`, `options['maxConcurrency']`, `options['maxScanRows']` |
| `setAtomicForCAS(bool)` / `isAtomicForCAS()` | — | Plain setters/getters, nothing thrown |
| `setColumnFamily(string)` / `getColumnFamily()` | — | Plain setters/getters, nothing thrown |
| `get(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Key validated non-empty + size limit before the RPC |
| `put(string, string, int $ttl = 0)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Same validation as `get()` plus value size |
| `delete(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Idempotent at the TiKV level |
| `getKeyTTL(string)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException` | Returns `null` when the key has no TTL |
| `compareAndSwap(...)` | `ClientClosedException`, `InvalidArgumentException`, `InvalidStateException`, `RegionException`, `GrpcException` | `InvalidStateException` with `CompareAndSwap requires atomic mode (enable via setAtomicForCAS(true))` when atomic mode is off |
| `putIfAbsent(...)` | `ClientClosedException`, `InvalidArgumentException`, `InvalidStateException`, `RegionException`, `GrpcException` | Delegates to `compareAndSwap()`; requires atomic mode (`setAtomicForCAS(true)`), same `InvalidStateException` message |
| `batchGet(array)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Empty input returns `[]` without RPC |
| `batchPut(array, int|array $ttl = 0)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | Per-key validation happens before any send |
| `batchDelete(array)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `BatchPartialFailureException` | As above |
| `scanIterator(...)` | `ClientClosedException`, `InvalidArgumentException` | `batchSize` validated synchronously in the factory call (`ScanIterator::__construct`, bounds 1..10240); the underlying scan RPCs happen during iteration |
| `scanPrefixIterator(string, int $batchSize = 1024, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException` | Same synchronous validation as `scanIterator()` |
| `scan(string, string, int $limit = 0, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `ScanLimitExceededException` | Limit validated (`'Scan limit must be 0 or greater'`; max 10240 per RPC). `limit: 0` returns the whole range by paging internally; collecting more than `options['maxScanRows']` rows throws `ScanLimitExceededException` instead of truncating (issue #191). A bounded scan returns at most `limit` rows, so a result whose count equals the limit may be truncated (issue #440) |
| `scanPrefix(string, int $limit = 0, bool $keyOnly = false)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `ScanLimitExceededException` | Delegates to `scan()`; same `limit: 0` pagination + guard semantics |
| `reverseScan(...)` | `ClientClosedException`, `InvalidArgumentException`, `RegionException`, `GrpcException`, `ScanLimitExceededException` | Limit validated; `limit: 0` pages internally (descending bound moves down each page) and is guarded by `options['maxScanRows']` |
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
| `__construct` | `InvalidArgumentException` | `retryDeadlineMs < 0`, `lockWaitTimeMs < 0` |
| `getTxnId()` / `getStartTs()` / `getCommitTs()` / `getStatus()` / `isPessimistic()` / `getPriority()` / `getWriteSet()` / `getReadSet()` | — | State getters, nothing thrown |
| `get(string)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException` | Locks resolved inline; a lock hit surfaces as `TxnRetryableException` (subclass of `TiKvException`) consumed by the retry executor |
| `batchGet(array)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException` | Per-region reads use the shared retry executor; locked keys are resolved and the read is retried before a terminal error is exposed |
| `scan(string, string, int $limit = 0)` | `InvalidArgumentException`, `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `RegionException`, `GrpcException`, `ScanLimitExceededException` | Limit normalized/rejected ('Scan limit must be 0 or greater', max 10240). `limit: 0` reads the whole range up to `Transaction::MAX_SCAN_LIMIT` (10240) rows; a range with more rows throws `ScanLimitExceededException` instead of returning a truncated result (issue #440). A bounded scan returns at most `limit` rows, so a result whose count equals the limit may be truncated |
| `set(string, string)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`, `RegionException`, `GrpcException` | Buffers the value locally; default pessimistic mode acquires the physical lock before returning, so lock errors surface here. A failed eager lock triggers best-effort cleanup rollback; if cleanup fails, the transaction remains active but unwritable and `rollback()` can be retried. Set `eagerPessimisticLocks => false` to defer locking to `commit()` |
| `delete(string)` | `InvalidStateException`, `TiKvException`, `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`, `RegionException`, `GrpcException` | Same eager pessimistic-lock behavior and failure cleanup as `set()`; the tombstone remains buffered until commit |
| `commit()` | `InvalidStateException`, `TiKvException` (incl. [`UndeterminedCommitException`](../src/Client/TxnKv/Exception/UndeterminedCommitException.php)), `TransactionConflictException`, `DeadlockException`, `LockWaitTimeoutException`, `RegionException`, `GrpcException` | See [double-apply section](#transactioncommit-and-double-apply); since issue #213 prewrite runs inside the shared retry executor, so region errors and resolved `locked` conflicts are auto-retried while deadlocks/conflicts still escape |
| `rollback()` | `InvalidStateException`, `TiKvException`, `RegionException`, `GrpcException` | Only an `Active` transaction may be rolled back; a failed RPC leaves it active but non-writable so rollback can be retried |
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
    // 4. Partially applied batch. getRegionErrors() is keyed by internal
    //    sub-batch indices (not region IDs, not user keys), so the affected
    //    keys are unknown — re-check per key instead of re-driving "just
    //    those"; idempotent operations can be retried whole (see operations.md).
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
    //    InvalidStoreAddressException, PdException, HealthCheckException,
    //    TxnRetryableException, ...). Default: do not retry. PdException
    //    additionally carries getErrorType() — see its retryability row.
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
  `InvalidStoreAddressException` are fatal immediately. A fatal
  `KeyNotInRegion` still drops the cached region for the key before the
  exception propagates, so the next request re-resolves instead of repeating
  the same misroute until the entry's TTL expires; the other fatal kinds do not
  drop it, because they say nothing about which region owns the key. (#233)

See also:

- [Operations guide](operations.md) — what each method does
- [Configuration guide](configuration.md) — `retryDeadlineMs`,
  `serverBusyBudgetMs`, `maxBackoffMs`, `maxConcurrency`, `maxScanRows` options
- [Troubleshooting](troubleshooting.md) — symptom-first problem solving
- [Advanced features](advanced.md) — retry-strategy chapter

## GC Safe Points and Long-Running Reads

TiKV's GC periodically removes old versions of data. Everything below the
cluster's **GC safe point** is unreachable for reads: a transaction whose
`startTs` is older than the safe point cannot read any more and is rejected
by TiKV with the abort text `GC life time is shorter than transaction
duration`.

### Read-side validation (on by default)

`TxnKvClient` validates every fresh start timestamp against the cluster's
GC safe point at `begin()`:

- The safe point is fetched from PD (`GetGCSafePoint`) and cached for **30 s**
  by default (configurable via `options['gcSafePointRefreshMs']`, must be
  `>= 1`), so validation adds no PD round trip per transaction.
- When the timestamp is already behind the safe point, `begin()` throws
  [`TxnAbortedByGcException`](../src/Client/TxnKv/Exception/TxnAbortedByGcException.php)
  immediately instead of letting the transaction fail later, mid-read.
- A PD failure while refreshing the safe point **degrades to a warning**
  (fail-open): begin() proceeds, the transaction simply runs unvalidated.
  Validation is a fast-fail optimization, never a new failure mode.
- Disable entirely with `options['gcSafePointValidation'] => false` (e.g.
  for tooling that must construct transactions against a PD subset).

The same failure is also caught at the read itself: `get()` and `scan()`
map TiKV's GC abort to the same `TxnAbortedByGcException` (previously the
abort fell through silently and the read returned as if the key did not
exist).

### Holding GC back for a long-running job

A scan, report job or batch export that may legitimately run longer than
the cluster's `gc_life_time` should register a **service safe point** with
a TTL before it starts, and refresh it periodically:

```php
$txn = $client->begin();
$client->holdGcSafePoint($txn->getStartTs(), ttlSeconds: 120);

try {
    foreach ($txn->scan($start, $end) as $row) {
        // ... long-running processing ...
        $client->holdGcSafePoint($txn->getStartTs(), ttlSeconds: 120); // refresh before expiry
    }
    $txn->commit();
} finally {
    $client->releaseGcSafePoint(); // orderly shutdown: let GC advance again
}
```

- While the registration is fresh, GC will not advance past the safe point
  you registered, and reads at that timestamp keep working.
- `holdGcSafePoint()` returns the resulting min safe point across all
  services (the value GC is actually held at), or `null` when the cluster's
  PD does not support service safe points (older PD / GC v1) — treat `null`
  as "GC is not held", and keep jobs under `gc_life_time`.
- Choose a TTL comfortably longer than your refresh period so a crashed
  worker cannot block cluster GC forever; a lapsed TTL releases the hold
  automatically.
- The default service ID is per client instance (`tikv-php-txnkv-<random>`),
  so two clients never overwrite each other's registration; pass an explicit
  `$serviceId` to share a hold between cooperating processes.
