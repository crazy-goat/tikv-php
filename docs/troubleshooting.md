# Troubleshooting Guide

Common issues and solutions for the TiKV PHP Client.

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Connection Issues](#connection-issues)
3. [Runtime Errors](#runtime-errors)
    - [Region Errors](#region-errors)
    - [TTL Errors](#ttl-errors)
    - [Transaction Failures](#transaction-failures)
    - [PD Metadata Errors](#pd-metadata-errors)
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

The library requires the **`ext-grpc`** PHP extension, version **1.57 or newer**
(`"ext-grpc": ">=1.57"` in `composer.json`). A missing extension is also
reported by Composer, before any code runs:

```
crazy-goat/tikv-client <version> requires ext-grpc >=1.57 -> it is missing from your system. Install or enable PHP's grpc extension.
```

`<version>` is the version of the package you install.
Contributors working on this repository see the root-package wording of the
same error instead:

```
Root composer.json requires PHP extension ext-grpc >=1.57 but it is missing from your system. Install or enable PHP's grpc extension.
```

**Solution:**

1. Check if gRPC is installed, and which version:
   ```bash
   php -m | grep grpc
   php -r 'echo phpversion("grpc"), PHP_EOL;'
   ```

2. Install gRPC extension:
   ```bash
   # Debian/Ubuntu (PECL - the build tools are needed, PECL compiles the
   # extension from source)
   sudo apt-get install php-dev build-essential autoconf
   sudo pecl install grpc

   # Debian/Ubuntu (distribution package from the sury.org PHP repository,
   # https://packages.sury.org/php/ - not part of stock Debian/Ubuntu)
   sudo apt-get install php-grpc

   # Alpine
   apk add php83-pecl-grpc

   # macOS (Homebrew)
   pecl install grpc
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

See [Installing the gRPC Extension](getting-started.md#installing-the-grpc-extension)
for the full walkthrough.

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
   rm -rf vendor/
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

5. If the error names `ext-grpc`, the gRPC extension is missing or too old:
   ```
   crazy-goat/tikv-client <version> requires ext-grpc >=1.57 -> it is missing from your system. Install or enable PHP's grpc extension.
   ```
   The extension must be 1.57 or newer — see
   [Installing the gRPC Extension](getting-started.md#installing-the-grpc-extension).

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

#### TLS Configuration Errors

These `InvalidArgumentException`s are raised while the TLS configuration is
built — by `RawKvClient::create()` / `TxnKvClient::create()` or by
`TlsConfigBuilder` — before any gRPC channel is opened, so they never reach
the `Handshake failed` error above. The message names the problem; find it in
the tables below.

**Reading a certificate file (`TlsConfigBuilder`)**

| Message | What it means and how to fix it |
| --- | --- |
| `TLS value looks like a file path but the file does not exist or is not readable` | Only the deprecated `caCert` / `clientCert` / `clientKey` keys guess the input type: a value without a `-----BEGIN` header is treated as a path, and that path is missing or unreadable. Use `caCertFile` (or `caCertPem` for inline content) instead. |
| `TLS file has disallowed extension "%s"` | The path's extension is not `.pem`, `.crt` or `.key`; `%s` is the lowercase extension it found (`""` when there is none). Rename the file, or use a `.pem` / `.crt` / `.key` copy. |
| `Cannot read TLS file: file does not exist or is not readable` | `realpath()` failed or the resolved file is not readable: a typo, a missing file, a dangling symlink or insufficient permissions. Check the path and the permissions of every directory on it. |
| `Invalid TLS base directory: directory does not exist` | The `$baseDir` argument or the `caCertBaseDir` / `clientCertBaseDir` option does not resolve to an existing directory. Use an absolute path to a directory that exists. |
| `TLS file path is outside the allowed base directory` | The canonicalized certificate path is neither the base directory itself nor beneath it. Move the file under the base directory, or point the base option at the directory that contains it. |
| `Cannot read TLS file: read failed` | The file passed the existence, readability and containment checks, but `file_get_contents()` still failed — typically a permission or I/O error, or the file was replaced between the check and the read. Verify that it is a regular, readable file. |
| `TLS client certificate and key must be given in the same form, both file paths or both inline PEM; got …` | Only the deprecated `clientCert` + `clientKey` pair guesses the form, and it must guess the same form for both halves. Give both as file paths or both as inline PEM, or use `clientCertFile` + `clientKeyFile` / `clientCertPem` + `clientKeyPem`. The message says which half was read as which. |

**Building the `TlsConfig`**

| Message | What it means and how to fix it |
| --- | --- |
| `TlsConfig requires $clientCert and $clientKey together; got $clientCert=true, $clientKey=false` | Exactly one of the two properties is set. The values after `got` are presence flags (`true` = set, `false` = missing), not certificate material. Provide both or neither; the `options['tls']` factory reports the same mistake earlier as `Incomplete client certificate pair`. The `$`-prefixed names are the `TlsConfig` properties, not the deprecated `options['tls']` keys of the same bare name. |
| `$caCert does not contain PEM data` (also `$clientCert` and `$clientKey`) | The named property has no `-----BEGIN` header, so it is not PEM — often a file path passed where content is expected. Pass the file to `withCaCertFile()` / `withClientCertFile()` instead. The leading `$` is the property name, not the deprecated option key of the same bare name. |
| `Partial TLS configuration: $clientCert and $clientKey are set but $caCert is not…` | A client certificate and key are configured without a CA certificate. A CA is required to verify the server: add `caCertFile` / `caCertPem`, or remove the client pair if you intend to use an insecure connection. |

The remaining `options['tls']` validation errors — a non-array `tls` value, an
unrecognised key, a non-string value and an incomplete client pair — are
listed in [TLS/SSL Configuration](configuration.md#tlsssl-configuration).

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
   // The client splits a batch by region (512 keys, 16 KiB per RPC).
   // Do not chunk the keys to make the RPC smaller.
   // See operations.md#limits.
   $client->batchGet($keys);
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
- When TiKV supplies `current_regions`, the client refreshes the region cache
  from that payload instead of asking PD for the region again; an empty or
  unusable payload falls back to invalidate-and-resolve-through-PD
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

**Solution:** The client retries internally with backoff. Under the default
30 s retry deadline a sustained `ServerIsBusy` episode surfaces as
`RetryBudgetExhaustedException`: the 60 s ServerBusy budget is charged from
the scheduled backoff before the sleep is clamped, and with the budget at
least ~10 s above the deadline the deadline binds first. `getPrevious()`
holds the `ServerIsBusy` error. If the budget is tuned to within ~10 s of
the deadline or below it, either bound can bind; both paths throw
`RetryBudgetExhaustedException`, with the `ServerIsBusy` error in
`getPrevious()`. If you see this repeatedly, shed load or scale the
cluster. See
[Retry Budgets Behind Every Operation](error-handling.md#retry-budgets-behind-every-operation)
and
[ServerBusy Budget vs Retry Deadline](configuration.md#serverbusy-budget-vs-retry-deadline).

#### Other RegionException Messages

The message template is `<operation> failed: <server error>`, e.g.
`RawGetKeyTTL failed: ...`, `RegionError failed: ...`. The server-side part
is sanitised — control characters and ANSI escape sequences are removed, and
the text is truncated to 512 bytes and marked with `...` (see
[Log Redaction](configuration.md#log-redaction)) — but it is not redacted;
there are more kinds than listed above.

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

### PD Metadata Errors

#### PD Response Header Error

**Error:**
```
PdException: PD <Method> failed: <pdpb.Error.message>
PdException: PD <Method> failed: unknown PD error (header error with empty message)
```
(`<Method>` is the failing PD RPC, e.g. `ScanRegions`, `GetRegion`,
`GetStore`, `LoadKeyspace`, `GetMembers`)

**What it means:** PD answered with a gRPC `OK` status and an error inside
`pdpb.ResponseHeader.error` — `NOT_BOOTSTRAPPED`, `ErrNotLeader`,
`INVALID_VALUE`, `REGION_NOT_FOUND`, … — with an **empty payload**. Since
issue #234 every PD response header is checked at one choke point, so the
error is raised instead of read as a successful empty result. The exception
carries `getErrorType()` (the raw `pdpb.ErrorType`),
`getErrorTypeName()` and `isNotLeader()`. The second message form is a typed
error whose text is empty: still an error, and deliberately says so rather
than reporting a blank reason.

**Likely cause:** The cluster is not bootstrapped yet or is being
bootstrapped, the endpoint answered is not the PD leader, or the request was
malformed. Notably `scanRegions()` used to answer `[]` here, which made
`deleteRange()` return successfully having deleted nothing.

**Solution:** Inspect `getErrorType()` first. `isNotLeader()` means the
client already rotated to another configured endpoint and retried once per
endpoint before this reached you — check that *all* configured PD endpoints
are reachable and that the member list is consistent
(`curl http://<pd-host>:2379/pd/api/v1/members`). `NOT_BOOTSTRAPPED` means
wait for the cluster (or check `pd-ctl` bootstrap state); `INVALID_VALUE`
means the request itself is wrong. No user data was touched, so re-driving
an idempotent operation once PD is healthy is safe.

### Base Exception

#### TiKvException

**Error:**
```
TiKvException: PD GetRegion returned no region for key
TiKvException: TSO request failed: gRPC error: ...
TiKvException: TSO response missing timestamp
TiKvException: TSO response contained an invalid physical timestamp
TransactionConflictException: Heartbeat failed: retryable: ...
TxnRetryableException: Heartbeat failed: locked key #a3f5c8e1d4b79026 (4 bytes)
TiKvException: Heartbeat failed: TxnNotFound
```
(the `#…` token is the keyed-hash redaction of the primary key; the 16 hex
characters depend on the process salt, so the exact value differs per process)

**What it means:** Failures raised directly as the base class instead of a
subclass: PD answered a region lookup with no region (fail-closed routing),
the timestamp oracle could not allocate a PD timestamp, or a transactional
heartbeat hit a key error (issue #492: the heartbeat surfaces the server's
`KeyError` variant — `retryable`/`abort` arrive as `TransactionConflictException`,
`locked` as `TxnRetryableException`, the remaining variants as the base class
with the variant named).

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
> V1 override, TTL operations behave exactly as described above. A
> TTL-enabled cluster cannot serve TxnKV — see
> [TxnKV Fails on a TTL-Enabled Cluster](#txnkv-fails-on-a-ttl-enabled-cluster).

### Transaction Failures

These are raised by the transactional (`TxnKv`) API during prewrite, commit
and pessimistic locking — the errors a transactional workload hits first and
most often. All of them mean **the operation did not complete** — on write
paths nothing was applied (reads may simply need re-running), except for an
undetermined commit, whose write may have landed. Retry with a *new*
transaction (fresh begin, re-read all values) rather than
replaying into the stale snapshot. See
[Transaction Operations](error-handling.md#transaction-operations) and
[Retrying transactions safely](error-handling.md#retrying-transactions-safely).

#### TxnKV Fails on a TTL-Enabled Cluster

Transaction operations fail with region or server errors on a cluster that
has `enable-ttl = true`.

**What it means:** The cluster runs in TiKV's V1TTL storage mode
(`[storage] enable-ttl = true`), which serves RawKV with TTL but **not**
transactional requests. TTL mode and TxnKV are mutually exclusive on a
single cluster (TiKV's `APIVersion.V1TTL` accepts only raw requests).

**Solution:** Use a separate cluster for TxnKV — a TiKV in default V1 mode
with `enable-ttl` unset (see `tikv-v1.toml`). A single cluster cannot serve
both RawKV-with-TTL and TxnKV.

> See [TTL Not Enabled](#ttl-not-enabled) for the interplay with TTL, and
> the compose override `docker-compose.txnkv.yml` (V1 mode) used by the
> TxnKV E2E suite.

#### Write Conflict

**Error:**
```
TransactionConflictException: Write conflict during prewrite
```
Also thrown from the commit and pessimistic-lock paths
(`Write conflict during pessimistic lock`); when TiKV returns its own
retryable/abort strings they are sanitised like every other server-supplied
message (control characters and ANSI escapes removed, truncated to 512 bytes
and marked with `...`) but not redacted.

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
LockWaitTimeoutException: Lock wait timeout for key: <redacted> (budget <maxBackoffMs>ms, per-request wait <lockWaitTimeMs>ms)
```

**What it means:** A pessimistic lock wait exhausted its wait budget
(`maxBackoffMs`, default 20000 ms) without acquiring the lock (issue #219).
`getTimeoutMs()` / `getBudgetMs()` report that budget; `getConfiguredWaitTimeoutMs()`
reports the configured per-request `lockWaitTimeMs`. The value actually sent
is capped by the remaining budget and by the gRPC deadline (write timeout
minus 100 ms), so it can be smaller than the configured value (issue #229). A
configured `0` is rendered as "TiKV default".

**Likely cause:** A competing transaction holds the lock longer than your
budget — long-running transaction, or a genuinely stuck lock.

**Solution:** Increase `maxBackoffMs` only if the holder is legitimately
slow; otherwise retry in a new transaction. The competing key is redacted in
the message; call `getKey()` for the actual key if your log pipeline is
allowed to see it. `lockWaitTimeMs` tunes how long *one* lock request may
block on the server (default 1000 ms); it can be set client-wide on
`TxnKvClient::create()` or per transaction on `begin(['lockWaitTimeMs' =>
N])` (issue #443). It never exceeds the remaining budget, and a positive
value is capped by the request's gRPC deadline (write timeout minus 100 ms)
so TiKV can answer before the client cancels the call (issue #229).

#### Txn Retryable (Lock Resolved Mid-Operation)

**Error:**
```
TxnRetryableException: Lock encountered, resolved - retry
```
Other real messages follow the same pattern: `Lock conflict during prewrite, resolved - retry`, `Lock encountered during BatchGet, resolved - retry`, `Lock encountered during Scan, resolved - retry`, `Lock encountered during rollback, resolved - retry`, `Retryable error during rollback: <server string>`.

**What it means:** The operation hit another transaction's lock, resolved it
(checking expiry / pushing forward), and expects the caller's retry loop to
take it from here. Normally consumed internally; it escapes only when the
surrounding executor gives up.

**Solution:** Treat like other transaction errors: retry in a new
transaction unless the operation is idempotent. The carried
`$e->backoffType` says how the internal loop would have backed off.

#### Undetermined Commit (Never Roll Back)

**Error:**
```
UndeterminedCommitException: Primary commit failed at the transport level; the transaction outcome is undetermined and was not rolled back
```

**What it means:** The `KvCommit` RPC for the transaction's **primary** key
failed at the transport level after it was sent, so the commit may already be
applied. `getStatus()` returns `TransactionStatus::Undetermined` and the
client closes the transaction; `__destruct()` only rolls back `Active`
transactions, so it never undoes a commit that may have landed (issue #216).

**Likely cause:** A dropped connection, a timeout or a killed proxy between
the client and the region leader during the primary commit. This is not a
write conflict or a deadlock — those leave the transaction's outcome definite.

**Solution:** Never roll back and never re-commit the same transaction object.
Resolve the outcome out of band before re-applying anything: re-check the
primary key's status (TiKV's `CheckTxnStatus`, or a read of the key in a
**new** transaction) and only re-run the business operation if the write did
not take effect. A blind retry can double-apply it. See
[Transaction commit and the new-transaction rule](error-handling.md#transaction-commit-and-the-new-transaction-rule)
for the full guidance and a retry loop.

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

**What it means:** Input validation failed *before* the operation sent its
request to TiKV — empty/oversized keys or values, negative or too-large scan
limits, bad option values. (A pessimistic `Transaction::scan()` refreshes its
read timestamp through PD first, so that TSO request may already have been
sent; the scan RPC is not.) Note this class extends `\InvalidArgumentException`
directly and is **not** a `TiKvException`, so `catch (TiKvException)` will
not catch it.

**Solution:** Fix the calling code; never retry (retrying reproduces the
same failure deterministically). Real messages include:

- `Key must not be empty in <method>`
- `Key size (<len>) exceeds maximum allowed size (<max>) in <method>`
- `Value size (<len>) exceeds maximum allowed size (<max>) in <method>`
- `Scan limit must be 0 or greater` / `Scan limit (<n>) exceeds maximum allowed scan limit of <max>` (max 10240 per RPC; `limit: 0` returns the whole range by paging internally, buffered up to `options['maxScanRows']` — for a range larger than that, or for constant memory, use the lazy [scan iterators](operations.md#iterating-large-ranges))
- `Unbounded scan collected <n> rows, exceeding the configured maximum of <max>` (`ScanLimitExceededException`) — a `limit: 0` scan matched more rows than `options['maxScanRows']` (default 100000) and refuses to silently truncate; switch to the lazy scan iterators or raise the option
- `Transactional scan collected <n> rows, exceeding the maximum of <max> rows` (`ScanLimitExceededException`) — a `Transaction::scan()` with `limit: 0` matched more rows than `Transaction::MAX_SCAN_LIMIT` (10240) and refuses to silently truncate; page the range explicitly (an explicit `limit` plus the last key you received), or use the lazy [scan iterators](operations.md#iterating-large-ranges) for a raw read
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
- `Transaction is not active` — transaction already committed or rolled back (including a live transaction rolled back by `TxnKvClient::close()`); start a new one
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

**What it means:** `BatchAsyncExecutor` aborted a fanned-out batch because the
wall-clock batch deadline expired before all regions answered; some
regions were never dispatched or their results were cancelled.

**Likely cause:** The batch is too large for `batchDeadlineMs` (60000 ms by
default), or TiKV nodes are slow or unreachable. Raise `batchDeadlineMs` in
`options['timeout']`, or set it to `0` to disable the batch deadline.
`getContext()['pendingRegions']` lists every fan-out slot without a result,
including the slots of windows that never started; when the call site supplied
fan-out descriptors, `getContext()['pendingRegionIds']` holds the corresponding
real TiKV region IDs.

**Solution:** A read (`batchGet`, scan, checksum) applies nothing, so retry the
whole operation. Raise `batchDeadlineMs` in `options['timeout']` if the batch is
too large. For writes, do not retry blindly: the batch is **partially applied**.
`getContext()['pendingRegions']` and `['dispatchedRegions']` hold internal
sub-batch or segment indices; `['pendingRegionIds']` and
`['dispatchedRegionIds']` (when present) hold the real TiKV region IDs. Neither
names your user keys. Treat the batch as partially applied and re-check per
key. For scans the deadline applies per region batch, not per `scan()` call.
See [BatchDeadlineExceededException](error-handling.md#exception-hierarchy).

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

**Solution:** Do not retry the whole batch blindly. `getRegionErrors()` maps
internal call indices to exceptions, and `getFailedRegionIds()` names the real
TiKV region IDs of the failed calls (so they can be correlated with `pd-ctl`
and cluster metrics). `getFailedCallIds()` distinguishes several sub-batches of
one region via `subBatchIndex`. The client still does not expose which user
keys belonged to a failed sub-batch, so you cannot re-drive "only the failing
keys": treat the batch as partially applied with unknown affected keys and
re-check per key. deleteRange and checksum are idempotent and safe to retry
whole; batchGet/batchDelete are idempotent per key; a batchPut whose key/value
set is fixed is safe to re-issue as a whole — but non-idempotent values must
not be retried blind (see [Batch Operations](operations.md#batch-operations)).
The per-operation verdicts are in
[Idempotency and Retry Safety](error-handling.md#idempotency-and-retry-safety).

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
stale cache entry expires — inspect the public `$e->storeId` property first. See
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
RetryBudgetExhaustedException: Retry attempt cap (30) exhausted for key #7d0f4a2c9e816b35 (28 bytes)
RetryBudgetExhaustedException: Retry deadline (30000 ms) exhausted for key #7d0f4a2c9e816b35 (28 bytes)
```
(the two messages come from the same class; since issue #269 the key is
redacted by `KeyRedactor` — a truncated keyed hash plus the byte length, so
the example above is literal in shape; the 16 hex characters depend on the
process salt and differ between processes — and the key is never shown
verbatim. Use `RetryBudgetExhaustedException::getRawKey()` if your code needs
the raw key.)

**What it means:** The internal retry loop gave up: one of its four bounds
was reached before the operation succeeded — the attempt cap (30 by default),
the wall-clock deadline (30000 ms by default, `options['retryDeadlineMs']`),
the cumulative backoff budget (`maxBackoffMs`) or the ServerBusy budget
(`serverBusyBudgetMs`). All four paths throw this class, so it always means
"the client stopped retrying"; the original region or transport error is
preserved in `getPrevious()`. With the defaults, a `ServerIsBusy` storm
reaches the deadline first, even though the ServerBusy budget is nominally
60 s (see
[ServerBusy Budget vs Retry Deadline](configuration.md#serverbusy-budget-vs-retry-deadline)).

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
   use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
   use Monolog\Handler\StreamHandler;
   use Monolog\Logger;

   $logger = new Logger('debug');
   $logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));
   $client = RawKvClient::create($pdEndpoints, logger: $logger);
   ```

   The snippet assumes `$pdEndpoints` (the PD endpoints) and `$client` already
   exist; only the logger and the client it builds are new.

2. **Check for retries:**
   - If seeing many retries, TiKV may be overloaded
   - Check TiKV metrics: `curl http://tikv:20180/metrics`

3. **Batch size too large:**
   ```php
   // The client already splits a batch into sub-batches of at most
   // 512 keys and 16 KiB (see operations.md#limits).
   // A smaller chunk does not make an RPC smaller.
   // Chunk only to bound memory or the scope of a partial failure.
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

> The `$lastKey . "\x00"` continuation is `KeyOrder::successor($lastKey)` in library code
> (`CrazyGoat\TiKV\Client\Util\KeyOrder`, issue #186) — the same byte string, but
> documented and unit-tested as the immediate successor of a key.

2. **Large values:**
   ```php
   // Check the size before put()
   if (strlen($data) > RawKvClient::MAX_VALUE_SIZE) {
       // put() rejects a value above 4 MiB with InvalidArgumentException.
       // Split $data into chunks. See operations.md#limits.
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

### Cluster Stuck in Import Mode

**Symptom:** The whole TiKV cluster is degraded for **all** clients after a
bulk-import job died — writes are slow or rejected, and TiKV logs show import
mode still active.

**Cause:** `RawKvClient::ingest()` switches every store into import mode for
the duration of the call and switches back in a `finally` block. `finally`
covers exceptions, but not process death: an OOM kill, a deployment restart, a
`max_execution_time` hit or a fatal error between the two `SwitchMode` calls
leaves every store in import mode. Import mode is cluster-wide state — it
affects every client, not just the one that set it.

**Diagnosis:**

- Correlate the degradation with an `ingest()` run whose PHP process died
  abnormally (check supervisor/systemd logs, OOM killer in `dmesg`, PHP-FPM
  `max_execution_time` warnings).
- The client logs `Switched store to import mode` / `Switched store to normal
  mode` (debug level) per store; a run whose log ends after the import-mode
  line without matching normal-mode lines is the culprit.

**Recovery:**

1. Re-run a successful `ingest()` call (any batch, even a tiny one) against
   the same cluster — its `finally` block issues `SwitchMode(Normal)` to every
   store PD reports, which restores normal mode.
2. Alternatively, issue the ImportSST `SwitchMode(Normal)` RPC to every store
   directly (the `import_sstpb.ImportSST/SwitchMode` gRPC service) — see
   `SstIngestor::switchStoresMode()` in `src/Client/RawKv/SstIngestor.php` for
   the exact request shape.

**Prevention:** run `ingest()` only from a dedicated, supervised CLI process
with no execution-time limit; keep batches small enough to re-run (region
errors are retried, but the import is not atomic across regions, so a
permanent failure can leave a partial import). See
[Bulk Import (SST Ingest)](operations.md#bulk-import-sst-ingest).

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
   $client->setAtomicForCAS(true);  // required: compareAndSwap() throws without it
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
   $client->setAtomicForCAS(true);  // required: putIfAbsent() throws without it
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
| TTL not working | Enable `enable-ttl` in TiKV config (requires wiping existing data) — see [TTL Errors](#ttl-errors) |
| TxnKV fails on TTL cluster | Use a V1-mode cluster (no `enable-ttl`) for TxnKV — see [TxnKV Fails on a TTL-Enabled Cluster](#txnkv-fails-on-a-ttl-enabled-cluster) |
| Data not found | Check key format, TTL, cluster |
| Write conflict / deadlock | New transaction, jittered backoff ([Transaction Failures](#transaction-failures)) |
| Undetermined commit | Never roll back; re-check the key in a new transaction ([Undetermined Commit](#undetermined-commit-never-roll-back)) |
| Batch partial failure | Re-check affected keys — sub-batch indices are not user keys ([Batch Partial Failure](#batch-partial-failure)) |

## See Also

- [Getting Started](getting-started.md) - Basic setup
- [Configuration](configuration.md) - Configuration options
- [Advanced Features](advanced.md) - Production patterns
- [Architecture](architecture.md) - System design
