# Configuration Guide

Complete guide to configuring the TiKV PHP Client for development and production environments.

## Table of Contents

1. [Basic Configuration](#basic-configuration)
2. [Connection Settings](#connection-settings)
3. [gRPC Channel Options](#grpc-channel-options)
4. [TLS/SSL Configuration](#tlsssl-configuration)
5. [Logging](#logging)
6. [Replica Reads](#replica-reads)
7. [Fast Commit Modes (TxnKV)](#fast-commit-modes-txnkv)
8. [Retry and Backoff](#retry-and-backoff)
9. [Caching](#caching)
10. [Timeouts](#timeouts)
10. [Production Configuration](#production-configuration)

## Basic Configuration

### Creating a Client

The simplest configuration:

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$client = RawKvClient::create(['127.0.0.1:2379']);
```

### Full Configuration Options

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a PSR-3 logger
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::INFO));

// Client options
$options = [
    // Concurrency cap for operations that fan out per-region/per-store RPCs
    // (batchScan, deleteRange/deletePrefix/checksum, SST store-mode
    // switches): at most this many units (ranges, regions or stores) are in
    // flight at once. Default: 16. Must be >= 1.
    'maxConcurrency' => 16,
    // GC safe-point validation at TxnKvClient::begin() (issue #422): when
    // true (default), a fresh start timestamp below the cluster's GC safe
    // point throws TxnAbortedByGcException immediately instead of failing
    // mid-read. A PD fetch failure degrades to a warning (fail-open).
    // TxnKvClient only — RawKvClient has no transaction timestamp.
    'gcSafePointValidation' => true,
    // How long the cached GC safe point stays fresh, in milliseconds.
    // Default: 30000 (30 s). Must be >= 1. Larger values reduce PD traffic
    // but let begin() accept timestamps up to this much staler; the read
    // itself still fails typed (TxnAbortedByGcException) when GC has passed.
    'gcSafePointRefreshMs' => 30000,
    // Low-resolution TSO timestamp cache (issue #420): maximum allowed
    // staleness in milliseconds of the timestamp served by
    // PdClientInterface::getLowResolutionTimestamp(). Used only by
    // staleness-tolerant consumers (lock resolution current_ts);
    // start/commit timestamps come from PD (pooled via tsoPoolSize, not a
    // fresh RPC per call). Default: unset = no caching (each call fetches
    // fresh). 0 bounds staleness but never returns a timestamp cached from
    // an earlier millisecond.
    'lowResTimestampMaxStalenessMs' => 200,
    // PD TSO timestamp pool (issue #292): the number of consecutive
    // timestamps requested per pooled Tso RPC. getTimestamp() serves from
    // the pool and refills when exhausted; N calls cost roughly
    // N / tsoPoolSize round trips. The pool is also bounded by a 5 ms
    // real-time-ordering window: a pooled timestamp fetched more than 5 ms
    // ago is discarded and refilled, because serving it could hand out a
    // start_ts below a concurrently committed commit_ts. Accepted on the
    // shared connection factory (RawKvClient::create() and
    // TxnKvClient::create()), but only affects transaction timestamp
    // consumers (TxnKv) — RawKV has no transaction timestamp. Default: 64.
    // 1 disables pooling (one Tso RPC per call). Must be >= 1 and <= 1000.
    // See "Timestamp Batching and Pooling" below.
    'tsoPoolSize' => 64,
    // Replica read preference (issue #421): an instance of
    // ReplicaReadPolicy controlling which peer serves read requests.
    // Default: leader-only (unchanged behaviour). See "Replica Reads".
    'replicaRead' => new ReplicaReadPolicy(ReplicaReadMode::PreferLeader),
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        'clientCertFile' => '/path/to/client.crt',
        'clientKeyFile' => '/path/to/client.key',
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,
    options: $options
);
```

## Connection Settings

### PD Endpoints

The client connects to PD (Placement Driver) to discover the cluster topology:

```php
// Single PD node
$client = RawKvClient::create(['192.168.1.100:2379']);

// Multiple PD nodes (for HA)
$client = RawKvClient::create([
    '192.168.1.100:2379',
    '192.168.1.101:2379',
    '192.168.1.102:2379',
]);
```

**Note**: Currently only the first endpoint is used. Future versions will support failover.

### Timestamp Batching and Pooling (issues #420, #292)

`PdClientInterface::getTimestamp()` hands out timestamps from a small
per-client pool: it requests `count = tsoPoolSize` (default 64)
consecutive timestamps in one `Tso` RPC and serves subsequent calls from
that range, refilling when it is exhausted (issue #292). N calls therefore
cost roughly N / `tsoPoolSize` round trips instead of N. The pool is
discarded — and a fresh grant requested — whenever it is exhausted, ages
past its real-time-ordering window (5 ms by default; serving a timestamp
fetched earlier risks handing out a `start_ts` below a concurrently
committed `commit_ts`), the process forks, or the cluster ID
changes; a PD error is never papered over and no timestamp is ever
fabricated locally. Set `tsoPoolSize` to 1 to restore one `Tso` RPC per
call.

Two additional APIs reduce PD traffic where that is safe and explicit:

- `getTimestampBatch(int $count)` requests up to `$count` timestamps in
  a single `Tso` RPC (`TsoRequest.count`) and hands them out in order
  (consecutive values; the 18-bit logical counter wraps into the next
  physical millisecond correctly). `$count` must be `>= 1` and
  `<= 1000` (`TimestampOracle::MAX_TIMESTAMP_POOL_SIZE`, the same cap as
  `tsoPoolSize`); a count outside that range throws
  `InvalidArgumentException`. PD may grant fewer timestamps than
  requested; the returned list never exceeds the grant. An explicit batch
  discards the pool (it advances PD beyond the pooled range).
- `getLowResolutionTimestamp()` returns a timestamp that is at most
  `lowResTimestampMaxStalenessMs` old (option below; default unset =
  fresh fetch, i.e. unchanged behaviour). It is used internally by lock
  resolution (`CheckTxnStatus.current_ts`) and is safe only for
  staleness-tolerant consumers — never for start/commit timestamps. When
  it does fetch, it uses a fresh (non-pooled) timestamp.

Additionally, the pessimistic-lock path in `TwoPhaseCommitter` acquires
one `for_update_ts` per locking pass instead of one per region, cutting
one PD round trip per additional region.

### Store Address Validation

Store addresses are not configured by the application — they are returned by PD
inside `GetStore` responses and are used as gRPC targets for all data traffic.
Since gRPC target strings may also carry schemes such as `unix:`,
`unix-abstract:` or `dns:///` — and a bare host can itself be a scheme name
(`unix:20160`, `dns:20160`, `ipv4:20160`, …) that grpc-core interprets as a
URI at `new Channel()` time — the client unconditionally rejects any store
address that is not a bare `host:port` (or a bracketed IPv6 `[addr]:port`)
with the port in the range 1–65535, and additionally rejects case-insensitively
any host equal to a reserved gRPC/URI scheme name (`unix`, `unix-abstract`,
`unix-gram`, `unix-dgram`, `dns`, `ipv4`, `ipv6`, `vsock`, `http`, `https`,
`tcp`, `tls`, `xds`, `google-c2p`, `google-c2p-experimental`). The rejection
is logged before throwing
`InvalidStoreAddressException`. This format check is applied everywhere a
PD-supplied address becomes a channel target — including the SST ingest
`SwitchMode` calls — and is never retried.

#### Default policy: derived from the configured PD endpoints

When neither `allowedStoreHosts` nor `storeHostPolicy` is configured, the
client derives a host policy from the **configured PD endpoints** (the first
argument of `create()`). The store host is classified **before** any rule is
applied:

- **bracketed IPv6 literals** (`[addr]:port`) are allowed only when
  byte-identical (`inet_pton`) to a configured PD IPv6 endpoint. Zone-id
  forms (`[fe80::1%eth0]:20160`) and IPv4-mapped forms
  (`[::ffff:10.0.0.1]:20160`) are rejected; no suffix or subnet rules apply
  to IPv6;
- **IPv4 literals** (dotted quads) are allowed only when they equal a
  configured PD IPv4 literal (e.g. PD at `127.0.0.1:2379` → store at
  `127.0.0.1:20160`) or fall into the same /16 subnet — first two octets
  (e.g. PD at `10.0.5.1:2379` → store at `10.0.5.9:20160`). IPs are compared
  by address bytes and are **never** suffix-matched: `10.0.0.1` does not
  match PD `127.0.0.1` even though both end in `.0.1`;
- **digit-leading hosts** (`2130706433`, `017700000001`, `0x7f000001`, …)
  are numeric-IP aliases resolved by the system resolver, and are rejected;
- **DNS names** are allowed when they equal a configured PD host exactly,
  are single-label names (no `.`) — same-network-namespace names such as
  compose/Kubernetes short names (`tikv1` next to `pd`) — or share the last
  two DNS labels with a configured PD host that is itself a real dotted DNS
  name (e.g. `tikv-0.tikv-hl.ns.svc` next to `pd-0.pd-hl.ns.svc`). PD entries
  that are IP literals, digit-leading numeric aliases or single-label names
  never contribute a suffix — with PD at `10.0.0.1:2379`, `attacker.0.1:20160`
  is rejected even though it textually ends in `.0.1`.

With the default policy, the **address port** is part of the trust decision
as well: ports below 1024 (privileged ports) are rejected unless explicitly
listed in `options['allowedStorePorts']`. When `allowedStorePorts` is set,
only ports listed in it are accepted (this applies to every default-policy
match branch: exact PD host, /16 subnet, single-label and shared suffix).
Standard TiKV ports (20160+) pass the default guard without configuration.

Anything else — `attacker.example.com`, unrelated domains — is rejected with a
logged `InvalidStoreAddressException`. This makes the rogue-PD redirect
protection active by default: no configuration is required, and a compromised
or on-path PD cannot redirect the client to an arbitrary host.

#### Explicit restrictions (opt-in)

You can restrict which hosts the client is allowed to connect to explicitly:

```php
$client = RawKvClient::create(
    ['192.168.1.100:2379'],
    options: [
        'allowedStoreHosts' => [
            'tikv-0.tikv.svc',  // exact hostname (subdomains NOT included),
                                // exact IPs like '192.168.1.10' also work
            '.tikv.svc',        // DNS suffix: the domain itself and any subdomain
            '10.0.0.0/8',       // IPv4/IPv6 CIDR range
            '2001:db8::1',      // exact IPv6 address (store form: [2001:db8::1]:20160)
        ],
        'allowedStorePorts' => [20160, 20161],  // optional: ports the store
                                                // address may use (default: null
                                                // = unrestricted on this path,
                                                // privileged-port guard on the
                                                // default path)
    ],
);
```

Note the difference between `tikv-0.tikv.svc` (exact hostname only — `evil.tikv-0.tikv.svc`
is rejected) and `.tikv.svc` (suffix — `tikv-0.tikv.svc` and any subdomain match).

With an explicit `allowedStoreHosts` list, ports are unrestricted unless
`allowedStorePorts` is set; when it is set, the port must be in the list
(host-only behavior is unchanged when the option is absent). `storeHostPolicy`
receives the full address and is never subject to the port policy.

Or provide a fully custom policy (a callable receiving the full `host:port`
string and returning whether it is allowed). When set, it overrides both
`allowedStoreHosts` and the default PD-derived policy:

```php
$client = RawKvClient::create(
    ['192.168.1.100:2379'],
    options: [
        'storeHostPolicy' => fn (string $address): bool => $address === '192.168.1.10:20160',
    ],
);
```

To allow any bare `host:port` (not recommended — this disables the default
rogue-PD protection), pass a `storeHostPolicy` that always returns `true`, or
list the exact entries you need in `allowedStoreHosts`.

The format check is unconditional and independent of the host policy. TLS with
hostname verification remains the stronger control — see the TLS section
below.

### gRPC Channel Options

The underlying `GrpcClient` sets channel arguments on every created gRPC
channel. Without them the gRPC C core's default 4 MB receive limit applies,
and a scan or batch read that returns more than that fails with
`RESOURCE_EXHAUSTED` (`Received message larger than max (… vs. 4194304)`) —
deterministically for that data, even though the client's scan limit allows
10240 rows. The defaults below are large enough for full-limit scan responses.

```php
$options = [
    'grpc' => [
        'maxReceiveMessageBytes' => 67108864, // default: 64 MB (gRPC core default: 4 MB)
        'maxSendMessageBytes'    => 67108864, // default: 64 MB
        'keepaliveTimeMs'        => 10000,    // default: 10 s — ping interval on idle connections
        'keepaliveTimeoutMs'     => 3000,     // default: 3 s
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: $options
);
```

All values must be ints `>= 1`. Keepalive pings (sent every `keepaliveTimeMs`,
even without in-flight calls) keep idle connections alive behind stateful
firewalls / NAT gateways — which typically drop idle flows after ~5 minutes —
so a dead connection is detected rather than blocking until TCP retransmission
gives up. Lowering `maxReceiveMessageBytes` is a valid hardening choice when
you know your values are small.

### Environment Variables

For flexibility, use environment variables:

```php
$pdEndpoints = getenv('PD_ENDPOINTS') 
    ? explode(',', getenv('PD_ENDPOINTS')) 
    : ['127.0.0.1:2379'];

$client = RawKvClient::create($pdEndpoints);
```

Set in your environment:

```bash
export PD_ENDPOINTS="192.168.1.100:2379,192.168.1.101:2379"
```

> **Note**: The library itself does not read any environment variables. The example above shows how your application can read `PD_ENDPOINTS` and pass it to the client. All configuration must be passed explicitly via constructor arguments or the `$options` array.

## TLS/SSL Configuration

> **Warning:** By default, when no TLS configuration is provided, the client connects
> in plaintext (unencrypted). A warning is logged on every insecure channel creation.
> To ensure TLS is always used, set `allowInsecure: false` (see below).

### Server Verification Only

Verify the server's certificate:

```php
$options = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        'caCertBaseDir' => '/path/to', // optional: restrict to base directory
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

### Mutual TLS (mTLS)

Client certificate authentication:

```php
$options = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        // Optional base-directory restrictions. Each *BaseDir applies only to
        // the matching file read (caCertBaseDir → caCertFile/withCaCertFile;
        // clientCertBaseDir → clientCertFile + clientKeyFile).
        'caCertBaseDir' => '/path/to',
        'clientCertFile' => '/path/to/client.crt',
        'clientKeyFile' => '/path/to/client.key',
        'clientCertBaseDir' => '/path/to',
    ],
];

$client = RawKvClient::create(['tikv.example.com:2379'], options: $options);
```

> **Important:** When providing a client certificate and key, a CA certificate is
> **required**. The configuration will throw `InvalidArgumentException` if
> `clientCertFile`/`clientKeyFile` (or `clientCertPem`/`clientKeyPem`) are
> provided without a CA certificate — this prevents accidental downgrade to
> plaintext.

### Using Certificate Content (Inline PEM)

You can pass certificate content directly instead of file paths — use the `*Pem` variants:

```php
$caCert = file_get_contents('/path/to/ca.crt');
$clientCert = file_get_contents('/path/to/client.crt');
$clientKey = file_get_contents('/path/to/client.key');

$options = [
    'tls' => [
        'caCertPem' => $caCert,
        'clientCertPem' => $clientCert,
        'clientKeyPem' => $clientKey,
    ],
];
```

### TLS Configuration Builder

For advanced TLS configuration:

```php
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;

$builder = new TlsConfigBuilder();
$builder->withCaCertFile('/path/to/ca.crt')
    ->withClientCertFile('/path/to/client.crt', '/path/to/client.key');

$tlsConfig = $builder->build();

// Use with custom client construction
$grpc = new GrpcClient($logger, $tlsConfig);
$pdClient = new PdClient($grpc, 'tikv.example.com:2379', $logger);
$client = new RawKvClient($pdClient, $grpc);
```

### Fail-Closed Mode (Require TLS)

By default, if no TLS configuration is provided, the client connects in plaintext
and logs a warning. To fail-closed — reject any connection that isn't using TLS —
set `allowInsecure: false` on the `GrpcClient`:

```php
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;

// Insecure connections will throw InvalidStateException
$grpc = new GrpcClient($logger, tlsConfig: $tlsConfig, allowInsecure: false);
```

TLS can also be enforced through the documented `create()` construction path.
When `options['allowInsecure']` is `false`, any attempt to open a gRPC channel
without usable TLS credentials throws `InvalidStateException`:

```php
$client = RawKvClient::create(
    ['tikv.example.com:2379'],
    options: [
        'tls' => ['caCertFile' => '/path/to/ca.crt'],
        'allowInsecure' => false, // enforce TLS — fail closed
    ],
);
```

To guard against TLS misconfiguration, the factory rejects:

- a non-array `options['tls']` value (e.g. `'tls' => '/path/to/ca.crt'` or
  `'tls' => true`) with `InvalidArgumentException` instead of silently
  connecting in plaintext;
- unrecognised keys inside `options['tls']` (e.g. the snake_case
  `ca_cert` typo) with `InvalidArgumentException` naming the offending key;
- a recognised key inside `options['tls']` with a non-string value (e.g.
  `'caCertFile' => 123`), which would otherwise be silently dropped and the
  connection would go out in plaintext;
- an incomplete client certificate pair (e.g. `clientCertFile` without
  `clientKeyFile`), which would otherwise be silently dropped the same way.

Recognised `options['tls']` keys: `caCertFile`, `caCertBaseDir`, `caCertPem`,
`caCert`, `clientCertFile`, `clientCertBaseDir`, `clientKeyFile`,
`clientCertPem`, `clientKeyPem`, `clientCert`, `clientKey`.

> **Note:** `allowInsecure` defaults to `true` (plaintext allowed) for
> backward compatibility; setting it to `false` together with a valid `tls`
> block is the recommended production configuration.


## Logging

### PSR-3 Logger Integration

The client supports any PSR-3 compatible logger:

```php
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);
```

### Log Levels

Choose appropriate log levels for your environment:

```php
// Development - verbose logging
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

// Production - warnings and errors only
$logger->pushHandler(new StreamHandler('/var/log/tikv.log', Logger::WARNING));
```

### What Gets Logged

The client logs:

- **DEBUG**: Connection attempts, cache operations, request details
- **INFO**: Successful operations, region cache hits/misses
- **WARNING**: Retry attempts, region invalidations
- **ERROR**: Failed operations, exhausted retries, fatal errors

### Structured Logging

Use JSON format for log aggregation:

```php
use Monolog\Formatter\JsonFormatter;

$handler = new StreamHandler('/var/log/tikv.log', Logger::INFO);
$handler->setFormatter(new JsonFormatter());
$logger->pushHandler($handler);
```

### Multiple Handlers

Log to multiple destinations:

```php
// Errors to stderr
$logger->pushHandler(new StreamHandler('php://stderr', Logger::ERROR));

// All messages to file
$logger->pushHandler(new StreamHandler('/var/log/tikv.log', Logger::DEBUG));

// Critical alerts to Slack/Email (using Monolog handlers)
$logger->pushHandler(new SlackWebhookHandler(...));
```

## Replica Reads

By default every read targets the region **leader**. The `replicaRead` option
(issue #421, available on both `RawKvClient` and `TxnKvClient`) lets reads be
served by follower replicas — useful in multi-AZ deployments to avoid
inter-AZ latency and to spread read load beyond the leader.

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;

$client = RawKvClient::create(
    ['127.0.0.1:2379'],
    options: [
        'replicaRead' => new ReplicaReadPolicy(
            mode: ReplicaReadMode::Follower,
            matchStoreLabels: ['zone' => 'az-1'], // optional: only replicas
                                                  // whose store labels match
                                                  // are eligible
            staleRead: false,
        ),
    ],
);
```

Modes:

| Mode             | Behaviour                                                                       |
|------------------|---------------------------------------------------------------------------------|
| `Leader`         | Default — all reads go to the leader (unchanged behaviour).                     |
| `Follower`       | Reads go to a follower replica (random among eligible).                         |
| `Mixed`          | Reads may go to the leader or any follower (random among all eligible peers).   |
| `PreferLeader`   | The leader is used when it matches the label filter; otherwise a follower.      |

Details:

- For non-leader targets the client sets `Context.replica_read = true` in the
  gRPC request; with `staleRead: true` it additionally sets
  `Context.stale_read = true` so any replica whose `safe_ts` covers the read
  timestamp may serve it locally.
- `matchStoreLabels` restricts eligible replicas to stores whose
  `metapb.Store.labels` contain all given key/value pairs (e.g.
  `['zone' => 'az-1']`). When no replica qualifies, selection degrades to the
  leader.
- A `DataIsNotReady` region error on a replica excludes that store for the
  current operation and the retry falls back to another replica or the leader.
- **Writes always target the leader**, regardless of the policy. On
  `TxnKvClient` the policy applies to transaction reads (`get`, `batchGet`,
  `scan`); pessimistic locking and commits remain leader-only.

```php
// Stale read: allow any replica with a sufficiently fresh safe_ts
$policy = new ReplicaReadPolicy(ReplicaReadMode::Mixed, staleRead: true);
```

## Fast Commit Modes (TxnKV)

By default every transaction runs the full two-phase commit: prewrite, a PD
round trip for the commit timestamp, then commit. Two opt-in modes (issue
#419, both **off by default**, selected per transaction on
`TxnKvClient::begin()`) remove round trips for small transactions:

```php
$txn = $client->begin([
    'enable1Pc' => true,          // one-phase commit
    'enableAsyncCommit' => true,  // async commit (ignored when 1PC applies)
]);
```

| Mode               | When it applies                                                            | What it saves                                  |
|--------------------|----------------------------------------------------------------------------|------------------------------------------------|
| `enable1Pc`        | Transaction touches a **single region** and has ≤ 128 keys.                 | The commit phase and the PD `getTimestamp()` round trip. |
| `enableAsyncCommit`| Transaction has ≤ 256 keys (any number of regions).                        | The commit phase (the PD round trip stays implicit in prewrite's `min_commit_ts`). |

Details:

- One-phase commit sets `try_one_pc` on the prewrite; TiKV commits the
  transaction inside the prewrite and returns the commit timestamp in
  `PrewriteResponse::one_pc_commit_ts`. When TiKV declines (e.g. the region
  is already busy, or the transaction outgrew the limit between grouping and
  prewrite), the client silently falls back to the normal two-phase path.
- Async commit sets `use_async_commit`, the secondary keys and
  `max_commit_ts` (start timestamp + a bounded TSO gap) on the primary
  region's prewrite, and derives the commit timestamp as the maximum of the
  returned `min_commit_ts` values (no extra increment — TiKV writes the data
  at `min_commit_ts`, so a reader whose snapshot ts equals it must see the
  write). The transaction is committed once the primary lock
  carries the decision; readers resolve such locks via
  `CheckTxnStatus`/`CheckSecondaryLocks`, which the client's lock resolver
  understands — it finalizes the whole transaction (all secondaries + the
  primary) with `KvResolveLock` at the commit ts derived from the
  `min_commit_ts` values (client-go's recovery protocol: an undecided
  transaction whose secondaries are all still locked is help-committed at
  `max(min_commit_ts)` rather than stalled until the lock TTL expires).
  The prewrite loop prewrites the primary region last (the commit-decision
  barrier, client-go parity). When TiKV declines, the client falls back to
  two-phase commit.
- If both options are set, one-phase commit is tried first (single region
  only); async commit covers multi-region small transactions.
- Both modes remain safe under TiKV's own server-side guards — the server
  never applies an unsafe fast commit, it just answers with a decline and the
  client proceeds with two-phase commit.

## Retry and Backoff

### Automatic Retry

The client automatically retries failed operations:

```php
// Default retry configuration (built-in)
$client = RawKvClient::create(['127.0.0.1:2379']);
// - Max backoff: 20 seconds (non-ServerBusy errors, cumulative)
// - Server busy budget: 60 seconds (cumulative ServerBusy sleep)
// - Retry deadline: 30 seconds (checked before each retry attempt AND before
//   each backoff sleep, which is clamped to the remaining budget; occupancy
//   can still exceed it by the duration of the last in-flight RPC, which is
//   bounded only by the gRPC timeout option (options['timeout']), not by the
//   retry deadline)

// Custom retry deadline (issue #294) — bounds the blocking backoff loop so a
// sustained ServerIsBusy episode cannot pin a PHP-FPM worker for minutes:
$client = RawKvClient::create(['127.0.0.1:2379'], [
    'retryDeadlineMs' => 5000, // 5 s wall-clock bound per operation; 0 disables
]);
```

> **Occupancy note:** the deadline is checked before every attempt AND
> before every backoff sleep (which is clamped to the remaining budget), so
> sleeping cannot overshoot it — but an in-flight RPC cannot be interrupted.
> Example: `retryDeadlineMs => 1000` with a gRPC call that runs 5 s ⇒ real
> occupancy ≈ 6 s (the deadline is enforced when that RPC returns).

> **Worker occupancy note:** `serverBusyBudgetMs` is effectively a
> worker-occupancy setting — it caps how long ONE operation may block a PHP
> process in `usleep()` while TiKV reports `ServerIsBusy`. The default of
> 60 seconds suits request-driven runtimes such as PHP-FPM. Long-running CLI
> workers that prefer to wait out long overload episodes can raise the budget
> (and/or disable the retry deadline with `'retryDeadlineMs' => 0`) via the
> `RawKvClient` constructor arguments.

### GC Safe Points (TxnKV)

`TxnKvClient::begin()` validates the fresh start timestamp against the
cluster's GC safe point (on by default; see [GC Safe Points and Long-Running
Reads](error-handling.md#gc-safe-points-and-long-running-reads) for the full
behaviour):

```php
// Default: validation on, 30 s cache refresh
$client = TxnKvClient::create(['127.0.0.1:2379']);

// Explicit: disable validation or retune the refresh interval
$client = TxnKvClient::create(['127.0.0.1:2379'], [
    'gcSafePointValidation' => true,
    'gcSafePointRefreshMs' => 30000, // ms; must be >= 1
]);
```

A start timestamp already behind the safe point throws
`TxnAbortedByGcException` at `begin()`. For jobs that must outlive
`gc_life_time`, hold GC back with `TxnKvClient::holdGcSafePoint()` /
`releaseGcSafePoint()` (service safe point with TTL) instead of raising the
cluster-wide `gc_life_time`.

### Retry Behavior

The client retries on these errors:

| Error Type | Backoff Strategy | Description |
|------------|------------------|-------------|
| `EpochNotMatch` | Jittered (2–500 ms) | Region epoch mismatch, small backoff (like client-go's `BoEpochNotMatch`) |
| `NotLeader` | Fast | Leader changed, quick retry |
| `ServerIsBusy` | Progressive | Server overloaded, increasing delays |
| `RegionNotFound` | Medium | Region not found, moderate delay |
| `StaleCommand` | Fast | Stale command, quick retry |
| gRPC errors | Progressive | Network issues, increasing delays |

### Non-Retryable Errors

These errors are not retried:

- `KeyNotInRegion` - Key outside region range
- `RaftEntryTooLarge` - Value too large
- `FlashbackInProgress` - Region in flashback mode

### Retry Bounds

Every retry loop is bounded by **two independent safety nets** to prevent
infinite loops when the underlying error has zero backoff delay (the only
remaining zero-sleep class, `BackoffType::None`, is the `DataIsNotReady`
replica-lag signal — `EpochNotMatch` gets a small jittered 2–500 ms backoff
since issue #241):

| Bound | Default | Description |
|-------|---------|-------------|
| `maxAttempts` | `30` | Maximum number of times the operation is invoked before the executor gives up. |
| `deadlineMs` | `30000` (`RetryExecutor::DEFAULT_RETRY_DEADLINE_MS`; `0` disables) | Wall-clock deadline from the start of the call. Checked before each attempt and before each backoff sleep (the sleep is clamped to the remaining budget); the executor terminates when it is reached, regardless of `sleepMs`. |

When either bound is reached the executor throws
`RetryBudgetExhaustedException` (extends `TiKvException`). This exception
exposes `attempts()` and `elapsedOrBackoffMs()` for diagnostics, and
`getRawKey()` for the un-redacted key (the key is redacted in the message —
see issue #269).

### Custom Retry (Advanced)

For custom retry logic, extend the client:

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Retry\BackoffType;

class CustomRawKvClient extends RawKvClient
{
    protected function classifyError(TiKvException $e): ?BackoffType
    {
        // Add custom error classification
        if (str_contains($e->getMessage(), 'CustomError')) {
            return BackoffType::Custom;
        }
        return parent::classifyError($e);
    }
}
```

## Caching

### Region Cache

The client caches region metadata to avoid repeated PD queries:

```php
// Default: In-memory region cache (enabled automatically)
$client = RawKvClient::create(['127.0.0.1:2379']);
```

**Cache behavior**:
- Cache entries expire automatically on region errors
- `NotLeader` errors update the cache with new leader info
- `EpochNotMatch` errors invalidate affected regions

### Store Cache

Store addresses are also cached:

- Store information cached to avoid repeated PD queries
- Automatically refreshed on cache misses

### Cache Statistics

Monitor cache performance via logs:

```
[INFO] Region cache hit: region_id=123
[INFO] Region cache miss: key="user:123"
[INFO] Invalidated region: region_id=123, reason=EpochNotMatch
```

## Timeouts

### Per-Operation Timeouts

The client supports configurable per-operation gRPC timeouts via `TimeoutConfig` / `options['timeout']`:

```php
$options = [
    'timeout' => [
        'readTimeoutMs' => 5000,         // default: 5000
        'writeTimeoutMs' => 5000,        // default: 5000
        'batchReadTimeoutMs' => 10000,   // default: 10000
        'batchWriteTimeoutMs' => 10000,  // default: 10000
        'scanTimeoutMs' => 20000,        // default: 20000
        'deleteRangeTimeoutMs' => 30000, // default: 30000
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: $options
);
```

When a timeout is exceeded, the gRPC call throws `GrpcException` which is caught and retried by the retry executor unless the budget is exhausted.

### Default Timeouts

By default all timeouts are set to sensible values (5s for reads/writes, 10s for batch, 20s for scans, 30s for delete-range). Set any value to `0` to disable it (not recommended in production).

### Handling Slow Operations

For additional application-level safeguards:

```php
// Using PHP's pcntl_alarm (CLI only)
pcntl_alarm(30);  // 30 second timeout

try {
    $value = $client->get('key');
} catch (Exception $e) {
    // Handle timeout
} finally {
    pcntl_alarm(0);
}
```

Or use async patterns:

```php
// For batch operations, process in chunks
$chunks = array_chunk($keys, 100);
foreach ($chunks as $chunk) {
    $results = $client->batchGet($chunk);
    // Process results
}
```

## Production Configuration

### Recommended Production Setup

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\JsonFormatter;

// Production logger
$logger = new Logger('tikv');

// Structured JSON logging for aggregation
$handler = new StreamHandler('/var/log/tikv.log', Logger::INFO);
$handler->setFormatter(new JsonFormatter());
$logger->pushHandler($handler);

// Error output for monitoring
$logger->pushHandler(new StreamHandler('php://stderr', Logger::ERROR));

// TLS configuration (recommended for production)
$options = [
    'tls' => [
        'caCertFile' => '/etc/ssl/certs/tikv-ca.crt',
        'clientCertFile' => '/etc/ssl/certs/tikv-client.crt',
        'clientKeyFile' => '/etc/ssl/private/tikv-client.key',
    ],
];

// Create client
$client = RawKvClient::create(
    pdEndpoints: ['tikv-pd-1:2379', 'tikv-pd-2:2379', 'tikv-pd-3:2379'],
    logger: $logger,
    options: $options
);
```

### Environment-Based Configuration

```php
// config/tikv.php
return [
    'endpoints' => explode(',', getenv('TIKV_PD_ENDPOINTS') ?: '127.0.0.1:2379'),
    'tls' => [
        'enabled' => getenv('TIKV_TLS_ENABLED') === 'true',
        'caCertFile' => getenv('TIKV_TLS_CA_CERT'),
        'clientCertFile' => getenv('TIKV_TLS_CLIENT_CERT'),
        'clientKeyFile' => getenv('TIKV_TLS_CLIENT_KEY'),
    ],
    'logging' => [
        'level' => getenv('TIKV_LOG_LEVEL') ?: 'warning',
        'path' => getenv('TIKV_LOG_PATH') ?: '/var/log/tikv.log',
    ],
];
```

> **Note**: The `TIKV_*` environment variables shown are **application-level conventions** — the library does not read any environment variables directly. Your application is responsible for reading env vars and passing the values to the client constructor.

### Health Checks

The client ships a first-class health check that probes PD without
touching any user data. Use it in load-balancer probes, Kubernetes
readiness/liveness checks, etc.:

```php
// Caller pattern: probe PD reachable + return the learned cluster ID.
try {
    $clusterId = $client->healthCheck();   // int|null
    // $clusterId is non-null when PD responded with a cluster-id header.
} catch (\CrazyGoat\TiKV\Client\Exception\HealthCheckException $e) {
    // PD was unreachable or returned a non-OK status.
}

// Throws ClientClosedException when called after $client->close().
```

Internally, `healthCheck()` issues the lightweight `GetMembers` RPC over
the same gRPC channel used for region lookups, so it exercises the full
network path without writing or reading user keys.

### Metrics and Observability

The library exposes an opt-in `MetricsInterface` for capturing operational
counters (RPC counts and latency, retry counts, region-cache hit/miss,
region-cache invalidations). The default is a zero-cost no-op, so callers
that do not opt in pay a single empty-method dispatch per call site.

```php
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;

class PrometheusMetrics implements MetricsInterface
{
    // ... your Prometheus / StatsD / OpenTelemetry adapter here ...
}

$metrics = new PrometheusMetrics();   // or new InMemoryMetrics() for tests
$client = RawKvClient::create(
    ['tikv.example.com:2379'],
    options: ['metrics' => $metrics],
);
```

The library emits the following counter tags:

| Method                         | Tag example                | Meaning                                           |
|--------------------------------|----------------------------|---------------------------------------------------|
| `rpcStarted()`                 | `'tikvpb.Tikv/KvGet'`      | One outbound gRPC call was dispatched             |
| `rpcCompleted()`               | `'tikvpb.Tikv/KvGet'`      | gRPC call returned (success or error); carries duration (ms) |
| `retryAttempted()`             | `'NotLeader'`, `'ServerBusy'`, … | A retryable error triggered the next attempt |
| `regionCacheHit()`             | `'region_resolution'`      | Region was found in the cache                     |
| `regionCacheMiss()`            | `'region_resolution'`      | Region was not in the cache, had to query PD      |
| `regionInvalidated()`          | `'region_error'`, `'not_leader'`, `'retry_region_error'`, `'lock_resolve'` | A region was actually removed from the cache — exactly once per actual drop, emitted from `RegionCache::invalidate()` with the caller's reason: top-level non-NotLeader region error (`RegionErrorHandler`), NotLeader handling in the retry loop (hint peer unknown / no hint), pre-retry invalidation on other retryable errors, or post-resolve cleanup in `LockResolver`. Invalidating an ID that is not cached emits nothing. |

Reasons are mutually exclusive per drop; a NotLeader response whose hinted
peer is still valid only switches the cached leader and emits nothing.
NotLeader drops are owned exclusively by the retry loop's leader handling —
`RegionErrorHandler::check()` deliberately leaves a NotLeader region cached
so that handler can switch-or-drop it.

`InMemoryMetrics` ships the same counters in-process, suitable for tests
and benchmarks — see `getRpcStarted()`, `getRpcSucceeded()`, `getRetries()`,
`getCacheHits()`, `getInvalidations()`, `getMeanLatencyMs()`, and `reset()`.

Inspect live counters at any time:

```php
$metrics = $client->getMetrics();     // returns whatever was injected
```

### Connection Pooling

The client maintains persistent gRPC channels:

- Channels are reused across requests
- Automatic connection management
- No explicit connection pool configuration needed

### Resource Limits

Consider PHP's resource limits:

```ini
; php.ini
memory_limit = 256M
max_execution_time = 300
```

For long-running processes (workers, daemons):

```php
// Periodic cleanup in long-running processes
while (true) {
    // Process work
    
    // Optional: Force reconnection periodically
    if ($iteration % 1000 === 0) {
        $client->close();
        $client = RawKvClient::create($pdEndpoints, logger: $logger);
    }
}
```

## Configuration Checklist

Before deploying to production:

- [ ] PD endpoints are correct and accessible
- [ ] Store address policy reviewed: default PD-derived policy, explicit `allowedStoreHosts`/`storeHostPolicy`, or TLS with hostname verification enforced
- [ ] TLS certificates are configured (if required)
- [ ] Logging is configured with appropriate level
- [ ] Log files have correct permissions
- [ ] Health checks are implemented
- [ ] Error handling is in place
- [ ] Resource limits are configured
- [ ] TiKV cluster has `enable-ttl=true` (if using TTL) **or** is in default V1 mode (no `enable-ttl`, required for TxnKV) — the two are mutually exclusive

## See Also

- [Operations Guide](operations.md) - Using the configured client
- [Advanced Features](advanced.md) - Production patterns
- [Troubleshooting](troubleshooting.md) - Solving configuration issues
