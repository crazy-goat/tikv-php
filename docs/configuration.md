# Configuration Guide

Complete guide to configuring the TiKV PHP Client for development and production environments.

## Table of Contents

1. [Basic Configuration](#basic-configuration)
2. [Connection Settings](#connection-settings)
3. [gRPC Channel Options](#grpc-channel-options)
4. [TLS/SSL Configuration](#tlsssl-configuration)
5. [Logging](#logging)
6. [Replica Reads](#replica-reads)
7. [Batch Commands Multiplexing (Experimental)](#batch-commands-multiplexing-experimental)
8. [Fast Commit Modes (TxnKV)](#fast-commit-modes-txnkv)
9. [Transaction Priority (TxnKV)](#transaction-priority-txnkv)
10. [Resource Control (Resource Groups)](#resource-control-resource-groups)
11. [Request Context Controls (issue #426, GAP-12)](#request-context-controls-issue-426-gap-12)
12. [Retry and Backoff](#retry-and-backoff)
13. [Caching](#caching)
14. [Timeouts](#timeouts)
15. [Production Configuration](#production-configuration)

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
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Create a PSR-3 logger
$logger = new Logger('tikv');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::INFO));

// Client options
$options = [
    // Concurrency cap for operations that fan out per-region/per-store RPCs
    // (an unbounded scan()/scanPrefix()/reverseScan() page, batchScan,
    // deleteRange/deletePrefix/checksum, SST store-mode switches): at most
    // this many units (ranges, regions or stores) are in flight at once.
    // Default: 16. Must be >= 1.
    'maxConcurrency' => 16,
    // Row cap for an unbounded scan (scan()/scanPrefix()/reverseScan() with
    // limit: 0, issue #191). limit: 0 returns the whole range by paging
    // internally (up to RawKvClient::MAX_SCAN_LIMIT = 10240 rows per RPC), so
    // the whole result is buffered in one PHP array. A negative limit is
    // rejected with InvalidArgumentException before any RPC. The guard is
    // evaluated once per fetched page (after that page is buffered), so it is
    // page-granular rather than a strict per-row memory bound: when it is
    // smaller than the page size a whole page is read before the throw, and
    // ScanLimitExceededException's getScannedRows() can exceed getMaxRows().
    // Bounded (limit > 0) scans and the lazy
    // scanIterator()/scanPrefixIterator() are unaffected. Default: 100000.
    // Must be >= 1.
    'maxScanRows' => 100000,
    // Payload cap for one RawKvClient::ingest() call (issue #297): the sum of
    // every key and value length, in bytes. 0 (default) = unlimited.
    // Exceeding it throws IngestTooLargeException before the cluster is
    // switched into import mode and before any RPC, so the caller can split
    // the batch. Unlike maxScanRows this is a whole-payload guard, not a
    // page-granular one.
    'maxIngestSize' => 0,
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
    // Expected PD cluster ID (issue #308, SEC-05). When set, every PD/TSO
    // response must carry a cluster ID equal to this value; a mismatch or a
    // missing cluster-ID header throws TiKvException before any data-path
    // request. Independently of this option, an established cluster ID can
    // never be overwritten by a value parsed out of an error string. Default:
    // unset = learn and adopt the first ID PD reports. Must be a positive int.
    // See "Cluster Identity Verification" below.
    'expectedClusterId' => 123456789,
    // Replica read preference (issue #421): an instance of
    // ReplicaReadPolicy controlling which peer serves read requests.
    // Default: leader-only (unchanged behaviour). See "Replica Reads".
    'replicaRead' => new ReplicaReadPolicy(ReplicaReadMode::PreferLeader),
    // Optional kvrpcpb.Context control fields (issue #426): isolation level,
    // not-fill-cache, server-side max execution duration, request source,
    // resource group tag/name and busy threshold. Applied to every request
    // this client sends. Omitted: none of the fields is written, so the
    // serialized Context stays byte-for-byte unchanged. See "Request Context
    // Controls" below.
    'requestContext' => new RequestContextOptions(
        notFillCache: true,
        requestSource: RequestContextOptions::DEFAULT_REQUEST_SOURCE,
    ),
    // Experimental BatchCommands stream multiplexing for the RawKV batch
    // fan-outs (issue #418). Must be a boolean, default false. See
    // "Batch Commands Multiplexing" below.
    'batchCommands' => false,
    // Minimum interval between two non-forced refreshRegionCache() calls, in
    // milliseconds (issue #425). The method is the PHP-process-model
    // replacement for client-go's background runner: a long-running worker
    // calls it from its own tick and it re-queries PD for the cached regions
    // at most once per interval. 0 (the default) disables the throttled
    // refresh, so an application that does not opt in pays no extra PD
    // traffic; a forced refresh still works. Must be >= 0. See "Region Cache
    // Refresh and Preload" below.
    'regionCacheRefreshMs' => 60000,
    // Resource control (issue #427): the TiKV resource group every request
    // belongs to, and an opaque tag used for query attribution. Both are
    // optional and default to unset, which sends neither field. The name
    // must be 1-64 characters of letters, digits, underscores or hyphens;
    // the tag must be a non-empty string. TxnKvClient::begin() accepts the
    // same two keys per transaction and overrides them individually. See
    // "Resource Control (Resource Groups)" below.
    'resourceGroup' => 'tenant-a',
    'resourceGroupTag' => 'nightly-job',
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        'clientCertFile' => '/path/to/client.crt',
        'clientKeyFile' => '/path/to/client.key',
    ],
    // Fail-closed TLS (issue #407): when false, the client refuses to open
    // a plaintext gRPC channel and throws InvalidStateException instead —
    // a missing or unusable TLS configuration then fails loudly instead of
    // silently downgrading to plaintext. Default: true (plaintext allowed,
    // with a warning on every insecure channel). See "Security: Fail-Closed
    // TLS" below.
    'allowInsecure' => false,
    // Maximum number of gRPC channels kept open at the same time. The least
    // recently used channel is evicted when the cap is reached. Default: 64
    // (GrpcClient::DEFAULT_MAX_CHANNELS). Must be >= 1.
    'maxChannels' => 64,
    // Idle time in milliseconds after which an unused channel is evicted
    // (and reopened on the next request). Default: 600000 (10 minutes,
    // GrpcClient::DEFAULT_IDLE_TTL_MS). Must be >= 1.
    'idleTtlMs' => 600000,
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,
    options: $options
);
```

### API V2 and Keyspaces

RawKV and TxnKV clients can use TiKV's keyspace-aware API V2. Set
`apiVersion` to `2` and provide a `keyspace` name; the name is resolved to its
numeric ID through PD and cached for the client. An omitted or empty name uses
PD's `DEFAULT` keyspace.

```php
$options = [
    'apiVersion' => 2,
    'keyspace' => 'tenant-a',
];

$raw = RawKvClient::create(['127.0.0.1:2379'], options: $options);
$txn = TxnKvClient::create(['127.0.0.1:2379'], options: $options);
```

API V2 prefixes RawKV keys with `r` and transactional keys with `x`, followed
by the three-byte keyspace ID. Region lookups use the same prefix with
memory-comparable encoding. Response keys are decoded back to user keys, and a
response containing a key from another keyspace raises
`KeyOutOfBoundsException`. API V2 RawKV always uses the `default` column
family, regardless of a configured legacy column-family setting.

The TiKV cluster must be started with `[storage] api-version = 2` and
`enable-ttl = true`. API V2 cannot be disabled after the server is enabled.
The repository includes `tikv-apiv2.toml`, `docker-compose.apiv2.yml`, and
`make test-e2e-apiv2` for the dedicated V2 smoke suite.

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

**Note**: All endpoints are used: the client discovers the PD leader via
`GetMembers` and fails over to another configured endpoint when the current
one fails on transport level (issue #416). Since issue #234 it also rotates on
a **not-leader** answer from PD — including the application-level form, where
PD replies with a gRPC `OK` status and an error inside
`pdpb.ResponseHeader.error` — retrying at most once per configured endpoint.
Every PD response header is checked at a single choke point, so a PD
application-level error (`NOT_BOOTSTRAPPED`, `INVALID_VALUE`, `ErrNotLeader`,
…) raises `PdException` (carrying `getErrorType()`) instead of degrading into
an empty result: `scanRegions()` can no longer answer `[]` for a failed
`ScanRegions`, so a `deleteRange()` that reports success now means the range
really was deleted. A leader URL PD advertises that is not one of the
configured endpoints is ignored, so a rogue or rewritten PD member list cannot
redirect traffic.

### Cluster Identity Verification (issue #308, SEC-05)

By default the client learns its cluster ID from the first PD response header
and adopts it: whatever cluster answers the configured endpoint becomes the
cluster the client talks to. That is convenient for a first connection, but it
also means a DNS-poisoned, stale or on-path PD — or simply a staging
configuration pointed at production — is indistinguishable from the intended
cluster, and it becomes the authority for region routing and store addresses.

Set `options['expectedClusterId']` to pin the cluster identity:

```php
$client = RawKvClient::create(
    ['127.0.0.1:2379'],
    options: ['expectedClusterId' => 7123456789012345678],
);
```

The value is the ID PD reports (`getClusterId()` / `healthCheck()`), for
example from `pd-ctl cluster` or a trusted client. When it is set:

- every PD response must carry a cluster ID equal to it; a different ID
  throws `TiKvException` (`Cluster ID mismatch: expected X, server reported
  Y`) before the response reaches the caller and before any data-path request
  is issued, and a response with no cluster-ID header at all throws
  `TiKvException` (`Cluster ID missing: expected X, response carried none`) —
  a server cannot bypass the check by omitting the header;
- a cluster ID parsed out of a `mismatch cluster id, need X but got Y` gRPC
  error string is validated the same way before the retry that adopts it, so a
  server cannot steer the client onto an ID of its choosing;
- `TimestampOracle` applies the same checks to TSO response headers and TSO
  cluster-id mismatch errors, so the timestamp path cannot be redirected
  either.

Independently of the option, once a cluster ID has been established, every
later PD response header must report the same ID; a different ID throws
`TiKvException` (`Cluster ID mismatch: established X, server reported Y`)
before the response reaches the caller. An error string naming a different ID
is rejected the same way instead of silently overwriting the established ID.
Both rules are unconditional: they also apply when `expectedClusterId` is
unset. A reported ID of `0` means "not reported" (proto3 omits an unset
scalar), so it is never adopted and never treated as a mismatch:
`healthCheck()` and `getClusterId()` keep returning `null` instead of pinning
the client to cluster 0. The header and error-string rules live in one shared
`ClusterIdGuard` used by `PdClient` and `TimestampOracle` (issue #250,
REG-19).

An absent or explicitly `null` option keeps the historical initial
learn-and-adopt behaviour: the first non-zero cluster ID PD reports is
adopted. Existing deployments that relied on an error string replacing an
already established ID do change — intentionally. The option must be a
positive integer; any other type, `0` or a negative value throws
`InvalidArgumentException` at client creation. The value must fit in a PHP
`int`: PD cluster IDs are `uint64`, and the protobuf runtime clamps a decoded
value above `PHP_INT_MAX` to `PHP_INT_MAX`, so a cluster whose ID exceeds
that bound cannot be pinned through this option. This is a partial control on
its own: it detects a wrong cluster but does not authenticate the server —
genuine server authentication comes from TLS (`allowInsecure: false` plus a
verified CA, see the TLS section below).

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
one PD round trip per additional region. The same timestamp is reused by
every retry of that pass: only a new statement allocates a new read
timestamp, so a retried lock cannot silently advance past a concurrent
commit and let a stale read-modify-write pass the prewrite constraint
check.

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
        'maxReceiveMessageBytes'      => 67108864, // default: 64 MB (gRPC core default: 4 MB)
        'maxSendMessageBytes'         => 67108864, // default: 64 MB
        'keepaliveTimeMs'             => 10000,    // default: 10 s — ping interval on idle connections
        'keepaliveTimeoutMs'          => 3000,     // default: 3 s
        'keepalivePermitWithoutCalls' => true,     // default: true — ping while no call is in flight
        'compression'                 => 'none',   // default: none; 'gzip' or 'deflate' to compress requests
        'initialWindowSizeBytes'      => 0,        // default: 0 = let the gRPC core auto-tune
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: $options
);
```

The byte limits and keepalive timings must be ints `>= 1`;
`keepalivePermitWithoutCalls` must be a bool, `compression` one of `none`,
`gzip` or `deflate`, and `initialWindowSizeBytes` an int `>= 0`.

Keepalive pings (sent every `keepaliveTimeMs`, even without in-flight calls
when `keepalivePermitWithoutCalls` is true) keep idle connections alive behind
stateful firewalls / NAT gateways — which typically drop idle flows after
~5 minutes — so a dead connection is detected rather than blocking until TCP
retransmission gives up.

`maxReceiveMessageBytes` is enforced by grpc-core as the
`grpc.max_receive_message_length` channel argument, so an oversized response
frame is rejected **before** its payload is materialised as a PHP string. That
is the control which bounds response memory; the client-side size check in
`GrpcResponseParser` runs only after the string exists and therefore cannot
prevent that allocation (SEC-16, issue #319). The 64 MB default is deliberately
larger than the gRPC core's 4 MB: full-limit scans and batch reads exceed 4 MB
and would fail with `RESOURCE_EXHAUSTED` (issue #265). Lowering
`maxReceiveMessageBytes` is a valid hardening choice when you know your values
are small.

Compression is off by default and applies to the **outgoing** side:
`'gzip'` or `'deflate'` sets `grpc.default_compression_algorithm` on every
channel, so requests are compressed. Responses are not controlled by this
option — the server compresses them (or not) based on the client's advertised
`grpc-accept-encoding`, which the core sends regardless of the default. The
trade-off is CPU for wire size on large values. The server must support the
algorithm, and gzip is the one every gRPC implementation ships; deflate is
optional.

`initialWindowSizeBytes` sets `grpc.http2.lookahead_bytes`, the gRPC core's
HTTP/2 initial stream window (0 leaves the core's BDP-based auto-tuning in
place). Raise it for known high-latency links with large responses. A positive
value also sets `grpc.http2.bdp_probe` to `0`: without that, BDP probing
re-tunes the window from its bandwidth estimate almost immediately, so the
configured value would only be a starting point. The trade-off is a fixed
window that no longer adapts to the link.

All three of `keepalivePermitWithoutCalls`, `compression` and
`initialWindowSizeBytes` add or change channel arguments, and ext-grpc keys
its persistent channels on those arguments. A client configured with a
different value therefore uses a distinct underlying channel (a separate TCP
connection) from a default-configured client for the same store address.

### Channel Pool (maxChannels, idleTtlMs)

The transport keeps one gRPC channel per address and caches it. Two options
size that cache:

```php
$options = [
    // Maximum number of channels kept open at the same time. The least
    // recently used channel is evicted when the cap is reached. Default: 64.
    'maxChannels' => 64,
    // Idle time in milliseconds after which an unused channel is evicted
    // (and reopened on the next request). Default: 600000 (10 minutes).
    'idleTtlMs' => 600000,
];

$client = RawKvClient::create(['127.0.0.1:2379'], options: $options);
```

Both must be ints `>= 1`. The defaults are the ones the transport uses when it
is built by hand (`GrpcClient::DEFAULT_MAX_CHANNELS`,
`GrpcClient::DEFAULT_IDLE_TTL_MS`).

Lower the idle TTL when the network drops idle flows earlier than the
10-minute default — a cloud NAT gateway typically does so after ~350 seconds,
which is exactly the case the keepalive pings above cannot cover (they keep
the *connection* alive, not the *entry* in the cache). Lowering the cap
limits the memory the channel cache can use; a channel is evicted only when
another one is needed, so the cap never breaks a request.

The transport drops an idle channel (one that was unused for `idleTtlMs`)
when it opens a new channel. It also drops idle channels when a request
reuses a cached channel, so the handle of a store that left the cluster does
not stay in the cache. This sweep runs at most once per interval. The
interval is one tenth of `idleTtlMs`, at least 1 second and at most
`idleTtlMs`. The sweep drops the cached handle and frees the slot. It does
not always close the connection: the ext-grpc persistent channel can outlive
the handle (see the FAQ entry "ext-grpc's persistent channels are shared and
reference-counted (#283)" in `docs/helpers/faq.md`). A channel with an
in-flight call and the channel that the request uses are never dropped.

A long-running worker that is quiet for a long time calls
`$client->reapIdleChannels()` from its own tick to run the sweep without a
request. `RawKvClient` and `TxnKvClient` have this method.

### Sharing One Transport Between Clients

The gRPC transport is **long-lived and shared**: it caches one channel per
address, and that cache only pays off when the same transport serves many
requests. A RawKV client and a transactional client that talk to the same
cluster can therefore use one transport instead of two:

```php
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

// The transport the application builds and owns.
// $tlsConfig is your TlsConfig (null = plaintext); $logger your PSR-3 logger.
// With a null $tlsConfig the transport opens plaintext channels; pass
// allowInsecure: false to refuse them (see "Security: Fail-Closed TLS").
$grpc = new GrpcClient($logger, $tlsConfig);

$options = ['grpcClient' => $grpc];

$raw = RawKvClient::create(['127.0.0.1:2379'], $logger, $options);
$txn = TxnKvClient::create(['127.0.0.1:2379'], $logger, $options);

// The application owns $grpc and closes it when it is done.
$grpc->close();
```

`options['grpcClient']` accepts a `GrpcClientInterface` (any implementation —
`GrpcClient`, a test double, an application-specific transport). The client
then uses it as it is and **never closes it**: the transport belongs to the
application, so closing one client cannot break the other clients sharing it.
A client that built its own transport (the normal case) closes it in
`close()` as before.

A `TxnKvClient` still owns the transactions it began when the transport is
shared: `close()` rolls the still-active ones back before releasing its other
resources, so the rollback can reach TiKV even though the client does not
close the transport. Finish transactions explicitly anyway — a rollback that
fails during `close()` is only logged (see
[`close()` semantics](operations.md#close-semantics)).

Because the supplied transport was configured by the application, the
transport options (`tls`, `allowInsecure`, `maxChannels`, `idleTtlMs`,
`grpc`) cannot be combined with it — they would configure a transport the
factory does not build, so they are rejected with an
`InvalidArgumentException` instead of being silently ignored. Configure them
on the `GrpcClient` you pass in.

One caveat of sharing: on a retryable transport error the client closes the
channel of the affected address. With a shared transport that affects every
client using the same address — the channel is dropped from the cache and
reopened on the next request, but a request in flight on it can be
interrupted. The close does **not** force a reconnect, though: ext-grpc keeps
the channel in its process-global persistent table, so the reopened channel is
the same underlying channel and the same TCP connection (see below).

Sharing the transport shares the **channels** only. Every client keeps its own
region cache and its own store-address cache, so the two clients do not warm
each other's caches. Store addresses are cached for 600 s plus up to 60 s of
jitter; a `StoreNotMatch` region error or a retryable transport failure against
a store evicts that store's entry early, so a TiKV pod that was rescheduled to
a new address is found again on the next attempt instead of after the TTL
(issue #247). The `closeChannel()` that follows a transport failure targets the
store the failed attempt used — a replica-read follower, or the leader the
attempt resolved — rather than whatever the region cache names as the leader
when the error is handled, and it runs only for `UNAVAILABLE`, not for a
`DEADLINE_EXCEEDED` on a slow but healthy connection (issue #248).

`options['metrics']` is a **client-side** option and is always applied: it
backs the client-side counters (region cache, retries). The transport's own
RPC counters follow the transport — with an injected transport they are the
ones it was built with, so pass the same backend to the transport
(`new GrpcClient(metrics: $metrics)`) to get them everywhere.

#### What the shared channel really is

ext-grpc keeps a **process-global** table of persistent channels, keyed by the
target, a hash of the channel arguments and a hash of the credentials
(`ChannelCredentials::createInsecure()` returns `null`, so a plaintext channel
contributes no credentials hash — a TLS and a plaintext client for the same
address never share a channel). Two handles built from the same key share
**one** underlying `grpc_channel` — as long as the key is in that table:
ext-grpc persists **one channel per target** by default, and a channel
created beyond that bound is private to its handle ("The number of channel for
the target is maxed out bounded"). A private channel is never shared, so it
is never at risk either.

`Channel::close()` is a **reference-count decrement**
(`php_grpc_channel_unref()`), not a destroy: the underlying channel survives
until its last holder releases it, and the persistent table is one of those
holders. Closing one client therefore never invalidates a channel another
client holds — it only drops that client's own reference to it. Verified
against the deployed extension (grpc 1.83.0; the persistent-channel code is
unchanged in the oldest supported release, 1.57).

The practical consequences for the API:

- `close()` releases this client's handles; the TCP connection stays warm in
  the process (until the process ends or the per-target persist bound drops
  the entry). It does not tear the connection down.
- `closeChannel()` drops the cached handle but does **not** force a
  reconnect: the persistent entry survives, so the next call reuses the same
  underlying channel and the same TCP connection. Forcing a real reconnect
  needs a new persistent entry.

Two clients with **different** channel arguments (`options['grpc']`) or
different credentials for the same store address do not share a channel: the
connection count per store therefore grows with the number of distinct channel
configurations in the process, so one shared transport with one configuration
is the cheapest shape.

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
        'caCertBaseDir' => '/path/to', // optional: restrict the file to one directory
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

### Restricting Certificate Paths (`*BaseDir`)

`caCertBaseDir` and `clientCertBaseDir` are optional and restrict **where the
certificate files may live**. The path is canonicalized with `realpath()`
before the comparison, so `..` segments and symlinks are resolved first, and
the canonicalized path has to start with the canonicalized base directory — a
path that does not is rejected with `InvalidArgumentException` ("TLS file path
is outside the allowed base directory"). That is what stops a certificate path
— or a symlink planted in the certificate directory — from loading a file you
did not intend.

Each option restricts only the read it belongs to:

| Option | Restricts |
| --- | --- |
| `caCertBaseDir` | `caCertFile` |
| `clientCertBaseDir` | `clientCertFile` **and** `clientKeyFile` |

Without a base directory the file is still canonicalized with `realpath()`, but
nothing keeps it inside one particular directory. Certificate files must have a
`.pem`, `.crt` or `.key` extension.

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

When both forms are given for the same credential, the explicit file path wins:
`caCertFile` takes priority over `caCertPem`, and a complete
`clientCertFile` + `clientKeyFile` pair takes priority over
`clientCertPem` + `clientKeyPem`. Supplying both is redundant rather than an
error, but the file is the one that is read.

### Deprecated TLS Option Keys

`caCert`, `clientCert` and `clientKey` are **deprecated**. They still work for
backward compatibility, but they guess between a file path and inline PEM
content from the value itself: a value containing a `-----BEGIN` header is read
as inline PEM, anything else is read as a file path (a path that does not exist
or is not readable throws `InvalidArgumentException`). For the client pair the
guess is made for both values together — if either value looks like a file
path, both are read as file paths — so mixing an inline PEM certificate with a
file path (or the other way round) fails with `TLS file has disallowed
extension ""`. The guess makes the same key mean two different things
depending on the string, and — because the legacy keys have no base-directory
counterpart — the `*BaseDir` restriction cannot be applied to them at all.

Use the explicit keys instead: `caCertFile`, `clientCertFile` + `clientKeyFile`
(with `caCertBaseDir` / `clientCertBaseDir` if you want the path restriction),
or the inline `caCertPem`, `clientCertPem` + `clientKeyPem`.

### TLS Configuration Builder

For advanced TLS configuration:

```php
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;

$builder = new TlsConfigBuilder();
$builder->withCaCertFile('/path/to/ca.crt')
    ->withClientCertFile('/path/to/client.crt', '/path/to/client.key');

$tlsConfig = $builder->build();

// Use with custom client construction
$grpc = new GrpcClient($logger, $tlsConfig);
// The store cache is the fourth argument of PdClient. It is mandatory: when
// you omit it, PdClient creates a default StoreCache. Pass your own to set
// the TTL, the size or the logger. A StoreNotMatch region error or a
// transport failure against a store drops that store's entry, so the next
// request re-resolves the address through PD (issue #247).
$storeCache = new StoreCache(logger: $logger);
$pdClient = new PdClient($grpc, 'tikv.example.com:2379', $logger, $storeCache);
// The client does not own the transport or the PD client it is given:
// close() and the destructor leave both open. Pass ownsGrpcClient: true
// and/or ownsPdClient: true to transfer ownership.
// Pass the same storeCache, or a StoreNotMatch / transport failure cannot
// drop the stale address: the client's resolver has no cache to invalidate
// while PdClient keeps serving that entry. Omitting storeCache disables
// early invalidation.
$client = new RawKvClient($pdClient, $grpc, storeCache: $storeCache);
```

> **Note:** the manual path above is for the rare case where the client has
> to be assembled by hand. `RawKvClient::create()` /
> `TxnKvClient::create()` build the transport, the store cache and the PD
> client for you, and accept the same TLS options through
> `options['tls']` — see [Basic Configuration](#basic-configuration).

**Who closes the connections on the manual path.** The rule of
[Sharing One Transport Between Clients](#sharing-one-transport-between-clients)
is "the connections belong to whoever created them", and it holds for the
factory path. A client assembled by hand is *given* a transport and a PD
client it did not create, so it does not own them: `close()` and the
destructor leave both open (issue #204). That makes sharing safe — destroying
one client cannot close the channels another one still uses. The client still
clears its own region cache.

If the application wants to hand ownership over instead, say so explicitly:

```php
$client = new RawKvClient(
    $pdClient,
    $grpc,
    ownsGrpcClient: true,
    ownsPdClient: true,
);
```

With `ownsGrpcClient: true` / `ownsPdClient: true` the client closes the
matching dependency in `close()`; the defaults are `false` for a
hand-assembled `RawKvClient`. `TxnKvClient` takes the same `ownsGrpcClient`
parameter.

### Security: Fail-Closed TLS

By default the client **permits plaintext gRPC** and logs a warning on every
insecure channel it opens. That is the historical behaviour and it stays the
default, but it means a client without any TLS configuration talks to the
cluster in plaintext, and only the log says so.

`allowInsecure: false` turns that into a fail-closed control: the client
refuses to open a channel it cannot secure, and the misconfiguration surfaces
as an `InvalidStateException` instead of as unencrypted traffic.

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$client = RawKvClient::create(
    ['tikv.example.com:2379'],
    options: [
        'tls' => ['caCertFile' => '/path/to/ca.crt'],
        'allowInsecure' => false,   // throws InvalidStateException if TLS is not configured
    ],
);
```

**What `allowInsecure: false` does**

- every attempt to open a gRPC channel checks the TLS configuration first;
- if the TLS configuration is enabled (a CA certificate — a client certificate
  and key need one too), the channel is opened with TLS — the option changes
  nothing else;
- if it is not, the client throws an `InvalidStateException` instead of
  opening a plaintext channel. The channel is never opened, so no request is
  sent in the clear.

The exception message is:

> Cannot create gRPC channel: TLS is not configured and insecure connections are not allowed. Provide a TlsConfig or set allowInsecure=true explicitly.

**What happens when no TLS configuration is given**

| Configuration | `allowInsecure` (default `true`) | `allowInsecure: false` |
|---|---|---|
| `tls` with a usable CA (and optionally a client certificate) | TLS channel, no warning | TLS channel, no warning — identical |
| no `tls` block at all | plaintext channel, a warning is logged for every channel | `InvalidStateException` on the first channel |
| a `tls` block the factory rejects (missing file, unknown key, half-configured client certificate) | `InvalidArgumentException` while the client is created | the same — the client is not created at all |

A `tls` block the factory *accepts* is never downgraded to plaintext either
way: the channel is created with TLS credentials, so a configuration that does
not work (an empty CA, a certificate the server does not trust) fails the
handshake instead of falling back to an unencrypted connection.

The `InvalidStateException` is raised when the first channel is opened — that
is, on the first request — not when the client is created. In the default API
version `create()` opens no channel, so a misconfigured client fails on use;
in API V2 mode (`options['apiVersion'] => 2`) the keyspace probe in
`create()` already needs a channel, so the failure surfaces there.

**How it differs from the default**

- the default (`true`) is *fail-open*: it downgrades to plaintext and only
  warns. The warning goes to the log, and a deployment that does not ship
  the log never learns that its traffic is unencrypted;
- `false` is *fail-closed*: the request fails and nothing is transmitted. The
  trade-off is that a missing TLS configuration becomes an outage instead of
  an unencrypted connection.

**Recommendation:** set `allowInsecure: false` together with a valid `tls`
block for every production deployment. It is the control a security review
asks for: proof that the client cannot silently fall back to plaintext when
a certificate path is misconfigured. The default stays `true` only for
backward compatibility.

The same option exists on the transport itself, for a client assembled by
hand:

```php
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;

// Insecure connections will throw InvalidStateException
$grpc = new GrpcClient($logger, tlsConfig: $tlsConfig, allowInsecure: false);
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

Recognised `options['tls']` keys:

- **Supported:** `caCertFile`, `caCertBaseDir`, `caCertPem`, `clientCertFile`,
  `clientCertBaseDir`, `clientKeyFile`, `clientCertPem`, `clientKeyPem`.
- **Deprecated:** `caCert`, `clientCert`, `clientKey` — see
  [Deprecated TLS Option Keys](#deprecated-tls-option-keys).

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

The region cache asks the logger **once** whether it can emit a `debug`
record at all, and it builds the log context of a debug record only when the
answer is yes. A logger that discards the debug records — the default
`NullLogger`, a Monolog logger above the `DEBUG` level, or any logger that
answers "no" to Monolog's `isHandling()` — therefore costs nothing on the hot
path, where the context of a region-cache record carries a redacted key. The
other components (the lock resolver, the transaction committer, the store
cache) still build their context unconditionally.

A logger that cannot be asked (a plain PSR-3 logger without a level API) is
assumed to handle `debug`, so a record is never lost. A `NullLogger`
*subclass* is assumed to handle `debug` as well, unless it keeps psr/log's
own `log()` and `debug()` implementations: psr/log does not declare that
class `final`, so a subclass that overrides either method may record what
it receives.

The answer is taken **once**, when the client is built, and it is not
re-examined afterwards. A logger that is reconfigured at runtime — a Monolog
handler whose level is lowered to `DEBUG`, say — keeps the answer the client
was given: the cache stays quiet, and a logger that was at `DEBUG` and is
raised to `INFO` keeps building a context that is discarded. Configure the
logger's level and handlers **before** you create the client, and create a
new client after you change them.

### Shutdown and destructors

Logging from a destructor is best-effort. PHP releases objects in an order
that is not guaranteed, so a handler's stream, buffer or network transport
may already be gone when a client's destructor closes its channels or rolls
back an abandoned transaction. The client therefore emits every record on a
close or destructor path through a guard that swallows a logger failure: a
broken logger cannot replace the original diagnostic with `Fatal error:
Uncaught ... thrown in destructor`. Those records are lost in that case.

Once PHP has entered its shutdown sequence, `Transaction::__destruct()` and
`TxnKvClient::close()` also skip the rollback RPC: ext-grpc may already be in
or past module shutdown, where a call's behaviour is undefined. An abandoned
transaction keeps its locks until the lock TTL expires, so callers that need
an immediate release must `commit()` or `rollback()` explicitly, or call
`close()` before shutdown. An explicit `close()` outside shutdown still rolls
back the live transactions before it releases the transport.

The flag is set by a callback registered with `register_shutdown_function()`,
and shutdown callbacks run in registration order. An object released by a
shutdown callback that was registered **before** the client was created —
a bootstrap cleanup function, say — is therefore destroyed before the flag
is set and keeps the normal destructor behaviour (including the rollback
RPC). Register the client's cleanup after the bootstrap callbacks, or call
`close()` explicitly, if that ordering matters.

### What Gets Logged

The client writes these levels:

- **DEBUG**: Region cache hits and misses, store cache hits and misses, channel opening and eviction, request detail. This level is verbose. Do not use it in production.
- **INFO**: Region and store invalidation, region leader switch, PD failover, transaction start, fallback from one-phase or async commit to two-phase commit.
- **WARNING**: Retry attempts (`Retrying operation`), failed regions in a batch, channel reconnects, partial prewrite failure, `Pessimistic lock retry budget exhausted`, a skipped GC safe-point check, and slow operations (only when the `slowLog` option is enabled — see [Slow Operation Logging](#slow-operation-logging)).
- **ERROR**: The general retry loop (`RetryExecutor`) writes `Retry attempt cap exhausted`, `Retry budget exhausted`, `ServerBusy budget exhausted`, `Retry deadline exhausted` and `Fatal error, not retrying`. The client also writes ERROR records for an exhausted batch deadline, a failed TSO request, an undetermined primary commit, and failed `close()` steps.

Region cache hits and misses are **not** INFO records. A logger at the `INFO` level does not show them.

Keys in the log details are redacted — see
[Log Redaction](#log-redaction) for the format.

### Log Redaction

The client never writes a raw key it holds into the log context: every key
it formats for a log line or an exception message passes through
[`KeyRedactor`](../src/Client/Util/KeyRedactor.php), which replaces it with a
truncated keyed hash and the total length of the key:

| Key | Logged as |
|---|---|
| `key_a` (5 bytes) | `#3c469e9d6c5875d3 (5 bytes)` |
| `user:session` (12 bytes) | `#60303ae22b998861 (12 bytes)` |
| empty key | `"" (0 bytes)` |

The digest is the first 16 hex characters (64 bits) of
`hash_hmac('sha256', $key, $salt)`. The salt is 32 random bytes generated on
first use, so the same key produces the same token for the lifetime of the
process — log correlation still works — while two processes (or two restarts)
produce different tokens. The digest is one-way: unlike the old hex prefix it
cannot be decoded back into the key, including for keys of 8 bytes or fewer.
The byte count is the length of the whole key. The examples above show the
shape only: the actual digest depends on the process salt, so do not copy
them as literal values.

Configure a fixed salt when tokens must be comparable across processes or
restarts. Use a high-entropy secret, for example 32 random bytes encoded as
hex:

```php
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

KeyRedactor::setSalt(getenv('TIKV_LOG_REDACTION_SALT') ?: null);
```

`setSalt(null)` restores the default (a fresh random salt per process); an empty
string is rejected. Call it before the first redaction: `setSalt()` throws
`LogicException` once a key has been redacted, because changing the salt
afterwards would break the in-process correlation the token exists for.

The redacted form is not quoted: pass it to an unquoted `%s` in a message
template (the empty-key form still carries its own quotes; see
[`helpers/faq.md`](helpers/faq.md)).

**Values are never logged at all** — not redacted, not truncated. The client
puts no value into any log context or exception message, so there is nothing
to redact on that side: only keys cross the log boundary, and only in their
redacted form.

**The channel the redactor does not cover: TiKV's own error text.** Two log
lines copy the message of a `RegionException` into the log context — the
aggregated prewrite-failure warning
(`TwoPhaseCommitter::logPrewritePartialFailure()`) and the pessimistic-lock
retry warning — and that message can be TiKV's own text:
`RegionException::fromRegionError()` copies `errorpb.Error::getMessage()`
verbatim. TiKV can name a key in such a message, and the client cannot
redact a key it never saw. Every key the client itself holds is redacted; a
key only the server ever saw is not. The structured `errorpb` fields that do
carry keys are never serialized into a message: `SstIngestor` appends only
the store error's `message` to a `RawWrite` failure, not the protobuf
(issue #307).

The redactor is applied wherever a key can reach a log line:

- the **region cache** (`RegionCache` — cache hits, misses, cached regions);
- the **retry executor** (`RetryExecutor` — retry attempts, budget
  exhaustion, region invalidation);
- the **lock resolver** (`LockResolver` — lock resolution, `CheckTxnStatus`);
- the **transaction committer** (`TwoPhaseCommitter` — heartbeats, the
  primary key of a commit). `Transaction` itself contains no redaction call
  of its own — the transactional path is covered through the lock resolver
  and the committer;
- the **slow-log paths** (`RawKvCrud`, `RawKvScanner`, `RawKvAtomic`,
  `RawKvBatch` and `RawKvRangeOps` — the "Slow TiKV operation" warnings).

Exception messages are redacted too, because a message is a logging channel:
Monolog, Sentry and uncaught-exception handlers all capture `getMessage()`.
The fail-closed region-resolution errors (`RegionResolver`,
`RegionGrouper`), the per-pair `KeyError` messages (`RegionErrorHandler`),
the region-shrink error (`RawKvRangeOps`), the heartbeat and primary-key
errors of the committer (`TwoPhaseCommitter` — a retryable
`TxnRetryableException` and a fatal `InvalidStateException`), the
retry-budget and lock-wait exceptions (`RetryBudgetExhaustedException`,
`LockWaitTimeoutException`), the SST-write store error (`SstIngestor` — only
the store error's message is appended, never the serialized protobuf) and
the API V2 keyspace check
(`KeyOutOfBoundsException`) therefore name the redacted key. When your own
code needs the raw key, read it from the typed accessor instead of parsing
it out of a message — `RetryBudgetExhaustedException::getRawKey()` and
`LockWaitTimeoutException::getKey()`.

#### Installing a custom redactor

`KeyRedactor::setRedactor()` replaces the default redactor:

```php
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

$secret = getenv('TIKV_LOG_REDACTION_SECRET');
if ($secret === false || $secret === '') {
    throw new \RuntimeException('TIKV_LOG_REDACTION_SECRET must be set');
}

KeyRedactor::setRedactor(
    fn (string $key): string => 'hmac:' . substr(hash_hmac('sha256', $key, $secret), 0, 16)
);
```

The default is already a keyed SHA-256 hash with a per-process CSPRNG salt, so
override it only when you need to match an existing token scheme (a prefix, a
different digest length, or an HMAC secret your log pipeline already uses).
If you only need tokens that are comparable across processes or restarts, use
`KeyRedactor::setSalt()` above instead.

The callable receives the raw key and returns the string that is logged in
its place. Install it once, before the first client is created and before the
first key is redacted. The setter is one-shot: it throws `LogicException`
after the first `redact()` call, so a debug helper or a dependency cannot
silently replace the redactor once the client is running. There is no way to
restore the default in a running process; configure the redactor at
bootstrap.

> **The redactor is process-global.** `KeyRedactor::setRedactor()` is a
> static method and the custom callable is held in a static property, so it
> applies to every client, every transaction and every key in the process —
> not to a single client instance. Set it once, before the first client is
> created, and remember that it also covers the exception messages.

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

### Slow Operation Logging

**Off by default.** Slow-operation logging is opt-in: without the `slowLog`
option the client measures nothing and logs nothing. Enable it by passing the
option to `RawKvClient::create()`:

```php
$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    logger: $logger,
    options: [
        'slowLog' => [
            'readThresholdMs'        => 100,  // default 100
            'writeThresholdMs'       => 200,  // default 200
            'batchReadThresholdMs'   => 500,  // default 500
            'batchWriteThresholdMs'  => 500,  // default 500
            'scanThresholdMs'        => 1000, // default 1000
            'deleteRangeThresholdMs' => 2000, // default 2000
            'checksumThresholdMs'    => 3000, // default 3000
        ],
    ],
);
```

Every key is optional and keeps its default when it is omitted. A threshold of
`0` disables the warning for that operation type, so `'readThresholdMs' => 0`
silences the read warning while the other six stay active. A negative value is
a configuration error and throws `InvalidArgumentException`, and so is a key
the client does not know or a value that is not an integer. Omitting the whole
`slowLog` key disables slow logging entirely.

| Option key               | Default | Operations covered                                                                    |
|--------------------------|---------|---------------------------------------------------------------------------------------|
| `readThresholdMs`        | 100     | `get()`, `getKeyTTL()`, `getKeyTTLResult()`                                            |
| `writeThresholdMs`       | 200     | `put()`, `delete()`, `compareAndSwap()`, `putIfAbsent()`                              |
| `batchReadThresholdMs`   | 500     | `batchGet()`                                                                           |
| `batchWriteThresholdMs`  | 500     | `batchPut()`, `batchDelete()`                                                          |
| `scanThresholdMs`        | 1000    | `scan()`, `scanPrefix()`, `reverseScan()`, `batchScan()`, `scanIterator()`, `scanPrefixIterator()` |
| `deleteRangeThresholdMs` | 2000    | `deleteRange()`, `deletePrefix()`                                                      |
| `checksumThresholdMs`    | 3000    | `checksum()`                                                                           |

There is **no threshold for SST ingest** (`ingest()`): it is never measured, so
no warning is emitted for it.

When an operation takes longer than its threshold, the client emits a PSR-3
**warning** with the message `Slow TiKV operation` and this context:

| Context field  | Meaning                                                                                                                                                     |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `operation`    | The operation type: `read`, `write`, `batch_read`, `batch_write`, `scan`, `delete_range` or `checksum`.                                                      |
| `key`          | The redacted key (`KeyRedactor::redact()`) — a truncated keyed hash plus the total length, e.g. `#3c469e9d6c5875d3 (5 bytes)`; the digest depends on the process salt. It is the key the operation is anchored on: the key itself for a single-key read or write, the **first key of the batch** for a batch operation, and the key the range starts at for a scan or range operation (the exclusive upper bound for a reverse scan, the start of the per-region sub-range when the range spans regions). |
| `duration_ms`  | The measured duration in milliseconds, rounded to two decimals.                                                                                               |
| `threshold_ms` | The threshold that was exceeded, in milliseconds.                                                                                                            |

An emitted line therefore looks like this (the prefix depends on the logger's
formatter):

```
WARNING  Slow TiKV operation  {"operation":"scan","key":"#3c469e9d6c5875d3 (5 bytes)","duration_ms":1420.5,"threshold_ms":1000}
```

The duration is measured around the RPC: for the batch fan-outs it covers the
wait for the responses (the round-trip latency), not the dispatch.

The warning is emitted through the PSR-3 logger passed to `create()`, so it
appears only when that logger's level includes `warning`.

`slowLog` is a RawKV feature. `TxnKvClient::create()` accepts the option (the
shared connection layer parses it for both clients), but the transactional path
carries no slow-operation instrumentation, so no warning is emitted for
transactions.

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

## Batch Commands Multiplexing (Experimental)

**Status: experimental, off by default** (issue #418). Enable with
`options['batchCommands'] => true` on `RawKvClient::create()`.

TiKV exposes a bidirectional streaming RPC,
`rpc BatchCommands(stream BatchCommandsRequest) returns (stream
BatchCommandsResponse)`, that packs many per-region requests into one wire
message and answers them possibly out of order, correlating each response by
`request_id` (tikv/rfcs `0004-batch-client-messages.md`; client-go uses it
by default). Amortising HTTP/2 framing and gRPC message overhead over many
operations reduces TiKV's gRPC CPU and per-operation latency at high QPS.

When the flag is on, the RawKV batch fan-outs — `batchGet()`, `batchPut()`
and `batchDelete()` — collect their per-region sub-requests within one
fan-out and multiplex every operation representable in the
`BatchCommandsRequest.Request` oneof (the raw KV read/write family:
`RawGet`, `RawPut`, `RawDelete`, `RawScan`, `RawBatchGet`, `RawBatchPut`,
`RawBatchDelete`, plus the txn single-read family) into **one round trip per
store address** instead of one unary RPC per sub-batch. Responses are
correlated by `request_id`, tolerating out-of-order delivery, and one open
stream per store address is reused across round trips.

Automatic fallback to the existing unary fan-out (with its full
retry/region-error semantics) happens when:

- the operation type is absent from the BatchCommands oneof
  (`RawCompareAndSwap`, `RawGetKeyTTL`, `RawChecksum`, `KvGC`, `KvImport`,
  and every other unary RPC outside the map above);
- the multiplexed response carries a region error — those entries are
  re-dispatched on the unary path, whose retry executor re-resolves on every
  attempt and invalidates the region cache only when the error is a routing
  error (`EpochNotMatch`, `RegionNotFound`, `StoreNotMatch`, …), not for
  `ServerIsBusy`-style store overload;
- the stream layer fails (peer closed the stream, drain deadline expired).

Properties and limitations of the current implementation:

- One stream per store address is opened lazily on a channel from the
  existing `GrpcClient` pool and carries a fixed 60 s lifetime deadline; a
  stream that fails is discarded and re-opened on the next round trip. The
  deadline is measured from stream OPEN, not from each exchange, so later
  round trips on an aging stream get a shrinking remaining budget.
- The raw batch operations are idempotent, so the unary fallback after a
  stream failure is safe to re-run — EXCEPT a re-run `batchPut` extends the
  per-key TTLs it sets (they are absolute-from-now), so entries that were
  already answered come back with a fresh TTL.
- The PHP client is synchronous: there is no background receive loop. The
  multiplexing window is a single fan-out (the issue's "per-request-cycle
  batching window" scope) — interleaved single-key traffic from unrelated
  call sites is NOT multiplexed; that would require an event loop the
  library does not have.
- TxnKV fan-outs (prewrite/commit/lock) and the range operations
  (`deleteRange`, `scan`, `batchScan`) are not multiplexed this cycle.
- The flag must be a boolean; anything else throws
  `InvalidArgumentException`. Keep it `false` unless benchmarking shows a
  win for your workload — the unary fan-out remains the default and the
  byte-identical behavior when the flag is off.

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
| `enable1Pc`        | Transaction fits in a **single prewrite batch**: one region, ≤ 128 keys, and within [`maxTxnBatchSize`/`maxTxnBatchCount`](#transaction-size-limits-and-request-batching-txnkv). | The commit phase and the PD `getTimestamp()` round trip. |
| `enableAsyncCommit`| Transaction has ≤ 256 keys (any number of regions).                        | The commit phase (the PD round trip stays implicit in prewrite's `min_commit_ts`). |

Details:

- One-phase commit sets `try_one_pc` on the prewrite; TiKV commits the
  transaction inside the prewrite and returns the commit timestamp in
  `PrewriteResponse::one_pc_commit_ts`. When TiKV declines (e.g. the region
  is already busy, or the transaction outgrew the limit between grouping and
  prewrite), the client silently falls back to the normal two-phase path.
- Async commit sets `use_async_commit`, the secondary keys and
  `max_commit_ts` (start timestamp + a bounded TSO gap) on the primary
  batch's prewrite, and derives the commit timestamp as the maximum of the
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
  The prewrite loop prewrites the primary batch last (the commit-decision
  barrier, client-go parity). When TiKV declines, the client falls back to
  two-phase commit.
- If both options are set, one-phase commit is tried first (single batch
  only); async commit covers multi-region small transactions.
- Both modes remain safe under TiKV's own server-side guards — the server
  never applies an unsafe fast commit, it just answers with a decline and the
  client proceeds with two-phase commit.

## Lock TTLs and the Primary Lock Keepalive (TxnKV)

A lock that a transaction holds expires after its TTL. After that, a
concurrent transaction can resolve the lock and roll the owner back. The
TTLs are set per transaction, on `begin()` (issue #429, GAP-15):

```php
$txn = $client->begin([
    'optimisticLockTtlMs'  => 3000,  // default
    'pessimisticLockTtlMs' => 30000, // default
]);
```

| Option | Default | Meaning |
|---|---|---|
| `optimisticLockTtlMs` | `3000` | Baseline TTL of the optimistic prewrite locks, in milliseconds. The prewrite adds 10 ms for each mutation and caps the result at 120000 ms. |
| `pessimisticLockTtlMs` | `30000` | TTL of the pessimistic locks and of the prewrite locks of a pessimistic transaction, in milliseconds. |

Each value must be an `int` from `1` to `120000`. Any other value throws
`InvalidArgumentException`.

### Heartbeat interval and TTL

TiKV counts a lock TTL from the start timestamp of the transaction: a lock
is expired when `start time + TTL` is in the past. For this reason the client
adds the time the transaction has already run (`uptime`) to every lock TTL: the
pessimistic-lock requests, the prewrite requests and the heartbeats all
carry `min(uptime + TTL, 3600000)` ms. A heartbeat can only raise a TTL, so
a value at or below the current TTL changes nothing. The client stops extending
a lock when the total TTL reaches one hour.

The client sends a heartbeat in these cases:

1. A pessimistic transaction sends a heartbeat inside `get()`, `batchGet()`,
   `scan()`, `set()`, `delete()` and `commit()` when less than half of the lock TTL
   remains. The first pessimistic lock starts the keepalive.
2. `commit()` first sends a heartbeat when less than half of the TTL remains
   (pessimistic transactions), and the prewrite sends a heartbeat between
   region prewrites (issue #218).
3. Your code calls `Transaction::heartbeat()`.

An optimistic transaction holds no lock before `commit()`, so it sends no
opportunistic heartbeat. A heartbeat for a lock that does not exist fails.

PHP has no background thread. A transaction that waits on an external call
without touching TiKV does not send a heartbeat. If the wait can be longer than
half of the TTL, call `Transaction::heartbeat()` before the wait. Without
an argument it asks for `uptime + TTL` of the transaction mode. An explicit
argument is a total TTL in milliseconds, counted from the start of the
transaction. The same applies to a single RPC that takes longer than the TTL.

If a heartbeat fails, the operation throws the typed exception of
`Transaction::heartbeat()` (issue #492) and the next operation tries again.
The usual cause is a lock that is already gone. In that case, roll the
transaction back.

### A single prewrite or commit RPC can outlive the lock TTL

The automatic keepalive has one inherent limit: it can only send a heartbeat
at a point where control returns to PHP. Those points are

1. at the start of `commit()` for a pessimistic transaction, before the
   prewrite phase,
2. after the primary region's prewrite has been acknowledged, and
3. after the parallel fan-out of the secondary prewrites has completed.

In the default two-phase path those are two separate checks. A multi-region
async commit deliberately prewrites the primary **last** (the primary lock is
the commit-decision barrier), so its primary prewrite follows the fan-out and
the two points coincide: it checks once, after the primary prewrite, and not
again when the fan-out returns.

There is no heartbeat **inside** a `KvPrewrite` or `KvCommit` call: the gRPC
call blocks in the extension and PHP has no timer that could run while it is
in flight. Nor is there one between the individual secondary batches, because
they are dispatched in parallel and the committer waits for the whole fan-out
before checking the clock. If one RPC — or the gap between the last prewrite
heartbeat and the primary commit, plus that commit RPC — takes longer than the
lock TTL, the lock expires and a concurrent reader may resolve (roll back) the
transaction. The later commit then fails with `TxnLockNotFound`. A
single-region transaction **that fits in one request batch** (the default for
small write sets) is the simplest case: it has exactly one prewrite RPC and
therefore no between-RPC point at which a heartbeat could be sent. A larger
single-region write set is split into a primary batch plus secondary batches
in the same region and gets the same batch-boundary checks as a multi-region
transaction, but no individual request can be heartbeated mid-flight.

Four settings determine whether that can happen:

| Setting | Effect |
|---|---|
| `optimisticLockTtlMs` / `pessimisticLockTtlMs` | The base lock lifetime. Optimistic locks use `min(optimisticLockTtlMs + 10 ms × mutations, 120000)`, where the mutation count is the **whole write set**, not one batch; pessimistic locks use `pessimisticLockTtlMs`. The value sent with the request is `min(uptime + base, 3600000)`, so a lock created at the start of prewrite expires roughly `base` milliseconds later (less only when the transaction has already run for more than `3600000 − base`). |
| `writeTimeoutMs` (default `5000`) | The upper bound of the gRPC deadline of **each** `KvPrewrite` and `KvCommit` attempt. The deadline actually sent is `min(writeTimeoutMs, remaining retry deadline)`, so one attempt can occupy the wire for at most this long and never past the operation's deadline. With the defaults, an optimistic transaction with fewer than ~200 mutations has a base TTL below `writeTimeoutMs` (`3000 + 10 × mutations`), so a single slow prewrite can outlive it. |
| Retry budget (`retryDeadlineMs`, default `30000`; `maxBackoffMs`, default `20000` sleep budget) | A region's prewrite may be attempted more than once. Each attempt gets up to `writeTimeoutMs`, capped by the remaining retry deadline, so one region can consume several timeout windows only until `retryDeadlineMs` is reached. |
| `batchDeadlineMs` (default `60000`) | The wall-clock deadline of the parallel secondary-prewrite fan-out (and of the secondary commit fan-out). It can exceed the lock TTL and does not itself extend any lock. |

Guidance:

- **Keep the per-RPC timeout below the lock TTL.** Set `writeTimeoutMs`
  comfortably below the effective base TTL, ideally below half of it (the
  point at which a heartbeat becomes due). For an optimistic transaction the
  default TTL is small, so either raise `optimisticLockTtlMs` (for example to
  `10000`, comfortably above the default `writeTimeoutMs` of `5000`) or lower
  `writeTimeoutMs`. `pessimisticLockTtlMs` defaults to `30000`, which already
  covers the default write timeout.
- **Make each request small.** `maxTxnBatchSize` and `maxTxnBatchCount` split
  a region's prewrite into smaller requests (see
  [Transaction Size Limits and Request Batching](#transaction-size-limits-and-request-batching-txnkv)).
  A smaller request finishes sooner and is less likely to outlive the TTL.
  Batching does **not** create extra heartbeat points: the secondary batches
  are sent in parallel and the heartbeat is checked only after all of them
  have returned.
- **Raise the TTL for a slow store or a large transaction.** The optimistic
  base TTL can be raised to `120000` ms and heartbeats extend a lock up to one
  hour (`MAX_TXN_TTL_MS`). Size it for the worst case of the whole prewrite
  phase — store latency, any retry windows, and the fan-out — not for the
  average request.
- **Do not rely on a heartbeat to rescue a hung RPC.** `writeTimeoutMs` is the
  only bound on one call; once the deadline fires, the attempt has already
  consumed that much of the lock's lifetime.

One-phase commit leaves no lock, and async commit carries the commit decision
on the primary lock during prewrite, so 1PC has no lock to expire and async
commit has no separate primary `KvCommit` RPC. Both still perform prewrites,
so the per-RPC timeout guidance above applies to them too.

## Pessimistic Lock Wait Timeout (TxnKV)

A pessimistic-lock request tells TiKV how long it may block on a contended
key before it answers with a "locked" error, which the client then retries
inside its lock-wait budget (`maxBackoffMs`). The wait is configured per
transaction, on `begin()` (issue #443, DIV-07):

```php
$txn = $client->begin([
    'lockWaitTimeMs' => 1000, // default; 0 = "let TiKV use its own default"
]);
```

| Option | Default | Meaning |
|---|---|---|
| `lockWaitTimeMs` | `1000` (the same timeout TiKV applies by default, `pessimistic-txn.wait-for-lock-timeout`) | How long TiKV may wait for a contended key, in milliseconds. `0` means "use TiKV's own default timeout" — the only value that is not shrunk as the budget is consumed. Must be `>= 0`; anything else throws `InvalidArgumentException`. |

The value is sent as `PessimisticLockRequest.wait_timeout` and is
`min(lockWaitTimeMs, remaining lock-wait budget)`: it never exceeds the
budget and shrinks as the budget is consumed (client-go's
`calculateEffectiveWaitTime`). TiKV caps the wait at its own
`pessimistic-txn.wait-for-lock-timeout` (1000 ms by default), so a larger
`lockWaitTimeMs` does not make the server wait longer. When the budget runs
out, the transaction fails with `LockWaitTimeoutException` — it never
proceeds to prewrite without the lock.

The wait is a **per-request** wait: the client retries inside the budget, so
the total time a contended key can block a worker is the budget **plus** the
server-side waits of the attempts — the budget counts the client's backoff
sleeps, not the time the lock RPCs themselves take. Lower the value to keep a
PHP worker slot free; raise it to let TiKV park the request instead of
answering early and having the client send it again.

## Transaction Size Limits and Request Batching (TxnKV)

A transaction buffers its write set in the PHP worker until `commit()`, so a
runaway write set can exhaust `memory_limit` and leave locks behind; and one
region with many mutations produces a single prewrite/commit request that TiKV
can reject with `RaftEntryTooLarge`. Both are configurable per transaction on
`begin()` (issue #430):

```php
$txn = $client->begin([
    'maxTxnEntryCount' => 10000,   // entries, 0 = unlimited (default)
    'maxTxnEntrySize'  => 1048576, // key+value bytes, 0 = unlimited
    'maxTxnTotalSize'  => 4194304, // whole write set, 0 = unlimited
    'trackReadSet'     => true,    // opt in to the read set (default false)
    'maxReadSetEntries' => 100000, // read-set entries, 0 = unlimited
    'maxReadSetTotalSize' => 8388608, // whole read set, 0 = unlimited
    'maxTxnBatchSize'  => 16384,   // bytes per prewrite/commit request
    'maxTxnBatchCount' => 512,     // mutations/keys per request
]);
```

| Option | Default | Meaning |
|---|---|---|
| `maxTxnEntryCount` | `0` (unlimited) | Maximum number of write-set entries. |
| `maxTxnEntrySize` | `0` (unlimited) | Maximum bytes of one entry, measured as key length plus value length. A delete contributes the key length only. |
| `maxTxnTotalSize` | `0` (unlimited) | Maximum bytes of the whole write set: the sum of every entry's key plus value length. |
| `trackReadSet` | `false` | Opt in to the read set populated by `get()`/`batchGet()`. Disabled by default because nothing in the commit path consumes it. Must be a bool. |
| `maxReadSetEntries` | `0` (unlimited) | Maximum number of tracked read-set entries. Has no effect unless `trackReadSet` is true. |
| `maxReadSetTotalSize` | `0` (unlimited) | Maximum bytes of the whole tracked read set: key length plus value length per entry. A not-found read contributes the key length only. Has no effect unless `trackReadSet` is true. |
| `maxTxnBatchSize` | `16384` | Maximum **payload** bytes of one prewrite, pessimistic-lock or commit request per region: key plus value length for a prewrite mutation, key length for a pessimistic-lock mutation or a commit key. Excludes request framing, `Context`, `primary_lock` and the async `secondaries` list; a single mutation larger than this still forms its own batch. Matches client-go's `DefTxnCommitBatchSize`. |
| `maxTxnBatchCount` | `512` | Maximum payload items — mutations (prewrite or pessimistic lock) or keys (commit) — of one request per region. Matches RawKV's `MAX_BATCH_LIMIT`. |

The size limits are **opt-in** (`0` = unlimited) because there is no single
correct value for every application; set them for untrusted or unbounded
workloads. Exceeding one throws `TransactionTooLargeException`. A write is
rejected from `set()`/`delete()` **before** it is buffered and before any RPC —
in a pessimistic transaction that means before the eager lock request, so a
rejected write leaves no server-side lock. A read is rejected from
`get()`/`batchGet()` before the fetched value is retained in the read set; the
RPC has already happened by then, because the value's size is only known after
the response arrives. The bound applies to what the transaction **retains**,
not to the peak of one call: a single `get()` response or a whole `KvBatchGet`
response is received in full before the check runs, so the peak of one read can
exceed `maxReadSetTotalSize` even though the value is not added to the map. The exception names the limit
(`getLimitName()`), the configured value (`getLimit()`) and the value the
operation would have produced (`getActual()`). Overwriting (or re-reading) an
existing key accounts for the entry it replaces, so a replace does not consume
additional budget.

`Transaction::getWriteSetSize()` / `getWriteSetCount()` and
`Transaction::getReadSetSize()` / `getReadSetCount()` return the current bytes
and entry counts, so a transaction can be logged or exported as a metric
without walking `getWriteSet()` / `getReadSet()`.

Read-set tracking is **off by default**, so a transaction that only reads does
not retain the read **values**. Memory can still grow slightly: a pessimistic
transaction records one read timestamp per distinct key read
(`readTsByKey`) until that key is written or the transaction ends.
`get()` and `batchGet()` populate the read-set map only when
`trackReadSet` is true; `scan()` never populates it, so a scan-heavy
transaction is not covered by the read-set limits at all (scans are bounded by
`Transaction::MAX_SCAN_LIMIT` instead). For a long-lived worker that opts in and
performs many large reads, set `maxReadSetEntries` and/or
`maxReadSetTotalSize`, or call `Transaction::clearReadSet()` after a batch of
reads, instead of allowing the transaction to grow until `memory_limit` is
exhausted.

Batching is always on. `TwoPhaseCommitter` splits each region's prewrite
mutations, pessimistic-lock keys and commit keys with
`RawKvSplitter::splitIntoBatches()` into requests bounded by both
`maxTxnBatchSize` and `maxTxnBatchCount` **in payload terms**: key plus value
bytes for a prewrite mutation, key bytes for a pessimistic-lock mutation or a
commit key, and item count. The bound does not cover request framing,
`Context`, `primary_lock` or the async `secondaries` list, so an async primary
prewrite can exceed `maxTxnBatchSize`; a single mutation larger than the budget
still forms its own batch. The primary key's batch is still committed first;
the remaining batches — including other batches of the primary's region — are
secondary commits and are fanned out in parallel. Pessimistic-lock requests
use the same split, and `is_first_lock` is set only on the batch whose
mutations contain the primary key, not on the first region batch (issue #225).
One-phase commit remains limited to a single batch (a transaction split across
several 1PC prewrites would be partially committed), and async commit carries
its decision on the batch that holds the primary key. A batch that fails before
the commit point still fails the whole transaction; after the primary commit,
secondary-batch failures are logged and the transaction stays committed,
exactly as for whole secondary regions.

Batching bounds each **request**, not the prewrite phase. The secondary
batches of all regions are dispatched in parallel, and the prewrite heartbeat
is checked only after the whole fan-out returns, so a smaller batch reduces
the duration of one `KvPrewrite` but does not add a heartbeat opportunity
between batches. If a single request can take longer than the lock TTL, lower
`writeTimeoutMs` or raise the lock TTL as well — see
[A single prewrite or commit RPC can outlive the lock TTL](#a-single-prewrite-or-commit-rpc-can-outlive-the-lock-ttl).

## Transaction Priority (TxnKV)

`TxnKvClient::begin()` accepts a `priority` option. It maps onto TiKV's
`kvrpcpb.CommandPri` enum and is carried on the `Context` of every read and
write request the transaction sends — its reads (`get`, `batchGet`, `scan`)
and its prewrite, pessimistic-lock, commit, heartbeat and rollback RPCs — so
TiKV's scheduler can separate the transaction from other traffic. The
lock-resolution RPCs (`KvCheckTxnStatus`, `KvCheckSecondaryLocks`,
`KvResolveLock`) are always sent with the Normal priority: they resolve
locks of *other* transactions, not work of this one.

```php
$txn = $client->begin([
    'priority' => 2, // 0 = Normal (default), 1 = Low, 2 = High
]);
```

| Value | `CommandPri` | Effect                                                       |
|-------|--------------|--------------------------------------------------------------|
| `0`   | `Normal`     | Default. The proto3 default, so the field is not sent at all. |
| `1`   | `Low`        | The requests are scheduled with a low priority.               |
| `2`   | `High`       | The requests are scheduled with a high priority.              |

The priority reaches the **read** requests (`KvGet`, `KvBatchGet`,
`KvScan`) as well as the **write** requests (pessimistic lock, prewrite,
commit, transaction heartbeat and rollback). A transaction without the
option sends exactly the same bytes as before — `Normal` is the proto3
default and is not serialized.

Typical use: a batch job that must not compete with the online traffic
begins its transactions with `'priority' => 1`, while the latency-sensitive
path uses the default or `'priority' => 2`.

The value is forwarded to TiKV unchanged — it is not validated, so use one
of the three values above.

## Resource Control (Resource Groups)

TiKV clusters can be split into **resource groups** through PD's resource
manager: each group has its own Request Unit quota, and TiKV enforces it only
for requests that carry the group name. The client participates with two
option keys, on both `RawKvClient::create()` / `TxnKvClient::create()` and —
for TxnKV — per transaction in `begin()`:

```php
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

$raw = RawKvClient::create(['127.0.0.1:2379'], options: [
    'resourceGroup' => 'tenant-a',
    'resourceGroupTag' => 'nightly-job',
]);

$txn = TxnKvClient::create(['127.0.0.1:2379'], options: [
    'resourceGroup' => 'tenant-a',
])->begin([
    'resourceGroup' => 'tenant-a-batch', // overrides the client default
    'resourceGroupTag' => 'import-2026-11-06',
]);
```

| Option | Wire field | Meaning |
|---|---|---|
| `resourceGroup` | `Context.resource_control_context.resource_group_name` | The resource group the request belongs to. 1–64 characters: letters, digits, `_` and `-`. |
| `resourceGroupTag` | `Context.resource_group_tag` | Opaque bytes for query attribution (for example a job or statement identifier). Must be non-empty when set. |

Both keys are optional. When they are unset, neither field is written to the
`Context`, so the serialized request is byte-identical to a request from
before this feature existed. Only the fields that are configured are set;
`begin()` overrides each key independently, so a transaction can change its
group while still inheriting the client-wide tag (and vice versa).

The fields reach **every** RPC built by the client: RawKV reads, writes, CAS,
batch operations, scans, range deletes/checksums and SST ingest; TxnKV reads,
prewrite, pessimistic locks, commit, heartbeat, rollback and lock resolution.
This gives attribution and server-side quota enforcement. It does **not**
perform client-side Request Unit accounting: the client does not read
`ExecDetailsV2`/`RUV2` from responses and does not feed a local token bucket,
so flow control remains entirely server-side (a larger follow-on, not part of
the current option).

Group names are validated locally against PD's naming rule; the tag is
forwarded as opaque bytes. The client does not verify that the group exists:
the cluster decides what happens to an unknown name — TiKV normally falls
back to the default/uncontrolled behaviour rather than returning an error.

`options['requestContext']` (below) can also carry `resourceGroupTag` and
`resourceGroupName`. The two option sets are independent and both reach the
wire: `resourceGroup` / `resourceGroupTag` are applied first, and a
non-default value of the same field in `requestContext` wins when both are
set. A client that uses only `resourceGroup` / `resourceGroupTag` keeps the
behaviour described above.

## Request Context Controls (issue #426, GAP-12)

`options['requestContext']` on `RawKvClient::create()` and
`TxnKvClient::create()` takes a
`CrazyGoat\TiKV\Client\Region\RequestContextOptions` object. It populates the
optional control fields of `kvrpcpb.Context` on every request the client
sends. The object is immutable and has no mutable setters: build a new one to
change a value.

```php
use CrazyGoat\Proto\Kvrpcpb\IsolationLevel;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;

$client = RawKvClient::create(['127.0.0.1:2379'], options: [
    'requestContext' => new RequestContextOptions(
        isolationLevel: IsolationLevel::SI,
        notFillCache: true,                       // full scan must not evict the block cache
        maxExecutionDurationMs: 2000,             // TiKV stops working on an abandoned request
        requestSource: 'nightly-export',          // load attribution in TiKV metrics
        resourceGroupTag: 'analytics',
        busyThresholdMs: 100,
        resourceGroupName: 'batch',
    ),
]);
```

| Field | `Context` field | Default | Effect |
|---|---|---|---|
| `isolationLevel` | `isolation_level` | `IsolationLevel::SI` (0) | TxnKV read isolation: `SI`, `RC` or `RCCheckTS`. |
| `notFillCache` | `not_fill_cache` | `false` | Keeps the request out of TiKV's block cache, so a scan does not evict the working set. |
| `maxExecutionDurationMs` | `max_execution_duration_ms` | `0` (unset) | Server-side execution bound in milliseconds. It is **not** derived from `timeout`: a client-side deadline still leaves TiKV working on an abandoned request, so set it explicitly. Must be between 0 and 3 600 000. |
| `requestSource` | `request_source` | `''` (unset) | Label TiKV records against the request. `RequestContextOptions::DEFAULT_REQUEST_SOURCE` (`'tikv-php'`) is provided for attribution but is **opt-in**: applying it by default would change the bytes of every request. |
| `resourceGroupTag` | `resource_group_tag` | `''` (unset) | Tag read by the resource manager. |
| `busyThresholdMs` | `busy_threshold_ms` | `0` (unset) | Server-side busy threshold in milliseconds. Must be between 0 and 3 600 000. |
| `resourceGroupName` | `resource_control_context.resource_group_name` | `''` (unset) | Resource manager group name. |

Every field defaults to its proto3 default and is written only when it differs
from that default, so a client without `requestContext` sends exactly the same
bytes as before. The values are validated when the object is constructed; an
unknown isolation level or an out-of-range duration throws
`InvalidArgumentException`.

**Transaction-level override (TxnKV).** `TxnKvClient::begin()` accepts a
`requestContext` option that replaces the client-level object for that
transaction only. It must be a `RequestContextOptions` instance or `null`;
`null` disables the client-level controls for the transaction. The
transaction-level value wins over the client-level one (it is a replacement,
not a field-level merge).

```php
$txn = $txnClient->begin([
    'requestContext' => new RequestContextOptions(notFillCache: true),
]);
```

The controls reach both clients' reads and writes: RawKV `RawGet`,
`RawPut`, `RawDelete`, `RawBatchGet`, `RawBatchPut`, `RawBatchDelete`,
`RawScan`, `RawDeleteRange`, `RawChecksum`, `RawCAS`, `RawGetKeyTTL`,
`Ingest`; TxnKV `KvGet`, `KvBatchGet`, `KvScan`, pessimistic lock, prewrite,
commit, heartbeat, rollback and the lock-resolution RPCs. `priority` is not
part of this object: it keeps its own transaction option (see
[Transaction Priority](#transaction-priority-txnkv)).

## Retry and Backoff

### Automatic Retry

The client automatically retries failed operations:

```php
// Default retry configuration (built-in)
$client = RawKvClient::create(['127.0.0.1:2379']);
// - Max backoff: 20 seconds (non-ServerBusy errors, cumulative)
// - Server busy budget: 60 seconds (cumulative ServerBusy sleep)
// - Retry deadline: 30 seconds (checked before each retry attempt AND before
//   each backoff sleep, which is clamped to the remaining budget; the TxnKV
//   data-path RPCs and the PD lookups of lock resolution are also capped by
//   the remaining budget, so they cannot outlive it)

// Custom retry deadline (issue #294) — bounds the blocking backoff loop so a
// sustained ServerIsBusy episode cannot pin a PHP-FPM worker for minutes:
$client = RawKvClient::create(['127.0.0.1:2379'], [
    'retryDeadlineMs' => 5000, // 5 s wall-clock bound per operation; 0 disables
]);
```

> **Occupancy note:** the deadline is checked before every attempt AND
> before every backoff sleep (which is clamped to the remaining budget), so
> sleeping cannot overshoot it. The TxnKV data-path RPCs (`KvGet`,
> `KvBatchGet`, `KvScan`, `KvPrewrite`, `KvCommit`, `KvTxnHeartBeat`,
> `KvPessimisticLock` and the rollback calls) are also given a deadline of
> `min(configured timeout, remaining budget)`, so one slow attempt cannot
> outlive the operation either (issue #257). A path that does not expose a
> live operation budget to its caller — a RawKV point/batch call, or the PD
> failover/discovery step inside a metadata call — remains bounded by its
> configured timeout.

> **Lock resolution (TxnKV):** a `KeyError.locked` conflict during prewrite,
> rollback or a transactional read is resolved inside the retry loop, so its
> `KvCheckTxnStatus` (and the PD TSO fetch it performs),
> `KvCheckSecondaryLocks` and `KvResolveLock` calls are capped by the
> operation's **remaining** retry deadline. The resolver converts that
> remaining budget into one absolute deadline at its entry, so the nested
> RPCs share the same live budget and none is given a deadline beyond the
> operation's `retryDeadlineMs`. Each PD `GetRegion`/`ScanRegions` lookup
> the resolution performs is capped by the remaining budget at the time of
> the attempt (issue #257), so a single lookup cannot outlive it. The cap is
> recomputed from the same relative value for each `ScanRegions` page and
> each PD failover endpoint, so a multi-page scan or a failover can consume
> the remaining budget once per page/endpoint; the resolution as a whole is
> bounded by the deadline plus one configured `pdTimeoutMs` per extra page
> or endpoint. A live lock is still reported as a retryable
> error (the executor owns the backoff) and the resolver never sleeps for the
> lock TTL; the executor's own deadline check raises
> `RetryBudgetExhaustedException` when the budget is spent. With
> `retryDeadlineMs => 0` the configured `lockResolveTimeoutMs` / `tsoTimeoutMs`
> apply unchanged.

> **Worker occupancy note:** `serverBusyBudgetMs` is a worker-occupancy
> setting — it caps how long ONE operation may block a PHP process in
> `usleep()` while TiKV reports `ServerIsBusy`. It is **not** the binding
> bound under the defaults: the 30 s deadline is reached first, because the
> budget sits far enough above it (see
> [ServerBusy Budget vs Retry Deadline](#serverbusy-budget-vs-retry-deadline)).
> The default of 60 seconds suits request-driven runtimes such as PHP-FPM.
> Long-running CLI workers that prefer to wait out long overload episodes
> must raise `serverBusyBudgetMs` **as well**: raising or disabling only
> `retryDeadlineMs` still caps the wait at the 60 s budget. Keep the
> deadline above the new budget (or disable it with
> `'retryDeadlineMs' => 0`) so the deadline does not bind first.

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

`ErrorClassifier` maps each retryable error to a real `BackoffType` case
(`src/Client/Retry/BackoffType.php`). The case name is what the retry log
records under `backoffType` and what `MetricsInterface::retryAttempted()`
receives, so a metrics adapter can label retries by backoff type (the
Prometheus example in [Metrics Collection](advanced.md#metrics-collection) uses
`retries_total{backoff_type}`).

For attempt *n*, `BackoffType::sleepMs()` computes
`min(cap, base × 2^n)` and then applies **equal jitter**: the actual sleep is
uniformly random in `[delay/2, delay]` (issue #242). Every case is jittered;
`None` is the only case without a sleep. The ranges below are the exponential
envelope before jitter, in milliseconds.

| `BackoffType` | Base → cap (ms) | Jitter | Retried error / condition |
|---------------|-----------------|--------|---------------------------|
| `None` | 0 (no sleep) | none (no sleep) | `DataIsNotReady` — replica lag on a follower or stale read |
| `EpochNotMatch` | 2 → 500 | equal | `EpochNotMatch` — stale region epoch |
| `ServerBusy` | 2000 → 10000 | equal | `ServerIsBusy`; gRPC `RESOURCE_EXHAUSTED`. Charged to the separate 60 s ServerBusy budget |
| `StaleCmd` | 2 → 1000 | equal | `StaleCommand` |
| `RegionMiss` | 2 → 500 | equal | `RegionNotFound`, `StoreNotMatch`, `MismatchPeerId`, `BucketVersionNotMatch`, `UndeterminedResult`, an `errorpb.Error` variant the client does not know yet (bounded retry) |
| `TiKvRpc` | 100 → 2000 | equal | gRPC `UNAVAILABLE`, `ABORTED`, `INTERNAL`, `UNKNOWN`, `DEADLINE_EXCEEDED`; a store in health cooldown (`StoreUnavailableException`) |
| `NotLeader` | 2 → 500 | equal | `NotLeader` — leader transfer |
| `DiskFull` | 500 → 5000 | equal | `DiskFull` |
| `RegionNotInitialized` | 2 → 1000 | equal | `RegionNotInitialized` |
| `ReadIndexNotReady` | 2 → 500 | equal | `ReadIndexNotReady` |
| `ProposalInMergingMode` | 2 → 500 | equal | `ProposalInMergingMode` |
| `RecoveryInProgress` | 100 → 10000 | equal | `RecoveryInProgress` |
| `IsWitness` | 1000 → 10000 | equal | `IsWitness` |
| `MaxTimestampNotSynced` | 2 → 500 | equal | `MaxTimestampNotSynced` |
| `TxnLock` | 200 → 3000 | equal | A `KeyError.locked` conflict (or `TxnLockNotFound`) during a TxnKV prewrite, rollback or read; chosen by the lock-resolution paths, not by `ErrorClassifier` |

The mapping lives in `ErrorClassifier::classifyByKind()` (the typed `ErrorKind`
carried by `RegionException`), `ErrorClassifier::classifyGrpcStatus()` (gRPC
status codes) and the message-text fallback in
`ErrorClassifier::classifyToBackoffType()` for non-region exceptions without a
typed kind.
See [Retry Strategies](advanced.md#retry-strategies) for the retry budgets and
why an application-level retry loop on top of the client is counterproductive.

Region errors are classified from the typed `errorpb.Error` oneof only. Message text is never inspected on that path: a TiKV wording change, or a key embedded in the message, cannot reclassify an error. A synthetic `RegionException` that wraps a response-level error string or a top-level `KeyError` is not a region error and stays fatal.

### Non-Retryable Errors

These errors are not retried:

- `KeyNotInRegion` - Key outside region range
- `RaftEntryTooLarge` - Value too large
- `FlashbackInProgress` - Region in flashback mode
- `FlashbackNotPrepared` - Region not prepared for flashback
- Any other gRPC status code: `CANCELLED`, `INVALID_ARGUMENT`, `NOT_FOUND`,
  `PERMISSION_DENIED`, `UNAUTHENTICATED`, `UNIMPLEMENTED`,
  `FAILED_PRECONDITION`, `ALREADY_EXISTS`, `OUT_OF_RANGE`, `DATA_LOSS`, and
  any unrecognised code (fail closed)

### Retry Bounds

Every retry loop is bounded by **two independent safety nets** to prevent
infinite loops when the underlying error has zero backoff delay (the only
remaining zero-sleep class, `BackoffType::None`, is the `DataIsNotReady`
replica-lag signal — `EpochNotMatch` gets a small jittered 2–500 ms backoff
since issue #241):

| Bound | Default | Description |
|-------|---------|-------------|
| `maxAttempts` | `30` | Maximum number of times the operation is invoked before the executor gives up. |
| `deadlineMs` | `30000` (`RetryExecutor::DEFAULT_RETRY_DEADLINE_MS`; `0` disables) | Wall-clock deadline from the start of the operation. Checked before each attempt and before each backoff sleep (the sleep is clamped to the remaining budget); the executor terminates when it is reached, regardless of `sleepMs`. The TxnKV data-path RPCs and the PD lookups of lock resolution are capped by the same remaining budget (issue #257). |

When either bound is reached the executor throws
`RetryBudgetExhaustedException` (extends `TiKvException`). The two sleep
budgets (`maxBackoffMs` and `serverBusyBudgetMs`) throw the same class, so
every exhaustion path reports "the client stopped retrying" rather than the
underlying region or transport error. This exception exposes `attempts()` and
`elapsedOrBackoffMs()` for diagnostics, `getPrevious()` for the original
error, and `getRawKey()` for the un-redacted key (the key is redacted in the
message — see issue #269).

The time budgets (`maxBackoffMs`, `serverBusyBudgetMs` and `deadlineMs`) are
scoped to **one logical operation** (issue #223, TXN-18):
`RetryExecutor::beginOperation()` returns an executor whose `execute()`
calls share one budget, and the client's fan-out operations (`scan()`,
`batchGet()`, `commit()`, `deleteRange()`, `checksum()`, …) use one for all
their regions and pages. A later operation starts from zero, so a read-heavy
transaction cannot starve its commit, while a single operation's fan-out is
bounded by one budget instead of one per region. `maxAttempts` stays per
retry loop (one region/page), so a wide fan-out does not exhaust it on
ordinary one-retry-per-region traffic.

### ServerBusy Budget vs Retry Deadline

`RetryExecutor` enforces two independent stop conditions for a
`ServerIsBusy` storm: the cumulative ServerBusy sleep budget
(`serverBusyBudgetMs`, 60 s by default) and the wall-clock retry deadline
(`retryDeadlineMs`, 30 s by default). They do not have equal weight.

The deadline is checked before every attempt **and** before every backoff
sleep, and the sleep is clamped to the remaining deadline. A ServerBusy
backoff therefore cannot push an operation past its deadline. The budget,
however, is charged from the scheduled backoff **before** that clamp, so
the accumulator can exceed the wall clock by up to one maximum ServerBusy
sleep. With the defaults (30 s deadline, 60 s budget) the accumulated
charge is bounded by roughly `deadline + 10 s` ≈ 40 s, still below the
60 s budget: the deadline always binds first and the budget never fires —
raising it alone changes nothing.

The deadline is guaranteed to bind first only while the budget is at least
one maximum ServerBusy backoff (~10 s) above it, as with the defaults. When
the two are within ~10 s of each other, either can bind; when the budget is
below the deadline, the budget usually binds first. The budget check runs
before `usleep()`, so an over-budget charge throws without sleeping.

Both paths throw the same exception type, so a `catch` block handles them
identically; only the message names the binding bound, which matters when
reading logs:

| Bound reached first | Exception | `getPrevious()` |
|---|---|---|
| Retry deadline (the default) | `RetryBudgetExhaustedException` (`Retry deadline (...) exhausted`) | the original `ServerIsBusy` error |
| ServerBusy budget | `RetryBudgetExhaustedException` (`ServerBusy budget (...) exhausted`) | the original `ServerIsBusy` error |

A sustained `ServerIsBusy` episode therefore surfaces as
`RetryBudgetExhaustedException`, not as the raw region error, whichever of the
two bounds binds first; the `ServerIsBusy` error is always available through
`getPrevious()`. Both budgets are scoped to one logical operation
(issue #223), like the other time budgets.

### Custom Retry (Advanced)

`RawKvClient` is `final` and its retry loop cannot be customised by
subclassing — there is no `classifyError()` hook on the client and no
`BackoffType::Custom` case. The supported extension point is the optional
`$classifier` callable accepted by `RetryExecutor::execute()`
(`src/Client/Retry/RetryExecutor.php`): it receives the caught
`TiKvException` and returns a `RetryVerdict`
(`src/Client/Retry/RetryVerdict.php`):

- `RetryVerdict::retry($backoffType)` retries with that `BackoffType` (see
  `src/Client/Retry/BackoffType.php` for the full case list);
- `RetryVerdict::fatal()` stops immediately and runs the executor's fatal
  path, including region invalidation and the broken-channel cleanup;
- `RetryVerdict::pass()` falls through to the built-in classification, which
  itself always answers Retry or Fatal and never Pass.

The backoff taxonomy mirrors client-go's set: besides the region-error curves,
`BackoffType` exposes `PDRpc` (500–3000 ms), `TxnLockFast` (10–3000 ms), and
`TxnNotFound` and `CommitTSLag` (2–500 ms each), the equivalents of client-go's
`BoPDRPC`, `BoTxnLockFast`, `BoTxnNotFound` and `BoCommitTSLag`.

Note that a `NotLeader` error is classified before your classifier is
consulted.

To use it you must construct the `RetryExecutor` yourself and invoke your
operation through it, rather than going through `RawKvClient`'s methods:

```php
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;

$executor = new RetryExecutor(
    maxBackoffMs: 20000,
    serverBusyBudgetMs: 60000,
    regionCache: $regionCache,
    grpc: $grpc,
    regionResolver: $regionResolver,
    logger: $logger,
);

$result = $executor->execute(
    $key,
    function () use ($key): string {
        // your RawKV RPC call
        return $value;
    },
    classifier: function (TiKvException $e): RetryVerdict {
        if (str_contains($e->getMessage(), 'CustomError')) {
            return RetryVerdict::retry(BackoffType::StaleCmd); // pick a real BackoffType case
        }
        return RetryVerdict::pass(); // fall through to the built-in ErrorClassifier
    },
);
```

For a user-level fan-out that must share one budget, call
`$executor = $executor->beginOperation()` once and pass the returned
executor to every `execute()` call of that logical operation; call
`beginOperation()` again for the next operation. A standalone executor keeps
the per-call reset described above.

Note that `maxAttempts` (default 30) and `deadlineMs` (default 30000,
`0` disables) are constructor parameters of `RetryExecutor` and are **not**
settable through `RawKvClient::create()` options — `create()` only exposes
`retryDeadlineMs` (see the [Automatic Retry](#automatic-retry) section
above); `maxBackoffMs` and `serverBusyBudgetMs` are likewise constructor
arguments of `RawKvClient`, not `create()` options.

## Caching

### Region Cache

The client caches region metadata to avoid repeated PD queries:

```php
// Default: In-memory region cache (enabled automatically)
$client = RawKvClient::create(['127.0.0.1:2379']);
```

**Cache behavior**:
- Cache entries are invalidated on routing errors
- `NotLeader` errors update the cache with new leader info when the hinted
  peer is a voter and not a witness; a hint naming a learner or a witness
  invalidates the region instead, so the retry re-resolves it through PD
  (issue #258)
- `EpochNotMatch` errors refresh the affected regions from TiKV's
  `current_regions` payload when it is usable, so the retry needs no PD
  `GetRegion`; an empty, undecodable, leaderless, older-epoch or
  non-covering payload invalidates the region and falls back to PD
  (issue #246)
- `ServerIsBusy`, `DiskFull`, `IsWitness` and `RecoveryInProgress` keep the
  cached region — the request reached the right region, so the retry reuses it
- Both scan paths read the cache first and ask PD only for the uncovered
  ranges, including TxnKV's `Transaction::scan()` (issue #425) — see
  [Scan-path Region Cache Use](#scan-path-region-cache-use)

### Region Cache Refresh and Preload

PHP has no background threads, so the client starts no timer and no loop. Two
explicit methods cover the two use cases a long-lived worker has (issue #425):

```php
$client = RawKvClient::create(
    ['127.0.0.1:2379'],
    options: ['regionCacheRefreshMs' => 60000],
);

// At worker boot: resolve and cache the regions a job will touch, without
// serving a user read. Each pair is a half-open [startKey, endKey) range; ''
// as the end key means +infinity.
$client->preloadRegions([['user:1000', 'user:2000'], ['job:', 'job;']]);

// From the worker's own tick (the same place reapIdleChannels() is called):
$refreshed = $client->refreshRegionCache();
```

`refreshRegionCache(bool $force = false)` re-queries PD for a bounded slice
of the cached regions through `GetRegion` on each region's start key:

- a newer epoch or leader updates the entry in place and refreshes its TTL;
- a split or merge that produced another region ID stores the replacement and
  drops the superseded entry silently (a superseding update, not an
  error-driven invalidation — no `regionInvalidated()` metric);
- a PD answer that does not cover the entry's start key drops the entry, so
  the next request resolves it again.

A non-forced call performs at most one refresh per `regionCacheRefreshMs`
and returns `0` when the interval has not elapsed. The default is `0`
(disabled, so a plain refresh is a no-op), so refreshing is opt-in; `60000`
(one minute) is a reasonable tick interval, and the first call after client
creation always refreshes. `$force = true` performs a one-off refresh
regardless of the interval. `preloadRegions()` works independently of the
option.

One tick refreshes at most `RegionCacheWarmer::DEFAULT_MAX_REGIONS_PER_REFRESH`
(128) entries, round-robin: successive ticks continue where the previous one
stopped, so a large cache is fully refreshed over several ticks without any
single tick turning into thousands of sequential PD round trips. The first PD
failure aborts the tick (one `getRegion()` already walks every configured PD
endpoint, so retrying the remaining entries only repeats the failure); the
entries keep their cached values.

Both methods are non-throwing on a PD failure: the failure is logged at
warning level and the affected cache entries are kept as they were. A
malformed or reversed range is a programmer error and throws
`InvalidArgumentException` before any RPC. Both are no-ops after `close()`.
The interval is measured on a monotonic clock, so a wall-clock step cannot
delay a tick.

Refresh cost is one PD round trip per refreshed entry, bounded per tick as
above, so size `regionCacheRefreshMs` against the cache size. A short interval
on a worker with a large warm cache sends more PD traffic than the cache
saves.

### Scan-path Region Cache Use

RawKV and TxnKV scans read the region cache first:

- **RawKV forward** (`scan()`, `scanPrefix()`, `batchScan()` and the
  iterators) and **TxnKV** (`Transaction::scan()`) enumerate regions through
  the cache. A contiguous cached run is served locally; PD is asked only for
  the uncovered gap, starting at the gap's first key, and the answer is cached
  for the next page and the next scan.
- **RawKV `reverseScan()`** resolves regions from the upper bound downwards
  through `RegionResolver::getRegionBelow()`: the cache answers the common
  case, and a miss asks PD for the region containing the cursor or, when the
  cursor sits exactly on a region boundary, for the adjacent region below it.
  It does not use `RegionScanSource`, so its cache counters are labelled
  `region_resolution` (per lookup), not `scan`.
- A split or merge the client has not seen yet is still handled by the usual
  region-error retry (the stale entry is invalidated from `EpochNotMatch`,
  then the retry re-resolves the gap through PD).
- The forward scan path increments the same counters as point lookups,
  labelled `scan` (RawKV) and `txn_scan` (TxnKV), so `regionCacheHit()` /
  `regionCacheMiss()` cover it; `reverseScan()` reports
  `region_resolution` (see [Metrics and Observability](#metrics-and-observability)).

### Store Cache

Store addresses are also cached:

- Store information cached to avoid repeated PD queries
- Automatically refreshed on cache misses

### Cache Statistics

Monitor cache performance via logs. The region cache writes these records (PSR-3 message and context):

```
DEBUG  Region cache hit    {"key":"#3c469e9d6c5875d3 (5 bytes)","regionId":123}
DEBUG  Region cache miss   {"key":"#3c469e9d6c5875d3 (5 bytes)"}
INFO   Region invalidated  {"regionId":123}
```

The hit and miss records need a logger that handles `DEBUG`. The `key` field is always redacted — see [Log Redaction](#log-redaction). The invalidation record has no `reason` field.

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
        'checksumTimeoutMs' => 30000,    // default: 30000
        'ingestTimeoutMs' => 60000,      // default: 60000 (SST ingest)
        'batchDeadlineMs' => 60000,      // default: 60000 — wall-clock deadline
                                         // for batch fan-out dispatch+wait across all
                                         // regions; exceeding it throws
                                         // BatchDeadlineExceededException after
                                         // cancelling in-flight futures.
                                         // 0 disables it. Must be >= every per-call
                                         // timeout used inside a batch fan-out
                                         // (not readTimeoutMs)
        'pdTimeoutMs' => 3000,           // default: 3000 (PD metadata RPCs)
        'tsoTimeoutMs' => 3000,          // default: 3000 (PD TSO)
        'lockResolveTimeoutMs' => 5000,  // default: 5000 (lock resolution)
    ],
];

$client = RawKvClient::create(
    pdEndpoints: ['127.0.0.1:2379'],
    options: $options
);
```

`checksumTimeoutMs` sizes `checksum()`, which reads a whole key range and is the operation most likely to need a longer deadline. `ingestTimeoutMs` sizes the SST ingest RPCs (the `SwitchMode` fan-out, the `RawWrite` stream and `Ingest`). Both are read from `options['timeout']` like every other key, so neither is a fixed deadline.

The last three are **metadata-plane** deadlines and are configured separately from the
store-RPC ones above because they address different failure modes: a hung PD (a
blackholed connection, a PD process that accepts but never answers) takes down every
region lookup and every transaction begin at once, while a slow store is a
per-region problem the retry executor already handles. `pdTimeoutMs` covers
`GetRegion` / `ScanRegions` / `GetStore` / `GetAllStores` / `GetMembers` /
`LoadKeyspace` and the GC safe-point RPCs; `tsoTimeoutMs` covers every `Tso` RPC
(so also the timestamp `checkTxnStatus()` fetches); `lockResolveTimeoutMs` covers
`KvCheckTxnStatus` / `KvCheckSecondaryLocks` / `KvResolveLock`. When lock
resolution runs inside a retry loop, both the TSO and the lock-resolution
deadline are further capped by that loop's remaining `retryDeadlineMs`; the
configured value is the upper bound, not an independent budget.

Every gRPC call carries a deadline. `GrpcClientInterface::call()`'s `$timeoutMs`
argument of `null` means the library default (30 s); a non-positive value is a
configuration error and throws `InvalidArgumentException` — see [Deadlines are
never infinite](#deadlines-are-never-infinite).

When a timeout is exceeded, the gRPC call throws `GrpcException` which is caught and retried by the retry executor unless the budget is exhausted.

### Default Timeouts

By default all timeouts are set to sensible values (5s for reads/writes, 10s for batch, 20s for scans, 30s for delete-range and checksum, 60s for SST ingest, 3s for PD and TSO, 5s for lock resolution). Every timeout except `batchDeadlineMs` must be greater than `0`: a `0` (or a negative value) is rejected when the `TimeoutConfig` is built, and again when it is passed through `options['timeout']` — the error names the offending key. Use a large finite value instead of disabling a deadline. `batchDeadlineMs` is the one exception: `0` disables the overall batch deadline.

`batchDeadlineMs` is finite by default (`TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS`, 60000 ms). When it is above `0`, it must not be less than any per-call timeout that a batch fan-out uses: `writeTimeoutMs`, `batchReadTimeoutMs`, `batchWriteTimeoutMs`, `scanTimeoutMs`, `deleteRangeTimeoutMs`, `checksumTimeoutMs` and `ingestTimeoutMs` (`readTimeoutMs` is not in the list: only point reads use it). If it is less, the `TimeoutConfig` constructor throws an `InvalidArgumentException` that names both fields. One wait is not interrupted by the deadline, so a deadline that is less than one call cannot be honoured. Such a setting has no meaning.

A batch that answers within a positive `batchDeadlineMs` is unaffected: the deadline is a ceiling, not a minimum. The batch deadline is checked at dispatch boundaries and at wait boundaries. It is not checked during one blocking wait. One wait on a retryable call runs that region's share of the operation's retry budget; all regions of a batch share one `RetryExecutor` budget (issue #223), so `retryDeadlineMs` bounds the fan-out as a whole (up to `RetryExecutor::DEFAULT_RETRY_DEADLINE_MS`, 30000 ms, plus one per-call timeout). The worst case is therefore `batchDeadlineMs` plus the operation's remaining retry deadline. For scans the batch deadline applies per region batch (up to `RegionScanBatch::MAX_REGIONS` regions), not per `scan()` call. One deadline covers all windows of a batch: a batch with many windows cannot run for the deadline times the number of windows.

Every key of the table above is configurable under its own name — the mapping is
derived from `TimeoutConfig`'s constructor, so a field that is added to the
class is configurable the moment it exists. A key the client does not know is
reported instead of silently dropped: `options['timeout'] => ['ingestTimeout' => 300000]`
(a missing `Ms`) throws `InvalidArgumentException` naming the unknown key
rather than quietly applying the 60-second default. A value of the wrong type
(`'readTimeoutMs' => '60000'`) is rejected the same way, and the group itself
must be an array — unless it is absent or explicitly `null`, which both mean
"not configured". The same holds for `options['slowLog']`.

### Deadlines are never infinite

A gRPC call created with an infinite deadline does not expire, and PHP cannot
interrupt one that is already blocking: `Grpc\Call::startBatch()` is a synchronous
call into the gRPC C core, where neither `max_execution_time` nor
`set_time_limit()` applies. A worker blocked on a half-open connection therefore
stays blocked until the kernel tears the connection down — minutes, or never — and
in PHP-FPM that exhausts the whole pool, taking down requests that never touch
TiKV. Long-running workers (Swoole, RoadRunner, queue consumers) hang instead, and
their jobs are never acknowledged.

So `GrpcClient` never builds an unbounded deadline implicitly. The `$timeoutMs`
argument of `call()`, `callAsync()` and `callStreaming()` reads as:

| `$timeoutMs` | Meaning                                                      |
|--------------|--------------------------------------------------------------|
| positive int | that many milliseconds from now                              |
| `null`       | the library default — `GrpcClient::DEFAULT_TIMEOUT_MS` (30 s)  |
| `0` or less  | a configuration error — `InvalidArgumentException` is thrown   |

A caller that omits the argument therefore gets a bounded call, and there is no
way to ask for an unbounded one. `0` used to be the explicit opt-out, but it is
also what an unset environment variable coerces to — `(int) getenv('TIKV_READ_TIMEOUT_MS')`
yields `0` when the variable is missing — so the opt-out turned a configuration
mistake into a call that never expires. Prefer a large finite value: an
unbounded call is a resource that a hung peer can pin indefinitely, in a process
that has no way to give the slot back.

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

Batch methods already send their sub-batches in parallel (default concurrency 16, `RawKvClient::DEFAULT_MAX_CONCURRENCY`). When you set the `batchDeadlineMs` timeout option to a value above 0, it bounds the whole fan-out. The default is `60000` ms, and `0` disables it. The deadline is checked between waits, so one slow region can run up to its whole retry loop past it (see [Timeouts](#timeouts)).

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
    // With options['expectedClusterId'] set, a response without a cluster-id
    // header is an error (wrapped in HealthCheckException), not null.
} catch (\CrazyGoat\TiKV\Client\Exception\HealthCheckException $e) {
    // PD was unreachable, returned a non-OK gRPC status, or answered with a
    // PD-level error in the response header (issue #234). The underlying
    // cause is available through $e->getPrevious() — a
    // \CrazyGoat\TiKV\Client\Exception\PdException there names the failing
    // RPC and carries getErrorType() (NOT_BOOTSTRAPPED, INVALID_VALUE, …).
}

// Throws ClientClosedException when called after $client->close().
```

Internally, `healthCheck()` issues the lightweight `GetMembers` RPC over
the same gRPC channel used for region lookups, so it exercises the full
network path without writing or reading user keys. Because `GetMembers` is
the RPC that reports the PD leader, a successful probe also **records the
current leader**: the client points its subsequent PD and TSO RPCs at it
(issue #234), which is why a probe is worth issuing after a PD topology
change rather than only as an uptime check.

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
| `regionCacheHit()`             | `'region_resolution'`, `'scan'`, `'txn_scan'` | Region was found in the cache. `region_resolution` is the per-lookup label (point reads and `reverseScan()`'s top-down resolver); the scan labels are emitted once per cache-served run of regions |
| `regionCacheMiss()`            | `'region_resolution'`, `'scan'`, `'txn_scan'` | Region was not in the cache, had to query PD. `region_resolution` is the per-lookup label; the scan labels are emitted once per `ScanRegions` call |
| `regionInvalidated()`          | `'region_error:epoch_not_match'`, `'region_error'`, `'not_leader'`, `'retry_region_error:epoch_not_match'`, `'retry_region_error'`, `'fatal_region_error'`, `'lock_resolve'`, `'refresh'` | A region was actually removed from the cache — exactly once per actual drop, emitted from `RegionCache::invalidate()` with the caller's reason: top-level non-NotLeader **routing** region error handled by `RegionErrorHandler::check()`, tagged with its kind (`region_error:<kind>`; plain `region_error` for an unclassified error), NotLeader handling in the retry loop (hint peer unknown / no hint), pre-retry invalidation on a retryable **routing** error tagged with its kind (`retry_region_error:<kind>`), pre-retry invalidation on an unclassified retryable error (`retry_region_error`), a fatal routing error (e.g. `KeyNotInRegion`) dropped before the exception is rethrown (#233), post-resolve cleanup in `LockResolver`, or a `refreshRegionCache()` tick that found PD no longer maps the entry's start key to its cached range (a refresh that supersedes the entry with a newer answer is a `put()` and emits nothing). Both `RegionErrorHandler::check()` and the retry executor keep the cached region for non-routing errors (`ServerIsBusy`, `DiskFull`, `IsWitness`, `RecoveryInProgress`, …) and emit nothing for them. A successful `EpochNotMatch` `current_regions` refresh by the retry executor is a superseding `put()` and emits no `regionInvalidated()`; `RegionErrorHandler::check()`'s earlier drop at TxnKV call sites keeps its own `region_error:epoch_not_match`, and the executor's empty/unusable-payload fallback keeps `retry_region_error:epoch_not_match` only where the stale entry is still cached (e.g. RawKV; at TxnKV sites `check()` already dropped it) (issue #246). Invalidating an ID that is not cached emits nothing. |

For calls sent through `GrpcClient::callAsync()` (batch fan-outs, scans, range operations, SST ingest, prewrite/commit fan-outs), `rpcCompleted()` fires when the future settles: on `wait()`, on a failure, on `cancel()`, or from `__destruct`. The duration covers the time from dispatch to settle, including the time before `wait()` is called (for example queueing in a windowed fan-out).

Reasons are mutually exclusive per drop; the current source emits
`'region_error:<kind>'`, `'region_error'` (unclassified handler error),
`'not_leader'`, `'retry_region_error:<kind>'`, `'retry_region_error'`
(unclassified retryable error), `'fatal_region_error'`, and `'lock_resolve'`
when those paths actually remove a region. A NotLeader response whose hinted
peer is still valid — a voter that is not a witness — only switches the
cached leader and emits nothing; a hint naming an unknown peer, a learner or
a witness is unusable and drops the region with `'not_leader'`, and a
non-routing error keeps the cached region on both the handler and the retry
executor paths.
At executor-owned call sites, NotLeader drops are owned exclusively by the
retry loop's leader handling: `RegionErrorHandler::check()` leaves the region
cached so it can switch to a valid hint. Call sites without a surrounding
retry executor pass `notLeaderOwnedByRetryExecutor: false` and self-invalidate
with the same `'not_leader'` reason. `LockResolver` takes the flag from its
caller and threads it through every region-error check it performs, including
the async-commit finalization (`resolveAsyncCommitLocks()`, issue #568).

`InMemoryMetrics` ships the same counters in-process, suitable for tests
and benchmarks — see `getRpcStarted()`, `getRpcSucceeded()`, `getRetries()`,
`getCacheHits()`, `getInvalidations()`, `getMeanLatencyMs()`, and `reset()`.

Inspect live counters at any time:

```php
$metrics = $client->getMetrics();     // returns whatever was injected
```

### Connection Pooling

The client maintains persistent gRPC channels:

- Channel handles are cached and reused for the life of the client; ext-grpc's
  persistent table can also reuse the underlying connection across PHP-FPM
  requests served by the same worker
- Connection management is automatic: a channel in a failed state is replaced
  on the next request
- The defaults need no configuration

The pool caches 64 channels by default — the `maxChannels` cap of `GrpcClient`.
The cap is **soft**: eviction never closes a channel that still carries calls,
because closing it would destroy those calls. When every cached channel is
busy — a batch that fans out over more stores than the cap holds — the pool
grows past the cap while the calls are in flight, and the overshoot is logged
at warning level:

```
Channel cache at capacity but all channels are busy; exceeding soft cap
```

That warning is the signal that the workload touches more stores than the cap
allows. The pool does not shrink back on its own when the calls settle: on a
later cache miss the least-recently-used idle channels are evicted down to
the cap, and a channel that stays unused is dropped after the idle TTL
(10 minutes by default). `maxChannels` and `idleTtlMs` are `create()`
options; the defaults are 64 and 600000 ms — see
[Channel Pool](#channel-pool-maxchannels-idlettlms) and
[Connection lifecycle and process model](operations.md#connection-lifecycle-and-process-model).

### Resource Limits

Consider PHP's resource limits:

```ini
; php.ini
memory_limit = 256M
max_execution_time = 300
```

For long-running processes (workers, daemons), create the client once and
reuse it for the life of the process. Closing and recreating it throws away
the region cache and the channel handles. Reap idle channel handles from the
worker tick instead:

```php
$client = RawKvClient::create($pdEndpoints, logger: $logger);

while (true) {
    // Process work

    // Drop channel handles that have been idle longer than idleTtlMs
    if ($iteration % 1000 === 0) {
        $client->reapIdleChannels();
    }
}
```

See [Connection lifecycle and process model](operations.md#connection-lifecycle-and-process-model)
for the cache defaults and the PHP-FPM guidance.

## Configuration Checklist

Before deploying to production:

- [ ] PD endpoints are correct and accessible
- [ ] Store address policy reviewed: default PD-derived policy, explicit `allowedStoreHosts`/`storeHostPolicy`, or TLS with hostname verification enforced
- [ ] TLS certificates are configured (if required)
- [ ] `allowInsecure` is `false` in production, so a broken TLS configuration fails instead of downgrading to plaintext (see [Security: Fail-Closed TLS](#security-fail-closed-tls))
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
