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
10. [Retry and Backoff](#retry-and-backoff)
11. [Caching](#caching)
12. [Timeouts](#timeouts)
13. [Production Configuration](#production-configuration)

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
    // Row cap for an unbounded scan (scan()/scanPrefix()/reverseScan() with
    // limit: 0, issue #191). limit: 0 returns the whole range by paging
    // internally (up to RawKvClient::MAX_SCAN_LIMIT = 10240 rows per RPC), so
    // the whole result is buffered in one PHP array. The guard is evaluated
    // once per fetched page (after that page is buffered), so it is
    // page-granular rather than a strict per-row memory bound: when it is
    // smaller than the page size a whole page is read before the throw, and
    // ScanLimitExceededException's getScannedRows() can exceed getMaxRows().
    // Bounded (limit > 0) scans and the lazy
    // scanIterator()/scanPrefixIterator() are unaffected. Default: 100000.
    // Must be >= 1.
    'maxScanRows' => 100000,
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
    // Experimental BatchCommands stream multiplexing for the RawKV batch
    // fan-outs (issue #418). Must be a boolean, default false. See
    // "Batch Commands Multiplexing" below.
    'batchCommands' => false,
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
each other's caches.

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
// The store cache is the fourth argument of PdClient: without it every
// store address is looked up in PD again on every request.
$storeCache = new StoreCache(logger: $logger);
$pdClient = new PdClient($grpc, 'tikv.example.com:2379', $logger, $storeCache);
// The client closes the transport it was given in close() — pass
// ownsGrpcClient: false to keep the ownership with the application.
$client = new RawKvClient($pdClient, $grpc);
```

> **Note:** the manual path above is for the rare case where the client has
> to be assembled by hand. `RawKvClient::create()` /
> `TxnKvClient::create()` build the transport, the store cache and the PD
> client for you, and accept the same TLS options through
> `options['tls']` — see [Basic Configuration](#basic-configuration).

**Who closes the transport on the manual path.** The rule of
[Sharing One Transport Between Clients](#sharing-one-transport-between-clients)
is "the transport belongs to whoever created it", and it holds for the factory
path. A client assembled by hand is *given* a transport it did not create and
still closes it: `close()` (and the destructor) closes `$grpc`, exactly like a
client built by `create()` closes the transport it built. If the application
keeps the transport — to share it with a second client, or to close it later —
say so explicitly:

```php
$client = new RawKvClient($pdClient, $grpc, ownsGrpcClient: false);
```

With `ownsGrpcClient: false` the client never closes the transport and the
application stays its owner. The flag defaults to `true` today; issue #204
tracks the change (a client should close only what it owns), so pass the flag
explicitly whenever the ownership matters. `TxnKvClient` takes the same
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

### What Gets Logged

The client logs:

- **DEBUG**: Connection attempts, cache operations, request details
- **INFO**: Successful operations, region cache hits/misses
- **WARNING**: Retry attempts, region invalidations, slow operations (only when the `slowLog` option is enabled — see [Slow Operation Logging](#slow-operation-logging))
- **ERROR**: Failed operations, exhausted retries, fatal errors

Keys in the log details are redacted — see
[Log Redaction](#log-redaction) for the exact format.

### Log Redaction

The client never writes a raw key it holds into the log context: every key
it formats for a log line or an exception message passes through
[`KeyRedactor`](../src/Client/Util/KeyRedactor.php), which replaces it with
the first `min(8, length)` bytes as lowercase hex plus the total length of
the key:

| Key | Logged as |
|---|---|
| `key_a` (5 bytes) | `"6b65795f61" (5 bytes)` |
| `user:session` (12 bytes) | `"757365723a736573... (12 bytes)` |
| empty key | `"" (0 bytes)` |

That is the exact output of the code, not a paraphrase of it. A key of 8
bytes or fewer is hex-encoded in full and quoted on both sides; a longer key
is quoted only at the front, and its 16-character hex prefix is followed by
`...` **without a closing quote**. The byte count is the length of the whole
key, not of the prefix. The shape is pinned by the unit tests — do not
"repair" the missing quote when you copy these strings, and do not quote the
redacted value a second time in a message template (pass it to an unquoted
`%s`; see [`helpers/faq.md`](helpers/faq.md)).

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
key only the server ever saw is not (tracked in issue #307).

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
`LockWaitTimeoutException`) and the API V2 keyspace check
(`KeyOutOfBoundsException`) therefore name the redacted key. When your own
code needs the raw key, read it from the typed accessor instead of parsing
it out of a message — `RetryBudgetExhaustedException::getRawKey()` and
`LockWaitTimeoutException::getKey()`.

#### Installing a stricter redactor

`KeyRedactor::setRedactor()` replaces the default redactor:

```php
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

KeyRedactor::setRedactor(
    fn (string $key): string => 'sha256:' . substr(hash('sha256', $key), 0, 16)
);
```

The callable receives the raw key and returns the string that is logged in
its place. Install a hash when an 8-byte prefix is still sensitive in your
key scheme — the prefix alone can leak a tenant identifier. Pass `null` to
restore the default:

```php
KeyRedactor::setRedactor(null);
```

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
a configuration error and throws `InvalidArgumentException`. Omitting the whole
`slowLog` key disables slow logging entirely.

| Option key               | Default | Operations covered                                                                    |
|--------------------------|---------|---------------------------------------------------------------------------------------|
| `readThresholdMs`        | 100     | `get()`, `getKeyTTL()`                                                                 |
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
| `key`          | The redacted key (`KeyRedactor::redact()`) — the first 8 bytes in hex plus the total length, with an ellipsis after the hex when the key is longer than 8 bytes, e.g. `"7573657231" (5 bytes)` for a short key and `"757365725f656d61... (28 bytes)` for a longer one. It is the key the operation is anchored on: the key itself for a single-key read or write, the **first key of the batch** for a batch operation, and the key the range starts at for a scan or range operation (the exclusive upper bound for a reverse scan, the start of the per-region sub-range when the range spans regions). |
| `duration_ms`  | The measured duration in milliseconds, rounded to two decimals.                                                                                               |
| `threshold_ms` | The threshold that was exceeded, in milliseconds.                                                                                                            |

An emitted line therefore looks like this (the prefix depends on the logger's
formatter):

```
WARNING  Slow TiKV operation  {"operation":"scan","key":"\"7573657231\" (5 bytes)","duration_ms":1420.5,"threshold_ms":1000}
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
  re-dispatched on the unary path, whose retry executor invalidates the
  region cache and re-resolves on every attempt;
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

`RawKvClient` is `final` and its retry loop cannot be customised by
subclassing — there is no `classifyError()` hook on the client and no
`BackoffType::Custom` case. The supported extension point is the optional
`$classifier` callable accepted by `RetryExecutor::execute()`
(`src/Client/Retry/RetryExecutor.php`): it receives the caught
`TiKvException` and returns a `BackoffType` (see
`src/Client/Retry/BackoffType.php` for the full case list) or `null`
to fall through to the built-in classification — and if that also returns
`null`, the error is fatal and the exception is rethrown. Note that a
`NotLeader` error is classified before your classifier is consulted.

To use it you must construct the `RetryExecutor` yourself and invoke your
operation through it, rather than going through `RawKvClient`'s methods:

```php
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;

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
    classifier: function (TiKvException $e): ?BackoffType {
        if (str_contains($e->getMessage(), 'CustomError')) {
            return BackoffType::StaleCmd; // pick a real BackoffType case
        }
        return null; // fall through to the built-in ErrorClassifier
    },
);
```

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
        'checksumTimeoutMs' => 30000,    // default: 30000
        'ingestTimeoutMs' => 60000,      // default: 60000 (SST ingest)
        'batchDeadlineMs' => 0,          // default: 0 (disabled) — wall-clock deadline
                                         // for batch fan-out dispatch+wait across all
                                         // regions; exceeding it throws
                                         // BatchDeadlineExceededException after
                                         // cancelling in-flight futures
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

The last three are **metadata-plane** deadlines and are configured separately from the
store-RPC ones above because they address different failure modes: a hung PD (a
blackholed connection, a PD process that accepts but never answers) takes down every
region lookup and every transaction begin at once, while a slow store is a
per-region problem the retry executor already handles. `pdTimeoutMs` covers
`GetRegion` / `ScanRegions` / `GetStore` / `GetAllStores` / `GetMembers` /
`LoadKeyspace` and the GC safe-point RPCs; `tsoTimeoutMs` covers every `Tso` RPC
(so also the timestamp `checkTxnStatus()` fetches); `lockResolveTimeoutMs` covers
`KvCheckTxnStatus` / `KvCheckSecondaryLocks` / `KvResolveLock`.

Every gRPC call carries a deadline. `GrpcClientInterface::call()`'s `$timeoutMs`
argument of `null` means the library default (30 s); a non-positive value is a
configuration error and throws `InvalidArgumentException` — see [Deadlines are
never infinite](#deadlines-are-never-infinite).

When a timeout is exceeded, the gRPC call throws `GrpcException` which is caught and retried by the retry executor unless the budget is exhausted.

### Default Timeouts

By default all timeouts are set to sensible values (5s for reads/writes, 10s for batch, 20s for scans, 30s for delete-range, 3s for PD and TSO, 5s for lock resolution). Every timeout except `batchDeadlineMs` must be greater than `0`: a `0` (or a negative value) is rejected when the `TimeoutConfig` is built, and again when it is passed through `options['timeout']` — the error names the offending key. Use a large finite value instead of disabling a deadline. `batchDeadlineMs` is the one exception: `0` disables the overall batch deadline.

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
| `regionCacheHit()`             | `'region_resolution'`      | Region was found in the cache                     |
| `regionCacheMiss()`            | `'region_resolution'`      | Region was not in the cache, had to query PD      |
| `regionInvalidated()`          | `'region_error'`, `'not_leader'`, `'retry_region_error'`, `'fatal_region_error'`, `'lock_resolve'` | A region was actually removed from the cache — exactly once per actual drop, emitted from `RegionCache::invalidate()` with the caller's reason: top-level non-NotLeader region error (`RegionErrorHandler`), NotLeader handling in the retry loop (hint peer unknown / no hint), pre-retry invalidation on other retryable errors, a fatal routing error (e.g. `KeyNotInRegion`) dropped before the exception is rethrown (#233), or post-resolve cleanup in `LockResolver`. Invalidating an ID that is not cached emits nothing. |

For calls sent through `GrpcClient::callAsync()` (batch fan-outs, scans, range operations, SST ingest, prewrite/commit fan-outs), `rpcCompleted()` fires when the future settles: on `wait()`, on a failure, on `cancel()`, or from `__destruct`. The duration covers the time from dispatch to settle, including the time before `wait()` is called (for example queueing in a windowed fan-out).

Reasons are mutually exclusive per drop; the current source emits
`'region_error'`, `'not_leader'`, `'retry_region_error'`,
`'fatal_region_error'`, and `'lock_resolve'`
when those paths actually remove a region. A NotLeader response whose hinted
peer is still valid only switches the cached leader and emits nothing.
At executor-owned call sites, NotLeader drops are owned exclusively by the
retry loop's leader handling: `RegionErrorHandler::check()` leaves the region
cached so it can switch to a valid hint. Call sites without a surrounding
retry executor pass `notLeaderOwnedByRetryExecutor: false` and self-invalidate
with the same `'not_leader'` reason.

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

The pool caches 64 channels by default — the `maxChannels` cap of `GrpcClient`.
The cap is **soft**: a channel that still carries calls is never closed,
because closing it would destroy those calls. When every cached channel is
busy — a batch that fans out over more stores than the cap holds — the pool
grows past the cap until the calls settle, and the overshoot is logged at
warning level:

```
Channel cache at capacity but all channels are busy; exceeding soft cap
```

That warning is the signal that the workload touches more stores than the cap
allows. The pool does not shrink back on its own when the calls settle: the
eviction removes one least-recently-used idle channel per new address, and a
channel that stays unused is dropped after the idle TTL (10 minutes by
default). `maxChannels` is a constructor argument of `GrpcClient`; a client
built by `RawKvClient::create()` uses the default of 64, because the factory
does not expose the knob yet.

The pool itself is configurable when the defaults do not fit: `maxChannels`
caps the number of open channels and `idleTtlMs` sets how long an unused one
is kept — see [Channel Pool](#channel-pool-maxchannels-idlettlms).
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
