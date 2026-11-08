<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Observability\NoOpTracer;
use CrazyGoat\TiKV\Client\Observability\SafeLogging;
use CrazyGoat\TiKV\Client\Observability\TraceCategory;
use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;
use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use Google\Protobuf\Internal\Message;
use Grpc\Call;
use Grpc\Channel;
use Grpc\ChannelCredentials;
use Grpc\Timeval;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * gRPC transport for TiKV and PD.
 *
 * The client keeps a cache of open channels (one per address, evicted when
 * idle or when the cap is reached), so it is meant to be **long-lived and
 * shared** between the requests of one process — creating one per request
 * throws the cache away and pays a reconnect every time.
 *
 * Ownership: the transport belongs to whoever created it. A client built by
 * `RawKvClient::create()` / `TxnKvClient::create()` creates the transport
 * itself and closes it in `close()`. A transport the application passes in
 * (the `grpcClient` option) belongs to the application: the client never
 * closes it, so the same transport can serve several clients. A client
 * assembled by hand (`new RawKvClient($pdClient, $grpc)`) does not own the
 * transport it is given, so it never closes it; pass `ownsGrpcClient: true`
 * to transfer ownership. See {@see GrpcClientInterface::close()}.
 *
 * ## ext-grpc channel semantics (issue #283)
 *
 * ext-grpc keeps a **process-global** table of persistent channels, keyed by
 * the target, a hash of the channel arguments and a hash of the credentials.
 * Two `Grpc\Channel` objects built from the same key therefore share **one**
 * underlying `grpc_channel` — as long as the key is in that table. The table
 * is bounded **per target** (one persisted channel per target by default), so
 * a channel created beyond that bound is private to its handle: ext-grpc logs
 * "The number of channel for the target is maxed out bounded" and does not
 * persist it. A private channel is never shared, so it is never at risk
 * either.
 *
 * Verified against the deployed extension (grpc 1.83.0,
 * `src/php/ext/grpc/channel.c`); the persistent-channel code is unchanged in
 * the oldest supported release (1.57), so the statement holds across the
 * supported range:
 *
 * - the persistent key is `target + sha1(channel arguments) + sha1(credentials)`.
 *   `ChannelCredentials::createInsecure()` returns `null`, so a plaintext
 *   channel is keyed by target and arguments only, while a TLS channel adds
 *   the credentials hash — a TLS and a plaintext client for the same address
 *   never share a channel;
 * - `Channel::close()` is a **reference-count decrement**
 *   (`php_grpc_channel_unref()`), not a destroy: the underlying channel
 *   survives until its last holder releases it, and the persistent table is
 *   one of those holders.
 *
 * A shared channel is always in the persistent table (a channel that is not
 * persisted is never shared), so `close()`, the idle eviction and the LRU
 * eviction of one client can never destroy a channel another client holds —
 * they only drop this client's own reference to it.
 *
 * The eviction paths leave a channel that still carries in-flight calls
 * alone (issue #278): `closeChannel()` and the idle and LRU evictions take
 * the channel out of the cache and defer its close until the last call
 * settles. `close()` is the exception — it closes the channels it deferred,
 * so shutting a client down ends the calls that were still running on them.
 */
final class GrpcClient implements GrpcClientInterface
{
    /**
     * Cached channels, keyed by address.
     *
     * `inFlight` counts the calls that were dispatched on the channel but
     * have not settled yet. A channel with in-flight calls is never
     * destroyed: closing it would kill the running calls (issue #278).
     *
     * @var array<string, array{channel: Channel, lastUsed: float, createdAt: float, inFlight: int}>
     */
    private array $channels = [];

    /**
     * Channels that left the cache while calls were still running on them.
     *
     * Their close is deferred until the last in-flight call settles, so an
     * eviction never destroys a running call (issue #278).
     *
     * @var array<int, array{address: string, channel: Channel, inFlight: int}>
     */
    private array $pendingCloses = [];

    private bool $closed = false;

    /**
     * Time (seconds) of the last idle sweep, or null before the first one.
     *
     * The sweep runs on every cache miss and, at most once per
     * `sweepDue()`, on the cache-hit path (issue #277).
     */
    private ?float $lastSweepAt = null;

    /** Whether the soft-cap overshoot of the current episode was logged. */
    private bool $softCapWarned = false;

    /**
     * Maximum number of channels kept open at the same time.
     *
     * The client is meant to be long-lived and shared: the channel cache is
     * only worth its memory when the same transport serves many requests.
     */
    public const DEFAULT_MAX_CHANNELS = 64;

    /** Idle time (ms) after which an unused channel is evicted. */
    public const DEFAULT_IDLE_TTL_MS = 600000; // 10 minutes
    /**
     * Defaults large enough for MAX_SCAN_LIMIT (10240 rows) responses — the
     * gRPC C core's own default receive limit is 4 MB, which a full-limit
     * scan with average-size values exceeds (issue #265).
     */
    public const DEFAULT_MAX_RECEIVE_MESSAGE_BYTES = 67108864; // 64 MB
    public const DEFAULT_MAX_SEND_MESSAGE_BYTES = 67108864; // 64 MB
    public const DEFAULT_KEEPALIVE_TIME_MS = 10000;
    public const DEFAULT_KEEPALIVE_TIMEOUT_MS = 3000;

    /**
     * Whether keepalive pings may be sent while no call is in flight
     * (grpc.keepalive_permit_without_calls). True by default: without it the
     * ping only runs during an RPC, which is exactly when the connection is
     * demonstrably alive, so idle NAT/firewall-dropped flows go undetected.
     */
    public const DEFAULT_KEEPALIVE_PERMIT_WITHOUT_CALLS = true;

    /**
     * Outgoing message compression (grpc.default_compression_algorithm).
     * Off by default, matching the gRPC core's own default.
     */
    public const COMPRESSION_NONE = 'none';
    public const COMPRESSION_GZIP = 'gzip';
    public const COMPRESSION_DEFLATE = 'deflate';
    public const DEFAULT_COMPRESSION = self::COMPRESSION_NONE;

    /**
     * HTTP/2 initial stream window size in bytes
     * (grpc.http2.lookahead_bytes). 0 leaves the gRPC core default in
     * place. A positive value also turns BDP probing off
     * (grpc.http2.bdp_probe => 0), because the probe would otherwise
     * re-tune the window from its bandwidth estimate and memory pressure
     * almost immediately, making the configured value only a starting
     * point. The trade-off is a fixed window: it no longer adapts to the
     * link's bandwidth-delay product.
     */
    public const DEFAULT_INITIAL_WINDOW_SIZE_BYTES = 0;

    /**
     * Numeric grpc_compression_algorithm values the channel argument expects
     * (0 is identity, i.e. no compression, and is passed by omitting the arg).
     *
     * @var array<string, int>
     */
    private const COMPRESSION_ALGORITHM_IDS = [
        self::COMPRESSION_DEFLATE => 1,
        self::COMPRESSION_GZIP => 2,
    ];

    /**
     * Deadline applied to a call whose `$timeoutMs` is null (issue #260).
     *
     * `null` used to mean "no deadline" (`Timeval::infFuture()`), which is
     * unreachable in practice: a blocking `Grpc\Call::startBatch()` inside the
     * C extension is not interrupted by `max_execution_time`, so a
     * half-open connection pinned a PHP-FPM worker for minutes or forever
     * and exhausted the whole pool. `null` now means "library default" and a
     * non-positive value is a configuration error (issue #276, GRPC-17):
     * `0` used to be the "no deadline" sentinel, but a value of `0` is
     * exactly what an unset environment variable coerces to, so it turned a
     * configuration mistake into an unbounded call.
     */
    public const DEFAULT_TIMEOUT_MS = 30000;

    /**
     * @param int $maxChannels Maximum number of channels kept open at the same
     *                        time (default 64). The least recently used channel
     *                        is evicted when the cap is reached. Must be >= 1.
     * @param int $idleTtlMs Idle time (ms) after which an unused channel is
     *                       evicted (default 10 minutes). A channel that is
     *                       evicted is reopened on the next request. Idle
     *                       channels are swept on every cache miss and, rate
     *                       limited, on cache hits (issue #277).
     * @param bool $allowInsecure When true (default), an insecure (plaintext) gRPC channel
     *                            is created when no TLS configuration is provided or when
     *                            the TLS configuration has no credentials set. When false,
     *                            an InvalidStateException is thrown if no TLS credentials
     *                            are available. Set this to false to ensure all connections
     *                            use TLS.
     * @param int $maxChannels Soft cap on the number of cached channels (default 64).
     *                         The cache may exceed it while every cached channel
     *                         has in-flight calls: a channel with live calls is never
     *                         closed, because closing it would destroy those calls
     *                         (issue #278). Exceeding the cap is logged at warning
     *                         level — raise the cap when the log shows up, it means
     *                         the workload touches more stores than the cap allows.
     *                         The cap is a constructor argument, so only a directly
     *                         built client can raise it: a client built by
     *                         `RawKvClient::create()` uses the default of 64.
     * @param int $maxReceiveMessageBytes grpc.max_receive_message_length channel argument
     *                                    in bytes (default 64 MB). The gRPC C core default
     *                                    is 4 MB, which scans/batch reads at MAX_SCAN_LIMIT
     *                                    can exceed, failing with RESOURCE_EXHAUSTED (issue #265).
     * @param int $maxSendMessageBytes grpc.max_send_message_length channel argument in bytes
     *                                 (default 64 MB).
     * @param int $keepaliveTimeMs grpc.keepalive_time_ms channel argument (default 10 s) —
     *                             how often to ping an idle connection so NAT/firewall-dropped
     *                             flows are detected instead of silently dead.
     * @param int $keepaliveTimeoutMs grpc.keepalive_timeout_ms channel argument (default 3 s).
     * @param bool $keepalivePermitWithoutCalls grpc.keepalive_permit_without_calls channel
     *                             argument (default true) — whether pings may be sent while no
     *                             call is in flight.
     * @param string $compression grpc.default_compression_algorithm channel argument: one of
     *                            {@see self::COMPRESSION_NONE} (default, no arg is emitted),
     *                            {@see self::COMPRESSION_GZIP} or {@see self::COMPRESSION_DEFLATE}.
     * @param int $initialWindowSizeBytes grpc.http2.lookahead_bytes channel argument in bytes
     *                                    (default 0 = leave the gRPC core default, which
     *                                    auto-tunes the HTTP/2 initial stream window). A
     *                                    positive value also sets grpc.http2.bdp_probe to 0,
     *                                    so the window is fixed instead of being re-tuned
     *                                    from the core's bandwidth estimate.
     * @param (\Closure(): float)|null $clock Source of the current time in seconds
     *                                         (default `microtime(true)`). Tests inject
     *                                         a fake clock to age channels without sleeping.
     * @param (\Closure(Channel, string, Timeval): Call)|null $callFactory
     *                                         Factory for `Grpc\Call` objects (default
     *                                         `new Call(...)`). Internal testing seam:
     *                                         it lets a test drive the send loop with a
     *                                         mock call, without a server.
     * @param (\Closure(?string, ?string, ?string): ChannelCredentials)|null $credentialsFactory
     *                                         Factory for TLS credentials (default
     *                                         `ChannelCredentials::createSsl(...)`). Internal
     *                                         testing seam: it exposes the certificate material
     *                                         a channel is rebuilt from, so a test can pin that
     *                                         eviction and reconnection reuse the intact
     *                                         `TlsConfig` (issue #315).
     */
    public function __construct(
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?TlsConfig $tlsConfig = null,
        private readonly int $maxChannels = self::DEFAULT_MAX_CHANNELS,
        private readonly int $idleTtlMs = self::DEFAULT_IDLE_TTL_MS,
        private readonly bool $allowInsecure = true,
        private readonly MetricsInterface $metrics = new NoOpMetrics(),
        private readonly int $maxReceiveMessageBytes = self::DEFAULT_MAX_RECEIVE_MESSAGE_BYTES,
        private readonly int $maxSendMessageBytes = self::DEFAULT_MAX_SEND_MESSAGE_BYTES,
        private readonly int $keepaliveTimeMs = self::DEFAULT_KEEPALIVE_TIME_MS,
        private readonly int $keepaliveTimeoutMs = self::DEFAULT_KEEPALIVE_TIMEOUT_MS,
        private readonly bool $keepalivePermitWithoutCalls = self::DEFAULT_KEEPALIVE_PERMIT_WITHOUT_CALLS,
        private readonly string $compression = self::DEFAULT_COMPRESSION,
        private readonly int $initialWindowSizeBytes = self::DEFAULT_INITIAL_WINDOW_SIZE_BYTES,
        private readonly ?\Closure $clock = null,
        private readonly ?\Closure $callFactory = null,
        private readonly ?\Closure $credentialsFactory = null,
        /**
         * Optional structured tracing callback (issue #434, GAP-20). The
         * transport emits a `kv_request`/`rpc_started` and `rpc_completed`
         * event per call; the application bridges them to its tracing SDK.
         */
        private readonly TraceEventInterface $tracer = new NoOpTracer(),
    ) {
        if ($this->maxChannels < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'maxChannels must be at least 1',
            );
        }
        if ($this->maxReceiveMessageBytes < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'maxReceiveMessageBytes must be at least 1',
            );
        }
        if ($this->maxSendMessageBytes < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'maxSendMessageBytes must be at least 1',
            );
        }
        if ($this->keepaliveTimeMs < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'keepaliveTimeMs must be at least 1',
            );
        }
        if ($this->keepaliveTimeoutMs < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'keepaliveTimeoutMs must be at least 1',
            );
        }
        $compressions = [self::COMPRESSION_NONE, self::COMPRESSION_GZIP, self::COMPRESSION_DEFLATE];
        if (!in_array($this->compression, $compressions, true)) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(sprintf(
                "compression must be one of 'none', 'gzip', 'deflate', got '%s'",
                $this->compression,
            ));
        }
        if ($this->initialWindowSizeBytes < 0) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'initialWindowSizeBytes must be greater than or equal to 0',
            );
        }
    }

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        // The timeout is validated before anything else happens: a
        // non-positive value is a configuration error and must not be
        // counted as an RPC, must not create (and cache) a channel and must
        // not be reported as a failed call (issue #276, GRPC-17).
        $timeout = $this->resolveTimeoutMs($timeoutMs);

        $operation = $service . '/' . $method;
        $start = hrtime(true);
        $this->startRpc($operation, $start);
        $success = false;
        try {
            $channel = $this->getChannel($address);

            $deadline = $this->deadline($timeout);

            $call = $this->createCall($channel, "/{$service}/{$method}", $deadline);

            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
                \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);

            $event = $call->startBatch([
                \Grpc\OP_RECV_INITIAL_METADATA => true,
                \Grpc\OP_RECV_MESSAGE => true,
                \Grpc\OP_RECV_STATUS_ON_CLIENT => true,
            ]);

            $status = GrpcResponseParser::extractStatus($event);

            if ($status['code'] !== \Grpc\STATUS_OK) {
                throw new GrpcException(
                    details: $status['details'],
                    grpcStatusCode: $status['code'],
                    targetAddress: $address,
                );
            }

            $response = GrpcResponseParser::deserialize($event, $responseClass);
            $success = true;

            return $response;
        } finally {
            $this->completeRpc($operation, $start, $success);
        }
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        iterable $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        // The timeout is validated before anything else happens: a
        // non-positive value is a configuration error and must not be
        // counted as an RPC, must not create (and cache) a channel and must
        // not be reported as a failed call (issue #276, GRPC-17).
        $timeout = $this->resolveTimeoutMs($timeoutMs);

        $operation = $service . '/' . $method;
        $start = hrtime(true);
        $this->startRpc($operation, $start);
        $success = false;
        $call = null;
        try {
            $channel = $this->getChannel($address);

            $deadline = $this->deadline($timeout);

            $call = $this->createCall($channel, "/{$service}/{$method}", $deadline);

            // Send initial metadata first.
            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
            ]);

            // Send each request message in its own batch (gRPC PHP extension
            // limitation: one OP_SEND_MESSAGE per batch). The iterable may be
            // a generator, so it is consumed exactly once, in order.
            foreach ($requests as $request) {
                $call->startBatch([
                    \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                ]);
            }

            // Close the client side of the stream.
            $call->startBatch([
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);

            // Receive the response.
            $event = $call->startBatch([
                \Grpc\OP_RECV_INITIAL_METADATA => true,
                \Grpc\OP_RECV_MESSAGE => true,
                \Grpc\OP_RECV_STATUS_ON_CLIENT => true,
            ]);

            $status = GrpcResponseParser::extractStatus($event);

            if ($status['code'] !== \Grpc\STATUS_OK) {
                throw new GrpcException(
                    details: $status['details'],
                    grpcStatusCode: $status['code'],
                    targetAddress: $address,
                );
            }

            $response = GrpcResponseParser::deserialize($event, $responseClass);
            $success = true;

            return $response;
        } catch (\Throwable $e) {
            // The call is abandoned mid-stream (a send failed, the response
            // status was an error, or the receive failed): cancel it so both
            // sides release the stream promptly instead of waiting for the
            // server-side deadline. The original failure must win over any
            // throwable from cancel() (issue #286, GRPC-27).
            $this->cancelCall($call);

            throw $e;
        } finally {
            $this->completeRpc($operation, $start, $success);
        }
    }

    /**
     * Cancel a call the client is abandoning.
     *
     * A throwable from `cancel()` is swallowed: it must never replace the
     * original transport exception that is propagating, the same discipline
     * {@see GrpcFuture::cancel()} applies (issue #286, GRPC-27).
     */
    private function cancelCall(?Call $call): void
    {
        if (!$call instanceof \Grpc\Call) {
            return;
        }

        try {
            $call->cancel();
        } catch (\Throwable) {
        }
    }

    /**
     * Build a gRPC call through the configured factory.
     *
     * The factory is an internal testing seam: production always takes the
     * `new Call(...)` branch (issue #286).
     */
    private function createCall(Channel $channel, string $path, Timeval $deadline): Call
    {
        $factory = $this->callFactory;
        if ($factory instanceof \Closure) {
            return $factory($channel, $path, $deadline);
        }

        return new Call($channel, $path, $deadline);
    }

    /**
     * Issue a unary gRPC call without waiting for the response.
     *
     * The send phase (initial metadata, request message, close) completes
     * before this method returns; the response is resolved later by calling
     * {@see GrpcFuture::wait()} on the returned future. This is the building
     * block for client-side fan-out: multiple sends can be issued to
     * different regions/stores first and awaited afterwards, so their
     * server-side latencies overlap instead of accumulating serially
     * (issue #295).
     *
     * @template T of Message
     * @param string $address Target address (e.g., "127.0.0.1:2379")
     * @param string $service Service name (e.g., "tikvpb.Tikv")
     * @param string $method Method name (e.g., "RawDeleteRange")
     * @param Message $request Protobuf request message
     * @param class-string<T> $responseClass Response message class name
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds;
     *                            null = the library default
     *                            ({@see self::DEFAULT_TIMEOUT_MS}); a
     *                            non-positive value throws
     * @return GrpcFuture Un-waited future resolving to T
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidStateException When the client has been closed
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException When $timeoutMs is not positive
     */
    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        // The timeout is validated before anything is opened or metered: a
        // non-positive value is a configuration error and must not create
        // (and cache) a channel, or emit rpcStarted(), before it is raised
        // (issue #276, GRPC-17).
        $timeout = $this->resolveTimeoutMs($timeoutMs);

        $operation = $service . '/' . $method;
        $start = hrtime(true);
        $this->startRpc($operation, $start);

        try {
            $channel = $this->getChannel($address);

            $deadline = $this->deadline($timeout);

            $call = $this->createCall($channel, "/{$service}/{$method}", $deadline);

            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
                \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
                \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
            ]);
        } catch (\Throwable $e) {
            // The RPC never started: complete it here, because no future
            // exists to settle it.
            $this->completeRpc($operation, $start, false);

            throw $e;
        }

        // The call is live on the channel from here on. Count it as
        // in-flight so that no eviction can close the channel under it
        // (issue #278); the counter is released when the future settles
        // — successfully, with an error, or on cancellation. The same
        // settlement records the RPC metric, so it is emitted exactly once
        // per call (issue #273).
        $this->markInFlight($address, $channel);

        return new GrpcFuture(
            $call,
            $responseClass,
            onSettled: function (bool $success) use ($channel, $operation, $start): void {
                $this->releaseInFlight($channel);
                $this->completeRpc($operation, $start, $success);
            },
            targetAddress: $address,
        );
    }

    /**
     * Record the start of an RPC. A metrics backend that throws is logged
     * and swallowed: the emission sits *before* the call, so a throw from it
     * would abort a call that never happened and replace the transport
     * exception the caller has to see (issue #759, GRPC-15).
     *
     * The `rpcStarted == rpcSucceeded + rpcFailed` invariant holds for every
     * backend that records what it is told. A backend that throws from
     * `rpcStarted()` alone breaks it — its own counters are not trustworthy
     * in that case.
     */
    private function startRpc(string $operation, int|float $startNs): void
    {
        try {
            $this->metrics->rpcStarted($operation);
        } catch (\Throwable $e) {
            $this->logger->warning('Metrics backend threw from rpcStarted()', [
                'operation' => $operation,
                'exception' => $e,
            ]);
        }

        try {
            $this->tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_started', [
                'operation' => $operation,
                'eventId' => (string) $startNs,
            ]);
        } catch (\Throwable $e) {
            $this->logger->warning('Trace backend threw from rpc_started', [
                'operation' => $operation,
                'exception' => $e,
            ]);
        }
    }

    /**
     * Record the end of an RPC. A metrics backend that throws is logged and
     * swallowed: this runs in `finally` blocks, where a throw would replace
     * the transport exception that is propagating (issue #274, GRPC-15).
     */
    private function completeRpc(string $operation, int|float $startNs, bool $success): void
    {
        $durationMs = (hrtime(true) - $startNs) / 1_000_000;

        try {
            $this->metrics->rpcCompleted($operation, $durationMs, $success);
        } catch (\Throwable $e) {
            $this->logger->warning('Metrics backend threw from rpcCompleted()', [
                'operation' => $operation,
                'exception' => $e,
            ]);
        }

        try {
            $this->tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_completed', [
                'operation' => $operation,
                'eventId' => (string) $startNs,
                'durationMs' => $durationMs,
                'success' => $success,
            ]);
        } catch (\Throwable $e) {
            $this->logger->warning('Trace backend threw from rpc_completed', [
                'operation' => $operation,
                'exception' => $e,
            ]);
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Release every channel this client opened.
     *
     * "Release" means this client's handles: ext-grpc keeps the underlying
     * channel in its process-global persistent table, so the TCP connection
     * stays warm in the process (until the process ends or the per-target
     * persist bound drops the entry). This call does not tear the connection
     * down.
     *
     * Closing this client never invalidates a channel another client in the
     * same process holds: ext-grpc reference-counts the underlying channel,
     * so the channel stays alive until its last holder releases it
     * (issue #283).
     *
     * The channels whose close was deferred because they still carried
     * in-flight calls (issue #278) are closed here: shutting a client down
     * ends the calls that were still running on them.
     *
     * @see GrpcClientInterface::close() for who may call this.
     */
    public function close(): void
    {
        $this->closed = true;

        while ($this->channels !== []) {
            $address = array_key_first($this->channels);
            $channel = $this->channels[$address]['channel'];
            unset($this->channels[$address]);
            $this->closeChannelObject($address, $channel);
        }

        // Channels that left the cache while calls were still running on
        // them: the client is shutting down, so they are closed now
        // instead of waiting for their last call to settle.
        foreach ($this->pendingCloses as $entry) {
            $this->closeChannelObject($entry['address'], $entry['channel']);
        }
        $this->pendingCloses = [];
    }

    /**
     * Close the channel cached for an address.
     *
     * A channel that still has in-flight calls is not closed (issue #278):
     * it leaves the cache — so the next call for that address gets a
     * fresh handle — and the close is deferred until its last in-flight
     * call settles.
     *
     * A fresh **handle** is not a fresh connection: ext-grpc keeps the
     * channel in its process-global persistent table, so the next
     * `getChannel()` resolves to the same underlying `grpc_channel` and
     * the same TCP connection (verified against grpc 1.83.0). The table is
     * bounded per target, so a channel that was created beyond that bound is
     * private to its handle — closing it really does release it. gRPC's own
     * reconnection after a broken connection is unaffected by this.
     *
     * No-op if the client has already been closed.
     *
     * @param string $address Channel address to close
     */
    public function closeChannel(string $address): void
    {
        if ($this->closed) {
            return;
        }

        if (!isset($this->channels[$address])) {
            return;
        }

        $entry = $this->channels[$address];
        unset($this->channels[$address]);

        if ($entry['inFlight'] > 0) {
            $this->deferChannelClose($address, $entry);

            return;
        }

        SafeLogging::log($this->logger, 'debug', 'Channel closed', ['address' => $address]);
        $this->closeChannelObject($address, $entry['channel']);
    }

    public function getChannel(string $address): Channel
    {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        $now = $this->now();

        // Check existing channel
        if (isset($this->channels[$address])) {
            try {
                $state = $this->channels[$address]['channel']->getConnectivityState();
            } catch (\Throwable $e) {
                // Channel might be closed already; treat as non-ready
                $this->logger->warning('Failed to get channel connectivity state, reconnecting', [
                    'address' => $address,
                    'exception' => $e,
                ]);
                $this->evictChannel($address);

                return $this->createChannel($address, $now);
            }

            // Reap channels in non-ready states to force reconnect on next use
            if (
                in_array($state, [
                    \Grpc\CHANNEL_FATAL_FAILURE,
                    \Grpc\CHANNEL_TRANSIENT_FAILURE,
                ], true)
            ) {
                $this->logger->warning('Channel in non-ready state, reconnecting', [
                    'address' => $address,
                    'state' => $state,
                ]);
                $this->evictChannel($address);
            } else {
                // Update last-used timestamp
                $this->channels[$address]['lastUsed'] = $now;
                $channel = $this->channels[$address]['channel'];

                // A client that only ever hits the cache would keep the
                // channels of departed stores for ever (issue #277). The
                // returned channel was just touched, so it is never reaped.
                if ($this->sweepDue($now)) {
                    $this->evictIdleChannels($now);
                }

                return $channel;
            }
        }

        // Evict idle channels before creating a new one
        $this->evictIdleChannels($now);

        // Enforce max channels cap (LRU eviction if at capacity)
        $this->enforceMaxChannels();

        return $this->createChannel($address, $now);
    }

    /**
     * Drop the cached channels that were idle longer than `idleTtlMs`.
     *
     * The idle sweep also runs inside `getChannel()`, but only when a request
     * arrives. A long-running worker that can be quiet for a long time calls
     * this from its own tick. A channel with in-flight calls is skipped.
     * No-op if the client has already been closed.
     */
    public function reapIdleChannels(): void
    {
        if ($this->closed) {
            return;
        }

        $this->evictIdleChannels($this->now());
    }

    /**
     * Get the current number of cached channels.
     */
    public function getChannelCount(): int
    {
        return count($this->channels);
    }

    /**
     * Evict channels that have been idle longer than the configured TTL.
     *
     * A channel with in-flight calls is never evicted: its idle time says
     * nothing about the calls that are still running on it, and closing it
     * would destroy them (issue #278).
     */
    private function evictIdleChannels(float $now): void
    {
        $this->lastSweepAt = $now;
        $idleThreshold = $now - ($this->idleTtlMs / 1000);

        foreach ($this->channels as $address => $entry) {
            if ($entry['inFlight'] > 0) {
                continue;
            }

            if ($entry['lastUsed'] < $idleThreshold) {
                $this->logger->debug('Evicting idle channel', [
                    'address' => $address,
                    'idleSeconds' => round($now - $entry['lastUsed'], 1),
                ]);
                $this->evictChannel($address);
            }
        }
    }

    /**
     * Whether the cache-hit path may run an idle sweep now.
     *
     * The sweep walks every cached channel, so the hot path runs it at most
     * once per interval: a tenth of the idle TTL, but at least one second and
     * never more than the TTL itself (issue #277).
     */
    private function sweepDue(float $now): bool
    {
        if ($this->lastSweepAt === null) {
            return true;
        }

        $ttlSeconds = $this->idleTtlMs / 1000;
        $interval = min($ttlSeconds, max(1.0, $ttlSeconds / 10));

        return $now - $this->lastSweepAt >= $interval;
    }

    /**
     * If the channel cache is at capacity, evict the least recently used idle
     * channels (the ones with no in-flight calls) until it is below the cap.
     *
     * `maxChannels` is a soft cap: a channel with in-flight calls is never
     * evicted, so when every cached channel is busy the cache is allowed to
     * grow past the cap instead of destroying running calls (issue #278).
     * The overshoot is logged at warning level, because it means the
     * workload touches more stores than the cap allows.
     */
    private function enforceMaxChannels(): void
    {
        if (count($this->channels) < $this->maxChannels) {
            // Back under the cap: the next overshoot is a new episode and
            // is logged again.
            $this->softCapWarned = false;

            return;
        }

        // Evict down to the cap, not one channel per call: an over-cap cache
        // would otherwise shrink by one address per new address and stay
        // above the cap for a long time. Every pass removes one channel from
        // the cache, so the loop ends.
        while (count($this->channels) >= $this->maxChannels) {
            // Find the least recently used channel that has no live calls.
            $lruAddress = null;
            $lruTime = PHP_FLOAT_MAX;
            foreach ($this->channels as $address => $entry) {
                if ($entry['inFlight'] > 0) {
                    continue;
                }
                if ($entry['lastUsed'] < $lruTime) {
                    $lruTime = $entry['lastUsed'];
                    $lruAddress = $address;
                }
            }

            if ($lruAddress === null) {
                $this->warnSoftCapExceeded();

                return;
            }

            $this->logger->debug('Evicting LRU channel to enforce max channels', [
                'address' => $lruAddress,
                'maxChannels' => $this->maxChannels,
            ]);
            $this->evictChannel($lruAddress);
        }

        $this->softCapWarned = false;
    }

    /**
     * Log the soft-cap overshoot once per episode, not once per new address:
     * a fan-out over a thousand stores would otherwise log ~1000 lines. The
     * flag is cleared when the cache is back under the cap.
     */
    private function warnSoftCapExceeded(): void
    {
        if ($this->softCapWarned) {
            return;
        }

        $this->softCapWarned = true;
        $this->logger->warning('Channel cache at capacity but all channels are busy; exceeding soft cap', [
            'maxChannels' => $this->maxChannels,
            // The size of the cache, not the number of busy channels:
            // every cached channel is busy in this branch.
            'cachedChannels' => count($this->channels),
        ]);
    }

    /**
     * Remove a channel from the cache and close it.
     *
     * A channel that still has in-flight calls is not closed (issue #278):
     * it leaves the cache and its close is deferred until the last
     * in-flight call settles.
     */
    private function evictChannel(string $address): void
    {
        if (!isset($this->channels[$address])) {
            return;
        }

        $entry = $this->channels[$address];
        unset($this->channels[$address]);

        if ($entry['inFlight'] > 0) {
            $this->deferChannelClose($address, $entry);

            return;
        }

        $this->closeChannelObject($address, $entry['channel']);
    }

    /**
     * Move a busy channel out of the cache and remember to close it later.
     *
     * @param array{channel: Channel, lastUsed: float, createdAt: float, inFlight: int} $entry
     */
    private function deferChannelClose(string $address, array $entry): void
    {
        $this->pendingCloses[spl_object_id($entry['channel'])] = [
            'address' => $address,
            'channel' => $entry['channel'],
            'inFlight' => $entry['inFlight'],
        ];

        SafeLogging::log($this->logger, 'debug', 'Channel close deferred until in-flight calls settle', [
            'address' => $address,
            'inFlight' => $entry['inFlight'],
        ]);
    }

    /**
     * Count a call that was just dispatched on a channel.
     */
    private function markInFlight(string $address, Channel $channel): void
    {
        if (!isset($this->channels[$address]) || $this->channels[$address]['channel'] !== $channel) {
            // The cached channel was replaced between the lookup and the
            // dispatch: there is no entry left to count the call on.
            return;
        }

        $this->channels[$address]['inFlight']++;
    }

    /**
     * Release the in-flight slot of a channel whose call just settled.
     *
     * The counter never drops below zero: a double settle — a failed
     * `wait()` followed by a `cancel()`, for instance — must not make a
     * busy channel look idle and evictable (issue #278).
     */
    private function releaseInFlight(Channel $channel): void
    {
        // The channels are compared by identity, not by their object id:
        // an id is reused once the channel it belonged to is gone, and a
        // late settle must not release a slot of a different channel.
        foreach ($this->pendingCloses as $id => $entry) {
            if ($entry['channel'] !== $channel) {
                continue;
            }

            $remaining = max(0, $entry['inFlight'] - 1);

            if ($remaining === 0) {
                unset($this->pendingCloses[$id]);
                $this->closeChannelObject($entry['address'], $entry['channel']);

                return;
            }

            $this->pendingCloses[$id]['inFlight'] = $remaining;

            return;
        }

        foreach ($this->channels as $address => $entry) {
            if ($entry['channel'] === $channel) {
                $this->channels[$address]['inFlight'] = max(0, $entry['inFlight'] - 1);

                return;
            }
        }
    }

    /**
     * Close a gRPC channel, catching and logging any exceptions.
     */
    private function closeChannelObject(string $address, Channel $channel): void
    {
        try {
            $channel->close();
        } catch (\Throwable $e) {
            // close() is destructor-reachable: a logger that is already
            // torn down during shutdown must not escape the destructor
            // (issue #287, GRPC-28).
            SafeLogging::log($this->logger, 'error', 'Failed to close gRPC channel', [
                'address' => $address,
                'exception' => $e,
            ]);
        }
    }

    /**
     * Create a new gRPC channel for the given address and store it in the cache.
     */
    private function createChannel(string $address, float $now): Channel
    {
        $tlsEnabled = $this->tlsConfig instanceof TlsConfig && $this->tlsConfig->isEnabled();

        $this->logger->debug('Opening gRPC channel', [
            'address' => $address,
            'tls' => $tlsEnabled,
        ]);

        if ($tlsEnabled) {
            $credentials = $this->createTlsCredentials();
        } else {
            if (!$this->allowInsecure) {
                throw new \CrazyGoat\TiKV\Client\Exception\InvalidStateException(
                    'Cannot create gRPC channel: TLS is not configured and insecure '
                    . 'connections are not allowed. Provide a TlsConfig or set '
                    . 'allowInsecure=true explicitly.'
                );
            }

            $this->logger->warning(
                'Opening insecure (plaintext) gRPC channel — all data will be '
                . 'transmitted without encryption. To disable this warning and '
                . 'ensure TLS is always used, configure a TlsConfig and set '
                . 'allowInsecure=false.',
                ['address' => $address],
            );

            $credentials = ChannelCredentials::createInsecure();
        }

        $channel = new Channel($address, [
            'credentials' => $credentials,
            ...$this->channelArgs(),
        ]);

        $this->channels[$address] = [
            'channel' => $channel,
            'lastUsed' => $now,
            'createdAt' => $now,
            'inFlight' => 0,
        ];

        return $channel;
    }

    /**
     * gRPC channel arguments applied to every created channel (issue #265,
     * #423).
     *
     * Note: adding channel arguments changes ext-grpc's persistent-channel
     * key, which is desirable — two GrpcClient instances configured
     * differently get distinct channels.
     *
     * Compression is emitted only when enabled (the gRPC core default is
     * identity, i.e. no compression), and the window size only when set, so
     * a default-configured client keeps the same channel key it had before
     * these options existed.
     *
     * `grpc.max_receive_message_length` is the enforcement point for the
     * response-size bound (SEC-16, issue #319): grpc-core rejects an
     * oversized frame before its payload is materialised as a PHP string.
     * The post-allocation guard in {@see GrpcResponseParser} cannot do that
     * and is only a secondary assertion.
     *
     * @return array<string, int|string|bool>
     */
    private function channelArgs(): array
    {
        $args = [
            'grpc.max_receive_message_length' => $this->maxReceiveMessageBytes,
            'grpc.max_send_message_length' => $this->maxSendMessageBytes,
            'grpc.keepalive_time_ms' => $this->keepaliveTimeMs,
            'grpc.keepalive_timeout_ms' => $this->keepaliveTimeoutMs,
            'grpc.keepalive_permit_without_calls' => $this->keepalivePermitWithoutCalls ? 1 : 0,
            'grpc.http2.max_pings_without_data' => 0,
        ];

        if ($this->compression !== self::COMPRESSION_NONE) {
            $args['grpc.default_compression_algorithm'] = self::COMPRESSION_ALGORITHM_IDS[$this->compression];
        }

        // gRPC C core has no "initial_window_size" channel argument; the
        // HTTP/2 initial stream window is set through
        // grpc.http2.lookahead_bytes (it feeds SetInitialWindowSize and
        // target_initial_window_size in the chttp2 transport). BDP probing
        // would re-tune that target almost immediately, so it is turned off
        // when a fixed window was asked for.
        if ($this->initialWindowSizeBytes > 0) {
            $args['grpc.http2.lookahead_bytes'] = $this->initialWindowSizeBytes;
            $args['grpc.http2.bdp_probe'] = 0;
        }

        return $args;
    }

    private function now(): float
    {
        return $this->clock instanceof \Closure ? ($this->clock)() : microtime(true);
    }

    /**
     * Resolve a call's `$timeoutMs` argument to a number of milliseconds
     * that is never "unset" (issue #260).
     *
     * `null` — a call site with no opinion — becomes the conservative
     * library default {@see self::DEFAULT_TIMEOUT_MS} rather than an
     * unbounded deadline. A non-positive value is rejected here
     * (issue #276, GRPC-17): it used to be the "no deadline" sentinel, but a
     * user-supplied `0` is far more likely to be an accident than an intent.
     *
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException When the resolved timeout is not positive
     */
    private function resolveTimeoutMs(?int $timeoutMs): int
    {
        $timeout = $timeoutMs ?? self::DEFAULT_TIMEOUT_MS;

        if ($timeout <= 0) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                sprintf('timeoutMs must be greater than 0, got %d', $timeout),
            );
        }

        return $timeout;
    }

    /**
     * Build the gRPC deadline for a call (issue #260).
     *
     * The single place where a deadline is derived from a timeout, so
     * {@see self::resolveTimeoutMs()} cannot be honoured in one entry point
     * and forgotten in another. It takes the **resolved** timeout (a
     * strictly positive number of milliseconds), so the rejection of a
     * non-positive value happens in {@see self::resolveTimeoutMs()} — before
     * a channel is opened (issue #276, GRPC-17).
     *
     * The guard below is defence in depth: ext-grpc does not reject a
     * negative `Timeval`, it builds a deadline already in the past.
     */
    private function deadline(int $timeoutMs): Timeval
    {
        // Defence in depth: the caller resolves the timeout (and rejects a
        // non-positive one) before it gets here, but a raw value that
        // reached this method would build a deadline already in the past —
        // every call would fail with DEADLINE_EXCEEDED (issue #276).
        if ($timeoutMs <= 0) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                sprintf('timeoutMs must be greater than 0, got %d', $timeoutMs),
            );
        }

        return Timeval::now()->add(new Timeval($timeoutMs * 1000));
    }

    private function createTlsCredentials(): ChannelCredentials
    {
        if (!$this->tlsConfig instanceof \CrazyGoat\TiKV\Client\Tls\TlsConfig) {
            throw new InvalidStateException('TLS config is required for TLS credentials');
        }

        $certChain = $this->tlsConfig->clientCert;
        $privateKey = $this->tlsConfig->clientKey;
        $factory = $this->credentialsFactory;

        if ($factory instanceof \Closure) {
            return $factory($this->tlsConfig->caCert, $certChain, $privateKey);
        }

        return ChannelCredentials::createSsl(
            $this->tlsConfig->caCert,
            $certChain,
            $privateKey,
        );
    }
}
