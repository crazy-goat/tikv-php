<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
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
 * assembled by hand (`new RawKvClient($pdClient, $grpc)`) closes the
 * transport it was given unless `ownsGrpcClient: false` is passed. See
 * {@see GrpcClientInterface::close()}.
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
     * Deadline applied to a call whose `$timeoutMs` is null (issue #260).
     *
     * `null` used to mean "no deadline" (`Timeval::infFuture()`), which is
     * unreachable in practice: a blocking `Grpc\Call::startBatch()` inside the
     * C extension is not interrupted by `max_execution_time`, so a
     * half-open connection pinned a PHP-FPM worker for minutes or forever
     * and exhausted the whole pool. `null` now means "library default" and a
     * caller who genuinely wants an unbounded call must say so with the
     * explicit sentinel `0`.
     */
    public const DEFAULT_TIMEOUT_MS = 30000;

    /**
     * @param int $maxChannels Maximum number of channels kept open at the same
     *                        time (default 64). The least recently used channel
     *                        is evicted when the cap is reached. Must be >= 1.
     * @param int $idleTtlMs Idle time (ms) after which an unused channel is
     *                       evicted (default 10 minutes). A channel that is
     *                       evicted is reopened on the next request.
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

        $operation = $service . '/' . $method;
        $this->metrics->rpcStarted($operation);
        $start = hrtime(true);
        $success = false;
        try {
            $channel = $this->getChannel($address);

            $deadline = $this->deadline($timeoutMs);

            $call = new Call(
                $channel,
                "/{$service}/{$method}",
                $deadline,
            );

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
                );
            }

            $response = GrpcResponseParser::deserialize($event, $responseClass);
            $success = true;

            return $response;
        } finally {
            $durationMs = (hrtime(true) - $start) / 1_000_000;
            $this->metrics->rpcCompleted($operation, $durationMs, $success);
        }
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        $operation = $service . '/' . $method;
        $this->metrics->rpcStarted($operation);
        $start = hrtime(true);
        $success = false;
        try {
            $channel = $this->getChannel($address);

            $deadline = $this->deadline($timeoutMs);

            $call = new Call(
                $channel,
                "/{$service}/{$method}",
                $deadline,
            );

            // Send initial metadata first.
            $call->startBatch([
                \Grpc\OP_SEND_INITIAL_METADATA => [],
            ]);

            // Send each request message in its own batch (gRPC PHP extension
            // limitation: one OP_SEND_MESSAGE per batch).
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
                );
            }

            $response = GrpcResponseParser::deserialize($event, $responseClass);
            $success = true;

            return $response;
        } finally {
            $durationMs = (hrtime(true) - $start) / 1_000_000;
            $this->metrics->rpcCompleted($operation, $durationMs, $success);
        }
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
     *                            ({@see self::DEFAULT_TIMEOUT_MS}), 0 = no deadline
     * @return GrpcFuture Un-waited future resolving to T
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidStateException When the client has been closed
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

        $channel = $this->getChannel($address);

        $deadline = $this->deadline($timeoutMs);

        $call = new Call(
            $channel,
            "/{$service}/{$method}",
            $deadline,
        );

        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);

        // The call is live on the channel from here on. Count it as
        // in-flight so that no eviction can close the channel under it
        // (issue #278); the counter is released when the future settles
        // — successfully, with an error, or on cancellation.
        $this->markInFlight($address, $channel);

        return new GrpcFuture(
            $call,
            $responseClass,
            onSettled: function (bool $success) use ($channel): void {
                $this->releaseInFlight($channel);
            },
        );
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

        $this->logger->debug('Channel closed', ['address' => $address]);
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
                return $this->channels[$address]['channel'];
            }
        }

        // Evict idle channels before creating a new one
        $this->evictIdleChannels($now);

        // Enforce max channels cap (LRU eviction if at capacity)
        $this->enforceMaxChannels();

        return $this->createChannel($address, $now);
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
     * If the channel cache is at capacity, evict the least recently used channel.
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
            return;
        }

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
            $this->logger->warning('Channel cache at capacity but all channels are busy; exceeding soft cap', [
                'maxChannels' => $this->maxChannels,
                // The size of the cache, not the number of busy channels:
                // every cached channel is busy in this branch.
                'cachedChannels' => count($this->channels),
            ]);

            return;
        }

        $this->logger->debug('Evicting LRU channel to enforce max channels', [
            'address' => $lruAddress,
            'maxChannels' => $this->maxChannels,
        ]);
        $this->evictChannel($lruAddress);
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

        $this->logger->debug('Channel close deferred until in-flight calls settle', [
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
            $this->logger->error('Failed to close gRPC channel', [
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
     * gRPC channel arguments applied to every created channel (issue #265).
     *
     * Note: adding channel arguments changes ext-grpc's persistent-channel
     * key, which is desirable — two GrpcClient instances configured
     * differently get distinct channels.
     *
     * @return array<string, int|string|bool>
     */
    private function channelArgs(): array
    {
        return [
            'grpc.max_receive_message_length' => $this->maxReceiveMessageBytes,
            'grpc.max_send_message_length' => $this->maxSendMessageBytes,
            'grpc.keepalive_time_ms' => $this->keepaliveTimeMs,
            'grpc.keepalive_timeout_ms' => $this->keepaliveTimeoutMs,
            'grpc.keepalive_permit_without_calls' => 1,
            'grpc.http2.max_pings_without_data' => 0,
        ];
    }

    private function now(): float
    {
        return microtime(true);
    }

    /**
     * Resolve a call's `$timeoutMs` argument to a number of milliseconds
     * that is never "unset" (issue #260).
     *
     * `null` — a call site with no opinion — becomes the conservative
     * library default {@see self::DEFAULT_TIMEOUT_MS} rather than an
     * unbounded deadline. A caller that genuinely wants no deadline has to
     * pass the explicit non-positive sentinel `0`, which is the same way
     * the rest of the library spells "disabled" (`TimeoutConfig`'s
     * `batchDeadlineMs = 0`).
     */
    private function resolveTimeoutMs(?int $timeoutMs): int
    {
        return $timeoutMs ?? self::DEFAULT_TIMEOUT_MS;
    }

    /**
     * Build the gRPC deadline for a call (issue #260).
     *
     * The single place where a deadline is derived from a `$timeoutMs`
     * argument, so {@see self::resolveTimeoutMs()} cannot be honoured in
     * one entry point and forgotten in another.
     */
    private function deadline(?int $timeoutMs): Timeval
    {
        $timeout = $this->resolveTimeoutMs($timeoutMs);

        return $timeout > 0
            ? Timeval::now()->add(new Timeval($timeout * 1000))
            : Timeval::infFuture();
    }

    private function createTlsCredentials(): ChannelCredentials
    {
        if (!$this->tlsConfig instanceof \CrazyGoat\TiKV\Client\Tls\TlsConfig) {
            throw new InvalidStateException('TLS config is required for TLS credentials');
        }

        $certChain = $this->tlsConfig->clientCert;
        $privateKey = $this->tlsConfig->clientKey;

        return ChannelCredentials::createSsl(
            $this->tlsConfig->caCert,
            $certChain,
            $privateKey,
        );
    }
}
