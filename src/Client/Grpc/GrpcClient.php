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

final class GrpcClient implements GrpcClientInterface
{
    /** @var array<string, array{channel: Channel, lastUsed: float, createdAt: float}> */
    private array $channels = [];

    private bool $closed = false;

    private const DEFAULT_MAX_CHANNELS = 64;
    private const DEFAULT_IDLE_TTL_MS = 600000; // 10 minutes
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
     * @param bool $allowInsecure When true (default), an insecure (plaintext) gRPC channel
     *                            is created when no TLS configuration is provided or when
     *                            the TLS configuration has no credentials set. When false,
     *                            an InvalidStateException is thrown if no TLS credentials
     *                            are available. Set this to false to ensure all connections
     *                            use TLS.
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

            $success = true;

            return GrpcResponseParser::deserialize($event, $responseClass);
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

            $success = true;

            return GrpcResponseParser::deserialize($event, $responseClass);
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

        return new GrpcFuture($call, $responseClass);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        $this->closed = true;

        while ($this->channels !== []) {
            $address = array_key_first($this->channels);
            $this->closeChannelEntry($address);
            unset($this->channels[$address]);
        }
    }

    public function closeChannel(string $address): void
    {
        if ($this->closed) {
            return;
        }

        if (isset($this->channels[$address])) {
            $this->logger->debug('Channel closed', ['address' => $address]);
            try {
                $this->channels[$address]['channel']->close();
            } catch (\Throwable $e) {
                $this->logger->error('Failed to close gRPC channel', [
                    'address' => $address,
                    'exception' => $e,
                ]);
            }
            unset($this->channels[$address]);
        }
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
                $this->closeChannelEntry($address);
                unset($this->channels[$address]);
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
                $this->closeChannelEntry($address);
                unset($this->channels[$address]);
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
     */
    private function evictIdleChannels(float $now): void
    {
        $idleThreshold = $now - ($this->idleTtlMs / 1000);

        foreach ($this->channels as $address => $entry) {
            if ($entry['lastUsed'] < $idleThreshold) {
                $this->logger->debug('Evicting idle channel', [
                    'address' => $address,
                    'idleSeconds' => round($now - $entry['lastUsed'], 1),
                ]);
                $this->closeChannelEntry($address);
                unset($this->channels[$address]);
            }
        }
    }

    /**
     * If the channel cache is at capacity, evict the least recently used channel.
     */
    private function enforceMaxChannels(): void
    {
        if (count($this->channels) < $this->maxChannels) {
            return;
        }
        // Find the least recently used channel.
        // Guaranteed to find one since count($this->channels) >= maxChannels >= 1.
        $lruAddress = array_key_first($this->channels);
        $lruTime = $this->channels[$lruAddress]['lastUsed'];
        foreach ($this->channels as $address => $entry) {
            if ($entry['lastUsed'] < $lruTime) {
                $lruTime = $entry['lastUsed'];
                $lruAddress = $address;
            }
        }
        $this->logger->debug('Evicting LRU channel to enforce max channels', [
            'address' => $lruAddress,
            'maxChannels' => $this->maxChannels,
        ]);
        $this->closeChannelEntry($lruAddress);
        unset($this->channels[$lruAddress]);
    }

    /**
     * Close a channel entry by address, catching and logging any exceptions.
     */
    private function closeChannelEntry(string $address): void
    {
        if (!isset($this->channels[$address])) {
            return;
        }

        try {
            $this->channels[$address]['channel']->close();
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
