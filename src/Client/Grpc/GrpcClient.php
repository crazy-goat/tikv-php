<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
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
     * @param bool $allowInsecure When true (default), an insecure (plaintext) gRPC channel
     *                            is created when no TLS configuration is provided or when
     *                            the TLS configuration has no credentials set. When false,
     *                            an InvalidStateException is thrown if no TLS credentials
     *                            are available. Set this to false to ensure all connections
     *                            use TLS.
     */
    public function __construct(
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?TlsConfig $tlsConfig = null,
        private readonly int $maxChannels = self::DEFAULT_MAX_CHANNELS,
        private readonly int $idleTtlMs = self::DEFAULT_IDLE_TTL_MS,
        private readonly bool $allowInsecure = true,
    ) {
        if ($this->maxChannels < 1) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                'maxChannels must be at least 1',
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

        $channel = $this->getChannel($address);

        $deadline = $timeoutMs !== null && $timeoutMs > 0
            ? Timeval::now()->add(new Timeval($timeoutMs * 1000))
            : Timeval::infFuture();

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

        return GrpcResponseParser::deserialize($event, $responseClass);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        $channels = $this->channels;
        $this->channels = [];

        foreach ($channels as $address => $entry) {
            try {
                $entry['channel']->close();
            } catch (\Throwable $e) {
                $this->logger->error('Failed to close gRPC channel', [
                    'address' => $address,
                    'exception' => $e,
                ]);
            }
        }
    }

    public function closeChannel(string $address): void
    {
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
        ]);

        $this->channels[$address] = [
            'channel' => $channel,
            'lastUsed' => $now,
            'createdAt' => $now,
        ];

        return $channel;
    }

    private function now(): float
    {
        return microtime(true);
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
