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
    /** @var array<string, Channel> */
    private array $channels = [];

    private bool $closed = false;

    public function __construct(
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?TlsConfig $tlsConfig = null,
    ) {
    }

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
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

    public function close(): void
    {
        $this->closed = true;

        $channels = $this->channels;
        $this->channels = [];

        foreach ($channels as $address => $channel) {
            try {
                $channel->close();
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
            $this->channels[$address]->close();
            unset($this->channels[$address]);
        }
    }

    public function getChannel(string $address): Channel
    {
        if ($this->closed) {
            throw new InvalidStateException('gRPC client is closed');
        }

        if (isset($this->channels[$address])) {
            $state = $this->channels[$address]->getConnectivityState();
            if ($state === \Grpc\CHANNEL_FATAL_FAILURE) {
                $this->logger->warning('Channel in fatal failure, reconnecting', ['address' => $address]);
                $this->closeChannel($address);
            }
        }

        if (!isset($this->channels[$address])) {
            $this->logger->debug('Opening gRPC channel', [
                'address' => $address,
                'tls' => $this->tlsConfig?->isEnabled() ?? false,
            ]);

            $tlsEnabled = $this->tlsConfig instanceof TlsConfig && $this->tlsConfig->isEnabled();
            $credentials = $tlsEnabled
                ? $this->createTlsCredentials()
                : ChannelCredentials::createInsecure();

            $this->channels[$address] = new Channel($address, [
                'credentials' => $credentials,
            ]);
        }

        return $this->channels[$address];
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
