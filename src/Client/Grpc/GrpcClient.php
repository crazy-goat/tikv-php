<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
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

        $status = $this->extractStatus($event);

        if ($status['code'] !== \Grpc\STATUS_OK) {
            throw new GrpcException(
                details: $status['details'],
                grpcStatusCode: $status['code'],
            );
        }

        return $this->deserializeResponse($event, $responseClass);
    }

    public function close(): void
    {
        foreach ($this->channels as $channel) {
            $channel->close();
        }
        $this->channels = [];
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

    /**
     * @return array{code: int, details: string}
     */
    private function extractStatus(mixed $event): array
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $status = $eventArray['status'] ?? null;
        if (is_object($status)) {
            $status = (array) $status;
        }

        /** @var array<string, mixed> $statusArray */
        $statusArray = is_array($status) ? $status : [];

        $code = $statusArray['code'] ?? 0;
        $details = $statusArray['details'] ?? '';

        return [
            'code' => is_int($code) ? $code : (is_string($code) ? (int) $code : 0),
            'details' => is_string($details) ? $details : (is_scalar($details) ? (string) $details : ''),
        ];
    }

    /**
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    private function deserializeResponse(mixed $event, string $responseClass): Message
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $message = $eventArray['message'] ?? null;

        /** @var T $response */
        $response = new $responseClass();

        if ($message !== null && $message !== '' && is_string($message)) {
            $response->mergeFromString($message);
        }

        return $response;
    }

    private function createTlsCredentials(): ChannelCredentials
    {
        if (!$this->tlsConfig instanceof \CrazyGoat\TiKV\Client\Tls\TlsConfig) {
            throw new \RuntimeException('TLS config is required for TLS credentials');
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
