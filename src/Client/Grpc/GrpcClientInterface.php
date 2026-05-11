<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;
use Psr\Log\LoggerInterface;

interface GrpcClientInterface
{
    /**
     * Create a new gRPC client.
     *
     * @param LoggerInterface $logger PSR-3 logger instance
     * @param TlsConfig|null $tlsConfig Optional TLS configuration for secure connections
     */
    public function __construct(
        LoggerInterface $logger = new \Psr\Log\NullLogger(),
        ?TlsConfig $tlsConfig = null,
    );

    /**
     * Execute a gRPC call.
     *
     * @template T of Message
     * @param string $address Target address (e.g., "127.0.0.1:2379")
     * @param string $service Service name (e.g., "pdpb.PD")
     * @param string $method Method name (e.g., "GetRegion")
     * @param Message $request Protobuf request message
     * @param class-string<T> $responseClass Response message class name
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
     * @return T Response message
     * @throws \CrazyGoat\TiKV\Client\Exception\GrpcException On gRPC error
     */
    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message;

    /**
     * Close all open channels and release resources.
     */
    public function close(): void;

    /**
     * Close a single channel by address, forcing reconnect on next call.
     *
     * @param string $address Channel address to close
     */
    public function closeChannel(string $address): void;

    /**
     * Get or create a gRPC channel for the given address.
     *
     * @param string $address Target address (e.g., "127.0.0.1:2379")
     * @return Channel The gRPC channel
     */
    public function getChannel(string $address): Channel;
}
