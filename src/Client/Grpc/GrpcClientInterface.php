<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

interface GrpcClientInterface
{
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
     * Issue a unary gRPC call without waiting for the response.
     *
     * The send phase completes before this method returns; the response is
     * resolved later by calling {@see GrpcFuture::wait()}. Enables
     * client-side fan-out: multiple sends can be issued to different
     * regions/stores first and awaited afterwards, so their server-side
     * latencies overlap (issue #295).
     *
     * @template T of Message
     * @param string $address Target address (e.g., "127.0.0.1:2379")
     * @param string $service Service name (e.g., "tikvpb.Tikv")
     * @param string $method Method name (e.g., "RawDeleteRange")
     * @param Message $request Protobuf request message
     * @param class-string<T> $responseClass Response message class name
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
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
    ): GrpcFuture;

    /**
     * Execute a client-streaming gRPC call.
     *
     * Opens a client-streaming RPC: sends a sequence of request messages
     * and receives a single response. Used for SST import (Write/Upload).
     *
     * @template T of Message
     * @param string $address Target address
     * @param string $service Service name (e.g., "import_sstpb.ImportSST")
     * @param string $method Method name (e.g., "Write")
     * @param Message[] $requests Sequence of request messages to stream
     * @param class-string<T> $responseClass Response message class name
     * @param int|null $timeoutMs Optional timeout in milliseconds
     * @return T Response message
     * @throws \CrazyGoat\TiKV\Client\Exception\GrpcException On gRPC error
     */
    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
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
     * No-op if the client has already been closed.
     *
     * @param string $address Channel address to close
     */
    public function closeChannel(string $address): void;

    /**
     * Get or create a gRPC channel for the given address.
     *
     * @param string $address Target address (e.g., "127.0.0.1:2379")
     * @return Channel The gRPC channel
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidStateException When the client has been closed
     */
    public function getChannel(string $address): Channel;
}
