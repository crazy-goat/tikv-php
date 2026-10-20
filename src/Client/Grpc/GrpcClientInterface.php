<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

/**
 * Transport contract: every call carries a deadline.
 *
 * A call's `$timeoutMs` argument is read as follows (issue #260):
 *
 * - a positive int — that many milliseconds from now;
 * - `null` — the **library default**, {@see GrpcClient::DEFAULT_TIMEOUT_MS}
 *   (30 s). `null` used to mean "no deadline", which is why a blocking
 *   `Grpc\Call::startBatch()` on a half-open connection could pin a worker
 *   past `max_execution_time`;
 * - a non-positive int (`0`) — the explicit opt-out: no deadline at all.
 *
 * The sentinel is `0` rather than a separate method because it is how the
 * rest of the library already spells "disabled" (`TimeoutConfig`'s
 * `batchDeadlineMs = 0`), and because a call site must be able to decide
 * per call.
 *
 * The ext-grpc semantics behind {@see GrpcClientInterface::close()} — a
 * `Grpc\Channel` is reference-counted and kept in ext-grpc's process-global
 * persistent table — are documented on {@see GrpcClient}: closing one
 * transport never invalidates a channel another transport in the same process
 * holds (issue #283).
 */
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
     * @param int|null $timeoutMs Deadline in milliseconds; null = the library
     *                            default ({@see GrpcClient::DEFAULT_TIMEOUT_MS}),
     *                            0 = no deadline
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
     * @param int|null $timeoutMs Deadline in milliseconds; null = the library
     *                            default ({@see GrpcClient::DEFAULT_TIMEOUT_MS}),
     *                            0 = no deadline
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
     * @param int|null $timeoutMs Deadline in milliseconds; null = the library
     *                            default ({@see GrpcClient::DEFAULT_TIMEOUT_MS}),
     *                            0 = no deadline
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
     * Close all open channels of this client.
     *
     * **Ownership.** The transport belongs to whoever created it:
     *
     * - a client built by `RawKvClient::create()` / `TxnKvClient::create()`
     *   creates the transport itself and closes it here;
     * - a transport the application passes in (the `grpcClient` option) stays
     *   the property of the application — the client never closes it, so the
     *   same transport can be shared by several clients. Close it yourself
     *   when the application is done with it;
     * - a client assembled by hand (`new RawKvClient($pdClient, $grpc)`) is
     *   given a transport it did not create and closes it here anyway — pass
     *   `ownsGrpcClient: false` to keep the ownership with the application.
     *
     * "Close" means this client's handles: ext-grpc keeps the underlying
     * channel in its process-global persistent table, so the TCP connection
     * stays warm in the process (verified against grpc 1.83.0) — this call
     * does not tear the connection down, and it never invalidates a channel
     * another transport in the same process holds (issue #283).
     *
     * Closing twice is safe: the second call is a no-op. Every call on a
     * closed transport throws
     * {@see \CrazyGoat\TiKV\Client\Exception\InvalidStateException}.
     */
    public function close(): void;

    /**
     * Close a single channel by address.
     *
     * The cached handle for that address is dropped, so the next call gets a
     * new `Grpc\Channel` object — but **not** a new connection: ext-grpc
     * keeps the channel in its process-global persistent table, so the new
     * handle resolves to the same underlying `grpc_channel` and the same TCP
     * connection (verified against grpc 1.83.0). Forcing a real reconnect
     * needs a new persistent entry and is a separate concern.
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
