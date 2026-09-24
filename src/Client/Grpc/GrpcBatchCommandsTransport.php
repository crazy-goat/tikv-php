<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsCorrelator;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamInterface;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamPool;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsTransportInterface;
use CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException;

/**
 * Real {@see BatchCommandsTransportInterface}: drives the per-store
 * BatchCommands stream over ext-grpc (issue #418).
 *
 * A stream that fails (closed by the peer, deadline exceeded, wire error)
 * is discarded from the pool; the next exchange re-opens it. The caller
 * (BatchCommandsMultiplexer's user) treats any throw as a signal to fall
 * back to the unary path.
 */
final readonly class GrpcBatchCommandsTransport implements BatchCommandsTransportInterface
{
    public function __construct(
        private BatchCommandsStreamPool $pool,
    ) {
    }

    public function exchange(
        string $address,
        BatchCommandsRequest $request,
        array $requestIds,
        int $timeoutMs,
    ): array {
        $stream = $this->pool->get($address);

        try {
            $stream->send($request);

            return BatchCommandsCorrelator::drain(
                $requestIds,
                fn (): ?BatchCommandsResponse => $stream->recv(),
                $timeoutMs,
            );
        } catch (\Throwable $e) {
            // Dead stream (closed by the peer, deadline fired, transport
            // error) — drop it so the next exchange re-opens one.
            $this->pool->discard($address);
            throw new BatchCommandsStreamException(
                sprintf('BatchCommands exchange to %s failed: %s', $address, $e->getMessage()),
                0,
                $e,
            );
        }
    }

    public function close(): void
    {
        $this->pool->close();
    }

    /**
     * Production wiring: a pool whose factory opens a real
     * BatchCommandsConnection on the GrpcClient's pooled channel for the
     * address (one stream per store, reused across round trips).
     */
    public static function forClient(GrpcClientInterface $grpc): self
    {
        return new self(new BatchCommandsStreamPool(
            static fn (string $address): BatchCommandsStreamInterface =>
                BatchCommandsConnection::open($grpc->getChannel($address)),
        ));
    }
}
