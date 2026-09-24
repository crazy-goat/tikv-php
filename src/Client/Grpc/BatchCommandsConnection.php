<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamInterface;
use Grpc\Call;
use Grpc\Channel;
use Grpc\Timeval;

/**
 * The client half of TiKV's bidirectional BatchCommands stream (issue #418,
 * GAP-04), implemented on ext-grpc's synchronous {@see Call::startBatch()} API.
 *
 * One instance wraps one HTTP/2 stream to one store. The stream is opened
 * with initial metadata only (no half-close) and then reused: every
 * exchange sends one BatchCommandsRequest message and keeps issuing
 * RECV_MESSAGE batches until the correlator has all request_ids — responses
 * may arrive out of order and split across several messages.
 *
 * The stream carries a fixed lifetime deadline (started at open) so that a
 * silently dead store cannot block a RECV batch forever: once the deadline
 * fires, recv() yields null (or startBatch fails) and the transport
 * discards the stream, falling back to unary calls.
 *
 * Not unit-testable without ext-grpc (constructs Grpc\Call directly — same
 * rule as RawKvBatch's async helpers); the protocol logic around it is.
 */
final class BatchCommandsConnection implements BatchCommandsStreamInterface
{
    /**
     * Lifetime of one multiplexing stream. Generous enough to amortise
     * streams over a batch workload, short enough that a dead stream is
     * re-opened instead of poisoning subsequent round trips.
     */
    public const DEFAULT_STREAM_DEADLINE_MS = 60000;

    private function __construct(private readonly Call $call, private bool $initialMetadataReceived = false)
    {
    }

    /**
     * Open a BatchCommands stream on the given channel.
     */
    public static function open(
        Channel $channel,
        int $deadlineMs = self::DEFAULT_STREAM_DEADLINE_MS,
    ): self {
        $deadline = $deadlineMs > 0
            ? Timeval::now()->add(new Timeval($deadlineMs * 1000))
            : Timeval::infFuture();

        $call = new Call($channel, '/tikvpb.Tikv/BatchCommands', $deadline);

        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
        ]);

        return new self($call);
    }

    public function send(BatchCommandsRequest $request): void
    {
        // One OP_SEND_MESSAGE per batch (ext-grpc limitation, same as
        // GrpcClient::callStreaming).
        $this->call->startBatch([
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
        ]);
    }

    public function recv(): ?BatchCommandsResponse
    {
        // ext-grpc quirk: the initial metadata must be received before (or
        // together with) the first message batch, otherwise the recv
        // completes without a message. After the first read, message-only
        // batches are fine.
        $batch = $this->initialMetadataReceived
            ? [\Grpc\OP_RECV_MESSAGE => true]
            : [
                \Grpc\OP_RECV_INITIAL_METADATA => true,
                \Grpc\OP_RECV_MESSAGE => true,
            ];
        $event = $this->call->startBatch($batch);
        $this->initialMetadataReceived = true;

        $payload = (array) $event;
        $raw = $payload['message'] ?? null;

        if (!is_string($raw) || $raw === '') {
            // Stream closed / deadline fired — no message on the wire.
            return null;
        }

        /** @var BatchCommandsResponse */
        return GrpcResponseParser::deserialize($event, BatchCommandsResponse::class);
    }

    public function close(): void
    {
        $this->call->startBatch([
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);
    }
}
