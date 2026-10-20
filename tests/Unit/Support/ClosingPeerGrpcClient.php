<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

/**
 * A transport decorator that closes a **peer** transport the Nth time a
 * channel is handed out (issue #283).
 *
 * The batch fan-out dispatches every sub-batch before it waits for the first
 * one, so the only way to close the peer transport *while a call is in
 * flight* is from inside the dispatch: this decorator closes the peer when
 * the fan-out asks for its second channel — after the first sub-batch has
 * been sent.
 */
final class ClosingPeerGrpcClient implements GrpcClientInterface
{
    private int $channelsHandedOut = 0;

    public function __construct(
        private readonly GrpcClientInterface $inner,
        private readonly GrpcClientInterface $peer,
        private readonly int $closeOnChannel = 2,
    ) {
    }

    public function getChannel(string $address): Channel
    {
        $this->channelsHandedOut++;
        if ($this->channelsHandedOut === $this->closeOnChannel) {
            $this->peer->close();
        }

        return $this->inner->getChannel($address);
    }

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        return $this->inner->call(
            $address,
            $service,
            $method,
            $request,
            $responseClass,
            $timeoutMs,
        );
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        return $this->inner->callAsync(
            $address,
            $service,
            $method,
            $request,
            $responseClass,
            $timeoutMs,
        );
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        return $this->inner->callStreaming(
            $address,
            $service,
            $method,
            $requests,
            $responseClass,
            $timeoutMs,
        );
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function closeChannel(string $address): void
    {
        $this->inner->closeChannel($address);
    }
}
