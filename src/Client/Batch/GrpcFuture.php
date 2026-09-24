<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcResponseParser;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use Google\Protobuf\Internal\Message;
use Grpc\Call;

final class GrpcFuture
{
    private bool $completed = false;
    private ?Message $result = null;
    private ?GrpcException $error = null;

    /**
     * @param class-string<Message> $responseClass
     * @param (\Closure(): Message)|null $waiter Synthetic completion source
     *                                           (test fakes / decorators);
     *                                           when set, `wait()` resolves
     *                                           through it instead of the
     *                                           gRPC call.
     */
    public function __construct(
        private readonly ?Call $call,
        /** @var class-string<Message> */
        private readonly string $responseClass,
        private readonly ?\Closure $waiter = null,
    ) {
    }

    /**
     * A future that is already resolved. Building block for test fakes of
     * {@see \CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface::callAsync()}
     * that need no ext-grpc (no `Grpc\Call` is touched).
     */
    public static function completed(Message $response): self
    {
        $future = new self(null, $response::class);
        $future->result = $response;
        $future->completed = true;

        return $future;
    }

    /**
     * A future whose result is produced lazily by $waiter on the first
     * `wait()`. Used by fakes that must model server-side latency between
     * the dispatch (callAsync) and the receive (wait) phases so that
     * client-side fan-out is measurable.
     *
     * @param (\Closure(): Message)|null $waiter
     */
    public static function fromWaiter(?\Closure $waiter): self
    {
        return new self(null, Message::class, $waiter);
    }

    public function wait(): Message
    {
        if ($this->completed) {
            if ($this->error instanceof \CrazyGoat\TiKV\Client\Exception\GrpcException) {
                throw $this->error;
            }
            if (!$this->result instanceof \Google\Protobuf\Internal\Message) {
                throw new GrpcException('Unexpected null result', GrpcStatusCode::Internal->value);
            }
            return $this->result;
        }

        if ($this->waiter instanceof \Closure) {
            try {
                $this->result = ($this->waiter)();
            } catch (GrpcException $e) {
                $this->error = $e;
                $this->completed = true;
                throw $e;
            }
            $this->completed = true;

            return $this->result;
        }

        $call = $this->call;
        if (!$call instanceof \Grpc\Call) {
            // Unreachable: the constructor guarantees a Call or a waiter.
            throw new GrpcException('GrpcFuture has no call and no waiter', GrpcStatusCode::Internal->value);
        }

        $event = $call->startBatch([
            \Grpc\OP_RECV_INITIAL_METADATA => true,
            \Grpc\OP_RECV_MESSAGE => true,
            \Grpc\OP_RECV_STATUS_ON_CLIENT => true,
        ]);

        $status = GrpcResponseParser::extractStatus($event);

        if ($status['code'] !== \Grpc\STATUS_OK) {
            $this->error = new GrpcException($status['details'], $status['code']);
            $this->completed = true;
            throw $this->error;
        }

        $this->result = GrpcResponseParser::deserialize($event, $this->responseClass);
        $this->completed = true;

        return $this->result;
    }

    public function cancel(): void
    {
        if ($this->completed) {
            return;
        }

        $this->error = new GrpcException('Call cancelled', GrpcStatusCode::Cancelled->value);
        $this->completed = true;

        // Swallow any throwable from the underlying call: cancel() must
        // never propagate, especially from __destruct() during shutdown.
        $call = $this->call;
        if (!$call instanceof \Grpc\Call) {
            return;
        }
        try {
            $call->cancel();
        } catch (\Throwable) {
        }
    }

    public function isCompleted(): bool
    {
        return $this->completed;
    }

    public function __destruct()
    {
        $this->cancel();
    }
}
