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
    private bool $settled = false;
    private ?Message $result = null;

    /**
     * The failure the call ended with, if any.
     *
     * Not only a {@see GrpcException}: a status that cannot be read and a
     * response that cannot be deserialized raise one, but a receive batch
     * that fails and a waiter that throws may raise anything at all. The
     * original throwable is kept and rethrown unchanged, so a caller sees
     * the same failure on every `wait()` of the same future.
     */
    private ?\Throwable $error = null;

    /**
     * @param class-string<Message> $responseClass
     * @param (\Closure(): Message)|null $waiter Synthetic completion source
     *                                           (test fakes / decorators);
     *                                           when set, `wait()` resolves
     *                                           through it instead of the
     *                                           gRPC call.
     * @param (\Closure(): void)|null $canceller Optional callback invoked
     *                                             when a waiter-backed future
     *                                             is cancelled.
     * @param (\Closure(bool): void)|null $onSettled Invoked exactly once when the
     *                                               call settles — the response
     *                                               was read, the call failed, the
     *                                               response could not be read, or
     *                                               the future was cancelled. The
     *                                               argument tells the caller
     *                                               whether the call succeeded.
     *                                               The channel cache uses the
     *                                               hook to release the in-flight
     *                                               slot of the channel the call
     *                                               ran on (issue #278).
     */
    public function __construct(
        private readonly ?Call $call,
        /** @var class-string<Message> */
        private readonly string $responseClass,
        private readonly ?\Closure $waiter = null,
        private readonly ?\Closure $canceller = null,
        private readonly ?\Closure $onSettled = null,
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
    public static function fromWaiter(?\Closure $waiter, ?\Closure $canceller = null): self
    {
        return new self(null, Message::class, $waiter, $canceller);
    }

    /**
     * Wait for the response of the call and return it.
     *
     * Every exit path of this method settles the future exactly once: the
     * response was read, the gRPC status was not OK, the status could not be
     * read, the response could not be deserialized, the receive batch failed
     * or the waiter threw. A path that did not settle would keep the
     * channel's in-flight slot busy forever, and the channel could then
     * never be evicted (issue #278).
     *
     * @throws GrpcException When the call failed or its response could not be read
     * @throws \Throwable     The failure of the underlying call or of the waiter,
     *                        rethrown unchanged
     */
    public function wait(): Message
    {
        if ($this->completed) {
            if ($this->error instanceof \Throwable) {
                throw $this->error;
            }
            if (!$this->result instanceof Message) {
                throw new GrpcException('Unexpected null result', GrpcStatusCode::Internal->value);
            }
            return $this->result;
        }

        if ($this->waiter instanceof \Closure) {
            try {
                $result = ($this->waiter)();
            } catch (\Throwable $e) {
                // The waiter failed — with a gRPC error or with anything
                // else: the call is over, so the in-flight slot of its
                // channel has to be released (#278).
                $this->fail($e);
                throw $e;
            }
            $this->result = $result;
            $this->completed = true;
            $this->settle(true);

            return $result;
        }

        $call = $this->call;
        if (!$call instanceof \Grpc\Call) {
            // Unreachable: the constructor guarantees a Call or a waiter.
            $error = new GrpcException('GrpcFuture has no call and no waiter', GrpcStatusCode::Internal->value);
            $this->fail($error);

            throw $error;
        }

        try {
            $event = $call->startBatch([
                \Grpc\OP_RECV_INITIAL_METADATA => true,
                \Grpc\OP_RECV_MESSAGE => true,
                \Grpc\OP_RECV_STATUS_ON_CLIENT => true,
            ]);
        } catch (\Throwable $e) {
            // The receive batch failed (a closed channel, for instance):
            // the call is over, so the in-flight slot of its channel has
            // to be released even though no response will ever arrive.
            $this->fail($e);
            throw $e;
        }

        try {
            $status = GrpcResponseParser::extractStatus($event);

            if ($status['code'] !== \Grpc\STATUS_OK) {
                throw new GrpcException($status['details'], $status['code']);
            }

            $result = GrpcResponseParser::deserialize($event, $this->responseClass);
        } catch (\Throwable $e) {
            // The status could not be read, or the response could not be
            // deserialized: the call is over either way, so the in-flight
            // slot of its channel has to be released (#278).
            $this->fail($e);
            throw $e;
        }

        $this->result = $result;
        $this->completed = true;
        $this->settle(true);

        return $result;
    }

    public function cancel(): void
    {
        if ($this->completed) {
            return;
        }

        $this->error = new GrpcException('Call cancelled', GrpcStatusCode::Cancelled->value);
        $this->completed = true;

        if ($this->canceller instanceof \Closure) {
            try {
                ($this->canceller)();
            } catch (\Throwable) {
            }
        }

        // Swallow any throwable from the underlying call: cancel() must
        // never propagate, especially from __destruct() during shutdown.
        $call = $this->call;
        if ($call instanceof \Grpc\Call) {
            try {
                $call->cancel();
            } catch (\Throwable) {
            }
        }

        $this->settle(false);
    }

    /**
     * Record a failed call and release the channel's in-flight slot.
     *
     * Every failure path of {@see self::wait()} ends here: the future is
     * completed (its outcome is known), the failure is remembered (a second
     * `wait()` rethrows it) and the settlement hook fires exactly once
     * (issue #278).
     */
    private function fail(\Throwable $error): void
    {
        $this->error = $error;
        $this->completed = true;
        $this->settle(false);
    }

    /**
     * Fire the settlement hook exactly once.
     *
     * A future can reach the settled state twice — a `wait()` that failed
     * followed by a `cancel()`, or a `cancel()` followed by a `wait()` — and
     * the hook must not run twice: the channel cache counts it as the
     * release of one in-flight call, and a double release would free a
     * slot that another call still occupies (issue #278).
     *
     * @param bool $success Whether the call completed successfully
     */
    private function settle(bool $success): void
    {
        if ($this->settled) {
            return;
        }

        $this->settled = true;

        $onSettled = $this->onSettled;
        if (!$onSettled instanceof \Closure) {
            return;
        }

        try {
            $onSettled($success);
        } catch (\Throwable) {
            // The hook is bookkeeping: it must never change the outcome
            // of the call it observes.
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
