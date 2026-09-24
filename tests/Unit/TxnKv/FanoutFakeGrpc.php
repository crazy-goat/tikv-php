<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

/**
 * Hand-written fake of GrpcClientInterface for the #291 fan-out tests.
 *
 * Models server-side latency BETWEEN the dispatch (callAsync) and the
 * receive (GrpcFuture::wait) phases: each send is recorded with its wall
 * time and its waiter sleeps only the latency not yet elapsed since the
 * dispatch, so genuinely parallel sends overlap — exactly the property the
 * fan-out must produce. A blocking client shows ~N x latency; a fanned-out
 * client ~latency per dependency stage.
 *
 * The responder closure decides the response per method/request; throwing
 * from it models a server error (delivered at wait time, like a real
 * response-borne failure).
 */
final class FanoutFakeGrpc implements GrpcClientInterface
{
    /** @var list<array{method: string, at: float}> */
    public array $dispatchLog = [];

    /** @var list<array{method: string, at: float}> */
    public array $waitLog = [];

    public int $callCount = 0;

    /**
     * Secondary keys carried by the most recent async-commit prewrite
     * request.
     *
     * @var list<string>|null
     */
    public ?array $lastPrewriteSecondaries = null;

    public function __construct(
        private readonly float $latencyMs,
        /** @var \Closure(string, Message): Message */
        private readonly \Closure $responder,
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
        $this->callCount++;
        usleep((int) ($this->latencyMs * 1000));

        /** @var Message */
        // @phpstan-ignore return.type (the fake returns the base Message by design)
        return ($this->responder)($method, $request);
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        $this->callCount++;
        $dispatchAt = microtime(true);
        $this->dispatchLog[] = ['method' => $method, 'at' => $dispatchAt];

        if ($method === 'KvPrewrite') {
            /** @var \CrazyGoat\Proto\Kvrpcpb\PrewriteRequest $request */
            $secondaries = [];
            foreach ($request->getSecondaries() as $secondary) {
                $secondaries[] = $secondary;
            }
            if ($request->getUseAsyncCommit()) {
                $this->lastPrewriteSecondaries = $secondaries;
            }
        }

        $responder = $this->responder;
        $latencyMs = $this->latencyMs;
        $waiter = function () use ($method, $request, $responder, $latencyMs, $dispatchAt): Message {
            $this->waitLog[] = ['method' => $method, 'at' => microtime(true)];
            $elapsedMs = (microtime(true) - $dispatchAt) * 1000;
            $remainingMs = $latencyMs - $elapsedMs;
            if ($remainingMs > 0) {
                usleep((int) ($remainingMs * 1000));
            }

            return $responder($method, $request);
        };

        return GrpcFuture::fromWaiter($waiter);
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        throw new \RuntimeException('callStreaming is not supported by FanoutFakeGrpc');
    }

    public function close(): void
    {
    }

    public function closeChannel(string $address): void
    {
    }

    public function getChannel(string $address): Channel
    {
        throw new \RuntimeException('getChannel is not supported by FanoutFakeGrpc');
    }
}
