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
     * Number of async calls dispatched but not yet received, and the peak it
     * reached. Used by the issue #789 cap tests: the fan-out must never hold
     * more un-awaited futures than its configured concurrency.
     */
    public int $inFlight = 0;

    public int $maxInFlight = 0;

    /**
     * Deadline passed to every call()/callAsync(), in call order.
     *
     * @var list<array{method: string, timeoutMs: ?int}>
     */
    public array $callTimeouts = [];

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
        $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];
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
        $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];
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

        $this->inFlight++;
        $this->maxInFlight = max($this->maxInFlight, $this->inFlight);

        $responder = $this->responder;
        $latencyMs = $this->latencyMs;
        $settled = false;
        $waiter = function () use (
            $method,
            $request,
            $responder,
            $latencyMs,
            $dispatchAt,
            &$settled,
        ): Message {
            $this->waitLog[] = ['method' => $method, 'at' => microtime(true)];
            $elapsedMs = (microtime(true) - $dispatchAt) * 1000;
            $remainingMs = $latencyMs - $elapsedMs;
            if ($remainingMs > 0) {
                usleep((int) ($remainingMs * 1000));
            }

            if (!$settled) {
                $settled = true;
                $this->inFlight--;
            }

            return $responder($method, $request);
        };

        return GrpcFuture::fromWaiter($waiter);
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        iterable $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        throw new \RuntimeException('callStreaming is not supported by FanoutFakeGrpc');
    }

    public function close(): void
    {
    }

    public function reapIdleChannels(): void
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
