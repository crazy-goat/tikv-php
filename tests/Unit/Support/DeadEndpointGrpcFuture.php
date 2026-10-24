<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use Google\Protobuf\Internal\Message;
use Grpc\Call;
use Grpc\Channel;
use Grpc\ChannelCredentials;
use Grpc\Timeval;

/**
 * Stand-in for `GrpcClientInterface::callAsync()` in tests that need a real
 * `GrpcFuture` without a TiKV server (issue #273).
 *
 * The batch fan-out used to build its own `Grpc\Call` on `getChannel()`, so
 * tests handed it a dead channel there. Since it goes through `callAsync()`,
 * the same dead endpoint is served from here: the request is really sent to
 * `127.0.0.1:1`, and waiting on the future fails with a transport error.
 * Needs ext-grpc, so callers gate on `GrpcExtensionGate` first.
 */
final class DeadEndpointGrpcFuture
{
    /**
     * @return \Closure(string, string, string, Message, string, ?int=): GrpcFuture
     */
    public static function callback(): \Closure
    {
        return static fn(
            string $address,
            string $service,
            string $method,
            Message $request,
            string $responseClass,
            ?int $timeoutMs = null,
        ): GrpcFuture => self::future($service, $method, $request, $responseClass, $timeoutMs);
    }

    private static function future(
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs,
    ): GrpcFuture {
        if (!is_subclass_of($responseClass, Message::class)) {
            throw new \InvalidArgumentException(sprintf('%s is not a protobuf message', $responseClass));
        }

        $timeout = $timeoutMs ?? GrpcClient::DEFAULT_TIMEOUT_MS;
        if ($timeout <= 0) {
            throw new InvalidArgumentException(sprintf('timeoutMs must be greater than 0, got %d', $timeout));
        }

        $channel = new Channel('127.0.0.1:1', [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        $call = new Call(
            $channel,
            "/{$service}/{$method}",
            Timeval::now()->add(new Timeval($timeout * 1000)),
        );
        $call->startBatch([
            \Grpc\OP_SEND_INITIAL_METADATA => [],
            \Grpc\OP_SEND_MESSAGE => ['message' => $request->serializeToString()],
            \Grpc\OP_SEND_CLOSE_FROM_CLIENT => true,
        ]);

        return new GrpcFuture($call, $responseClass);
    }
}
