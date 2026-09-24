<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;

/**
 * Compatibility shim for PHPUnit mocks of GrpcClientInterface after the
 * #291 fan-out: the transactional RPC paths (prewrite, commit, rollback,
 * pessimistic lock, batchGet) now dispatch through callAsync() and await
 * the response afterwards.
 *
 * These tests configure the blocking call(); stubbing callAsync() to
 * delegate to whatever call() is configured to do (and wrapping the
 * response in an already-resolved GrpcFuture) keeps every existing
 * willReturnCallback / expectation matcher on call() working unchanged —
 * the mocked call() is what actually runs. Tests that need to model
 * server-side latency between dispatch and wait (the fan-out wall-clock
 * tests) use a hand-written GrpcClientInterface fake instead of this shim.
 */
trait MockGrpcCallAsyncShim
{
    private function shimCallAsync(GrpcClientInterface&MockObject $grpc): void
    {
        $grpc->method('callAsync')->willReturnCallback(
            static function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use ($grpc): GrpcFuture {
                /** @var class-string<Message> $responseClass */
                $response = $grpc->call($address, $service, $method, $request, $responseClass, $timeoutMs);

                return GrpcFuture::completed($response);
            },
        );
    }
}
