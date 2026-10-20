<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Grpc\Call;
use PHPUnit\Framework\TestCase;

class CheckedGrpcFutureTest extends TestCase
{
    use GrpcExtensionGate;

    public function testFromGrpcFutureReturnsInnerOnAccess(): void
    {
        $this->requireGrpcExtension();
        $call = $this->createMock(Call::class);
        $future = new GrpcFuture($call, RawGetResponse::class);
        $checked = CheckedGrpcFuture::fromGrpcFuture($future);

        $this->assertSame($future, $checked->inner());
        $this->assertFalse($checked->isCompleted());
    }

    public function testFromCallableHasNoInnerFuture(): void
    {
        $checked = CheckedGrpcFuture::fromCallable(
            static fn(): RawGetResponse => new RawGetResponse(),
        );

        $this->assertNull($checked->inner());
        $this->assertTrue($checked->isCompleted(), 'fromCallable without inner future is always completed');
    }

    public function testWaitForExecutorRunsUnderlyingWaitAndCheck(): void
    {
        $this->requireGrpcExtension();
        $response = new RawGetResponse();
        $response->setValue('hello');

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $response->serializeToString(),
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $checked = CheckedGrpcFuture::fromGrpcFuture($future);

        $result = $checked->waitForExecutor();
        $this->assertInstanceOf(RawGetResponse::class, $result);
        $this->assertSame('hello', $result->getValue());
        $this->assertTrue($checked->isCompleted());
    }

    public function testWaitForExecutorSurfacesRegionErrorsAsExceptions(): void
    {
        $this->requireGrpcExtension();
        $response = new RawGetResponse();
        // RegionErrorHandler::check() inspects the top-level region_error;
        // set one to exercise the path.
        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('boom');
        $response->setRegionError($regionError);

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $response->serializeToString(),
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $checked = CheckedGrpcFuture::fromGrpcFuture($future);

        $this->expectException(TiKvException::class);
        $checked->waitForExecutor();
    }

    /**
     * Issue #340 [TEST-20]: a decorator built with fromCallable() has to
     * keep the decorated future's inner GrpcFuture reachable.
     *
     * RawKvBatch::wrapWithLogging() rebuilds the future it decorates as
     * fromCallable() whenever a SlowLogConfig is set, and
     * BatchAsyncExecutor::cancelAll() cancels the in-flight futures of a
     * failed batch through CheckedGrpcFuture::inner(). A wrapper that drops
     * the inner future therefore turns the slow log into "no cancellation
     * at all" — the gRPC calls of a failed batch would leak exactly when
     * the process is under stress.
     */
    public function testFromCallableWithInnerFutureKeepsCancellationCapability(): void
    {
        /** @var array<string, int> $cancels */
        $cancels = [];

        $inner = GrpcFuture::fromWaiter(
            static fn(): RawGetResponse => new RawGetResponse(),
            static function () use (&$cancels): void {
                $cancels['inner'] = ($cancels['inner'] ?? 0) + 1;
            },
        );

        $checked = CheckedGrpcFuture::fromCallable(
            static fn(): RawGetResponse => new RawGetResponse(),
            $inner,
        );

        // The inner future stays reachable, and the completion state is the
        // inner future's — not the wrapper's.
        self::assertSame($inner, $checked->inner());
        self::assertFalse($checked->isCompleted());

        $checked->cancel();

        self::assertSame(['inner' => 1], $cancels);
        self::assertTrue($checked->isCompleted());
    }

    public function testCancelDoesNotThrowOrThrow(): void
    {
        // fromGrpcFuture: cancel reaches the underlying GrpcFuture.
        $this->requireGrpcExtension();
        $call = $this->createMock(Call::class);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $checked = CheckedGrpcFuture::fromGrpcFuture($future);
        $checked->cancel();

        // fromCallable: cancel is a no-op (no underlying I/O to cancel).
        $synthetic = CheckedGrpcFuture::fromCallable(
            static fn(): \Google\Protobuf\Internal\Message => new RawGetResponse(),
        );
        $synthetic->cancel();
        $this->assertTrue($synthetic->isCompleted());
    }
}
