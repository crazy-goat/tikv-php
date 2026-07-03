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

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testFromGrpcFutureReturnsInnerOnAccess(): void
    {
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

    public function testCancelDoesNotThrowOrThrow(): void
    {
        // fromGrpcFuture: cancel reaches the underlying GrpcFuture.
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
