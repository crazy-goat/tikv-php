<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Grpc\Call;
use PHPUnit\Framework\TestCase;

class GrpcFutureTest extends TestCase
{
    use GrpcExtensionGate;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testConstruction(): void
    {
        $call = $this->createMock(Call::class);
        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->assertInstanceOf(GrpcFuture::class, $future);
    }

    public function testWaitReturnsDeserializedMessageOnSuccess(): void
    {
        $response = new RawGetResponse();
        $response->setValue('hello');
        $serialized = $response->serializeToString();

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $serialized,
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $result = $future->wait();

        $this->assertInstanceOf(RawGetResponse::class, $result);
        /** @var RawGetResponse $result */
        $this->assertSame('hello', $result->getValue());
    }

    public function testWaitThrowsGrpcExceptionOnNonOkStatus(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 2, 'details' => 'Unavailable'],
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('Unavailable');
        $future->wait();
    }

    public function testWaitReturnsEmptyMessageOnNullResult(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => null,
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $result = $future->wait();

        $this->assertInstanceOf(RawGetResponse::class, $result);
        $this->assertSame('', $result->getValue());
    }

    public function testWaitIsIdempotentReturnsCachedResult(): void
    {
        $response = new RawGetResponse();
        $response->setValue('cached');
        $serialized = $response->serializeToString();

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $serialized,
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);
        $result1 = $future->wait();
        $result2 = $future->wait();

        /** @var RawGetResponse $result1 */
        /** @var RawGetResponse $result2 */
        $this->assertSame('cached', $result1->getValue());
        $this->assertSame('cached', $result2->getValue());
    }

    public function testWaitIsIdempotentReThrowsCachedError(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 4, 'details' => 'DeadlineExceeded'],
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('DeadlineExceeded');

        try {
            $future->wait();
        } catch (GrpcException) {
        }

        $future->wait();
    }

    public function testCancelMarksCompletedAndCallsUnderlyingCall(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->assertFalse($future->isCompleted());
        $future->cancel();
        $this->assertTrue($future->isCompleted());
    }

    public function testCancelIsIdempotent(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);

        $future->cancel();
        $future->cancel();
        $future->cancel();

        $this->assertTrue($future->isCompleted());
    }

    public function testWaitAfterCancelThrowsCancelledException(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);
        $future->cancel();

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('Call cancelled');
        $future->wait();
    }

    public function testCancelAfterWaitIsNoOp(): void
    {
        $response = new RawGetResponse();
        $response->setValue('done');
        $serialized = $response->serializeToString();

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $serialized,
            ]);
        $call->expects($this->never())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);
        $future->wait();
        $future->cancel();

        $this->assertTrue($future->isCompleted());
    }

    public function testDestructWithoutWaitCancelsPendingCall(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);
        unset($future);

        $this->expectNotToPerformAssertions();
    }

    public function testDestructAfterWaitDoesNotCancel(): void
    {
        $response = new RawGetResponse();
        $response->setValue('done');
        $serialized = $response->serializeToString();

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $serialized,
            ]);
        $call->expects($this->never())
            ->method('cancel');

        $future = new GrpcFuture($call, RawGetResponse::class);
        $future->wait();
        unset($future);

        $this->expectNotToPerformAssertions();
    }

    public function testCancelSwallowsUnderlyingCallException(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('cancel')
            ->willThrowException(new \RuntimeException('boom'));

        $future = new GrpcFuture($call, RawGetResponse::class);
        $future->cancel();

        $this->assertTrue($future->isCompleted());
    }
}
