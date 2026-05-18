<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use Grpc\Call;
use PHPUnit\Framework\TestCase;

class GrpcFutureTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('grpc')) {
            $this->markTestSkipped('grpc extension is not loaded');
        }
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

    public function testWaitThrowsGrpcExceptionOnNullResult(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => null,
            ]);

        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('Unexpected null result');
        $future->wait();
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
}
