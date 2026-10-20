<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcResponseParser;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Google\Protobuf\Internal\Message;
use Grpc\Call;
use PHPUnit\Framework\TestCase;

class GrpcFutureTest extends TestCase
{
    use GrpcExtensionGate;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    protected function tearDown(): void
    {
        // The message-size limit is a process-wide setting: a test that
        // lowers it must not leak into the next one.
        GrpcResponseParser::setMaxMessageSize(0);
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

    public function testWaitThrowsWhenOkStatusCarriesNoMessage(): void
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
        $this->expectExceptionMessage('no response body');
        $future->wait();
    }

    public function testWaitReturnsEmptyMessageForZeroLengthBody(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => '',
            ]);

        $result = (new GrpcFuture($call, RawGetResponse::class))->wait();

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

    // ========================================================================
    // Settlement hook (issue #278)
    // ========================================================================

    public function testOnSettledFiresOnceOnSuccess(): void
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

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        $future->wait();

        $this->assertSame([true], $settled);
    }

    public function testOnSettledFiresOnceOnError(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 2, 'details' => 'Unavailable'],
            ]);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
        } catch (GrpcException) {
        }

        $this->assertSame([false], $settled);
    }

    public function testOnSettledFiresOnceOnCancellation(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())->method('cancel');

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        $future->cancel();

        $this->assertSame([false], $settled);
    }

    public function testOnSettledFiresOnlyOnceOnDoubleSettle(): void
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
        $call->expects($this->never())->method('cancel');

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        $future->wait();
        $future->cancel();

        $this->assertSame([true], $settled);
    }

    // ========================================================================
    // The settlement hook fires on every exit path of wait() (#278)
    //
    // A path that does not settle keeps the in-flight slot of the channel
    // busy forever: the channel can then never be evicted.
    // ========================================================================

    public function testOnSettledFiresOnceWhenTheReceiveBatchFails(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willThrowException(new \RuntimeException('channel closed'));

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the failed receive batch to throw');
        } catch (\RuntimeException $e) {
            $this->assertSame('channel closed', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testOnSettledFiresOnceWhenTheStatusCannotBeRead(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                // No status at all: the event does not prove that the call
                // succeeded.
                'message' => '',
            ]);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the unreadable status to throw');
        } catch (GrpcException $e) {
            $this->assertStringContainsString('no status', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testOnSettledFiresOnceWhenTheStatusCodeHasAnUnexpectedType(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => '0', 'details' => 'OK'],
                'message' => '',
            ]);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the malformed status to throw');
        } catch (GrpcException $e) {
            $this->assertStringContainsString('unexpected type', $e->getMessage());
        }

        $this->assertSame([false], $settled);
    }

    public function testOnSettledFiresOnceWhenTheResponseCannotBeDeserialized(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => null,
            ]);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the missing response body to throw');
        } catch (GrpcException $e) {
            $this->assertStringContainsString('no response body', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testOnSettledFiresOnceWhenTheResponseIsTooLarge(): void
    {
        $response = new RawGetResponse();
        $response->setValue('a response that does not fit');
        $serialized = $response->serializeToString();

        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $serialized,
            ]);

        GrpcResponseParser::setMaxMessageSize(1);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the oversized response to throw');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString('exceeds maximum allowed', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testOnSettledFiresOnceWhenTheWaiterThrowsAGrpcException(): void
    {
        $settled = [];
        $future = new GrpcFuture(
            null,
            RawGetResponse::class,
            static function (): Message {
                throw new GrpcException('Unavailable', 2);
            },
            null,
            function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the failing waiter to throw');
        } catch (GrpcException $e) {
            $this->assertStringContainsString('Unavailable', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testOnSettledFiresOnceWhenTheWaiterThrowsANonGrpcException(): void
    {
        $settled = [];
        $future = new GrpcFuture(
            null,
            RawGetResponse::class,
            static function (): Message {
                throw new \RuntimeException('boom');
            },
            null,
            function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        try {
            $future->wait();
            $this->fail('Expected the failing waiter to throw');
        } catch (\RuntimeException $e) {
            $this->assertSame('boom', $e->getMessage());
        }

        $this->assertSame([false], $settled);
        $this->assertTrue($future->isCompleted());
    }

    public function testAFailedWaitRethrowsTheSameFailureAndSettlesOnlyOnce(): void
    {
        $call = $this->createMock(Call::class);
        $call->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'message' => '',
            ]);

        $settled = [];
        $future = new GrpcFuture(
            $call,
            RawGetResponse::class,
            onSettled: function (bool $success) use (&$settled): void {
                $settled[] = $success;
            },
        );

        $first = null;
        try {
            $future->wait();
        } catch (GrpcException $e) {
            $first = $e;
        }

        $second = null;
        try {
            $future->wait();
        } catch (GrpcException $e) {
            $second = $e;
        }

        $this->assertInstanceOf(GrpcException::class, $first);
        $this->assertSame($first, $second);

        // A cancel after the failure must not settle the future a second
        // time: the channel cache would release a slot another call still
        // occupies.
        $future->cancel();

        $this->assertSame([false], $settled);
    }
}
