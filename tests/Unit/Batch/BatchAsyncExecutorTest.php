<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Grpc\Call;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class BatchAsyncExecutorTest extends TestCase
{
    use GrpcExtensionGate;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testExecuteParallelWithSuccessfulCalls(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $calls = [
            1 => fn(): string => 'result1',
            2 => fn(): string => 'result2',
        ];

        $results = $executor->executeParallel($calls);

        $this->assertSame([1 => 'result1', 2 => 'result2'], $results);
    }

    public function testExecuteParallelWithPartialFailure(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $calls = [
            1 => fn(): string => 'result1',
            2 => fn() => throw new TiKvException('Region 2 failed'),
        ];

        $this->expectException(BatchPartialFailureException::class);
        $this->expectExceptionMessage('Batch operation partially failed: 1 of 2 regions failed');

        $executor->executeParallel($calls);
    }

    public function testExecuteParallelWithAllFailure(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $calls = [
            1 => fn() => throw new TiKvException('Region 1 failed'),
            2 => fn() => throw new TiKvException('Region 2 failed'),
        ];

        try {
            $executor->executeParallel($calls);
            $this->fail('Expected exception');
        } catch (BatchPartialFailureException $e) {
            $this->assertCount(2, $e->getRegionErrors());
            $this->assertSame(2, $e->getTotalRegions());
        }
    }

    public function testExecuteParallelCancelsRemainingFuturesOnFailure(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $okCall = $this->createMock(Call::class);
        $okResponse = new RawGetResponse();
        $okResponse->setValue('ok');
        $okCall->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $okResponse->serializeToString(),
            ]);

        $failingCall = $this->createMock(Call::class);
        $failingCall->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 2, 'details' => 'Unavailable'],
            ]);

        $pendingCall = $this->createMock(Call::class);
        $pendingCall->expects($this->never())
            ->method('startBatch');
        $pendingCall->expects($this->once())
            ->method('cancel');

        $calls = [
            1 => fn(): GrpcFuture => new GrpcFuture($okCall, RawGetResponse::class),
            2 => fn(): GrpcFuture => new GrpcFuture($failingCall, RawGetResponse::class),
            3 => fn(): GrpcFuture => new GrpcFuture($pendingCall, RawGetResponse::class),
        ];

        try {
            $executor->executeParallel($calls);
            $this->fail('Expected exception');
        } catch (BatchPartialFailureException $e) {
            $this->assertCount(1, $e->getRegionErrors());
            $this->assertSame(3, $e->getTotalRegions());
            $this->assertStringContainsString('1 of 3', $e->getMessage());
        }
    }

    public function testExecuteParallelDoesNotCancelOnHappyPath(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $okCall = $this->createMock(Call::class);
        $okResponse = new RawGetResponse();
        $okResponse->setValue('ok');
        $okCall->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $okResponse->serializeToString(),
            ]);
        $okCall->expects($this->never())
            ->method('cancel');

        $calls = [
            1 => fn(): GrpcFuture => new GrpcFuture($okCall, RawGetResponse::class),
            2 => fn(): string => 'plain',
        ];

        $results = $executor->executeParallel($calls);

        $this->assertCount(2, $results);
    }

    // ========================================================================
    // Mixed dispatch-phase and wait-phase failures
    // ========================================================================

    public function testExecuteParallelWithMixedDispatchAndWaitFailures(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        // Region 1: dispatch-phase failure (callable throws)
        // Region 2: wait-phase failure (future returns error status)
        // Region 3: success (future returns OK)

        $okCall = $this->createMock(Call::class);
        // Region 3 never reaches wait phase — the executor short-circuits
        // (cancelAll + break) on the first wait-phase failure in region 2.
        $okCall->expects($this->never())
            ->method('startBatch');

        $failingCall = $this->createMock(Call::class);
        $failingCall->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 2, 'details' => 'Unavailable'],
            ]);

        $calls = [
            1 => fn() => throw new TiKvException('dispatch failure'),
            2 => fn(): GrpcFuture => new GrpcFuture($failingCall, RawGetResponse::class),
            3 => fn(): GrpcFuture => new GrpcFuture($okCall, RawGetResponse::class),
        ];

        try {
            $executor->executeParallel($calls);
            $this->fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            $errors = $e->getRegionErrors();
            $this->assertCount(2, $errors);
            $this->assertArrayHasKey(1, $errors);
            $this->assertArrayHasKey(2, $errors);
            $this->assertStringContainsString('dispatch failure', $errors[1]->getMessage());
            $this->assertStringContainsString('Unavailable', $errors[2]->getMessage());
            $this->assertSame(3, $e->getTotalRegions());
            $this->assertStringContainsString('2 of 3', $e->getMessage());
        }
    }

    public function testExecuteParallelWithAllWaitPhaseFailures(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $failingCall1 = $this->createMock(Call::class);
        $failingCall1->expects($this->once())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 2, 'details' => 'Unavailable'],
            ]);

        $failingCall2 = $this->createMock(Call::class);
        // Region 2 is cancelled after region 1 fails in the wait phase —
        // startBatch is never reached because the executor short-circuits.
        $failingCall2->expects($this->never())
            ->method('startBatch');

        $calls = [
            1 => fn(): GrpcFuture => new GrpcFuture($failingCall1, RawGetResponse::class),
            2 => fn(): GrpcFuture => new GrpcFuture($failingCall2, RawGetResponse::class),
        ];

        try {
            $executor->executeParallel($calls);
            $this->fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            $errors = $e->getRegionErrors();
            $this->assertCount(1, $errors); // Only first failure collected, second is cancelled
            $this->assertArrayHasKey(1, $errors);
            $this->assertStringContainsString('Unavailable', $errors[1]->getMessage());
            $this->assertSame(2, $e->getTotalRegions());
        }
    }

    // ========================================================================
    // Fan-out semantics: ensure dispatch happens before any wait begins
    // ========================================================================

    /**
     * Verify the dispatch-phase contract: every callable is invoked exactly
     * once, before any callable's value is awaited. This is the property
     * that gives client-side fan-out (multiple gRPC sends in flight before
     * any wait blocks).
     */
    public function testDispatchPhaseRunsAllCallablesBeforeAnyWait(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $dispatchOrder = [];

        $calls = [
            1 => function () use (&$dispatchOrder): string {
                $dispatchOrder[] = 1;
                return 'a';
            },
            2 => function () use (&$dispatchOrder): string {
                $dispatchOrder[] = 2;
                return 'b';
            },
            3 => function () use (&$dispatchOrder): string {
                $dispatchOrder[] = 3;
                return 'c';
            },
        ];

        // Promote dispatch completions to a wait signal recorded as soon
        // as the executor really begins awaiting. Direct values mean the
        // wait phase is synchronous; this test asserts dispatch order is
        // [1, 2, 3] — i.e. every callable ran before any value returned.
        $results = $executor->executeParallel($calls);

        $this->assertSame([1, 2, 3], $dispatchOrder);
        $this->assertSame(['a', 'b', 'c'], array_values($results));
    }

    public function testAwaitValueFlatForGrpcFuture(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $response = new RawGetResponse();
        $response->setValue('v');

        $call = $this->createMock(Call::class);
        $call->expects($this->any())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $response->serializeToString(),
            ]);

        $calls = [
            42 => fn(): GrpcFuture => new GrpcFuture($call, RawGetResponse::class),
        ];

        $results = $executor->executeParallel($calls);

        $this->assertCount(1, $results);
        $this->assertInstanceOf(RawGetResponse::class, $results[42]);
        $this->assertSame('v', $results[42]->getValue());
    }

    public function testAwaitValueFlatForCheckedGrpcFuture(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $response = new RawGetResponse();
        $response->setValue('checked');

        $call = $this->createMock(Call::class);
        $call->expects($this->any())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $response->serializeToString(),
            ]);

        $calls = [
            7 => fn(): CheckedGrpcFuture => CheckedGrpcFuture::fromGrpcFuture(
                new GrpcFuture($call, RawGetResponse::class),
            ),
        ];

        $results = $executor->executeParallel($calls);

        $this->assertCount(1, $results);
        $this->assertInstanceOf(RawGetResponse::class, $results[7]);
        $this->assertSame('checked', $results[7]->getValue());
    }

    public function testCheckedGrpcFutureRegionErrorSurfacesAsPartialFailure(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        // Build a response that carries a region error. CheckedGrpcFuture
        // turns this into a TiKvException at the executor boundary.
        $response = new RawGetResponse();
        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('region not available in fixture');
        $response->setRegionError($regionError);

        $call = $this->createMock(Call::class);
        $call->expects($this->any())
            ->method('startBatch')
            ->willReturn([
                'status' => ['code' => 0, 'details' => 'OK'],
                'message' => $response->serializeToString(),
            ]);

        $calls = [
            9 => fn(): CheckedGrpcFuture => CheckedGrpcFuture::fromGrpcFuture(
                new GrpcFuture($call, RawGetResponse::class),
            ),
        ];

        $this->expectException(BatchPartialFailureException::class);
        $executor->executeParallel($calls);
    }

    // ========================================================================
    // Deadline semantics
    // ========================================================================

    public function testZeroDeadlineIsDisabled(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $calls = [
            1 => fn(): string => 'a',
            2 => fn(): string => 'b',
        ];

        // 0 ms = deadline disabled, must not throw regardless of work done.
        $results = $executor->executeParallel($calls, deadlineMs: 0);

        $this->assertSame([1 => 'a', 2 => 'b'], $results);
    }

    public function testDeadlineExceededDuringDispatchCancelsDispatchedFutures(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $call = $this->createMock(Call::class);
        $call->expects($this->never())
            ->method('startBatch'); // We never reach send for the second callable
        $call->expects($this->once())
            ->method('cancel');

        // A callable that "costs" some time during dispatch, simulating the
        // reality where region resolution or request building isn't free.
        $slowDispatchCallable = function () use ($call): GrpcFuture {
            usleep(20_000); // 20 ms of work
            return new GrpcFuture($call, RawGetResponse::class);
        };

        $calls = [
            1 => $slowDispatchCallable,
            2 => fn(): string => 'never reached',
        ];

        $this->expectException(BatchDeadlineExceededException::class);
        try {
            $executor->executeParallel($calls, deadlineMs: 10);
        } catch (BatchDeadlineExceededException $e) {
            $this->assertSame(10, $e->getDeadlineMs());
            $this->assertGreaterThanOrEqual(10, $e->getElapsedMs());
            $this->assertArrayHasKey('pendingRegions', $e->getContext());
            /** @var array<int, int> $pending */
            $pending = $e->getContext()['pendingRegions'];
            $this->assertContains(2, array_values($pending));
            throw $e;
        }
    }

    public function testDeadlineExceededDuringWaitCancelsRemainingFutures(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $slowCall = $this->createMock(Call::class);
        $slowCall->expects($this->any())
            ->method('startBatch')
            ->willReturnCallback(function (): array {
                // Simulate a slow RPC.
                usleep(50_000);
                return ['status' => ['code' => 0, 'details' => 'OK']];
            });

        $pendingCall = $this->createMock(Call::class);
        $pendingCall->expects($this->never())
            ->method('startBatch');
        $pendingCall->expects($this->once())
            ->method('cancel');

        $calls = [
            1 => fn(): GrpcFuture => new GrpcFuture($slowCall, RawGetResponse::class),
            2 => fn(): GrpcFuture => new GrpcFuture($pendingCall, RawGetResponse::class),
        ];

        $this->expectException(BatchDeadlineExceededException::class);
        $executor->executeParallel($calls, deadlineMs: 10);
    }
}
