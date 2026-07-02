<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
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
}
