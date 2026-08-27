<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use PHPUnit\Framework\TestCase;

/**
 * Concurrency-cap and fan-out behaviour introduced by issue #295.
 *
 * The in-flight model mirrors the real wire path: a per-region callable
 * issues its gRPC send during the dispatch phase (in-flight count grows)
 * and the response is awaited during the wait phase (count shrinks). The
 * simulated latency lives in the future's wait, exactly where server-side
 * latency blocks the real client.
 */
final class ParallelRangeOpsCapTest extends TestCase
{
    /** @var int highest simultaneously in-flight request count observed */
    private int $maxInFlight = 0;

    private int $inFlight = 0;

    /**
     * A callable modelling one RPC: the send registers in-flight and starts
     * the server-side processing clock ($latencyUs from NOW); the wait
     * releases the slot once the simulated response would have arrived.
     *
     * Because the "server" keeps processing while other futures are being
     * dispatched or awaited, later waits in a window sleep (close to) zero —
     * exactly how real gRPC fan-out overlaps server-side latencies.
     */
    private function rpcCallable(int $latencyUs): callable
    {
        return function () use ($latencyUs): CheckedGrpcFuture {
            $this->inFlight++;
            $this->maxInFlight = max($this->maxInFlight, $this->inFlight);
            $respondAtNs = hrtime(true) + $latencyUs * 1000;

            return CheckedGrpcFuture::fromCallable(function () use ($respondAtNs): RawDeleteRangeResponse {
                $remainingNs = $respondAtNs - hrtime(true);
                if ($remainingNs > 0) {
                    usleep((int) ceil($remainingNs / 1000));
                }
                $this->inFlight--;
                assert($this->inFlight >= 0);

                return new RawDeleteRangeResponse();
            });
        };
    }

    public function testCappedExecutionNeverExceedsMaxConcurrency(): void
    {
        $executor = new BatchAsyncExecutor();
        $total = 50;
        $cap = 16;
        $calls = [];
        for ($i = 0; $i < $total; $i++) {
            $calls[$i] = $this->rpcCallable(1000);
        }

        $results = $executor->executeParallelCapped($calls, $cap);

        $this->assertCount($total, $results);
        $this->assertLessThanOrEqual($cap, $this->maxInFlight, 'Fan-out must never exceed the concurrency cap');
        $this->assertSame($cap, $this->maxInFlight, 'Windows must actually saturate the cap (true fan-out)');
    }

    public function testCappedExecutionWithFewerCallsThanCap(): void
    {
        $executor = new BatchAsyncExecutor();
        $calls = [];
        for ($i = 0; $i < 5; $i++) {
            $calls[$i] = $this->rpcCallable(1000);
        }

        $results = $executor->executeParallelCapped($calls, 16);

        $this->assertCount(5, $results);
        $this->assertSame(5, $this->maxInFlight);
    }

    public function testExecuteParallelCappedRejectsZeroConcurrency(): void
    {
        $executor = new BatchAsyncExecutor();

        $this->expectException(\InvalidArgumentException::class);
        $executor->executeParallelCapped([fn(): null => null], 0);
    }

    public function testExecuteParallelCappedAcceptsEmptyCallSet(): void
    {
        $executor = new BatchAsyncExecutor();

        $this->assertSame([], $executor->executeParallelCapped([], 16));
    }

    public function testResultsKeepOriginalKeysAcrossWindows(): void
    {
        $executor = new BatchAsyncExecutor();
        $calls = [];
        for ($i = 0; $i < 40; $i++) {
            $calls[100 + $i] = fn(): string => 'v' . $i;
        }

        $results = $executor->executeParallelCapped($calls, 7);

        $this->assertSame(array_keys($calls), array_keys($results));
        $this->assertSame('v39', $results[139]);
    }

    public function testWindowedFanOutIsFasterThanSerial(): void
    {
        $total = 50;
        $latencyUs = 5000; // 5 ms simulated server latency per call

        // Serial baseline: N blocking round trips.
        $serialStart = hrtime(true);
        for ($i = 0; $i < $total; $i++) {
            usleep($latencyUs);
        }
        $serialMs = (hrtime(true) - $serialStart) / 1e6;

        // Fanned out with the default window of 16: ~4 windows.
        $executor = new BatchAsyncExecutor();
        $calls = [];
        for ($i = 0; $i < $total; $i++) {
            $calls[$i] = $this->rpcCallable($latencyUs);
        }
        $parallelStart = hrtime(true);
        $executor->executeParallelCapped($calls, 16);
        $parallelMs = (hrtime(true) - $parallelStart) / 1e6;

        $this->assertLessThan(
            $serialMs / 2,
            $parallelMs,
            sprintf(
                'Windowed fan-out (%.1fms) must beat serial execution (%.1fms) for %d x %dms calls',
                $parallelMs,
                $serialMs,
                $total,
                $latencyUs / 1000,
            ),
        );
    }

    public function testCheckedFuturesResolveResponsesThroughTheExecutor(): void
    {
        $scanResponse = new RawScanResponse();
        $checksumResponse = new RawChecksumResponse();
        $scanFuture = $this->futureResolving($scanResponse);
        $checksumFuture = $this->futureResolving($checksumResponse);

        $executor = new BatchAsyncExecutor();
        $results = $executor->executeParallelCapped([
            0 => fn(): CheckedGrpcFuture => $scanFuture,
            1 => fn(): CheckedGrpcFuture => $checksumFuture,
            2 => fn(): RawDeleteRangeResponse => new RawDeleteRangeResponse(),
        ], 2);

        $this->assertInstanceOf(RawScanResponse::class, $results[0]);
        $this->assertInstanceOf(RawChecksumResponse::class, $results[1]);
        $this->assertInstanceOf(RawDeleteRangeResponse::class, $results[2]);
    }

    private function futureResolving(mixed $response): CheckedGrpcFuture
    {
        return CheckedGrpcFuture::fromCallable(fn(): mixed => $response);
    }
}
