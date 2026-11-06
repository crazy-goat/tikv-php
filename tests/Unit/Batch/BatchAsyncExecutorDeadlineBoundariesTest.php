<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * The batch deadline is a boundary check, not an interrupt: it is evaluated
 * before each dispatch and before each wait, and never inside one blocking
 * wait (issue #341).
 *
 * These cases deliberately use a budget (200 ms) that is far above anything
 * the executor does between two boundaries and a blocking operation (500 ms)
 * that is far above the budget, so the phase that throws is unambiguous on a
 * loaded machine. They need no ext-grpc: a waiter-backed {@see GrpcFuture}
 * models server latency without a {@see \Grpc\Call}.
 */
final class BatchAsyncExecutorDeadlineBoundariesTest extends TestCase
{
    private const DEADLINE_MS = 200;
    private const BLOCKING_MS = 500_000;

    public function testTheDeadlineIsCheckedBeforeTheNextDispatch(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());
        $dispatched = [];

        $calls = [
            1 => static function () use (&$dispatched): string {
                $dispatched[] = 1;
                usleep(self::BLOCKING_MS);

                return 'a';
            },
            2 => static function () use (&$dispatched): string {
                $dispatched[] = 2;

                return 'b';
            },
        ];

        try {
            $executor->executeParallel($calls, self::DEADLINE_MS);
            self::fail('the deadline must stop the batch before the second dispatch');
        } catch (BatchDeadlineExceededException $e) {
            self::assertSame(self::DEADLINE_MS, $e->getDeadlineMs());
            self::assertGreaterThanOrEqual(self::DEADLINE_MS, $e->getElapsedMs());
            self::assertSame([1], $e->getContext()['dispatchedRegions']);
            self::assertSame([2], $e->getContext()['pendingRegions']);
            self::assertSame([1], $dispatched, 'the second callable must not run');
        }
    }

    public function testTheDeadlineIsCheckedBeforeTheNextWait(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());
        $waited = [];

        $calls = [
            1 => static function () use (&$waited): GrpcFuture {
                $waited[] = 1;

                return GrpcFuture::fromWaiter(static function (): Message {
                    usleep(self::BLOCKING_MS);

                    return new RawGetResponse();
                });
            },
            2 => static function () use (&$waited): GrpcFuture {
                $waited[] = 2;

                return GrpcFuture::fromWaiter(static fn(): Message => new RawGetResponse());
            },
        ];

        try {
            $executor->executeParallel($calls, self::DEADLINE_MS);
            self::fail('the deadline must stop the batch before the second wait');
        } catch (BatchDeadlineExceededException $e) {
            self::assertSame(self::DEADLINE_MS, $e->getDeadlineMs());
            self::assertGreaterThanOrEqual(self::DEADLINE_MS, $e->getElapsedMs());
            self::assertSame([2], $e->getContext()['pendingRegions']);
            self::assertSame([1, 2], $waited, 'both sends happen before the first wait');
        }
    }

    public function testABatchThatFinishesWithinTheDeadlineIsUnaffected(): void
    {
        $executor = new BatchAsyncExecutor(new NullLogger());

        $calls = [
            1 => static fn(): string => 'a',
            2 => static fn(): string => 'b',
        ];

        self::assertSame(
            [1 => 'a', 2 => 'b'],
            $executor->executeParallel($calls, self::DEADLINE_MS),
        );
    }
}
