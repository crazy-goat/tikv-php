<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RetryExecutorTest extends TestCase
{
    private RegionCacheInterface&MockObject $regionCache;
    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private RetryExecutor $executor;

    protected function setUp(): void
    {
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->executor = new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            $regionResolver,
            new NullLogger(),
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(1, 1, 1, 1, 1);
    }

    public function testExecuteSucceedsOnFirstTry(): void
    {
        $result = $this->executor->execute('key', fn(): string => 'success');
        $this->assertSame('success', $result);
    }

    private function executorWithDeadline(int $deadlineMs): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            deadlineMs: $deadlineMs,
        );
    }

    /**
     * Issue #569: the operation budget exposes the live remaining deadline,
     * so a nested call made later in the attempt sees the time the attempt
     * has already spent (a snapshot taken before the attempt would overstate
     * it).
     */
    public function testCurrentBudgetExposesTheLiveRemainingDeadline(): void
    {
        $executor = $this->executorWithDeadline(5000)->beginOperation();
        $budget = $executor->currentBudget();
        self::assertNotNull($budget);

        $observed = $executor->execute(
            'key',
            function () use ($budget): int {
                $before = $budget->remainingDeadlineMs();
                usleep(20_000);
                $after = $budget->remainingDeadlineMs();

                self::assertGreaterThan(0, $before);
                self::assertLessThanOrEqual(5000, $before);
                self::assertLessThan($before, $after, 'the remaining budget must shrink during the attempt');

                return $after;
            },
        );

        $this->assertGreaterThan(0, $observed);
    }

    /**
     * Issue #569: `0` is the "no deadline" sentinel, so a disabled deadline
     * keeps the nested call's historical behaviour.
     */
    public function testCurrentBudgetReportsZeroWhenTheDeadlineIsDisabled(): void
    {
        $executor = $this->executorWithDeadline(0)->beginOperation();
        $budget = $executor->currentBudget();
        self::assertNotNull($budget);

        $observed = $executor->execute(
            'key',
            static fn(): int => $budget->remainingDeadlineMs(),
        );

        $this->assertSame(0, $observed);
    }

    /**
     * A standalone executor starts a fresh budget per execute() call, so it
     * has no operation-scoped budget to read (issue #223, #569).
     */
    public function testStandaloneExecutorHasNoCurrentBudget(): void
    {
        $this->assertNull($this->executor->currentBudget());
    }

    public function testExecuteRaftEntryTooLargeThrowsImmediately(): void
    {
        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('RaftEntryTooLarge');

        $this->executor->execute('key', function (): never {
            throw new TiKvException('RaftEntryTooLarge');
        });
    }

    public function testExecuteEpochNotMatchRetriesAndSucceeds(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        $callCount = 0;
        $result = $this->executor->execute('key', function () use (&$callCount): string {
            $callCount++;
            if ($callCount === 1) {
                throw new TiKvException('EpochNotMatch');
            }
            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertSame(2, $callCount);
    }

    public function testExecuteFatalErrorLogsMessage(): void
    {
        $logger = $this->createMock(LoggerInterface::class);

        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
        $executor = new RetryExecutor(20000, 600000, $this->regionCache, $this->grpc, $regionResolver, $logger);

        $logger->expects($this->once())
            ->method('error')
            ->with(
                'Fatal error, not retrying',
                $this->callback(fn(array $ctx): bool => $ctx['error'] === 'RaftEntryTooLarge'),
            );

        $this->expectException(TiKvException::class);
        $executor->execute('key', function (): never {
            throw new TiKvException('RaftEntryTooLarge');
        });
    }

    public function testNotLeaderWithHintSwitchesLeader(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->expects($this->once())
            ->method('switchLeader')
            ->with(1, 3)
            ->willReturn(true);

        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $leader = new Peer();
        $leader->setId(30);
        $leader->setStoreId(3);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $callCount = 0;
        $result = $this->executor->execute('key', function () use ($error, &$callCount): string {
            $callCount++;
            if ($callCount === 1) {
                throw RegionException::fromRegionError($error);
            }
            return 'found';
        });

        $this->assertSame('found', $result);
        $this->assertSame(2, $callCount);
    }

    public function testExecuteWithCustomClassifierRetriesOnTxnLock(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());

        $classifier = fn(TiKvException $e): ?BackoffType => match (true) {
            str_contains($e->getMessage(), 'Lock') || str_contains($e->getMessage(), 'locked') => BackoffType::TxnLock,
            default => null,
        };

        $callCount = 0;
        $result = $this->executor->execute('key', function () use (&$callCount): string {
            $callCount++;
            if ($callCount === 1) {
                throw new TiKvException('Lock conflict');
            }
            return 'resolved';
        }, $classifier);

        $this->assertSame('resolved', $result);
        $this->assertSame(2, $callCount);
    }

    // ========================================================================
    // Attempt-cap and deadline safety (issue #72)
    //
    // EpochNotMatch / other zero-backoff errors used to drive an infinite
    // zero-sleep busy loop because totalBackoffMs never grew when
    // sleepMs=0. The attempt cap and wall-clock deadline bound the loop
    // independently of accumulated sleep time.
    // ========================================================================

    public function testEpochNotMatchLoopTerminatesAfterMaxAttempts(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        $maxAttempts = 5;
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
        );

        $this->expectException(RetryBudgetExhaustedException::class);

        $callCount = 0;
        try {
            $executor->execute('key', function () use (&$callCount): never {
                $callCount++;
                throw new TiKvException('EpochNotMatch');
            });
        } finally {
            $this->assertSame($maxAttempts, $callCount, 'Operation should run exactly maxAttempts times');
        }
    }

    public function testWallClockDeadlineTerminatesZeroBackoffLoop(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        // Cap attempts high so the deadline (not the attempt cap) is what
        // trips the loop. The deadline must stay comfortably above the
        // first-attempt latency: a 1 ms deadline can already be spent by the
        // pre-attempt check on a loaded CI runner, so the closure never runs
        // (issue #872). 100 ms leaves room for the first attempt while still
        // keeping the test fast, because the classifier below maps the error
        // to BackoffType::None (zero sleep), so the deadline check runs on
        // every iteration.
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: 100000,
            deadlineMs: 100,
        );

        $this->expectException(RetryBudgetExhaustedException::class);

        $callCount = 0;
        try {
            $executor->execute(
                'key',
                function () use (&$callCount): never {
                    $callCount++;
                    throw new TiKvException('EpochNotMatch');
                },
                static fn(TiKvException $e): BackoffType => BackoffType::None,
            );
        } finally {
            // The first attempt must run, and the deadline must terminate the
            // loop before the 100000-attempt cap is reached.
            $this->assertGreaterThanOrEqual(1, $callCount);
            $this->assertLessThan(100000, $callCount, 'Deadline, not the attempt cap, must end the loop');
        }
    }

    public function testDefaultMaxAttemptsIsApplied(): void
    {
        // No maxAttempts argument => DEFAULT_MAX_ATTEMPTS cap is used.
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        $this->expectException(RetryBudgetExhaustedException::class);

        $callCount = 0;
        try {
            $this->executor->execute('key', function () use (&$callCount): never {
                $callCount++;
                throw new TiKvException('EpochNotMatch');
            });
        } finally {
            $this->assertSame(RetryExecutor::DEFAULT_MAX_ATTEMPTS, $callCount);
        }
    }

    public function testNonZeroBackoffLoopStillTerminatesAfterMaxAttempts(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        $maxAttempts = 3;
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
        );

        $this->expectException(RetryBudgetExhaustedException::class);

        $callCount = 0;
        try {
            $executor->execute('key', function () use (&$callCount): never {
                $callCount++;
                throw new TiKvException('StaleCommand');
            });
        } finally {
            $this->assertSame($maxAttempts, $callCount);
        }
    }

    public function testConstructorRejectsNonPositiveMaxAttempts(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: 0,
        );
    }

    public function testConstructorRejectsNegativeDeadline(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            deadlineMs: -1,
        );
    }

    public function testRetryBudgetExhaustedCarriesPreviousException(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('invalidate');

        $maxAttempts = 2;
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
        );

        // The original TiKvException thrown by the operation should be carried
        // as the previous exception on the RetryBudgetExhaustedException, so
        // callers can inspect the underlying region/grpc failure that was
        // being retried.
        $originalMessage = 'EpochNotMatch';
        $originalException = null;

        $this->expectException(RetryBudgetExhaustedException::class);

        try {
            $executor->execute('key', function () use (&$originalException, $originalMessage): never {
                $originalException = new TiKvException($originalMessage);
                throw $originalException;
            });
        } catch (RetryBudgetExhaustedException $e) {
            $previous = $e->getPrevious();
            $this->assertInstanceOf(TiKvException::class, $previous);
            $this->assertSame($originalException, $previous);
            $this->assertSame($originalMessage, $previous->getMessage());
            throw $e;
        }
    }
}
