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
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
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
        // trips the loop. Deadline is intentionally tiny: usleep is not
        // called for BackoffType::None, so the deadline check happens on
        // every iteration.
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: new NullLogger(),
            maxAttempts: 100000,
            deadlineMs: 1, // 1 ms — guaranteed to elapse on first check
        );

        $this->expectException(RetryBudgetExhaustedException::class);

        $callCount = 0;
        try {
            $executor->execute('key', function () use (&$callCount): never {
                $callCount++;
                throw new TiKvException('EpochNotMatch');
            });
        } finally {
            // Deadline caps wall-clock time even when sleepMs=0 each iteration.
            $this->assertGreaterThanOrEqual(1, $callCount);
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
}
