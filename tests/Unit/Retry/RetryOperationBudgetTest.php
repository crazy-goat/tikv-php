<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryBudget;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #223 (TXN-18): the retry budget belongs to one logical operation.
 *
 * A standalone RetryExecutor starts a fresh budget on every execute() call
 * (issue #271), which made the budget per region/page rather than per
 * operation. beginOperation() returns an executor whose execute() calls share
 * one budget, so a fan-out stays inside one maxBackoffMs / serverBusyBudgetMs
 * / deadline bound, while the next operation starts from zero.
 *
 * The attempt cap is deliberately NOT part of the shared budget: it is a
 * termination backstop for one retry loop, not a time budget, and a large
 * fan-out must not exhaust it on one retry per region. It stays per
 * execute() call and is pinned below.
 */
class RetryOperationBudgetTest extends TestCase
{
    /**
     * EpochNotMatch/StaleCmd backoff is 2 ms base with equal jitter, so the
     * first two sleeps are drawn from [1, 2] and [2, 4] ms: their sum is at
     * most 6 and at least 3. A budget of 6 therefore always admits exactly
     * two retries and the third charge (8-16 ms) always exceeds it.
     */
    private const TWO_RETRY_BUDGET_MS = 6;

    public function testRemainingDeadlineIsOneAtExhaustion(): void
    {
        // The budget started 100 ms ago with a 50 ms deadline: it is spent.
        $budget = new RetryBudget((int) (microtime(true) * 1000) - 100, 50);

        self::assertSame(
            1,
            $budget->remainingDeadlineMs(),
            'an exhausted budget must return 1, never 0 ("no deadline")',
        );
    }

    public function testRemainingDeadlineIsZeroWhenDisabled(): void
    {
        self::assertSame(0, (new RetryBudget(0, 0))->remainingDeadlineMs());
        self::assertSame(0, (new RetryBudget((int) (microtime(true) * 1000), 0))->remainingDeadlineMs());
    }

    public function testBeginOperationSharesTheBackoffBudgetAcrossExecuteCalls(): void
    {
        $executor = $this->executor(
            maxBackoffMs: self::TWO_RETRY_BUDGET_MS,
            deadlineMs: 0,
        )->beginOperation();
        $classifier = static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::StaleCmd);

        $firstAttempts = 0;
        try {
            $executor->execute('first', function () use (&$firstAttempts): never {
                $firstAttempts++;
                throw new TiKvException('first');
            }, $classifier);
        } catch (TiKvException) {
            // Expected: the first execute() exhausts the shared budget.
        }
        self::assertSame(3, $firstAttempts, 'The first execute() must spend the whole 6 ms budget');

        $secondAttempts = 0;
        $caught = null;
        try {
            $executor->execute('second', function () use (&$secondAttempts): never {
                $secondAttempts++;
                throw new TiKvException('second');
            }, $classifier);
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
        self::assertSame('second', $caught->getPrevious()?->getMessage());
        self::assertSame(
            1,
            $secondAttempts,
            'A second execute() on the same operation must charge the exhausted budget, not start a new one',
        );
    }

    public function testBeginOperationStartsAFreshBudgetForTheNextOperation(): void
    {
        $executor = $this->executor(
            maxBackoffMs: self::TWO_RETRY_BUDGET_MS,
            deadlineMs: 0,
        );
        $classifier = static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::StaleCmd);

        // Operation A exhausts its budget and throws.
        $first = $executor->beginOperation();
        try {
            $first->execute('first', function (): never {
                throw new TiKvException('first');
            }, $classifier);
        } catch (TiKvException) {
            // Expected: the budget is exhausted.
        }

        // Operation B is a new beginOperation(): it must get the full budget
        // and recover after two retries.
        $second = $executor->beginOperation();
        $attempts = 0;
        $result = $second->execute('second', function () use (&$attempts): string {
            $attempts++;
            if ($attempts <= 2) {
                throw new TiKvException('second-transient');
            }
            return 'ok';
        }, $classifier);

        self::assertSame('ok', $result);
        self::assertSame(3, $attempts, 'The next operation must start from a fresh budget');
    }

    public function testBeginOperationSharesTheWallClockDeadlineAcrossExecuteCalls(): void
    {
        $executor = $this->executor(
            maxBackoffMs: 20000,
            deadlineMs: 200,
        )->beginOperation();

        $first = $executor->execute('first', function (): string {
            usleep(300_000);
            return 'first';
        });
        self::assertSame('first', $first);

        $secondAttempts = 0;
        $caught = null;
        try {
            $executor->execute('second', function () use (&$secondAttempts): string {
                $secondAttempts++;
                return 'second';
            });
        } catch (RetryBudgetExhaustedException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
        self::assertStringContainsString('Retry deadline', $caught->getMessage());
        self::assertSame(
            0,
            $secondAttempts,
            'The deadline must be checked before the second execute() runs its operation',
        );
    }

    public function testBeginOperationSharesTheServerBusyBudgetAcrossExecuteCalls(): void
    {
        $executor = $this->executor(
            maxBackoffMs: PHP_INT_MAX,
            deadlineMs: 0,
            serverBusyBudgetMs: 2000,
        )->beginOperation();
        $classifier = static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::ServerBusy);

        // ServerBusy sleeps are 2000 ms base with equal jitter, i.e.
        // [1000, 2000] and [2000, 4000] ms. The first charge always fits the
        // 2000 ms budget, so the first execute() retries once; the second
        // charge is at least 3000 ms in total and exhausts it.
        $firstAttempts = 0;
        try {
            $executor->execute('first', function () use (&$firstAttempts): never {
                $firstAttempts++;
                throw new TiKvException('ServerIsBusy');
            }, $classifier);
        } catch (TiKvException) {
            // Expected: the shared ServerBusy budget is exhausted.
        }
        self::assertSame(2, $firstAttempts, 'The first execute() must spend the ServerBusy budget');

        $secondAttempts = 0;
        $caught = null;
        try {
            $executor->execute('second', function () use (&$secondAttempts): never {
                $secondAttempts++;
                throw new TiKvException('ServerIsBusy');
            }, $classifier);
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
        self::assertSame('ServerIsBusy', $caught->getPrevious()?->getMessage());
        self::assertSame(
            1,
            $secondAttempts,
            'A second execute() on the same operation must charge the exhausted ServerBusy budget',
        );
    }

    public function testBeginOperationStartsAFreshDeadlineForTheNextOperation(): void
    {
        $executor = $this->executor(
            maxBackoffMs: 20000,
            deadlineMs: 200,
        )->beginOperation();

        // Exhaust the operation's wall-clock deadline with a successful but
        // slow first execute().
        $first = $executor->execute('first', function (): string {
            usleep(300_000);
            return 'first';
        });
        self::assertSame('first', $first);

        $secondAttempts = 0;
        try {
            $executor->execute('second', function () use (&$secondAttempts): string {
                $secondAttempts++;
                return 'second';
            });
        } catch (RetryBudgetExhaustedException) {
            // Expected: the same operation's deadline is already spent.
        }
        self::assertSame(0, $secondAttempts);

        // A new beginOperation() is a new logical operation, so it must get a
        // fresh deadline instead of inheriting the exhausted one.
        $fresh = $executor->beginOperation();
        self::assertSame(
            'fresh',
            $fresh->execute('third', static fn (): string => 'fresh'),
            'A new beginOperation() must start a fresh wall-clock deadline',
        );
    }

    public function testTheAttemptCapStaysPerExecuteCallWithinOneOperation(): void
    {
        $executor = $this->executor(
            maxBackoffMs: PHP_INT_MAX,
            deadlineMs: 0,
            maxAttempts: 3,
        )->beginOperation();

        // BackoffType::None is the only zero-sleep class (issue #241), so the
        // loop reaches the cap without spending any wall clock.
        $classifier = static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None);

        foreach (['first', 'second'] as $key) {
            $attempts = 0;
            $caught = null;
            try {
                $executor->execute($key, function () use (&$attempts): never {
                    $attempts++;
                    throw new TiKvException('transient');
                }, $classifier);
            } catch (RetryBudgetExhaustedException $e) {
                $caught = $e;
            }

            self::assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
            self::assertSame(
                3,
                $attempts,
                'Every execute() keeps its own attempt cap; a fan-out must not exhaust it for later regions',
            );
        }
    }

    private function executor(
        int $maxBackoffMs,
        int $deadlineMs,
        int $maxAttempts = RetryExecutor::DEFAULT_MAX_ATTEMPTS,
        int $serverBusyBudgetMs = 10000,
    ): RetryExecutor {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        // Nothing cached: the invalidation paths find no region, so the
        // assertions are about the budgets alone.
        $regionCache->method('getByKey')->willReturn(null);

        return new RetryExecutor(
            maxBackoffMs: $maxBackoffMs,
            serverBusyBudgetMs: $serverBusyBudgetMs,
            regionCache: $regionCache,
            grpc: $this->createMock(GrpcClientInterface::class),
            regionResolver: new RegionResolver(
                $this->createMock(PdClientInterface::class),
                $regionCache,
            ),
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
            deadlineMs: $deadlineMs,
            metrics: new InMemoryMetrics(),
        );
    }
}
