<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #184's third mechanism: a shared `RetryExecutor` must not starve a
 * later operation of its retries.
 *
 * `RetryExecutor` is a *documented user-constructible collaborator* and the
 * client reuses one instance across many operations — `Transaction` memoizes a
 * single executor for the whole transaction's lifetime, `RawKvScanner` reuses
 * one per scan, and `RawKvBatch` is handed the caller's executor for every
 * sub-batch. Every value the retry loop accumulates therefore has to be
 * per-invocation state, and the loops that are documented as per-operation
 * limits (`maxBackoffMs`, `serverBusyBudgetMs`, `deadlineMs`) were the ones
 * carried over:
 *
 *  - the two sleep budgets lived in instance fields (issue #271), so the first
 *    operation consumed them and every later one died on its first retryable
 *    error;
 *  - the *fatal* path of issue #233 sits in the same function: a fatal
 *    `KeyNotInRegion` throws out of it, and what it leaves behind for the next
 *    invocation was never asserted;
 *  - and two states that were never instance fields but are documented
 *    per-operation and equally load-bearing: the wall-clock deadline's
 *    `$startTimeMs` and the attempt counter.
 *
 * The two sleep budgets themselves are already pinned by issue #243's tests
 * (`RetryExecutorTest::testBackoffBudgetResetsPerExecuteCall`,
 * `testServerBusyBudgetResetsPerExecuteCall`,
 * `testReusedExecutorRetriesNormallyOnSecondCallAfterFirstConsumesBudget`) and
 * are deliberately not repeated here; reentrancy is pinned by
 * `testNestedExecuteDoesNotCorruptOuterAttemptCounter`.
 *
 * ## What reproduces a pre-fix state and what is a forward pin
 *
 * Verified by mutating a scratch copy, not assumed:
 *
 *  - {@see testAFatalRoutingErrorLeavesTheNextOperationWithAFullBudget()}
 *    **reproduces the issue's pre-#271 state** (hoisting `totalBackoffMs` /
 *    `serverBusyBackoffMs` back into instance fields) and fails on it.
 *  - {@see testEveryInvocationGetsAFreshWallClockDeadline()} and
 *    {@see testTheAttemptCapIsCountedPerInvocation()} are **forward pins**:
 *    `$startTimeMs` and `$attempt` were locals even pre-#271 (the issue says
 *    so: "resets only `attempt`"), so the exact pre-fix shape passes them.
 *    They fail on the mutations that matter — hoisting either into an instance
 *    field, or removing the per-call reset — which is why they are here: the
 *    three per-invocation bounds must not be split again, and nothing else
 *    asserts it.
 */
class RetryExecutorFreshBudgetTest extends TestCase
{
    /**
     * Long enough that the clamped ServerBusy sleep is unambiguous, short
     * enough that the whole test is a fraction of a second: with this
     * deadline every invocation sleeps its whole budget once and then dies on
     * the pre-attempt deadline check.
     */
    private const DEADLINE_MS = 120;

    /**
     * EpochNotMatch's backoff is 2 ms base, 500 ms cap, equal jitter (issue
     * #241/#242), so sleep #0 is drawn from [1, 2] ms and sleep #1 from
     * [2, 4] ms. A budget of 6 therefore always admits exactly two retries
     * (worst case 2 + 4 = 6, which is not `> 6`) and never a third
     * (2 + 4 + 8 = 14 > 6) — the un-flakeable sizing rule from
     * docs/helpers/faq.md.
     */
    private const TWO_RETRY_BUDGET_MS = 6;

    public function testEveryInvocationGetsAFreshWallClockDeadline(): void
    {
        $executor = $this->executor(
            maxBackoffMs: 0,           // the ServerBusy budget is the one under test
            serverBusyBudgetMs: 10000, // must not bind inside the deadline
            deadlineMs: self::DEADLINE_MS,
        );

        $first = $this->timeServerBusyExhaustion($executor, 'first_key');
        $second = $this->timeServerBusyExhaustion($executor, 'second_key');

        self::assertInstanceOf(RetryBudgetExhaustedException::class, $first['exception']);
        self::assertInstanceOf(
            RetryBudgetExhaustedException::class,
            $second['exception'],
            'the second invocation must die on its OWN wall-clock deadline, not on one left over '
            . 'from the first (issue #184)',
        );

        // The observable form of "a full budget": the second invocation slept
        // its own window. The backoff is clamped to the remaining deadline, so
        // a fresh window means one full sleep; a $startTimeMs hoisted into an
        // instance field would leave the second invocation with no time at all
        // and it would throw before running the operation again. 80 ms of the
        // 120 ms window is margin for scheduling overhead only — the two
        // outcomes are ~0 ms apart and ~120 ms apart, not 80 and 120.
        self::assertGreaterThanOrEqual(
            80,
            $second['elapsedMs'],
            'a shared executor must start every invocation\'s wall-clock deadline from zero, '
            . 'or a long first operation starves every later one of its retry time (issue #184)',
        );
    }

    /**
     * The issue's own claim, as a failing test on its pre-#271 shape: a shared
     * executor that just spent most of its budget on operation A starves
     * operation B of retries entirely. The #233 change is what makes the case
     * interesting — a fatal routing error propagates out of the *same* function
     * that accumulates the budget, so "the operation that threw" and "the
     * budget it left behind" are the same event.
     */
    public function testAFatalRoutingErrorLeavesTheNextOperationWithAFullBudget(): void
    {
        $executor = $this->executor(
            maxBackoffMs: self::TWO_RETRY_BUDGET_MS,
            serverBusyBudgetMs: 10000,
            deadlineMs: 0, // the sleep budget is the bound under test
        );

        // Operation A spends two of its three attempts on a retryable region
        // error and then dies on a fatal one — the #233 shape, where the
        // fatal `throw` happens AFTER the invalidation, so it propagates out of
        // execute() with whatever the loop had already spent.
        $aCalls = 0;
        $caught = null;
        try {
            $executor->execute('key_a', function () use (&$aCalls): string {
                $aCalls++;
                if ($aCalls <= 2) {
                    throw $this->epochNotInMatchError();
                }

                throw $this->keyNotInRegionError();
            });
        } catch (RegionException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(RegionException::class, $caught, 'operation A must surface its fatal error');
        self::assertSame(ErrorKind::KeyNotInRegion, $caught->errorKind);
        self::assertSame(3, $aCalls, 'A must have retried twice before the fatal error');

        // Operation B, same executor, same failure shape. It must get the same
        // two retries A got: its own worst-case spend is 2 + 4 = 6, which the
        // budget admits, while a carried-over budget could not admit the first
        // retry at all — A's minimum spend (1 + 2 = 3) already exceeds B's
        // maximum single sleep (2), so `total > budget` fires on B's first
        // retry under EVERY jitter draw. Both directions are un-flakeable.
        $bCalls = 0;
        $result = $executor->execute('key_b', function () use (&$bCalls): string {
            $bCalls++;
            if ($bCalls <= 2) {
                throw $this->epochNotInMatchError();
            }

            return 'ok';
        });

        self::assertSame('ok', $result);
        self::assertSame(3, $bCalls, 'B must start from a full budget, exactly like A');
    }

    /**
     * The issue's other half, pinned as a value: the defaults a client builds
     * its executors with must bound how long ONE request can pin a worker.
     *
     * `RawKvClient::DEFAULT_SERVER_BUSY_BUDGET_MS` used to be 600000 and
     * `RetryExecutor::deadlineMs` defaulted to `0` — deadline disabled — so
     * the two mechanisms compounded into a single worst case: a sustained
     * `ServerIsBusy` episode against a merely slow TiKV could block a
     * PHP-FPM worker for ~10 minutes of blocking `usleep()`, and neither
     * `max_execution_time` nor `set_time_limit()` interrupts a sleep or a call
     * inside the gRPC C extension. #294 cut the budget to 60000 and gave the
     * loop a 30 s wall-clock bound. They are independent limits and are
     * asserted as such: the 30 s wall clock is the tighter of the two for a
     * ServerBusy storm (ServerBusy sleeps are 1–2 s each, so 60 s of budget
     * would not be spent before the wall clock runs out), while the sleep
     * budget is what bounds a *non*-ServerBusy retry storm, whose sleeps are
     * milliseconds and which therefore exhausts `maxBackoffMs` long before any
     * wall clock. Collapsing them into one number would lose one of the two.
     *
     * `Transaction` mirrors the same defaults with no options array, so both
     * are asserted here rather than only on the RawKV side.
     */
    public function testTheDefaultWorkerOccupancyBoundsAreFinite(): void
    {
        self::assertSame(
            60000,
            RawKvClient::DEFAULT_SERVER_BUSY_BUDGET_MS,
            'the ServerBusy budget is a worker-occupancy setting; it was reduced from '
            . '600000 ms (issue #294) because ten minutes of blocking usleep() per request '
            . 'exhausts a PHP-FPM pool while TiKV is merely slow (issue #184)',
        );
        self::assertSame(30000, RetryExecutor::DEFAULT_RETRY_DEADLINE_MS);
        self::assertGreaterThan(0, RawKvClient::DEFAULT_RETRY_DEADLINE_MS);
        self::assertSame(
            RetryExecutor::DEFAULT_RETRY_DEADLINE_MS,
            Transaction::DEFAULT_RETRY_DEADLINE_MS,
            'Transaction has no options array, so its deadline is the constant — and it must '
            . 'be the same finite value (issue #294)',
        );
        self::assertLessThan(
            RawKvClient::DEFAULT_SERVER_BUSY_BUDGET_MS,
            RawKvClient::DEFAULT_RETRY_DEADLINE_MS,
            'the 30 s wall-clock deadline is the tighter of the two, so a ServerBusy storm is '
            . 'bounded by it and the 60 s sleep budget is a second, independent limit — the '
            . 'two must not be collapsed into one value (issue #260\'s separation decision)',
        );
    }

    /**
     * The attempt counter is per-invocation too: a shared executor that
     * reached its cap on operation A must still allow `maxAttempts` attempts on
     * operation B. (#271 moved `$attempt` into the same locals as the budgets;
     * the cap is the only bound here, so it is the one that would show it.)
     *
     * The issue's pre-#271 code did reset `$attempt` per call, so this passes
     * on the exact pre-fix shape; it fails on the mutation that matters —
     * dropping the per-call reset, which is what an un-fixed nested call
     * (`$this->attempt = 0` inside a retried closure) amounts to.
     */
    public function testTheAttemptCapIsCountedPerInvocation(): void
    {
        $executor = $this->executor(
            maxBackoffMs: PHP_INT_MAX, // no sleep budget; the cap is the only bound
            serverBusyBudgetMs: PHP_INT_MAX,
            deadlineMs: 0,
            maxAttempts: 4,
        );

        // BackoffType::None is the only zero-sleep class (issue #241), so the
        // loop runs to the cap without sleeping a millisecond.
        $classifier = static fn (TiKvException $e): BackoffType => BackoffType::None;

        foreach (['a', 'b'] as $key) {
            $attempts = 0;
            $caught = null;
            try {
                $executor->execute('key_' . $key, function () use (&$attempts): string {
                    $attempts++;

                    throw new RegionException(operation: 'KvGet', message: 'transient');
                }, $classifier);
            } catch (RetryBudgetExhaustedException $e) {
                $caught = $e;
            }

            self::assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
            self::assertSame(
                4,
                $attempts,
                "operation {$key} must get the full attempt cap on a shared executor (issue #184)",
            );
        }
    }

    /**
     * Run one ServerBusy-forever invocation and report how it died and how
     * long it took.
     *
     * @return array{exception: ?TiKvException, elapsedMs: int}
     */
    private function timeServerBusyExhaustion(RetryExecutor $executor, string $key): array
    {
        $startMs = (int) (microtime(true) * 1000);
        $caught = null;

        try {
            $executor->execute($key, function (): string {
                throw new TiKvException('ServerIsBusy');
            });
        } catch (TiKvException $e) {
            $caught = $e;
        }

        return [
            'exception' => $caught,
            'elapsedMs' => (int) (microtime(true) * 1000) - $startMs,
        ];
    }

    private function executor(
        int $maxBackoffMs,
        int $serverBusyBudgetMs,
        int $deadlineMs,
        int $maxAttempts = RetryExecutor::DEFAULT_MAX_ATTEMPTS,
    ): RetryExecutor {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        // Nothing cached: the invalidation paths must find no region, so the
        // assertions are about the budgets alone and not about the cache.
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

    private function epochNotInMatchError(): RegionException
    {
        $error = new Error();
        $error->setMessage('region error from TiKV');
        $error->setEpochNotMatch(new EpochNotMatch());

        return RegionException::fromRegionError($error);
    }

    private function keyNotInRegionError(): RegionException
    {
        $error = new Error();
        $error->setMessage('region error from TiKV');
        $error->setKeyNotInRegion(new KeyNotInRegion());

        return RegionException::fromRegionError($error);
    }
}
