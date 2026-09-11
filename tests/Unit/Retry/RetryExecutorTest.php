<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class RetryExecutorTest extends TestCase
{
    private RegionCacheInterface&MockObject $regionCache;
    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private LoggerInterface&MockObject $logger;

    protected function setUp(): void
    {
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);

        // By default, getByKey returns null to avoid cache invalidation path
        $this->regionCache->method('getByKey')->willReturn(null);
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private function createExecutor(
        int $maxBackoffMs = 10000,
        int $serverBusyBudgetMs = 10000,
        int $maxAttempts = RetryExecutor::DEFAULT_MAX_ATTEMPTS,
        int $deadlineMs = 0,
        ?LoggerInterface $logger = null,
        ?InMemoryMetrics $metrics = null,
    ): RetryExecutor {
        return new RetryExecutor(
            maxBackoffMs: $maxBackoffMs,
            serverBusyBudgetMs: $serverBusyBudgetMs,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $logger ?? $this->logger,
            maxAttempts: $maxAttempts,
            deadlineMs: $deadlineMs,
            metrics: $metrics ?? new InMemoryMetrics(),
        );
    }

    // ========================================================================
    // Successful execution
    // ========================================================================

    public function testSuccessfulExecutionReturnsResult(): void
    {
        $executor = $this->createExecutor();

        $result = $executor->execute('test_key', fn(): string => 'success');

        $this->assertSame('success', $result);
    }

    // ========================================================================
    // Total backoff budget exhaustion (non-ServerBusy errors)
    // ========================================================================

    public function testTotalBackoffBudgetExhaustedThrowsOriginalException(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10, // very small budget
            serverBusyBudgetMs: 10000,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        // RegionMiss: baseMs=2, capMs=500, equal jitter (issue #242), so each
        // sleep is drawn from [expo/2, expo]: 1-2, 2-4, 4-8 ms.
        // Worst-case spend 2+4+8=14 > 10 exhausts the budget; even in the
        // best case (1+2+4=7) the next attempt adds 8-16 ms and crosses 10,
        // so the budget is always exhausted and the original error rethrown.
        $operation = function (): void {
            throw new TiKvException('test error');
        };

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('test error');

        $executor->execute('test_key', $operation, $classifier);
    }

    public function testTotalBackoffBudgetExhaustionLogsError(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->atLeastOnce())
            ->method('error')
            ->with('Retry budget exhausted');

        $executor = $this->createExecutor(
            maxBackoffMs: 10,
            serverBusyBudgetMs: 10000,
            logger: $logger,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        $operation = function (): void {
            throw new TiKvException('test error');
        };

        try {
            $executor->execute('test_key', $operation, $classifier);
        } catch (TiKvException) {
            // expected
        }
    }

    /**
     * The backoff budget must reset on every execute() call (issue #243):
     * Transaction memoizes a single RetryExecutor for its whole lifetime, so
     * a budget carried across execute() calls would be exhausted permanently.
     * The first call exhausts maxBackoffMs and throws; the second call must
     * get a full budget and therefore succeed with the same retry schedule.
     */
    public function testBackoffBudgetResetsPerExecuteCall(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 6, // RegionMiss backoff 2, 4: two retries fit (2+4=6), three do not
            serverBusyBudgetMs: 10000,
        );
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        $calls = 0;
        // A fresh operation per execute() call: fails its first two attempts
        // (RegionMiss backoff 2, then 4; 2+4=6 <= 6 allows both retries),
        // then succeeds — so every call must exercise the full schedule.
        // $alwaysFail makes the operation fail forever (budget exhaustion).
        $operationFactory = function (bool $alwaysFail = false) use (&$calls): \Closure {
            $failures = 0;
            return function () use (&$calls, &$failures, $alwaysFail): string {
                $calls++;
                if ($alwaysFail || $failures < 2) {
                    $failures++;
                    throw new TiKvException('test error');
                }
                return 'success';
            };
        };

        // First call: exhausts its budget (2+4+8=14 > 6) and rethrows.
        try {
            $executor->execute('test_key', $operationFactory(true), $classifier);
            $this->fail('Expected first execute() to exhaust the budget');
        } catch (TiKvException $e) {
            $this->assertSame('test error', $e->getMessage());
        }
        $this->assertSame(3, $calls);

        // Second call on the SAME executor: fresh budget, 2 retries, success.
        // With a carried-over budget this would throw immediately.
        $this->assertSame('success', $executor->execute('test_key', $operationFactory(), $classifier));
        $this->assertSame(6, $calls);
    }

    /**
     * The ServerBusy budget must also reset per execute() call (issue #243),
     * for the same reason as the total backoff budget. ServerBusy sleep has
     * jitter (1000 + random_int(0, 1000) ms), so the reset is asserted via
     * the logged 'serverBusyBackoffMs' of each exhaustion: after a reset it
     * is one sleep (1000–2000 ms), with a carried-over budget it would be
     * the sum of two sleeps (2000–4000 ms). The two ranges touch at exactly
     * 2000 ms (a fresh budget CAN be 2000 when both jitter draws are maximal),
     * so a strict <2000 assertion would flake. Instead we run three
     * executions and assert each logged budget is <= 2000: a carried-over
     * budget (2000–4000 for the 2nd, 3000–6000 for the 3rd) exceeds 2000
     * unless every jitter draw is maximal (~1e-9), while a fresh budget can
     * never exceed it — no false failures, negligible false passes.
     */
    public function testServerBusyBudgetResetsPerExecuteCall(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $loggedBudgets = [];
        $logger->method('error')->willReturnCallback(
            function (string $message, array $context) use (&$loggedBudgets): void {
                if ($message === 'ServerBusy budget exhausted') {
                    $loggedBudgets[] = $context['serverBusyBackoffMs'];
                }
            },
        );

        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 100, // below the minimum ServerBusy sleep (~1000ms)
            logger: $logger,
        );
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::ServerBusy;

        $operation = function (): never {
            throw new TiKvException('server busy');
        };

        for ($i = 0; $i < 3; $i++) {
            try {
                // The never-returning operation always throws; the budget
                // check then rethrows it after one ServerBusy sleep.
                $executor->execute('test_key', $operation, $classifier);
            } catch (TiKvException $e) {
                $this->assertSame('server busy', $e->getMessage());
            }
        }

        // Each exhaustion must see a fresh (single-sleep) ServerBusy budget;
        // see the docblock for why this is <= 2000 rather than < 2000.
        $this->assertCount(3, $loggedBudgets);
        foreach ($loggedBudgets as $serverBusyBackoffMs) {
            $this->assertLessThanOrEqual(
                2000,
                $serverBusyBackoffMs,
                'Later execute() calls must start the ServerBusy budget from 0, not carry over earlier sleeps',
            );
        }
    }

    // ========================================================================
    // ServerBusy budget exhaustion
    // ========================================================================

    public function testServerBusyBudgetExhaustedThrowsOriginalException(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 100, // smaller than minimum ServerBusy sleep (~1000ms)
        );

        // ServerBusy: baseMs=2000, equalJitter=true
        // attempt 0: sleepMs = 1000 + random_int(0, 1000), so >= 1000
        // serverBusyBackoffMs = 0 + >=1000 > 100 → throw on first retry
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::ServerBusy;

        $operation = function (): void {
            throw new TiKvException('server busy');
        };

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('server busy');

        $executor->execute('test_key', $operation, $classifier);
    }

    public function testServerBusyBudgetExhaustionLogsError(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->atLeastOnce())
            ->method('error')
            ->with('ServerBusy budget exhausted');

        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 100,
            logger: $logger,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::ServerBusy;

        $operation = function (): void {
            throw new TiKvException('server busy');
        };

        try {
            $executor->execute('test_key', $operation, $classifier);
        } catch (TiKvException) {
            // expected
        }
    }

    // ========================================================================
    // Max attempts exhaustion
    // ========================================================================

    public function testMaxAttemptsExhaustedThrowsRetryBudgetExhausted(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 2,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::None;

        $operation = function (): void {
            throw new TiKvException('test error');
        };

        $this->expectException(RetryBudgetExhaustedException::class);
        $this->expectExceptionMessage('Retry attempt cap (2) exhausted');

        $executor->execute('test_key', $operation, $classifier);
    }

    public function testMaxAttemptsExhaustionCarriesLastError(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 3,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::None;

        $lastError = new TiKvException('last error message');
        $operation = function () use ($lastError): void {
            throw $lastError;
        };

        $this->expectException(RetryBudgetExhaustedException::class);

        try {
            $executor->execute('test_key', $operation, $classifier);
        } catch (RetryBudgetExhaustedException $e) {
            $this->assertSame(3, $e->attempts());
            $this->assertStringContainsString('Retry attempt cap (3) exhausted', $e->getMessage());
            // The raw key must never reach the exception message; it is
            // exposed only through the typed accessor (issue #269, GRPC-10).
            $this->assertStringNotContainsString('test_key', $e->getMessage());
            $this->assertSame('test_key', $e->getRawKey());
            $previous = $e->getPrevious();
            $this->assertSame($lastError, $previous);
            throw $e;
        }
    }

    /**
     * Regression test for issue #269 (GRPC-10): a key that embeds personal
     * data must never appear verbatim in the RetryBudgetExhaustedException
     * message — exception messages are a logging channel. The raw key stays
     * available to code through getRawKey().
     *
     * Uses an explicit BackoffType::None classifier so the retry loop runs
     * to the attempt cap without sleeping (see docs/helpers/faq.md).
     */
    public function testAttemptCapExhaustionRedactsRawKeyInMessage(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 2,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::None;
        $rawKey = 'user_email:alice@example.com';

        $operation = function (): void {
            throw new TiKvException('test error');
        };

        $this->expectException(RetryBudgetExhaustedException::class);

        try {
            $executor->execute($rawKey, $operation, $classifier);
        } catch (RetryBudgetExhaustedException $e) {
            $message = $e->getMessage();
            $this->assertStringNotContainsString($rawKey, $message);
            $this->assertStringContainsString(KeyRedactor::redact($rawKey), $message);
            $this->assertStringContainsString('Retry attempt cap (2) exhausted for key', $message);
            // The raw key is still available to calling code, just not in
            // the message.
            $this->assertSame($rawKey, $e->getRawKey());
            throw $e;
        }
    }

    // ========================================================================
    // Deadline exhaustion
    // ========================================================================

    public function testDeadlineExhaustedThrowsRetryBudgetExhausted(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 100000, // high enough that deadline fires first
            deadlineMs: 1, // very short deadline
        );

        // Use None backoff (sleepMs=0) so the loop iterates without delay
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::None;

        $operation = function (): void {
            throw new TiKvException('test error');
        };

        $this->expectException(RetryBudgetExhaustedException::class);
        $this->expectExceptionMessage('Retry deadline (1 ms) exhausted');

        $executor->execute('test_key', $operation, $classifier);
    }

    /**
     * Regression test for issue #269 (GRPC-10): the wall-clock deadline
     * branch must redact the key in the exception message too, and expose it
     * only via getRawKey(). BackoffType::None keeps the deadline check on a
     * zero-sleep loop, so the test is fast.
     */
    public function testDeadlineExhaustionRedactsRawKeyInMessage(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 100000, // high enough that the deadline fires first
            deadlineMs: 1,
        );

        $classifier = fn(TiKvException $e): BackoffType => BackoffType::None;
        $rawKey = 'user_email:alice@example.com';

        $operation = function (): void {
            throw new TiKvException('test error');
        };

        $this->expectException(RetryBudgetExhaustedException::class);

        try {
            $executor->execute($rawKey, $operation, $classifier);
        } catch (RetryBudgetExhaustedException $e) {
            $message = $e->getMessage();
            $this->assertStringNotContainsString($rawKey, $message);
            $this->assertStringContainsString(KeyRedactor::redact($rawKey), $message);
            $this->assertStringContainsString('Retry deadline (1 ms) exhausted for key', $message);
            $this->assertSame($rawKey, $e->getRawKey());
            throw $e;
        }
    }

    public function testDeadlineNotHitWhenOperationSucceeds(): void
    {
        $executor = $this->createExecutor(
            deadlineMs: 5000, // generous deadline
        );

        $result = $executor->execute('test_key', fn(): string => 'fast success');

        $this->assertSame('fast success', $result);
    }

    // ========================================================================
    // Custom classifier
    // ========================================================================

    public function testCustomClassifierIsUsedAndOverridesInternalClassification(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 2,
        );

        $operation = function (): void {
            throw new TiKvException('CustomError');
        };

        $classifier = function (TiKvException $e): \CrazyGoat\TiKV\Client\Retry\BackoffType {
            $this->assertStringContainsString('CustomError', $e->getMessage());
            return BackoffType::ServerBusy;
        };

        $this->expectException(RetryBudgetExhaustedException::class);

        $executor->execute('test_key', $operation, $classifier);
    }

    // ========================================================================
    // Constructor validation
    // ========================================================================

    public function testConstructorRejectsZeroMaxAttempts(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxAttempts must be >= 1');

        new RetryExecutor(
            maxBackoffMs: 1000,
            serverBusyBudgetMs: 1000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            maxAttempts: 0,
        );
    }

    public function testConstructorRejectsNegativeDeadline(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('deadlineMs must be >= 0');

        new RetryExecutor(
            maxBackoffMs: 1000,
            serverBusyBudgetMs: 1000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            deadlineMs: -1,
        );
    }

    // ========================================================================
    // Metrics emission
    // ========================================================================

    public function testNoRetryMeansNoMetricsEmitted(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor(metrics: $metrics);

        $result = $executor->execute('test_key', fn(): string => 'success');

        $this->assertSame('success', $result);
        $this->assertSame(0, $metrics->getRetries('NotLeader'));
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'));
    }

    public function testRetryableErrorIncrementsRetryMetric(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor(metrics: $metrics);

        $attempts = 0;
        try {
            $executor->execute('retry_key', function () use (&$attempts): void {
                $attempts++;
                // "RegionNotFound" matches the message-fallback classifier → BackoffType::RegionMiss.
                throw new TiKvException('RegionNotFound oh no');
            });
        } catch (TiKvException) {
            // expected — we just want the retry loop to run to exhaustion
        }

        $this->assertGreaterThan(1, $attempts, 'Should have attempted > 1 time');
        $this->assertGreaterThan(
            0,
            $metrics->getRetries('RegionMiss'),
            'RetryExecutor should have emitted retries tagged with RegionMiss'
        );
    }

    public function testRegionInvalidationMetricEmittedOnRetry(): void
    {
        $metrics = new InMemoryMetrics();

        $executor = $this->createExecutor(metrics: $metrics);

        try {
            // DataIsNotReady classifies as BackoffType::None (sleepMs=0) —
            // the only remaining zero-sleep class since EpochNotMatch got a
            // real backoff (issue #241). Total backoff stays at 0 so the
            // 30-attempt cap kicks in, giving us a deterministic number of
            // retries (DEFAULT_MAX_ATTEMPTS - 1 = 29 attempts to retry).
            $executor->execute('retry_key', function (): void {
                throw new TiKvException('DataIsNotReady something');
            });
        } catch (RetryBudgetExhaustedException) {
            // expected
        }

        $attemptCap = RetryExecutor::DEFAULT_MAX_ATTEMPTS - 1;
        $this->assertGreaterThanOrEqual(
            $attemptCap,
            $metrics->getRetries('None'),
            'RetryExecutor should have recorded a retry metric for every attempt past the first'
        );
    }

    // ========================================================================
    // Budget reset across calls (issue #271)
    //
    // RetryExecutor is reused across many operations (Transaction,
    // RawKvScanner, RawKvBatch). maxBackoffMs is a per-operation limit, so
    // each execute() call must start with a full budget — otherwise the
    // first operation silently consumes most of it and later operations
    // fail on their first retryable error.
    // ========================================================================

    public function testReusedExecutorRetriesNormallyOnSecondCallAfterFirstConsumesBudget(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 63,
            serverBusyBudgetMs: 10000,
        );

        // RegionMiss: baseMs=2, capMs=500, equal jitter (issue #242), so the
        // n-th retry sleeps in [2^n, 2^(n+1)] ms and a call's worst-case
        // contribution is 2+4+...+2^(k+1) = 2^(k+2)-2. maxBackoffMs=63 lets
        // the first call accumulate its worst case 2+4+8+16+32 = 62 ms and
        // still succeed (best case 1+2+4+8+16=31). The second call's single
        // retry then crosses 63 only if the budget were carried over between
        // calls (worst case 62+2=64 > 63), so this detects carry-over
        // probabilistically while the "second call succeeds" invariant below
        // holds deterministically.
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        $firstCalls = 0;
        $firstResult = $executor->execute('first_key', function () use (&$firstCalls): string {
            $firstCalls++;
            if ($firstCalls <= 5) {
                throw new TiKvException('first error');
            }
            return 'first-ok';
        }, $classifier);

        $this->assertSame('first-ok', $firstResult);
        $this->assertSame(6, $firstCalls, 'First call should have retried 5 times');

        // A reused executor must retry normally on the second call with a
        // full budget, not carry the first call's spend forward.
        $secondCalls = 0;
        $secondResult = $executor->execute('second_key', function () use (&$secondCalls): string {
            $secondCalls++;
            if ($secondCalls === 1) {
                throw new TiKvException('second error');
            }
            return 'second-ok';
        }, $classifier);

        $this->assertSame('second-ok', $secondResult);
        $this->assertSame(2, $secondCalls, 'Second call should have retried once with a fresh budget');
    }

    // ========================================================================
    // EpochNotMatch minimum backoff (issue #241, REG-10)
    //
    // EpochNotMatch used to classify as BackoffType::None (sleepMs=0): the
    // retries were zero-delay and free, and the only bound was the attempt
    // cap. They now get a small jittered backoff (2–500 ms, like client-go's
    // BoEpochNotMatch) whose sleeps feed totalBackoffMs, so repeated errors
    // exhaust the per-operation backoff budget before the attempt cap.
    // ========================================================================

    public function testEpochNotMatchRetriesExhaustBackoffBudgetBeforeAttemptCap(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 50,
            serverBusyBudgetMs: 10000,
        );

        $attempts = 0;
        $startMs = (int) (microtime(true) * 1000);
        try {
            $executor->execute('epoch_key', function () use (&$attempts): void {
                $attempts++;
                throw new TiKvException('EpochNotMatch something');
            });
        } catch (TiKvException) {
            // Budget exhaustion rethrows the ORIGINAL error (not
            // RetryBudgetExhaustedException) for sleep-budget classes.
        }

        $elapsedMs = (int) (microtime(true) * 1000) - $startMs;

        // The budget (50 ms) must fire long before the 30-attempt cap:
        // jittered sleeps are 1-2, 2-4, 4-8, 8-16, 16-32, 32-64... ms, so
        // only a handful of retries fit.
        $this->assertLessThan(RetryExecutor::DEFAULT_MAX_ATTEMPTS, $attempts);
        $this->assertGreaterThanOrEqual(2, $attempts, 'at least one retry must happen before exhaustion');

        // The sleeps are real, non-zero wall-clock time (a zero-sleep loop
        // would finish in well under 10 ms).
        $this->assertGreaterThanOrEqual(10, $elapsedMs);
    }

    public function testEpochNotMatchRetriesProduceAtMostOnePdGetRegionPerAttempt(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $pdCalls = 0;
        $this->pdClient->method('getRegion')->willReturnCallback(function () use (&$pdCalls, $region): RegionInfo {
            $pdCalls++;
            return $region;
        });

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);
        $executor = $this->createExecutor(
            maxBackoffMs: 50,
            serverBusyBudgetMs: 10000,
        );

        $attempts = 0;
        try {
            // Mirror real usage: the retried closure re-resolves the region
            // on every attempt (cache miss → PD GetRegion) — exactly one
            // PD call per attempt is the upper bound.
            $executor->execute('epoch_key', function () use (&$attempts, $resolver): void {
                $attempts++;
                $resolver->getRegionInfo('epoch_key');
                throw new TiKvException('EpochNotMatch something');
            });
        } catch (TiKvException) {
            // expected
        }

        $this->assertSame($attempts, $pdCalls, 'each attempt must cost at most one PD GetRegion call');
        $this->assertLessThan(RetryExecutor::DEFAULT_MAX_ATTEMPTS, $attempts);
    }

    public function testNestedExecuteDoesNotCorruptOuterAttemptCounter(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            maxAttempts: 5,
        );

        // execute() must be reentrant: the state (attempt count, budgets) is
        // per-invocation, so a nested execute() call inside a retried
        // closure must not reset or corrupt the outer loop's counters.
        //
        // The outer operation fails every time and nests a succeeding
        // execute() on each attempt. EpochNotMatch is classified as
        // BackoffType::EpochNotMatch (small 2/4/8… ms sleeps since issue
        // #241), so the budget does not exhaust and the outer loop is
        // bounded by the attempt cap (5). Under the
        // old instance-field implementation the nested call reset
        // $this->attempt to 0 on every iteration, so the counter never
        // reached the cap and the loop would never have terminated.
        $nestedCalls = 0;
        $outerCalls = 0;
        try {
            $executor->execute('outer_key', function () use ($executor, &$nestedCalls, &$outerCalls): string {
                $outerCalls++;
                $executor->execute('inner_key', function () use (&$nestedCalls): string {
                    $nestedCalls++;
                    return 'inner-ok';
                });
                throw new TiKvException('EpochNotMatch something');
            });
        } catch (RetryBudgetExhaustedException) {
            // expected
        }

        // The nested calls must not have reset the outer attempt counter:
        // the cap fires after exactly maxAttempts outer attempts.
        $this->assertSame(5, $outerCalls, 'Outer loop must hit its own attempt cap; nested calls must not reset it');
        $this->assertSame(5, $nestedCalls, 'Inner execute() should have succeeded once per outer attempt');
    }

    // ========================================================================
    // Wall-clock deadline (issue #237, REG-06)
    // ========================================================================

    public function testDeadlineParameterDefaultsToNonZeroConstant(): void
    {
        $constructor = (new \ReflectionClass(RetryExecutor::class))->getConstructor();
        $this->assertNotNull($constructor);
        $deadlineParam = null;
        foreach ($constructor->getParameters() as $parameter) {
            if ($parameter->getName() === 'deadlineMs') {
                $deadlineParam = $parameter;
                break;
            }
        }
        $this->assertNotNull($deadlineParam, 'constructor must have a $deadlineMs parameter');

        $this->assertSame(RetryExecutor::DEFAULT_RETRY_DEADLINE_MS, $deadlineParam->getDefaultValue());
        $this->assertGreaterThan(0, RetryExecutor::DEFAULT_RETRY_DEADLINE_MS);
    }

    public function testBackoffSleepIsClampedToRemainingDeadline(): void
    {
        // ServerIsBusy classifies as BackoffType::ServerBusy whose first
        // sleep is ~1000-2000 ms (equal jitter). With a 50 ms deadline the
        // sleep must be clamped to the remaining budget instead of running
        // the full interval — before the #237 fix the loop overshot the
        // deadline by up to one whole ServerBusy sleep (~2 s).
        $executor = $this->createExecutor(
            maxBackoffMs: 0,           // disable the non-ServerBusy budget
            serverBusyBudgetMs: 10000, // cannot bind within the deadline
            deadlineMs: 50,
        );

        $startMs = (int) (microtime(true) * 1000);

        try {
            $executor->execute('key', function (): string {
                throw new TiKvException('ServerIsBusy');
            });
        } catch (RetryBudgetExhaustedException $e) {
            $this->assertStringContainsString('deadline', $e->getMessage());
        } finally {
            $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
            // Clamp + pre-sleep deadline check must keep the loop far below
            // one full ServerBusy sleep; 900 ms gives CI generous margin.
            $this->assertLessThan(900, $elapsedMs, 'Backoff sleep must be clamped to the remaining deadline');
        }
    }
}
