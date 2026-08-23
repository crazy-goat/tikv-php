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
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
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

        // Use a custom classifier to ensure deterministic backoff (no jitter)
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        // RegionMiss: baseMs=2, capMs=500, equalJitter=false
        // attempt 0: sleepMs=2, totalBackoffMs=0+2=2, 2<=10 → retry
        // attempt 1: sleepMs=4, totalBackoffMs=2+4=6, 6<=10 → retry
        // attempt 2: sleepMs=8, totalBackoffMs=6+8=14, 14>10 → throw
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
            $this->assertStringContainsString('test_key', $e->getMessage());
            $previous = $e->getPrevious();
            $this->assertSame($lastError, $previous);
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
            // EpochNotMatch → BackoffType::None (sleepMs=0). Total backoff stays
            // at 0 so the 30-attempt cap kicks in, giving us a deterministic
            // number of retries (DEFAULT_MAX_ATTEMPTS - 1 = 29 attempts to retry).
            $executor->execute('retry_key', function (): void {
                throw new TiKvException('EpochNotMatch something');
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

        // RegionMiss: baseMs=2, capMs=500, equalJitter=false, so the n-th
        // retry sleeps 2^(n+1) ms and a call's running contribution is
        // 2+4+...+2^k = 2^(k+1)-2. maxBackoffMs=63 lets the first call
        // accumulate 2+4+8+16+32 = 62 ms (most of the budget) and still
        // succeed. The second call's single retry (+2) then crosses 63 only
        // if the budget were carried over between calls (62+2=64 > 63).
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

    public function testNestedExecuteDoesNotCorruptOuterAttemptCounter(): void
    {
        $executor = $this->createExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
        );

        // execute() must be reentrant: the state (attempt count, budgets) is
        // per-invocation, so a nested execute() call inside a retried
        // closure must not reset or corrupt the outer loop's counters.
        $classifier = fn(TiKvException $e): BackoffType => BackoffType::RegionMiss;

        $nestedCalls = 0;
        $outerCalls = 0;
        $result = $executor->execute('outer_key', function () use (
            $executor,
            $classifier,
            &$nestedCalls,
            &$outerCalls,
        ): string {
            $outerCalls++;
            // First outer attempt nests a full execute() that itself retries
            // once, then the outer operation fails and must be retried too.
            if ($outerCalls === 1) {
                $executor->execute('inner_key', function () use (&$nestedCalls): string {
                    $nestedCalls++;
                    if ($nestedCalls === 1) {
                        throw new TiKvException('inner error');
                    }
                    return 'inner-ok';
                }, $classifier);
                throw new TiKvException('outer error');
            }
            return 'outer-ok';
        }, $classifier);

        $this->assertSame('outer-ok', $result);
        $this->assertSame(2, $outerCalls, 'Outer op runs twice; nested call must not lift the cap');
        $this->assertSame(2, $nestedCalls, 'Inner operation should have retried once within its own execute()');
    }
}
