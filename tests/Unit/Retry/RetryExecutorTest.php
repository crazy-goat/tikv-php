<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
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
        $operation = function () {
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

        $operation = function () {
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

        $operation = function () {
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

        $operation = function () {
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

        $operation = function () {
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
        $operation = function () use ($lastError): never {
            throw $lastError;
        };

        try {
            $executor->execute('test_key', $operation, $classifier);
            $this->fail('Expected RetryBudgetExhaustedException');
        } catch (RetryBudgetExhaustedException $e) {
            $this->assertSame(3, $e->attempts());
            $this->assertStringContainsString('test_key', $e->getMessage());
            $previous = $e->getPrevious();
            $this->assertSame($lastError, $previous);
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

        $operation = function () {
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

        $operation = function () {
            throw new TiKvException('CustomError');
        };

        $classifier = function (TiKvException $e): ?BackoffType {
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
}
