<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Detailed (issue #434, GAP-20) retry metrics: backoff durations and
 * per-store attempt outcomes, emitted only to a DetailedMetricsInterface
 * backend.
 */
class RetryExecutorDetailedMetricsTest extends TestCase
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

        $this->regionCache->method('getByKey')->willReturn(null);
    }

    private function createExecutor(InMemoryMetrics $metrics, int $maxAttempts = 30): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            maxAttempts: $maxAttempts,
            metrics: $metrics,
        );
    }

    public function testSuccessfulAttemptRecordsStoreSuccess(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor($metrics);

        $result = $executor->execute(
            'key',
            static fn (): string => 'ok',
            targetStoreId: static fn (): int => 5,
        );

        $this->assertSame('ok', $result);
        $this->assertSame(1, $metrics->getStoreSuccesses(5));
        $this->assertSame(0, $metrics->getStoreFailures(5));
        $this->assertSame(0, $metrics->getBackoffCount('None'));
    }

    public function testRetryRecordsBackoffAndStoreFailure(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor($metrics);
        $attempts = 0;

        $result = $executor->execute(
            'key',
            static function () use (&$attempts): string {
                $attempts++;
                if ($attempts === 1) {
                    throw new TiKvException('transient');
                }

                return 'ok';
            },
            static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None),
            targetStoreId: static fn (): int => 5,
        );

        $this->assertSame('ok', $result);
        $this->assertSame(1, $metrics->getBackoffCount('None'));
        $this->assertSame(1, $metrics->getStoreFailures(5));
        $this->assertSame(['unknown' => 1], $metrics->getStoreErrorKinds(5));
        $this->assertSame(1, $metrics->getStoreSuccesses(5));
    }

    public function testAttemptWithoutAKnownStoreRecordsNothing(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor($metrics);

        $executor->execute('key', static fn (): string => 'ok');

        $this->assertSame(0, $metrics->getStoreSuccesses(0));
        $this->assertSame(0, $metrics->getStoreFailures(0));
    }

    public function testTransportFailureIsLabelledTransport(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor($metrics, maxAttempts: 1);

        try {
            $executor->execute(
                'key',
                static fn (): string => throw new GrpcException('down', GrpcStatusCode::Unavailable->value),
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None),
                targetStoreId: static fn (): int => 5,
            );
        } catch (TiKvException) {
            // expected: the original failure is rethrown after retrying
        }

        $this->assertSame(['transport' => 1], $metrics->getStoreErrorKinds(5));
    }

    public function testRegionErrorIsLabelledByKind(): void
    {
        $metrics = new InMemoryMetrics();
        $executor = $this->createExecutor($metrics, maxAttempts: 1);

        try {
            $executor->execute(
                'key',
                static fn (): string => throw new RegionException(
                    'RawGet',
                    'busy',
                    errorKind: ErrorKind::ServerIsBusy,
                ),
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None),
                targetStoreId: static fn (): int => 5,
            );
        } catch (TiKvException) {
            // expected
        }

        $this->assertSame(['server_is_busy' => 1], $metrics->getStoreErrorKinds(5));
    }
}
