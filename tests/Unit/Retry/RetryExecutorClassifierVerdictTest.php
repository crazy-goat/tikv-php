<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #534: the classifier contract is a RetryVerdict, not `?BackoffType`.
 *
 * The three verdicts must behave differently at the executor boundary:
 * Retry retries, Fatal runs the fatal path (including the broken-channel
 * cleanup), and Pass falls through to the built-in classifier.
 */
class RetryExecutorClassifierVerdictTest extends TestCase
{
    private const KEY = 'some_key';
    private const DEAD_ADDRESS = 'dead-store:20160';

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

    private function createExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            deadlineMs: 0,
            metrics: new InMemoryMetrics(),
        );
    }

    public function testFatalVerdictClosesTheBrokenChannelAndRethrows(): void
    {
        $error = new GrpcException(
            'connection reset',
            GrpcStatusCode::Unavailable->value,
            self::DEAD_ADDRESS,
        );

        // The bug in #534: the CAS classifier forced a fatal verdict by
        // throwing, which skipped closeChannel(). A fatal verdict must run
        // the same cleanup as the retryable path.
        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with(self::DEAD_ADDRESS);

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor()->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    $calls++;
                    throw $error;
                },
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            );
        } catch (GrpcException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught, 'the original exception must propagate unchanged');
        self::assertSame(1, $calls, 'a fatal verdict must not be retried');
    }

    public function testFatalVerdictDoesNotFallThroughToTheBuiltInClassifier(): void
    {
        // The built-in classifier would retry DataIsNotReady immediately;
        // an explicit Fatal must win over that (the old null verdict could
        // not express this without throwing).
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor()->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    $calls++;
                    throw $error;
                },
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            );
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught);
        self::assertSame(1, $calls, 'Fatal must stop the chain before the built-in classifier');
    }

    public function testPassVerdictFallsThroughToTheBuiltInClassifier(): void
    {
        // DataIsNotReady maps to BackoffType::None, so the fallback retry is
        // zero-sleep and the test stays fast.
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $result = $this->createExecutor()->execute(
            self::KEY,
            function () use (&$calls, $error): string {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return 'ok';
            },
            static fn (TiKvException $e): RetryVerdict => RetryVerdict::pass(),
        );

        self::assertSame('ok', $result);
        self::assertSame(2, $calls, 'Pass must defer to the built-in classifier, which retries');
    }

    public function testFatalVerdictForANonBrokenStatusKeepsTheChannelOpen(): void
    {
        // UNAUTHENTICATED is fatal but does not prove the channel is broken,
        // so the fatal path must not tear it down (issue #248's rule).
        $this->grpc->expects($this->never())->method('closeChannel');

        $error = new GrpcException('Unauthenticated', GrpcStatusCode::Unauthenticated->value);

        $this->expectException(GrpcException::class);

        $this->createExecutor()->execute(
            self::KEY,
            static function () use ($error): string {
                throw $error;
            },
            static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
        );
    }
}
