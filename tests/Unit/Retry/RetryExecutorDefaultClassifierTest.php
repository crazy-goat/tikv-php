<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #719: the executor accepts a client-level classifier.
 *
 * It is the seam `options['retryClassifier']` is built on: an executor-level
 * classifier is consulted when the per-call classifier has no opinion, and
 * before the built-in ErrorClassifier, so a custom classification can extend
 * or override the built-in table without rebuilding the executor.
 */
class RetryExecutorDefaultClassifierTest extends TestCase
{
    private const KEY = 'some_key';

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

    /**
     * @param (callable(TiKvException): RetryVerdict)|null $classifier
     */
    private function createExecutor(?callable $classifier = null): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            deadlineMs: 0,
            classifier: $classifier,
        );
    }

    public function testExecutorClassifierCanRetryAnErrorTheBuiltInClassifierCallsFatal(): void
    {
        // UNAUTHENTICATED is fatal to the built-in classifier (issue #240);
        // the custom classifier overrides that with an immediate retry.
        $error = new GrpcException('Unauthenticated', GrpcStatusCode::Unauthenticated->value);

        $calls = 0;
        $result = $this->createExecutor(
            static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None),
        )->execute(
            self::KEY,
            function () use (&$calls, $error): string {
                ++$calls;
                if ($calls === 1) {
                    throw $error;
                }

                return 'ok';
            },
        );

        self::assertSame('ok', $result);
        self::assertSame(2, $calls, 'a Retry verdict from the executor classifier must retry');
    }

    public function testExecutorClassifierCanMarkABuiltInRetryableErrorFatal(): void
    {
        // DataIsNotReady is retried immediately by the built-in classifier;
        // a Fatal verdict from the client-level classifier must win.
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor(
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            )->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    ++$calls;
                    throw $error;
                },
            );
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught);
        self::assertSame(1, $calls, 'a Fatal verdict must stop the loop before the built-in classifier');
    }

    public function testPassFromTheExecutorClassifierFallsThroughToTheBuiltInClassifier(): void
    {
        // DataIsNotReady maps to BackoffType::None, so the fallback retry is
        // zero-sleep and the test stays fast.
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $result = $this->createExecutor(
            static fn (TiKvException $e): RetryVerdict => RetryVerdict::pass(),
        )->execute(
            self::KEY,
            function () use (&$calls, $error): string {
                ++$calls;
                if ($calls === 1) {
                    throw $error;
                }

                return 'ok';
            },
        );

        self::assertSame('ok', $result);
        self::assertSame(2, $calls, 'Pass must defer to the built-in classifier, which retries');
    }

    public function testPerCallClassifierWinsOverTheExecutorClassifier(): void
    {
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor(
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None),
            )->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    ++$calls;
                    throw $error;
                },
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            );
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught);
        self::assertSame(1, $calls, 'the per-call classifier must be consulted first');
    }

    public function testConstructorAcceptsANonClosureCallable(): void
    {
        // An invokable object is a callable but not a Closure; the executor
        // must normalise it (issue #719 review).
        $classifier = new class {
            public function __invoke(TiKvException $e): RetryVerdict
            {
                return RetryVerdict::fatal();
            }
        };

        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor($classifier)->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    ++$calls;
                    throw $error;
                },
            );
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught);
        self::assertSame(1, $calls, 'an invokable object must be normalised like a Closure');
    }

    public function testBeginOperationKeepsTheExecutorClassifier(): void
    {
        $error = new TiKvException('DataIsNotReady');

        $calls = 0;
        $caught = null;
        try {
            $this->createExecutor(
                static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            )->beginOperation()->execute(
                self::KEY,
                function () use (&$calls, $error): string {
                    ++$calls;
                    throw $error;
                },
            );
        } catch (TiKvException $e) {
            $caught = $e;
        }

        self::assertSame($error, $caught);
        self::assertSame(1, $calls, 'beginOperation() must carry the classifier to the derived executor');
    }
}
