<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\BucketVersionNotMatch;
use CrazyGoat\Proto\Errorpb\DataIsNotReady;
use CrazyGoat\Proto\Errorpb\DiskFull;
use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\IsWitness;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\Errorpb\MaxTimestampNotSynced;
use CrazyGoat\Proto\Errorpb\MismatchPeerId;
use CrazyGoat\Proto\Errorpb\ProposalInMergingMode;
use CrazyGoat\Proto\Errorpb\ReadIndexNotReady;
use CrazyGoat\Proto\Errorpb\RecoveryInProgress;
use CrazyGoat\Proto\Errorpb\RegionNotFound;
use CrazyGoat\Proto\Errorpb\RegionNotInitialized;
use CrazyGoat\Proto\Errorpb\ServerIsBusy;
use CrazyGoat\Proto\Errorpb\StaleCommand;
use CrazyGoat\Proto\Errorpb\StoreNotMatch;
use CrazyGoat\Proto\Errorpb\UndeterminedResult;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\NotLeaderHint;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #245 (REG-14): RetryExecutor must drop the cached region only for a
 * retryable error that says the routing information is stale. A busy leader
 * (ServerIsBusy), a full disk (DiskFull), a witness peer (IsWitness) or a
 * store in recovery (RecoveryInProgress) reached the right region, so
 * invalidating it would only add a PD GetRegion to every retry attempt.
 *
 * These tests drive the real executor against a mocked RegionCacheInterface,
 * because the assertion is about the call itself: invalidate() must not be
 * invoked for a non-routing retry and must be invoked exactly once for each
 * routing kind. The metric label is pinned separately against a real
 * RegionCache, which is the single emission point for regionInvalidated().
 */
class RetryExecutorRoutingInvalidationTest extends TestCase
{
    private const KEY = 'some_key';
    private const REGION_ID = 42;

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
    }

    private function createExecutor(
        ?RegionCacheInterface $cache = null,
        ?InMemoryMetrics $metrics = null,
    ): RetryExecutor {
        $cache ??= $this->regionCache;

        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $cache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $cache),
            logger: $this->logger,
            maxAttempts: 2,
            metrics: $metrics ?? new InMemoryMetrics(),
        );
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    /**
     * @param callable(Error): void $setVariant
     */
    private function regionError(callable $setVariant): RegionException
    {
        $error = new Error();
        $error->setMessage('region error from TiKV');
        $setVariant($error);

        return RegionException::fromRegionError($error);
    }

    /**
     * A zero-sleep classifier keeps the regression tests fast and
     * deterministic: the error kind under test is still the one carried by
     * the RegionException, which is what the invalidation gate reads.
     */
    private function zeroSleepClassifier(): \Closure
    {
        return static fn (TiKvException $e): RetryVerdict => RetryVerdict::retry(BackoffType::None);
    }

    /**
     * @return array<string, array{callable(Error): void, ErrorKind, BackoffType}>
     */
    public static function provideNonRoutingRetryableVariants(): array
    {
        return [
            'ServerIsBusy' => [static function (Error $error): void {
                $error->setServerIsBusy(new ServerIsBusy());
            }, ErrorKind::ServerIsBusy, BackoffType::ServerBusy],
            'DiskFull' => [static function (Error $error): void {
                $error->setDiskFull(new DiskFull());
            }, ErrorKind::DiskFull, BackoffType::DiskFull],
            'IsWitness' => [static function (Error $error): void {
                $error->setIsWitness(new IsWitness());
            }, ErrorKind::IsWitness, BackoffType::IsWitness],
            'RecoveryInProgress' => [static function (Error $error): void {
                $error->setRecoveryInProgress(new RecoveryInProgress());
            }, ErrorKind::RecoveryInProgress, BackoffType::RecoveryInProgress],
            'StaleCommand' => [static function (Error $error): void {
                $error->setStaleCommand(new StaleCommand());
            }, ErrorKind::StaleCommand, BackoffType::StaleCmd],
            'DataIsNotReady' => [static function (Error $error): void {
                $error->setDataIsNotReady(new DataIsNotReady());
            }, ErrorKind::DataIsNotReady, BackoffType::None],
            'ReadIndexNotReady' => [static function (Error $error): void {
                $error->setReadIndexNotReady(new ReadIndexNotReady());
            }, ErrorKind::ReadIndexNotReady, BackoffType::ReadIndexNotReady],
            'ProposalInMergingMode' => [static function (Error $error): void {
                $error->setProposalInMergingMode(new ProposalInMergingMode());
            }, ErrorKind::ProposalInMergingMode, BackoffType::ProposalInMergingMode],
            'MaxTimestampNotSynced' => [static function (Error $error): void {
                $error->setMaxTimestampNotSynced(new MaxTimestampNotSynced());
            }, ErrorKind::MaxTimestampNotSynced, BackoffType::MaxTimestampNotSynced],
            'MismatchPeerId' => [static function (Error $error): void {
                $error->setMismatchPeerId(new MismatchPeerId());
            }, ErrorKind::MismatchPeerId, BackoffType::RegionMiss],
            'BucketVersionNotMatch' => [static function (Error $error): void {
                $error->setBucketVersionNotMatch(new BucketVersionNotMatch());
            }, ErrorKind::BucketVersionNotMatch, BackoffType::RegionMiss],
            'UndeterminedResult' => [static function (Error $error): void {
                $error->setUndeterminedResult(new UndeterminedResult());
            }, ErrorKind::UndeterminedResult, BackoffType::RegionMiss],
        ];
    }

    #[DataProvider('provideNonRoutingRetryableVariants')]
    public function testNonRoutingRetryableErrorRetriesWithoutInvalidating(
        callable $setVariant,
        ErrorKind $expectedKind,
        BackoffType $expectedBackoff,
    ): void {
        $error = $this->regionError($setVariant);
        $this->assertSame($expectedKind, $error->errorKind, 'fixture must carry the typed kind under test');
        $this->assertSame(
            $expectedBackoff,
            ErrorClassifier::classify($error)->backoffType(),
            'fixture must be retryable through the real classifier, otherwise it never reaches this branch',
        );

        // The whole point of #245: no lookup and no drop for a non-routing
        // retryable error. Asserting getByKey() too proves the executor does
        // not even inspect the cache on this path.
        $this->regionCache->expects($this->never())->method('getByKey');
        $this->regionCache->expects($this->never())->method('invalidate');

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
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(2, $calls, 'a non-routing retryable error must still be retried');
    }

    /**
     * @return array<string, array{callable(Error): void, ErrorKind}>
     */
    public static function provideRoutingRetryableVariants(): array
    {
        return [
            'EpochNotMatch' => [static function (Error $error): void {
                $error->setEpochNotMatch(new EpochNotMatch());
            }, ErrorKind::EpochNotMatch],
            'RegionNotFound' => [static function (Error $error): void {
                $error->setRegionNotFound(new RegionNotFound());
            }, ErrorKind::RegionNotFound],
            'StoreNotMatch' => [static function (Error $error): void {
                $error->setStoreNotMatch(new StoreNotMatch());
            }, ErrorKind::StoreNotMatch],
            'RegionNotInitialized' => [static function (Error $error): void {
                $error->setRegionNotInitialized(new RegionNotInitialized());
            }, ErrorKind::RegionNotInitialized],
        ];
    }

    #[DataProvider('provideRoutingRetryableVariants')]
    public function testRoutingRetryableErrorInvalidatesTheRegionWithTheKindTaggedReason(
        callable $setVariant,
        ErrorKind $expectedKind,
    ): void {
        $error = $this->regionError($setVariant);
        $this->assertSame($expectedKind, $error->errorKind);

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'retry_region_error:' . $expectedKind->value);

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
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(2, $calls, 'a routing error must still be retried');
    }

    public function testNotLeaderWithoutHintInvalidatesTheRegion(): void
    {
        $notLeader = new NotLeaderHint(regionId: self::REGION_ID);
        $error = new RegionException(
            operation: 'KvGet',
            message: 'not leader',
            notLeader: $notLeader,
        );

        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'not_leader');

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
        );

        $this->assertSame('ok', $result);
        $this->assertSame(2, $calls, 'NotLeader without a usable hint must be retried');
    }

    public function testFatalKeyNotInRegionInvalidatesTheRegion(): void
    {
        $error = $this->regionError(static function (Error $error): void {
            $error->setKeyNotInRegion(new KeyNotInRegion());
        });

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'fatal_region_error');
        $this->grpc->expects($this->never())->method('closeChannel');

        $caught = null;
        try {
            $this->createExecutor()->execute(self::KEY, static function () use ($error): string {
                throw $error;
            });
        } catch (RegionException $e) {
            $caught = $e;
        }

        $this->assertSame($error, $caught, 'KeyNotInRegion stays fatal');
    }

    public function testUnclassifiedRetryableErrorStillInvalidatesConservatively(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'retry_region_error');

        $calls = 0;
        $result = $this->createExecutor()->execute(
            self::KEY,
            function () use (&$calls): string {
                $calls++;
                if ($calls === 1) {
                    // A message-only classification: no typed ErrorKind, so
                    // the executor cannot tell whether this was a routing
                    // error and must invalidate.
                    throw new TiKvException('EpochNotMatch something');
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(2, $calls);
    }

    public function testMetricLabelNamesTheRoutingErrorKind(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->region());

        $error = $this->regionError(static function (Error $error): void {
            $error->setEpochNotMatch(new EpochNotMatch());
        });

        $calls = 0;
        $result = $this->createExecutor($cache, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $error): string {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            1,
            $metrics->getInvalidations('retry_region_error:epoch_not_match'),
            'the metric label must name the triggering ErrorKind',
        );
        $this->assertSame(
            0,
            $metrics->getInvalidations('retry_region_error'),
            'a classified routing error must not fall back to the generic label',
        );
    }
}
