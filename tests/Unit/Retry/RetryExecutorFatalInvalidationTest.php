<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\FlashbackInProgress;
use CrazyGoat\Proto\Errorpb\FlashbackNotPrepared;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Errorpb\RaftEntryTooLarge;
use CrazyGoat\Proto\Errorpb\RegionNotFound;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #233 (REG-02): retryability and region-cache invalidation are two
 * INDEPENDENT decisions, expressed as two functions —
 * ErrorClassifier::classify() (via RetryExecutor::classifyError()) and
 * RetryExecutor::invalidatesRoutingOnFatal().
 *
 * A routing error means the cached region that produced it is wrong, so the
 * entry must be dropped whether or not the request is retried. Before this
 * fix the invalidation lived BELOW the fatal `throw $e`, so a fatal
 * KeyNotInRegion — the one error whose entire meaning is "your cached
 * routing information is wrong" — rethrew without removing the entry that
 * caused it, and every later request for that key repeated the same fatal
 * error until the entry expired (ttlSeconds + jitterSeconds = 660 s with the
 * defaults).
 *
 * These tests drive the executor against a REAL RegionCache (not a mock):
 * the assertion the issue asks for is "the cache no longer CONTAINS the
 * region", which a mocked RegionCacheInterface can only restate as "invalidate
 * was called". Every RawKV call site runs RegionErrorHandler::check($response)
 * WITHOUT a cache/regionId, so RetryExecutor's own invalidation is the only
 * one on those paths — which is exactly why the fatal-path gap was a real
 * outage for RawKV users. One test drives the opposite composition, the TxnKV
 * one (check() WITH a cache/regionId), where the drop has already happened by
 * the time the fatal path runs and must therefore not be counted twice.
 */
class RetryExecutorFatalInvalidationTest extends TestCase
{
    private const KEY = 'some_key';
    private const REGION_ID = 42;

    private GrpcClientInterface&MockObject $grpc;

    private PdClientInterface&MockObject $pdClient;

    private LoggerInterface&MockObject $logger;

    private InMemoryMetrics $metrics;

    private RegionCache $cache;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->metrics = new InMemoryMetrics();
        $this->cache = new RegionCache(metrics: $this->metrics);
    }

    private function createExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 63,
            serverBusyBudgetMs: 10000,
            regionCache: $this->cache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->cache),
            logger: $this->logger,
            metrics: $this->metrics,
        );
    }

    private function cachedRegion(): RegionInfo
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
     * Build the RegionException exactly as a real call site would: from the
     * protobuf region error, so the typed ErrorKind is detected from the
     * oneof rather than injected by hand.
     *
     * @param callable(Error): void $setVariant
     */
    private function regionError(callable $setVariant): RegionException
    {
        $error = new Error();
        $error->setMessage('region error from TiKV');
        $setVariant($error);

        return RegionException::fromRegionError($error);
    }

    // ========================================================================
    // The bug: a fatal routing error left the misrouting entry cached
    // ========================================================================

    public function testFatalKeyNotInRegionInvalidatesTheCachedRegionAndRethrows(): void
    {
        $this->cache->put($this->cachedRegion());
        $executor = $this->createExecutor();

        // A fatal error that says nothing about retrying but everything
        // about routing: the region that was just used does not own the key.
        $error = $this->regionError(static function (Error $error): void {
            $error->setKeyNotInRegion(new KeyNotInRegion());
        });
        $this->assertSame(
            ErrorKind::KeyNotInRegion,
            $error->errorKind,
            'fixture must carry the typed kind the classifier keys on',
        );

        // A RegionException means the RPC completed at transport level, so
        // the fatal path must not touch the gRPC channel (the retryable
        // branch's closeChannel() is for transport failures only).
        $this->grpc->expects($this->never())->method('closeChannel');

        $calls = 0;
        $caught = null;
        try {
            $executor->execute(self::KEY, function () use (&$calls, $error): string {
                $calls++;
                throw $error;
            });
        } catch (RegionException $e) {
            $caught = $e;
        }

        $this->assertSame($error, $caught, 'the original exception must propagate unchanged');
        $this->assertSame(1, $calls, 'KeyNotInRegion stays fatal — it is not retried');
        $this->assertNull(
            $this->cache->getByKey(self::KEY),
            'the region that misrouted the key must be gone from the cache before the rethrow',
        );
        $this->assertSame(1, $this->metrics->getInvalidations('fatal_region_error'));
        $this->assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
    }

    // ========================================================================
    // Fatal but NOT routing: the cache must be left alone
    // ========================================================================

    /** @return array<string, array{callable(Error): void}> */
    public static function provideFatalNonRoutingVariants(): array
    {
        return [
            'RaftEntryTooLarge' => [static function (Error $error): void {
                $error->setRaftEntryTooLarge(new RaftEntryTooLarge());
            }],
            'FlashbackInProgress' => [static function (Error $error): void {
                $error->setFlashbackInProgress(new FlashbackInProgress());
            }],
            'FlashbackNotPrepared' => [static function (Error $error): void {
                $error->setFlashbackNotPrepared(new FlashbackNotPrepared());
            }],
        ];
    }

    /**
     * The request reached the right region and the region rejected the
     * request itself: the entry was not the problem, so dropping it would
     * only buy a re-resolve. These three kinds stay fatal AND keep the entry.
     */
    #[DataProvider('provideFatalNonRoutingVariants')]
    public function testFatalNonRoutingRegionErrorKeepsTheCachedRegion(callable $setVariant): void
    {
        $this->cache->put($this->cachedRegion());
        $executor = $this->createExecutor();

        $error = $this->regionError($setVariant);
        $this->assertNull(ErrorClassifier::classify($error), 'fixture must be a fatal kind');

        $calls = 0;
        $caught = null;
        try {
            $executor->execute(self::KEY, function () use (&$calls, $error): string {
                $calls++;
                throw $error;
            });
        } catch (RegionException $e) {
            $caught = $e;
        }

        $this->assertSame($error, $caught, 'the original exception must propagate unchanged');
        $this->assertSame(1, $calls, 'these kinds stay fatal — they are not retried');
        $this->assertInstanceOf(
            RegionInfo::class,
            $this->cache->getByKey(self::KEY),
            'a non-routing error must not drop the cached region',
        );
        $this->assertSame(0, $this->metrics->getInvalidations('fatal_region_error'));
        $this->assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
    }

    /**
     * A non-RegionException fatal error says nothing about routing and must
     * never invalidate. UNAUTHENTICATED / PERMISSION_DENIED are transport
     * failures with a fatal status, InvalidStoreAddressException is a PD
     * answer, TxnAbortedByGcException is a GC verdict and an unclassifiable
     * TiKvException is simply unknown — none of them is evidence that a
     * cached region is wrong.
     *
     * @return array<string, array{TiKvException}>
     */
    public static function provideFatalNonRegionErrors(): array
    {
        return [
            // The gRPC details text is free-form, so a retry keyword in it
            // must not be able to smuggle a routing decision either.
            'GrpcException UNAUTHENTICATED mentioning NotLeader' => [
                new GrpcException('NotLeader for UNAUTHENTICATED', 16),
            ],
            'GrpcException PERMISSION_DENIED' => [
                new GrpcException('permission denied', 7),
            ],
            'InvalidStoreAddressException' => [
                new InvalidStoreAddressException('PD returned store address "evil.example:20160"'),
            ],
            // A GC verdict, and the only fatal that is NOT a TxnKvException
            // subclass of TiKvException checked explicitly by the classifier
            // (ErrorClassifier::classify() returns null for it by type, before
            // the message fallback, issue #422).
            'TxnAbortedByGcException' => [
                new TxnAbortedByGcException('GC life time is shorter than transaction duration'),
            ],
            'unclassifiable TiKvException' => [
                new TiKvException('SomeUnknownError'),
            ],
        ];
    }

    #[DataProvider('provideFatalNonRegionErrors')]
    public function testFatalNonRegionErrorKeepsTheCachedRegion(TiKvException $error): void
    {
        $this->cache->put($this->cachedRegion());
        $executor = $this->createExecutor();

        $this->assertNull(ErrorClassifier::classify($error), 'fixture must be a fatal classification');

        $calls = 0;
        $caught = null;
        try {
            $executor->execute(self::KEY, function () use (&$calls, $error): string {
                $calls++;
                throw $error;
            });
        } catch (TiKvException $e) {
            $caught = $e;
        }

        $this->assertSame($error, $caught, 'the original exception must propagate unchanged');
        $this->assertSame(1, $calls, 'a fatal error must not be retried');
        $this->assertInstanceOf(
            RegionInfo::class,
            $this->cache->getByKey(self::KEY),
            'a non-region error carries no routing verdict — the cache must be left alone',
        );
        $this->assertSame(0, $this->metrics->getInvalidations('fatal_region_error'));
    }

    // ========================================================================
    // A call site that already dropped the region: one drop, one count (#474)
    // ========================================================================

    /**
     * The composed call-site shape: TxnReader / LockResolver / TwoPhaseCommitter
     * pass the cache to RegionErrorHandler::check(), which drops the region and
     * emits regionInvalidated('region_error') itself before throwing. The
     * executor's fatal path must then find nothing to do, so the single drop
     * is counted exactly once (issue #474's single-emission rule) and under
     * check()'s reason, not the executor's.
     *
     * Pinned because the guarantee is indirect: the new code looks the region
     * up BY KEY, not by the $regionId that served the request, so it finds
     * nothing only because check() removed that entry and no other cached
     * region covers the key. A put() of the correct region between the two
     * calls would be dropped too and would emit a second drop — the same shape
     * the retryable path already has, and benign there for the same reason
     * (RegionCache::invalidate() emits only on an actual removal).
     */
    public function testCallSiteThatAlreadyDroppedTheRegionCountsExactlyOneInvalidation(): void
    {
        $this->cache->put($this->cachedRegion());
        $executor = $this->createExecutor();
        $cache = $this->cache;

        $error = new Error();
        $error->setMessage('region error from TiKV');
        $error->setKeyNotInRegion(new KeyNotInRegion());
        $response = new RawGetResponse();
        $response->setRegionError($error);

        $calls = 0;
        $caught = null;
        try {
            $executor->execute(self::KEY, function () use (&$calls, $cache, $response): string {
                $calls++;
                // check() drops the entry and throws, exactly as the TxnKV
                // call sites do.
                RegionErrorHandler::check($response, $cache, self::REGION_ID);

                return 'ok';
            });
            $this->fail('Expected the fatal RegionException to propagate');
        } catch (RegionException $e) {
            $caught = $e;
        }

        $this->assertInstanceOf(RegionException::class, $caught);
        $this->assertSame(1, $calls, 'KeyNotInRegion stays fatal — it is not retried');
        $this->assertNull(
            $this->cache->getByKey(self::KEY),
            'check() already dropped the entry, so the fatal path has nothing left to drop',
        );
        $this->assertSame(
            1,
            $this->metrics->getInvalidations('region_error'),
            'check() owns this drop and counts it once',
        );
        $this->assertSame(
            0,
            $this->metrics->getInvalidations('fatal_region_error'),
            'the executor must not count the same drop a second time',
        );
    }

    // ========================================================================
    // The retryable path is unchanged (issue #245 is the one that will narrow
    // it, and it must land on this same predicate)
    // ========================================================================

    /**
     * @return array<string, array{callable(Error): void, string}>
     */
    public static function provideRetryableRoutingVariants(): array
    {
        return [
            // No leader hint, so handleNotLeader() owns the drop ('not_leader').
            'NotLeader' => [static function (Error $error): void {
                $notLeader = new NotLeader();
                $notLeader->setRegionId(42);
                $error->setNotLeader($notLeader);
            }, 'not_leader'],
            'EpochNotMatch' => [static function (Error $error): void {
                $error->setEpochNotMatch(new EpochNotMatch());
            }, 'retry_region_error'],
            'RegionNotFound' => [static function (Error $error): void {
                $error->setRegionNotFound(new RegionNotFound());
            }, 'retry_region_error'],
        ];
    }

    #[DataProvider('provideRetryableRoutingVariants')]
    public function testRetryableRoutingErrorStillInvalidatesTheCachedRegion(
        callable $setVariant,
        string $expectedReason,
    ): void {
        $this->cache->put($this->cachedRegion());
        $executor = $this->createExecutor();

        $error = $this->regionError($setVariant);

        $calls = 0;
        $result = $executor->execute(self::KEY, function () use (&$calls, $error): string {
            $calls++;
            if ($calls === 1) {
                throw $error;
            }

            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertSame(2, $calls, 'a retryable routing error must still be retried');
        $this->assertNull(
            $this->cache->getByKey(self::KEY),
            'the retryable path must keep invalidating exactly as before',
        );
        $this->assertSame(1, $this->metrics->getInvalidations($expectedReason));
        $this->assertSame(0, $this->metrics->getInvalidations('fatal_region_error'));
    }

    // ========================================================================
    // The predicate itself: a routing decision, independent of retryability
    // ========================================================================

    /**
     * The complete routing/non-routing table. It is exhaustive over
     * ErrorKind on purpose: the production predicate is a `match` with no
     * `default` arm (PHPStan reports match.unhandled at level 9, so a new
     * ErrorKind case fails `composer lint`), and testRoutingTableCoversEvery
     * ErrorKindCase() fails the suite if someone adds a case and fixes the
     * lint by widening the match instead of deciding. A `in_array()` list
     * would force no decision at all.
     *
     * @return array<string, array{ErrorKind, bool}>
     */
    public static function provideRoutingDecisionByKind(): array
    {
        return [
            // Routing: the cached region may be the wrong one.
            'KeyNotInRegion'        => [ErrorKind::KeyNotInRegion, true],
            'EpochNotMatch'          => [ErrorKind::EpochNotMatch, true],
            'RegionNotFound'         => [ErrorKind::RegionNotFound, true],
            'StoreNotMatch'          => [ErrorKind::StoreNotMatch, true],
            'NotLeader'              => [ErrorKind::NotLeader, true],
            'RegionNotInitialized'   => [ErrorKind::RegionNotInitialized, true],

            // Not routing: the request arrived at the right region.
            'ServerIsBusy'           => [ErrorKind::ServerIsBusy, false],
            'DiskFull'               => [ErrorKind::DiskFull, false],
            'IsWitness'              => [ErrorKind::IsWitness, false],
            'RecoveryInProgress'     => [ErrorKind::RecoveryInProgress, false],
            'StaleCommand'           => [ErrorKind::StaleCommand, false],
            'DataIsNotReady'         => [ErrorKind::DataIsNotReady, false],
            'ReadIndexNotReady'      => [ErrorKind::ReadIndexNotReady, false],
            'ProposalInMergingMode'  => [ErrorKind::ProposalInMergingMode, false],
            'MaxTimestampNotSynced'  => [ErrorKind::MaxTimestampNotSynced, false],
            'MismatchPeerId'         => [ErrorKind::MismatchPeerId, false],
            'BucketVersionNotMatch'  => [ErrorKind::BucketVersionNotMatch, false],
            'UndeterminedResult'     => [ErrorKind::UndeterminedResult, false],
            'RaftEntryTooLarge'      => [ErrorKind::RaftEntryTooLarge, false],
            'FlashbackInProgress'    => [ErrorKind::FlashbackInProgress, false],
            'FlashbackNotPrepared'   => [ErrorKind::FlashbackNotPrepared, false],
        ];
    }

    #[DataProvider('provideRoutingDecisionByKind')]
    public function testInvalidatesRoutingOnFatalPerErrorKind(ErrorKind $kind, bool $expected): void
    {
        $exception = new RegionException(
            operation: 'KvGet',
            message: 'region error from TiKV',
            errorKind: $kind,
        );

        $this->assertSame($expected, RetryExecutor::invalidatesRoutingOnFatal($exception));
    }

    /**
     * The table is derived from ErrorKind::cases() rather than from the
     * production match, so a new enum case cannot be added without a
     * deliberate routing/non-routing decision being recorded here.
     */
    public function testRoutingTableCoversEveryErrorKindCase(): void
    {
        $covered = array_keys(self::provideRoutingDecisionByKind());
        sort($covered);
        $cases = array_map(
            static fn (ErrorKind $kind): string => $kind->name,
            ErrorKind::cases(),
        );
        sort($cases);

        $this->assertSame(
            $cases,
            $covered,
            'every ErrorKind case needs an explicit invalidatesRoutingOnFatal() decision (issue #233)',
        );
    }

    /**
     * A fatal error that is not a RegionException never invalidates, however
     * its message reads.
     */
    #[DataProvider('provideFatalNonRegionErrors')]
    public function testInvalidatesRoutingOnFatalRejectsNonRegionErrors(TiKvException $error): void
    {
        $this->assertFalse(RetryExecutor::invalidatesRoutingOnFatal($error));
    }

    /**
     * A RegionException without a typed kind carries no routing verdict
     * either. Note such an exception is classified retryable (RegionMiss), so
     * it never reaches the fatal path in the first place — the assertion
     * pins the boundary of the predicate rather than a reachable case.
     */
    public function testInvalidatesRoutingOnFatalRejectsRegionExceptionWithoutTypedKind(): void
    {
        $this->assertFalse(
            RetryExecutor::invalidatesRoutingOnFatal(
                new RegionException(operation: 'KvGet', message: 'region error from TiKV'),
            ),
        );
    }
}
