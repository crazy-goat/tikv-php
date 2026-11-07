<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Errorpb\RegionNotFound;
use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Observability\InMemoryTracer;
use CrazyGoat\TiKV\Client\Observability\TraceCategory;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class LockResolverTest extends TestCase
{
    private const TEST_KEY = 'test-key';
    private const LOCK_TS = 100;
    private const CALLER_START_TS = (1 << 42) | 17;
    private const TSO_TIMESTAMP = (1 << 42) | 999;

    private const REGION_ID = 1;
    private const LEADER_STORE_ID = 1;
    private const LEADER_PEER_ID = 1;
    private const EPOCH_CONF_VER = 1;
    private const EPOCH_VERSION = 1;

    private const STORE_ADDRESS = 'addr:20160';

    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;
    private LoggerInterface&MockObject $logger;
    private RegionInfo $region;

    /** @var CheckTxnStatusRequest[] Requests captured from KvCheckTxnStatus calls */
    private array $checkTxnStatusRequests = [];

    /** @var ResolveLockRequest[] Requests captured from KvResolveLock calls */
    private array $resolveLockRequests = [];

    /** @var list<string> Method names of every gRPC call, in order */
    private array $callSequence = [];

    /** @var list<array{method: string, timeoutMs: ?int}> Captured per-call deadlines */
    private array $callTimeouts = [];

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->checkTxnStatusRequests = [];
        $this->resolveLockRequests = [];
        $this->callSequence = [];
        $this->callTimeouts = [];

        $this->region = new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: self::LEADER_PEER_ID,
            leaderStoreId: self::LEADER_STORE_ID,
            epochConfVer: self::EPOCH_CONF_VER,
            epochVersion: self::EPOCH_VERSION,
        );
    }

    /**
     * @param (callable(int): int)|null $tso Optional TSO fetch callback; the
     *        default returns a fresh timestamp immediately.
     */
    private function createResolver(
        int $callerStartTs = self::CALLER_START_TS,
        ?callable $tso = null,
        ?InMemoryMetrics $metrics = null,
        ?InMemoryTracer $tracer = null,
    ): LockResolver {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        // Fresh TSO for every checkTxnStatus; individual tests override with expects().
        if ($tso !== null) {
            $this->pdClient->method('getLowResolutionTimestamp')->willReturnCallback($tso);
        } else {
            $this->pdClient->method('getLowResolutionTimestamp')->willReturn(self::TSO_TIMESTAMP);
        }

        return new LockResolver(
            $this->grpc,
            $regionResolver,
            $this->regionCache,
            $this->pdClient,
            $callerStartTs,
            logger: $this->logger,
            metrics: $metrics ?? new InMemoryMetrics(),
            tracer: $tracer ?? new InMemoryTracer(),
        );
    }

    private function makeStore(string $address = self::STORE_ADDRESS): Store
    {
        $store = new Store();
        $store->setAddress($address);
        return $store;
    }

    private function makeCheckTxnStatusResponse(
        int $commitVersion = 0,
        int $lockTtl = 0,
        int $action = Action::NoAction,
        ?Error $regionError = null,
        ?KeyError $keyError = null,
    ): CheckTxnStatusResponse {
        $response = new CheckTxnStatusResponse();
        if ($regionError instanceof \CrazyGoat\Proto\Errorpb\Error) {
            $response->setRegionError($regionError);
        }
        if ($keyError instanceof KeyError) {
            $response->setError($keyError);
        }
        $response->setCommitVersion($commitVersion);
        $response->setLockTtl($lockTtl);
        $response->setAction($action);
        return $response;
    }

    private function makeLockInfo(
        string $key = self::TEST_KEY,
        int $lockVersion = self::LOCK_TS,
        string $primaryLock = '',
    ): LockInfo {
        $lock = new LockInfo();
        $lock->setKey($key);
        $lock->setLockVersion($lockVersion);
        if ($primaryLock !== '') {
            $lock->setPrimaryLock($primaryLock);
        }
        return $lock;
    }

    /**
     * @param CheckTxnStatusResponse ...$checkResponses Responses returned in order
     *                                            for successive KvCheckTxnStatus calls
     */
    private function mockGrpcCalls(CheckTxnStatusResponse ...$checkResponses): void
    {
        $checkIndex = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                mixed $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ) use (
                &$checkIndex,
                $checkResponses,
            ): object {
                $this->callSequence[] = $method;
                $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];
                if ($method === 'KvCheckTxnStatus') {
                    if ($request instanceof CheckTxnStatusRequest) {
                        $this->checkTxnStatusRequests[] = $request;
                    }
                    if (!isset($checkResponses[$checkIndex])) {
                        throw new \RuntimeException('Unexpected extra KvCheckTxnStatus call');
                    }
                    return $checkResponses[$checkIndex++];
                }
                if ($method === 'KvResolveLock') {
                    return new ResolveLockResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            });
    }

    /**
     * Mock the gRPC client like mockGrpcCalls(), but additionally capture
     * the KvResolveLock request object so tests can assert its fields.
     *
     * @param CheckTxnStatusResponse ...$checkResponses Responses returned in order
     *                                            for successive KvCheckTxnStatus calls
     */
    private function mockGrpcCallsAndCaptureResolve(CheckTxnStatusResponse ...$checkResponses): void
    {
        $checkIndex = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                mixed $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ) use (
                &$checkIndex,
                $checkResponses,
            ): object {
                $this->callSequence[] = $method;
                $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];
                if ($method === 'KvCheckTxnStatus') {
                    if ($request instanceof CheckTxnStatusRequest) {
                        $this->checkTxnStatusRequests[] = $request;
                    }
                    if (!isset($checkResponses[$checkIndex])) {
                        throw new \RuntimeException('Unexpected extra KvCheckTxnStatus call');
                    }
                    return $checkResponses[$checkIndex++];
                }
                if ($method === 'KvResolveLock') {
                    if ($request instanceof ResolveLockRequest) {
                        $this->resolveLockRequests[] = $request;
                    }
                    return new ResolveLockResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            });
    }

    public function testConstruction(): void
    {
        $resolver = $this->createResolver();
        $this->assertInstanceOf(LockResolver::class, $resolver);
    }

    public function testGetGrpcReturnsGrpcClient(): void
    {
        $resolver = $this->createResolver();
        $this->assertSame($this->grpc, $resolver->getGrpc());
    }

    public function testGetRegionInfoPopulatesCacheOnMiss(): void
    {
        $putCalled = false;
        $this->regionCache->method('getByKey')
            ->willReturnCallback(function () use (&$putCalled): ?RegionInfo {
                if ($putCalled) { // @phpstan-ignore if.alwaysFalse
                    return $this->region;
                }
                return null;
            });

        $this->regionCache->expects($this->atLeastOnce())
            ->method('put')
            ->with($this->identicalTo($this->region))
            ->willReturnCallback(function () use (&$putCalled): void {
                $putCalled = true;
            });

        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->with(self::TEST_KEY)
            ->willReturn($this->region);

        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCalls($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testGetRegionInfoReturnsCachedRegionWithoutPdcall(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);

        $this->pdClient->expects($this->never())
            ->method('getRegion');

        $this->regionCache->expects($this->never())
            ->method('put');

        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCalls($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testGetRegionInfoFetchesFromPdOnCacheMiss(): void
    {
        $putCalled = false;
        $this->regionCache->method('getByKey')
            ->willReturnCallback(function () use (&$putCalled): ?RegionInfo {
                if ($putCalled) { // @phpstan-ignore if.alwaysFalse
                    return $this->region;
                }
                return null;
            });

        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->with(self::TEST_KEY)
            ->willReturn($this->region);

        $this->regionCache->expects($this->once())
            ->method('put')
            ->with($this->identicalTo($this->region))
            ->willReturnCallback(function () use (&$putCalled): void {
                $putCalled = true;
            });

        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCalls($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    // ========================================================================
    // resolveLock() — committed path
    // ========================================================================

    public function testResolveLockWithCommitTsGetsCommitted(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->mockGrpcCallsAndCaptureResolve($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertCount(1, $this->resolveLockRequests, 'exactly one KvResolveLock call');
        $request = $this->resolveLockRequests[0];
        $this->assertSame(1, (int) $request->getCommitVersion(), 'committed lock must carry the real commitTs');
        $this->assertSame(self::LOCK_TS, (int) $request->getStartVersion());
    }

    public function testResolveLockRecordsDetailedMetricsAndTrace(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->mockGrpcCallsAndCaptureResolve($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $metrics = new InMemoryMetrics();
        $tracer = new InMemoryTracer();
        $resolver = $this->createResolver(metrics: $metrics, tracer: $tracer);
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertSame(1, $metrics->getLockResolutions('resolved'));
        $this->assertSame(0, $metrics->getLockResolutions('live'));
        $this->assertSame(
            ['lock_resolve_started', 'lock_resolve_completed'],
            $tracer->eventNames(TraceCategory::TXN_LOCK_RESOLVE),
        );
    }

    public function testResolveLockWithZeroCommitTsRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0),
        );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertCount(1, $this->checkTxnStatusRequests, 'expired status requires one check');
        $this->assertCount(1, $this->resolveLockRequests, 'exactly one KvResolveLock call');
        $request = $this->resolveLockRequests[0];
        $this->assertSame(0, (int) $request->getCommitVersion(), 'rolled-back lock must carry commitVersion 0');
        $this->assertSame(self::LOCK_TS, (int) $request->getStartVersion());
    }

    public function testResolveLockWithActiveLockThrowsRetryableWithoutResolving(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 30000),
        );

        $resolver = $this->createResolver();

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
            $this->fail('Expected active lock to request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertCount(1, $this->checkTxnStatusRequests, 'a live lock requires one status check');
        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame([], $this->resolveLockRequests, 'a live transaction must never be rolled back');
    }

    public function testResolveLockWithLockActionImmediatelyRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // TiKV reports that the lock has expired; a single status check is
        // sufficient before issuing the rollback resolution.
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(action: Action::TTLExpireRollback),
        );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertCount(1, $this->checkTxnStatusRequests, 'exactly one KvCheckTxnStatus call');
        $this->assertCount(1, $this->resolveLockRequests, 'exactly one KvResolveLock call');
        $this->assertSame(['KvCheckTxnStatus', 'KvResolveLock'], $this->callSequence);
        $request = $this->resolveLockRequests[0];
        $this->assertSame(0, (int) $request->getCommitVersion(), 'expired lock must be rolled back, not committed');
        $this->assertSame(self::LOCK_TS, (int) $request->getStartVersion());
    }

    public function testResolveLockWithRegionErrorThrows(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $regionError = new Error();
        $regionError->setMessage('region not found');
        $this->grpc->method('call')->willReturn(
            $this->makeCheckTxnStatusResponse(regionError: $regionError),
        );

        $this->expectException(RegionException::class);

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    // ========================================================================
    // checkTxnStatus() — an errored response is not a status (issue #224)
    // ========================================================================

    public function testErroredCheckTxnStatusIsRetryableAndNeverRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $keyError = new KeyError();
        $keyError->setRetryable('status check failed');
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(keyError: $keyError),
        );

        $resolver = $this->createResolver();

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
            $this->fail('Expected an errored CheckTxnStatus to request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
            $this->assertStringContainsString(
                'CheckTxnStatus failed: retryable: status check failed',
                $e->getMessage(),
            );
        }

        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame([], $this->resolveLockRequests, 'an errored status check must never drive a rollback');
    }

    public function testRegionErrorDuringCheckTxnStatusIsRetriedAndSuccessfulResponseDecides(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $regionNotFound = new RegionNotFound();
        $regionNotFound->setRegionId(self::REGION_ID);
        $regionError = new Error();
        $regionError->setMessage('region not found');
        $regionError->setRegionNotFound($regionNotFound);

        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(regionError: $regionError),
            $this->makeCheckTxnStatusResponse(commitVersion: 42),
        );

        $resolver = $this->createResolver();
        $executor = new RetryExecutor(
            maxBackoffMs: 1000,
            serverBusyBudgetMs: 1000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
            logger: $this->logger,
            maxAttempts: 5,
            deadlineMs: 0,
        );

        $executor->execute(self::TEST_KEY, function () use ($resolver): void {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
        });

        $this->assertSame(
            ['KvCheckTxnStatus', 'KvCheckTxnStatus', 'KvResolveLock'],
            $this->callSequence,
            'the region error must be retried, then the successful response decides',
        );
        $this->assertCount(1, $this->resolveLockRequests);
        $this->assertSame(42, (int) $this->resolveLockRequests[0]->getCommitVersion());
    }

    public function testGenuineNotCommittedResponseStillRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(action: Action::LockNotExistRollback),
        );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertSame(['KvCheckTxnStatus', 'KvResolveLock'], $this->callSequence);
        $this->assertCount(1, $this->resolveLockRequests);
        $this->assertSame(0, (int) $this->resolveLockRequests[0]->getCommitVersion());
    }

    // ========================================================================
    // checkTxnStatus() must send PD TSO timestamps, never hrtime()-derived values
    // ========================================================================

    public function testCheckTxnStatusSendsCallerStartTsAndFreshTsoFromPd(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $freshTso = (1 << 42) | 4242;
        $this->pdClient->expects($this->once())
            ->method('getLowResolutionTimestamp')
            ->with($this->greaterThan(0)) // the TSO call must carry a finite timeout
            ->willReturn($freshTso);

        $this->mockGrpcCalls($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertCount(1, $this->checkTxnStatusRequests);
        $request = $this->checkTxnStatusRequests[0];
        $this->assertSame(self::CALLER_START_TS, $request->getCallerStartTs());
        $this->assertSame($freshTso, $request->getCurrentTs());
    }

    public function testCheckTxnStatusSendsTsoMagnitudeTimestamps(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->mockGrpcCalls($this->makeCheckTxnStatusResponse(commitVersion: 1));

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());

        $this->assertCount(1, $this->checkTxnStatusRequests);
        $request = $this->checkTxnStatusRequests[0];
        // Regression guard: hrtime()-derived values are ~1e9, genuine TSO
        // values are physical_ms << 18 (roughly 1e17).
        $this->assertGreaterThan(1 << 40, $request->getCallerStartTs());
        $this->assertGreaterThan(1 << 40, $request->getCurrentTs());
    }

    // ========================================================================
    // Multiple different keys
    // ========================================================================

    public function testMultipleDifferentKeysFetchFromPdIndependently(): void
    {
        $region1 = new RegionInfo(regionId: 1, leaderPeerId: 1, leaderStoreId: 1, epochConfVer: 1, epochVersion: 1);
        $region2 = new RegionInfo(regionId: 2, leaderPeerId: 2, leaderStoreId: 2, epochConfVer: 1, epochVersion: 1);

        $pdRegionCallCount = 0;
        $this->pdClient->method('getRegion')
            ->willReturnCallback(function () use (&$pdRegionCallCount, $region1, $region2): RegionInfo {
                $pdRegionCallCount++;
                return match ($pdRegionCallCount) { // @phpstan-ignore match.unhandled
                    1 => $region1,
                    2 => $region2,
                };
            });

        $this->pdClient->method('getStore')
            ->willReturnCallback(
                fn(int $storeId): Store => $this->makeStore(
                    $storeId === 1 ? 'addr:20160' : 'addr:20161',
                ),
            );

        $putCalledForKey1 = false;
        $putCalledForKey2 = false;
        $this->regionCache->method('getByKey')
            ->willReturnCallback(function (string $key) use (
                &$putCalledForKey1,
                &$putCalledForKey2,
                $region1,
                $region2,
            ): ?RegionInfo {
                if ($key === 'key-a' && $putCalledForKey1) { // @phpstan-ignore booleanAnd.rightAlwaysFalse
                    return $region1;
                }
                if ($key === 'key-b' && $putCalledForKey2) { // @phpstan-ignore booleanAnd.rightAlwaysFalse
                    return $region2;
                }
                return null;
            });

        $this->regionCache->method('put')
            ->willReturnCallback(function (RegionInfo $region) use (&$putCalledForKey1, &$putCalledForKey2): void {
                if ($region->regionId === 1) {
                    $putCalledForKey1 = true;
                }
                if ($region->regionId === 2) {
                    $putCalledForKey2 = true;
                }
            });

        $grpcCallCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function () use (&$grpcCallCount): object {
                $grpcCallCount++;
                return match ($grpcCallCount) { // @phpstan-ignore match.unhandled
                    1, 3 => $this->makeCheckTxnStatusResponse(commitVersion: 1),
                    2, 4 => new ResolveLockResponse(),
                };
            });

        $resolver = $this->createResolver();
        $resolver->resolveLock('key-a', $this->makeLockInfo(key: 'key-a'));
        $resolver->resolveLock('key-b', $this->makeLockInfo(key: 'key-b'));

        $this->assertSame(2, $pdRegionCallCount, 'PD should be queried once per unique key');
    }

    // ========================================================================
    // resolveLock() — invalidation reason tag (#474)
    //
    // LockResolver::invalidateRegionFor() drops the region through
    // RegionCache::invalidate(), which emits the metric itself; the resolver
    // only supplies the 'lock_resolve' reason.
    // ========================================================================

    public function testResolveLockInvalidatesWithLockResolveReason(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 1),
                new ResolveLockResponse(),
            );

        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'lock_resolve');

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testResolveLockNotLeaderInvalidatesWhenNotOwnedByRetryExecutor(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // CheckTxnStatus answers with a NotLeader-carrying region error; the
        // resolveLock() caller has NO enclosing execute(), so check() must
        // self-invalidate with 'not_leader' before throwing.
        $protoLeader = new Peer();
        $protoLeader->setId(20);
        $protoLeader->setStoreId(3);
        $notLeader = new NotLeader();
        $notLeader->setRegionId(self::REGION_ID);
        $notLeader->setLeader($protoLeader);
        $regionError = new Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $this->grpc->expects($this->once())
            ->method('call')
            ->willReturn($this->makeCheckTxnStatusResponse(regionError: $regionError));

        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'not_leader');

        $this->expectException(RegionException::class);

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), notLeaderOwnedByRetryExecutor: false);
    }

    public function testResolveLockNotLeaderLeavesRegionCachedWhenExecutorOwned(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // Default ownership (executor-owned): check() must NOT invalidate —
        // the enclosing handleNotLeader() owns NotLeader drops.
        $notLeader = new NotLeader();
        $notLeader->setRegionId(self::REGION_ID);
        $regionError = new Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $this->grpc->expects($this->once())
            ->method('call')
            ->willReturn($this->makeCheckTxnStatusResponse(regionError: $regionError));

        $this->regionCache->expects($this->never())->method('invalidate');

        $this->expectException(RegionException::class);

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    // ========================================================================
    // resolveLock() — live locks delegate backoff to the retry executor (#206)
    // ========================================================================

    public function testResolveLockDoesNotSleepForLiveLockRegardlessOfDeadline(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 20000),
        );

        $resolver = $this->createResolver();
        $startMs = microtime(true) * 1000;

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 30000);
            $this->fail('Expected live lock to throw retryable exception');
        } catch (TxnRetryableException) {
            $elapsedMs = (microtime(true) * 1000) - $startMs;
            $this->assertLessThan(1000, $elapsedMs, 'LockResolver must not sleep for the TTL');
        }

        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame([], $this->resolveLockRequests);
    }

    /**
     * Issue #569: a live lock whose TTL is longer than the remaining retry
     * deadline is still reported as retryable (the #206 contract), but the
     * resolver must not sleep for the TTL and must cap its own RPC by the
     * remaining budget. The caller's executor stops at the deadline.
     */
    public function testResolveLockDoesNotSleepPastTheRemainingDeadlineForLongLockTtl(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 60000),
        );

        $resolver = $this->createResolver();
        $startMs = microtime(true) * 1000;

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 250);
            $this->fail('Expected live lock to request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
            $elapsedMs = (microtime(true) * 1000) - $startMs;
            $this->assertLessThan(1000, $elapsedMs, 'the resolver must not sleep for the lock TTL');
        }

        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame([], $this->resolveLockRequests);
        $this->assertLessThanOrEqual(250, $this->callTimeouts[0]['timeoutMs'] ?? \PHP_INT_MAX);
    }

    /**
     * Issue #569: a TTL that fits in the remaining deadline keeps the
     * resolve-then-retry contract — the resolver reports a retryable lock
     * and the caller's executor owns the bounded backoff.
     */
    public function testResolveLockStillRequestsRetryWhenLockTtlFitsRemainingDeadline(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 1000),
        );

        $resolver = $this->createResolver();

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 30000);
            $this->fail('Expected live lock to request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame([], $this->resolveLockRequests);
    }

    /**
     * Issue #569: the remaining retry deadline also caps the lock-resolution
     * RPCs themselves, not only the TTL comparison. Otherwise one
     * `lockResolveTimeoutMs` (5 s by default) could outlive a nearly spent
     * retry budget.
     */
    public function testResolveLockCapsLockResolutionRpcTimeoutByRemainingDeadline(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 1),
        );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 250);

        $this->assertSame(['KvCheckTxnStatus', 'KvResolveLock'], $this->callSequence);
        $this->assertCount(2, $this->callTimeouts);
        foreach ($this->callTimeouts as $call) {
            $this->assertNotNull($call['timeoutMs']);
            $this->assertGreaterThan(0, $call['timeoutMs']);
            $this->assertLessThanOrEqual(250, $call['timeoutMs']);
        }
    }

    /**
     * Issue #569 (N3): the remaining deadline is converted into one absolute
     * deadline at the public entry. A slow TSO fetch must shrink the
     * KvCheckTxnStatus timeout instead of letting each nested RPC start with
     * the full remaining budget again.
     */
    public function testResolveLockSharesOneLiveDeadlineAcrossNestedRpcs(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 1),
        );

        $tsoTimeoutMs = null;
        $resolver = $this->createResolver(
            tso: function (int $timeoutMs) use (&$tsoTimeoutMs): int {
                $tsoTimeoutMs = $timeoutMs;
                usleep(50_000);

                return self::TSO_TIMESTAMP;
            },
        );
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 250);

        $checkTxnStatusTimeoutMs = null;
        foreach ($this->callTimeouts as $call) {
            if ($call['method'] === 'KvCheckTxnStatus') {
                $checkTxnStatusTimeoutMs = $call['timeoutMs'];
            }
        }

        $this->assertNotNull($tsoTimeoutMs);
        $this->assertGreaterThan(0, $tsoTimeoutMs);
        $this->assertLessThanOrEqual(250, $tsoTimeoutMs);
        $this->assertNotNull($checkTxnStatusTimeoutMs);
        $this->assertGreaterThan(0, $checkTxnStatusTimeoutMs);
        $this->assertLessThan(
            $tsoTimeoutMs,
            $checkTxnStatusTimeoutMs,
            'the RPC after the TSO fetch must see the time it spent',
        );
    }

    /**
     * Issue #569 (N7): a user-facing `retryDeadlineMs` of PHP_INT_MAX must
     * saturate the absolute deadline instead of overflowing the int return
     * type. The configured lock-resolution timeout still applies.
     */
    public function testResolveLockWithMaximumRemainingDeadlineKeepsConfiguredTimeout(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 1),
        );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), \PHP_INT_MAX);

        $this->assertSame(['KvCheckTxnStatus', 'KvResolveLock'], $this->callSequence);
        foreach ($this->callTimeouts as $call) {
            $this->assertSame(
                TimeoutConfig::DEFAULT_LOCK_RESOLVE_TIMEOUT_MS,
                $call['timeoutMs'],
            );
        }
    }

    /**
     * Issue #569: without a deadline (standalone resolver, no enclosing
     * `RetryExecutor`), the configured `lockResolveTimeoutMs` applies and a
     * live lock keeps raising the retryable exception — the historical
     * behaviour is unchanged.
     */
    public function testResolveLockWithoutDeadlineKeepsConfiguredRpcTimeout(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->mockGrpcCallsAndCaptureResolve(
            $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 60000),
        );

        $resolver = $this->createResolver();

        try {
            $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
            $this->fail('Expected live lock to request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertSame(['KvCheckTxnStatus'], $this->callSequence);
        $this->assertSame(
            TimeoutConfig::DEFAULT_LOCK_RESOLVE_TIMEOUT_MS,
            $this->callTimeouts[0]['timeoutMs'] ?? null,
        );
    }
}
