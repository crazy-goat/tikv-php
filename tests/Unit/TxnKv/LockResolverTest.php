<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
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

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->checkTxnStatusRequests = [];

        $this->region = new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: self::LEADER_PEER_ID,
            leaderStoreId: self::LEADER_STORE_ID,
            epochConfVer: self::EPOCH_CONF_VER,
            epochVersion: self::EPOCH_VERSION,
        );
    }

    private function createResolver(int $callerStartTs = self::CALLER_START_TS): LockResolver
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        // Fresh TSO for every checkTxnStatus; individual tests override with expects().
        $this->pdClient->method('getTimestamp')->willReturn(self::TSO_TIMESTAMP);

        return new LockResolver(
            $this->grpc,
            $regionResolver,
            $this->regionCache,
            $this->pdClient,
            $callerStartTs,
            logger: $this->logger,
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
    ): CheckTxnStatusResponse {
        $response = new CheckTxnStatusResponse();
        if ($regionError instanceof \CrazyGoat\Proto\Errorpb\Error) {
            $response->setRegionError($regionError);
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
            ) use (
                &$checkIndex,
                $checkResponses,
            ): object {
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

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 1),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testResolveLockWithZeroCommitTsRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 0),
                $this->makeCheckTxnStatusResponse(commitVersion: 0),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testResolveLockWithActiveLockWaitsThenRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 50),
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 0),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
    }

    public function testResolveLockWithLockActionImmediatelyRollsBack(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(action: Action::TTLExpireRollback),
                $this->makeCheckTxnStatusResponse(action: Action::TTLExpireRollback),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
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
    // checkTxnStatus() must send PD TSO timestamps, never hrtime()-derived values
    // ========================================================================

    public function testCheckTxnStatusSendsCallerStartTsAndFreshTsoFromPd(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $freshTso = (1 << 42) | 4242;
        $this->pdClient->expects($this->once())
            ->method('getTimestamp')
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
    // resolveLock() — lock-TTL wait capped by remaining retry deadline (#470)
    // ========================================================================

    public function testResolveLockCapsTtlWaitByRemainingDeadline(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // A 20 s TTL (== the default maxBackoffMs cap) with only 100 ms of
        // remaining operation budget: the wait must be capped at ~100 ms.
        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 20000),
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 0),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();

        $startMs = microtime(true) * 1000;
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 100);
        $elapsedMs = (microtime(true) * 1000) - $startMs;

        // Well below even one ServerBusy backoff jitter step (~1-2 s), so a
        // regression to the uncapped 20 s sleep fails this loudly.
        $this->assertLessThan(2500, $elapsedMs, 'Lock TTL wait must be capped by the remaining deadline');
    }

    public function testResolveLockWithZeroRemainingDeadlineKeepsLegacyWait(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 60),
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 0),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();

        $startMs = microtime(true) * 1000;
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo());
        $elapsedMs = (microtime(true) * 1000) - $startMs;

        // usleep guarantees at least the requested time; allow scheduler slack.
        $this->assertGreaterThanOrEqual(40, $elapsedMs, 'Default (0) must keep the full TTL wait');
        $this->assertLessThan(2000, $elapsedMs, 'Sanity: one short bounded sleep');
    }

    public function testResolveLockWithRemainingAboveTtlWaitsFullTtl(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 60),
                $this->makeCheckTxnStatusResponse(commitVersion: 0, lockTtl: 0),
                new ResolveLockResponse(),
            );

        $resolver = $this->createResolver();

        $startMs = microtime(true) * 1000;
        $resolver->resolveLock(self::TEST_KEY, $this->makeLockInfo(), 5000);
        $elapsedMs = (microtime(true) * 1000) - $startMs;

        $this->assertGreaterThanOrEqual(40, $elapsedMs, 'A deadline above the TTL must not shorten the wait');
        $this->assertLessThan(2000, $elapsedMs);
    }
}
