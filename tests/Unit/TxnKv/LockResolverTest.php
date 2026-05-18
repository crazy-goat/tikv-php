<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class LockResolverTest extends TestCase
{
    private const TEST_KEY = 'test-key';
    private const LOCK_TS = 100;
    private const CALLER_START_TS = 200;

    private const REGION_ID = 1;
    private const LEADER_STORE_ID = 1;
    private const LEADER_PEER_ID = 1;
    private const EPOCH_CONF_VER = 1;
    private const EPOCH_VERSION = 1;

    private const STORE_ADDRESS = 'addr:20160';

    private GrpcClientInterface& \PHPUnit\Framework\MockObject\MockObject $grpc;
    private PdClientInterface& \PHPUnit\Framework\MockObject\MockObject $pdClient;
    private RegionCacheInterface& \PHPUnit\Framework\MockObject\MockObject $regionCache;
    private LoggerInterface& \PHPUnit\Framework\MockObject\MockObject $logger;
    private RegionInfo $region;
    private CheckTxnStatusResponse& \PHPUnit\Framework\MockObject\MockObject $checkResponse;
    private ResolveLockResponse& \PHPUnit\Framework\MockObject\MockObject $resolveResponse;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);

        $this->region = new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: self::LEADER_PEER_ID,
            leaderStoreId: self::LEADER_STORE_ID,
            epochConfVer: self::EPOCH_CONF_VER,
            epochVersion: self::EPOCH_VERSION,
        );

        $this->checkResponse = $this->createMock(CheckTxnStatusResponse::class);
        $this->checkResponse->method('getRegionError')->willReturn(null);
        $this->checkResponse->method('getError')->willReturn(null);
        $this->checkResponse->method('getAction')->willReturn(0);
        $this->checkResponse->method('getLockTtl')->willReturn(0);
        $this->checkResponse->method('getCommitVersion')->willReturn(1);

        $this->resolveResponse = $this->createMock(ResolveLockResponse::class);
    }

    private function createResolver(): LockResolver
    {
        return new LockResolver(
            $this->grpc,
            $this->pdClient,
            $this->regionCache,
            20000,
            $this->logger,
        );
    }

    private function mockStore(): void
    {
        $store = $this->createMock(\CrazyGoat\Proto\Metapb\Store::class);
        $store->method('getAddress')->willReturn(self::STORE_ADDRESS);
        $this->pdClient->method('getStore')->willReturn($store);
    }

    private function mockGrpcCalls(): void
    {
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
            ): object {
                if ($responseClass === CheckTxnStatusResponse::class) {
                    return $this->checkResponse;
                }
                return $this->resolveResponse;
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

        $this->mockStore();
        $this->mockGrpcCalls();

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, self::LOCK_TS, self::CALLER_START_TS);
    }

    public function testGetRegionInfoReturnsCachedRegionWithoutPdcall(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->region);

        $this->pdClient->expects($this->never())
            ->method('getRegion');

        $this->regionCache->expects($this->never())
            ->method('put');

        $this->mockStore();
        $this->mockGrpcCalls();

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, self::LOCK_TS, self::CALLER_START_TS);
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

        $this->mockStore();
        $this->mockGrpcCalls();

        $resolver = $this->createResolver();
        $resolver->resolveLock(self::TEST_KEY, self::LOCK_TS, self::CALLER_START_TS);
    }

    public function testMultipleDifferentKeysFetchFromPdIndependently(): void
    {
        $region1 = new RegionInfo(regionId: 1, leaderPeerId: 1, leaderStoreId: 1, epochConfVer: 1, epochVersion: 1);
        $region2 = new RegionInfo(regionId: 2, leaderPeerId: 2, leaderStoreId: 2, epochConfVer: 1, epochVersion: 1);

        $store1 = $this->createMock(\CrazyGoat\Proto\Metapb\Store::class);
        $store1->method('getAddress')->willReturn('addr:20160');
        $store2 = $this->createMock(\CrazyGoat\Proto\Metapb\Store::class);
        $store2->method('getAddress')->willReturn('addr:20161');

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
            ->willReturnCallback(function (int $storeId) use ($store1, $store2): \CrazyGoat\Proto\Metapb\Store {
                if ($storeId === 1) {
                    return $store1;
                }
                return $store2;
            });

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

        $checkResp1 = $this->createMock(CheckTxnStatusResponse::class);
        $checkResp1->method('getRegionError')->willReturn(null);
        $checkResp1->method('getError')->willReturn(null);
        $checkResp1->method('getAction')->willReturn(0);
        $checkResp1->method('getLockTtl')->willReturn(0);
        $checkResp1->method('getCommitVersion')->willReturn(1);

        $checkResp2 = $this->createMock(CheckTxnStatusResponse::class);
        $checkResp2->method('getRegionError')->willReturn(null);
        $checkResp2->method('getError')->willReturn(null);
        $checkResp2->method('getAction')->willReturn(0);
        $checkResp2->method('getLockTtl')->willReturn(0);
        $checkResp2->method('getCommitVersion')->willReturn(1);

        $resolveResp1 = $this->createMock(ResolveLockResponse::class);
        $resolveResp2 = $this->createMock(ResolveLockResponse::class);

        $grpcCallCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function () use (
                &$grpcCallCount,
                $checkResp1,
                $checkResp2,
                $resolveResp1,
                $resolveResp2,
            ): object {
                $grpcCallCount++;
                return match ($grpcCallCount) { // @phpstan-ignore match.unhandled
                    1 => $checkResp1,
                    2 => $resolveResp1,
                    3 => $checkResp2,
                    4 => $resolveResp2,
                };
            });

        $resolver = $this->createResolver();
        $resolver->resolveLock('key-a', self::LOCK_TS, self::CALLER_START_TS);
        $resolver->resolveLock('key-b', self::LOCK_TS, self::CALLER_START_TS);

        $this->assertSame(2, $pdRegionCallCount, 'PD should be queried once per unique key');
    }
}
