<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvRangeOps;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RetryBudgetSharedAcrossRegionsTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;

    private function defaultRegion(
        string $startKey = '',
        string $endKey = '',
        int $regionId = 1,
        int $leaderStoreId = 1,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    protected function setUp(): void
    {
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
    }

    /**
     * Multi-region scan shares one backoff budget across all regions.
     *
     * StaleCmd backoff: baseMs=2, capMs=1000
     * Exponential: attempt 0→2, attempt 1→4, attempt 2→8, attempt 3→16
     * With maxBackoffMs=10: 2+4+8=14 > 10 → budget exhausted after 3 retries
     *
     * Under old per-region code, each region gets its own 10ms budget,
     * allowing ~3 retries per region. Under shared budget, the ~3 retries
     * are spread across ALL regions.
     */
    public function testScanSharesBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$retryCount): RawScanResponse {
            $retryCount++;
            throw new TiKvException('StaleCommand');
        });

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 10,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $scanner->scan('a', 'z', 100, false);

        // Shared budget: 3 retries total (2+4+8=14 > 10).
        // Old per-region: ~3 per region × 2 regions = ~6 retries.
        $this->assertLessThanOrEqual(4, $retryCount, 'Shared budget should limit total retries across all regions');
    }

    /**
     * Multi-region deleteRange shares one backoff budget.
     */
    public function testDeleteRangeSharesBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$retryCount): RawDeleteRangeResponse {
            $retryCount++;
            throw new TiKvException('StaleCommand');
        });

        $rangeOps = new RawKvRangeOps(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            new TimeoutConfig(),
            maxBackoffMs: 10,
            serverBusyBudgetMs: 600000,
            logger: new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $rangeOps->deleteRange('a', 'z');

        $this->assertLessThanOrEqual(4, $retryCount, 'Shared budget should limit total retries across all regions');
    }

    /**
     * checksum shares one backoff budget across regions.
     */
    public function testChecksumSharesBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$retryCount): void {
            $retryCount++;
            throw new TiKvException('StaleCommand');
        });

        $rangeOps = new RawKvRangeOps(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            new TimeoutConfig(),
            maxBackoffMs: 10,
            serverBusyBudgetMs: 600000,
            logger: new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $rangeOps->checksum('a', 'z');

        $this->assertLessThanOrEqual(4, $retryCount, 'Shared budget should limit total retries across all regions');
    }

    /**
     * reverseScan shares one backoff budget across regions.
     */
    public function testReverseScanSharesBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$retryCount): RawScanResponse {
            $retryCount++;
            throw new TiKvException('StaleCommand');
        });

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 10,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $scanner->reverseScan('z', 'a', 100, false);

        $this->assertLessThanOrEqual(4, $retryCount, 'Shared budget should limit total retries across all regions');
    }

    /**
     * Each public operation gets its own fresh budget.
     * Two separate scan() calls should each get their own budget.
     */
    public function testSeparateOperationsGetFreshBudgets(): void
    {
        $region = $this->defaultRegion('a', 'z', regionId: 1);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$retryCount): RawScanResponse {
            $retryCount++;
            if ($retryCount <= 2) {
                throw new TiKvException('EpochNotMatch');
            }
            $pair = new KvPair();
            $pair->setKey('key1');
            $pair->setValue('val1');
            $response = new RawScanResponse();
            $response->setKvs([$pair]);
            return $response;
        });

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        // First call: retries twice then succeeds
        $result1 = $scanner->scan('a', 'z', 100, false);
        $this->assertCount(1, $result1);

        // Second call: gets a fresh budget, retries twice, succeeds
        $result2 = $scanner->scan('a', 'z', 100, false);
        $this->assertCount(1, $result2);

        $this->assertSame(4, $retryCount);
    }

    /**
     * Verify the budget exhaustion is logged with the correct context.
     */
    public function testBudgetExhaustionIsLogged(): void
    {
        $region = $this->defaultRegion('a', 'z', regionId: 1);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')->willReturnCallback(function (): never {
            throw new TiKvException('StaleCommand');
        });

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->atLeastOnce())
            ->method('error')
            ->with(
                'Retry budget exhausted',
                $this->callback(fn(array $ctx): bool => isset($ctx['totalBackoffMs'], $ctx['maxBackoffMs'])),
            );

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 1,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: $logger,
        );

        $this->expectException(TiKvException::class);
        $scanner->scan('a', 'z', 100, false);
    }

    /**
     * Budget accumulates correctly: first region consumes part of budget,
     * second region exhausts the remainder.
     */
    /**
     * A multi-region scan has one RetryExecutor reused per region, but
     * maxBackoffMs is a per-operation (per-region) limit (issue #271). Each
     * region must therefore retry normally with a full budget, instead of
     * the first region consuming the budget and later regions failing on
     * their first retryable error.
     *
     * StaleCmd backoff: baseMs=2, capMs=1000, exponential 2,4,8,...
     * With maxBackoffMs=6 every region can afford exactly two retries
     * (2+4=6). If the budget were shared across all four regions, region 2
     * would exhaust it on its very first retry (6+2=8 > 6) and the scan
     * would abort. A per-region budget lets all four retry twice and succeed.
     */
    public function testEachRegionWithinScanRetriesWithFreshBudget(): void
    {
        $region1 = $this->defaultRegion('a', 'b', regionId: 1);
        $region2 = $this->defaultRegion('b', 'c', regionId: 2);
        $region3 = $this->defaultRegion('c', 'd', regionId: 3);
        $region4 = $this->defaultRegion('d', 'z', regionId: 4);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2, $region3, $region4]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => match (true) {
                $key < 'b' => $region1,
                $key < 'c' => $region2,
                $key < 'd' => $region3,
                default => $region4,
            },
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // Per-region retry counter: each region throws StaleCommand on its
        // first two attempts, then succeeds with a single k/v pair.
        $failuresPerRegion = [0, 0, 0, 0];
        $this->grpc->method('call')->willReturnCallback(function (
            $address,
            $service,
            $method,
            $request,
            $responseClass,
        ) use (&$failuresPerRegion): RawScanResponse {
            $rawRequest = $request;
            $key = $rawRequest->getStartKey();
            $regionIndex = match (true) {
                $key < 'b' => 0,
                $key < 'c' => 1,
                $key < 'd' => 2,
                default => 3,
            };

            if ($failuresPerRegion[$regionIndex] < 2) {
                $failuresPerRegion[$regionIndex]++;
                throw new TiKvException('StaleCommand');
            }

            $response = new RawScanResponse();
            $pair = new KvPair();
            $pair->setKey($key . '-key');
            $pair->setValue('val');
            $response->setKvs([$pair]);
            return $response;
        });

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 6,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        $results = $scanner->scan('a', 'z', 100, false);

        // All four regions succeeded after retrying, so each got its own
        // budget. A shared budget would have thrown on region 2's first retry.
        $this->assertCount(4, $results);
        $this->assertSame([2, 2, 2, 2], $failuresPerRegion);
    }

    public function testBudgetAccumulatesAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // maxBackoffMs=14: allows exactly 3 retries (2+4+8=14)
        // Region1 uses attempt 0 (2ms) and attempt 1 (4ms) → succeeds
        // Region2 uses attempt 2 (8ms) → 14ms total, budget exhausted
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$callCount): RawScanResponse {
            $callCount++;
            throw new TiKvException('StaleCommand');
        });

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 14,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $scanner->scan('a', 'z', 100, false);

        // With budget=14: attempt 0 (2ms) + attempt 1 (4ms) + attempt 2 (8ms) = 14ms
        // Budget exhausted at attempt 2 → 3 retries total
        $this->assertLessThanOrEqual(4, $callCount, 'Budget exhausted across both regions');
    }
}
