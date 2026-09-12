<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
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
     * Each region gets its own independent backoff budget (issue #271).
     *
     * StaleCmd backoff: baseMs=2, capMs=1000, exponential 2,4,8,...
     * With maxBackoffMs=14, region 1 can afford exactly three retries
     * (2+4+8=14) and then succeeds. Region 2 retries once (+2ms) and
     * succeeds. If the budget were shared, region 1's full 14ms spend would
     * leave nothing for region 2, whose first retry (14+2=16 > 14) would
     * abort the scan. Per-region, both regions succeed.
     */
    public function testRegionsHaveIndependentBackoffBudgets(): void
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

        // Per-region retry counts: region 1 spends its full 14ms budget
        // (three retries) then succeeds; region 2 retries once and succeeds.
        $failuresPerRegion = [0, 0];
        $this->grpc->method('call')->willReturnCallback(function (
            $address,
            $service,
            $method,
            $request,
            $responseClass,
        ) use (&$failuresPerRegion): RawScanResponse {
            $rawRequest = $request;
            $key = $rawRequest->getStartKey();
            $regionIndex = $key < 'm' ? 0 : 1;

            if ($failuresPerRegion[$regionIndex] < ($regionIndex === 0 ? 3 : 1)) {
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
            maxBackoffMs: 14,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );

        $results = $scanner->scan('a', 'z', 100, false);

        // Both regions succeeded. A shared budget (region 1's 14ms spend
        // carrying over to region 2) would have aborted on region 2's first
        // retry (14+2=16 > 14).
        $this->assertCount(2, $results);
        $this->assertSame(
            [3, 1],
            $failuresPerRegion,
            'Region 2 must retry with its own fresh budget after region 1 spent its full 14ms',
        );
    }

    /**
     * A multi-region deleteRange where every region's send fails: since the
     * parallelised fan-out (issue #295) dispatches both regions before any
     * wait, the failures surface together as a BatchPartialFailureException
     * instead of aborting at the first failing region. deleteRange is
     * idempotent so retrying the whole operation stays safe.
     */
    public function testDeleteRangeExhaustsBudgetInFirstRegion(): void
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
        // Dispatch-phase failure (address resolution / budget exhaustion in
        // the retried send path) — one attempt per region.
        $this->grpc->method('callAsync')->willReturnCallback(
            function () use (&$retryCount): never {
                $retryCount++;
                throw new TiKvException('StaleCommand');
            },
        );

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

        try {
            $rangeOps->deleteRange('a', 'z');
            $this->fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            // Both regions fail together as a partial failure, but each one
            // still burns its OWN retry budget first: with maxBackoffMs=10
            // StaleCmd backoff exhausts a region after at least 3 attempts
            // (issue #271 semantics preserved per region). With jitter
            // (issue #242) the sleeps are randomly halved, so a region can
            // fit a 4th attempt into the budget (retryCount 6..8); a shared
            // budget would land below 6.
            $this->assertCount(2, $e->getRegionErrors());
            $this->assertGreaterThanOrEqual(6, $retryCount, 'Each region exhausts its own 3-attempt budget');
            $this->assertLessThanOrEqual(8, $retryCount);
        }
    }

    /**
     * A multi-region checksum where every region's send fails: since the
     * parallelised fan-out (issue #295) dispatches both regions before any
     * wait, the failures surface together as a BatchPartialFailureException
     * instead of aborting at the first failing region.
     */
    public function testChecksumExhaustsBudgetInFirstRegion(): void
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
        $this->grpc->method('callAsync')->willReturnCallback(
            function () use (&$retryCount): never {
                $retryCount++;
                throw new TiKvException('StaleCommand');
            },
        );

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

        try {
            $rangeOps->checksum('a', 'z');
            $this->fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            // Same per-region budget semantics: each region gets at least its
            // 3 attempts (2 regions >= 6 calls). With jitter (issue #242) the
            // StaleCmd sleeps are halved at random, so a region can squeeze a
            // 4th attempt into the maxBackoffMs=10 budget (3 regions worth of
            // calls max: 6 + 2 = 8). Shared-budget semantics would still land
            // below 6, so the lower bound is the invariant under test.
            $this->assertCount(2, $e->getRegionErrors());
            $this->assertGreaterThanOrEqual(6, $retryCount, 'Each region exhausts its own 3-attempt budget');
            $this->assertLessThanOrEqual(8, $retryCount);
        }
    }

    /**
     * A multi-region reverseScan with each region on its own budget: the
     * first region exhausts its own maxBackoffMs and the scan aborts before
     * a later region is scanned (per-region semantics, issue #271).
     */
    public function testReverseScanExhaustsBudgetInFirstRegion(): void
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

        $this->assertLessThanOrEqual(4, $retryCount, 'First region exhausts its own per-region budget');
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
            string $address,
            string $service,
            string $method,
            RawScanRequest $request,
            string $responseClass,
        ) use (&$failuresPerRegion): RawScanResponse {
            $key = $request->getStartKey();
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

    /**
     * Issue #190: a retried deleteRange/checksum dispatch must re-resolve the
     * region inside the retry closure instead of replaying the stale region
     * captured before the loop.
     *
     * The first resolution is a cache hit on the stale leader (store 1); the
     * first callAsync fails with a retryable StaleCommand, the executor's
     * invalidation lookup sees the stale region again, and the next
     * resolution misses the cache so PD answers with the region under the new
     * leader (store 2). The second callAsync then throws a deliberately
     * non-retryable error, so the loop stops after exactly two attempts with
     * no dependency on backoff jitter.
     *
     * @param callable(RawKvRangeOps): void $operation
     */
    private function assertRetryReresolvesToNewLeader(callable $operation): void
    {
        $region1 = $this->defaultRegion('', 'z', regionId: 1, leaderStoreId: 1);
        $region2 = $this->defaultRegion('', 'z', regionId: 1, leaderStoreId: 2);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress('tikv1:20160');
        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress('tikv2:20160');

        // First resolution: cache hit on the stale leader. Second: the
        // RetryExecutor's invalidation lookup. Third: post-invalidation miss,
        // so the resolver falls through to PD.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($region1, $region1, null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1]);
        $this->pdClient->method('getRegion')->willReturn($region2);
        $this->pdClient->method('getStore')->willReturnCallback(
            static fn(int $storeId): Store => $storeId === 2 ? $store2 : $store1,
        );

        /** @var list<string> $addresses */
        $addresses = [];
        $this->grpc->expects($this->exactly(2))->method('callAsync')->willReturnCallback(
            function (string $address) use (&$addresses): never {
                $addresses[] = $address;
                if (count($addresses) === 1) {
                    throw new TiKvException('StaleCommand');
                }

                // Unclassified by ErrorClassifier -> non-retryable: the loop
                // stops after the second attempt.
                throw new TiKvException('fatal-test-error');
            },
        );

        $rangeOps = new RawKvRangeOps(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            logger: new NullLogger(),
        );

        try {
            $operation($rangeOps);
            $this->fail('Expected the non-retryable dispatch error to propagate');
        } catch (TiKvException) {
            // The fan-out wraps dispatch failures in
            // BatchPartialFailureException (a TiKvException); either way the
            // second, non-retryable error is what stops the loop.
        }

        $this->assertSame(
            ['tikv1:20160', 'tikv2:20160'],
            $addresses,
            'The retried dispatch must re-resolve the region and target the new leader store',
        );
    }

    public function testDeleteRangeRetryReresolvesRegionAndTargetsNewLeader(): void
    {
        $this->assertRetryReresolvesToNewLeader(
            static function (RawKvRangeOps $ops): void {
                $ops->deleteRange('a', 'z');
            },
        );
    }

    public function testChecksumRetryReresolvesRegionAndTargetsNewLeader(): void
    {
        $this->assertRetryReresolvesToNewLeader(
            static function (RawKvRangeOps $ops): void {
                $ops->checksum('a', 'z');
            },
        );
    }

    /**
     * Issue #190: when a retry re-resolves a region that shrank (split) so it
     * no longer covers the clipped sub-range, the operation must fail loudly
     * instead of sending the partial range. For checksum this is critical:
     * the raw read is silently clamped by the region snapshot, so a partial
     * success would be XOR-merged as if complete. The guard fires on the
     * second resolution, before any second send, so exactly one callAsync is
     * made.
     *
     * @param callable(RawKvRangeOps): void $operation
     */
    private function assertShrunkRegionFailsClosed(callable $operation): void
    {
        $region1 = $this->defaultRegion('', 'z', regionId: 1, leaderStoreId: 1);
        $region2 = $this->defaultRegion('', 'k', regionId: 1, leaderStoreId: 2);

        // First resolution: cache hit on the (full) stale region. Second: the
        // RetryExecutor's invalidation lookup. Third: miss, so PD answers with
        // the shrunken region whose end key no longer covers a..z.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($region1, $region1, null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1]);
        $this->pdClient->method('getRegion')->willReturn($region2);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // First attempt is dispatched and fails retryably; the second is
        // aborted by the coverage guard before its send.
        $this->grpc->expects($this->once())->method('callAsync')->willReturnCallback(
            function (): never {
                throw new TiKvException('StaleCommand');
            },
        );

        $rangeOps = new RawKvRangeOps(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            logger: new NullLogger(),
        );

        try {
            $operation($rangeOps);
            $this->fail('Expected a shrunken region to fail closed');
        } catch (TiKvException $e) {
            $this->assertStringContainsString('shrank', $e->getMessage());
        }
    }

    public function testDeleteRangeFailsClosedWhenRegionShrankAfterRetry(): void
    {
        $this->assertShrunkRegionFailsClosed(
            static function (RawKvRangeOps $ops): void {
                $ops->deleteRange('a', 'z');
            },
        );
    }

    public function testChecksumFailsClosedWhenRegionShrankAfterRetry(): void
    {
        $this->assertShrunkRegionFailsClosed(
            static function (RawKvRangeOps $ops): void {
                $ops->checksum('a', 'z');
            },
        );
    }
}
