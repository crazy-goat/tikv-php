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
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
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
     * One scan() call shares one wall-clock deadline across its regions
     * (issue #223, TXN-18): region 1 consumes more than the deadline while
     * its RPC runs, so region 2 must not get a fresh window and the scan
     * aborts before region 2 is sent. The pre-#223 per-execute() deadline
     * restarted for every region and let the scan succeed.
     */
    public function testRegionsShareOneWallClockDeadlineWithinOneScan(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $region1Calls = 0;
        $region2Calls = 0;
        $this->grpc->method('call')->willReturnCallback(function (
            string $address,
            string $service,
            string $method,
            RawScanRequest $request,
            string $responseClass,
        ) use (
            &$region1Calls,
            &$region2Calls
): RawScanResponse {
            $key = $request->getStartKey();
            $response = new RawScanResponse();
            $pair = new KvPair();
            $pair->setKey($key . '-key');
            $pair->setValue('val');
            $response->setKvs([$pair]);

            if (KeyOrder::lt($key, 'm')) {
                $region1Calls++;
                usleep(300_000);
                return $response;
            }

            $region2Calls++;
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
            retryDeadlineMs: 200,
        );

        $caught = null;
        try {
            $scanner->scan('a', 'z', 100, false);
        } catch (RetryBudgetExhaustedException $e) {
            $caught = $e;
        }

        $this->assertInstanceOf(RetryBudgetExhaustedException::class, $caught);
        $this->assertStringContainsString('Retry deadline', $caught->getMessage());
        $this->assertSame(1, $region1Calls);
        $this->assertSame(
            0,
            $region2Calls,
            'Region 2 must share region 1\'s elapsed deadline instead of starting a fresh one',
        );
    }

    /**
     * A multi-region deleteRange shares one backoff budget across its
     * regions (issue #223): region 1 exhausts maxBackoffMs, then region 2's
     * first retryable failure is charged against the same exhausted budget
     * and gets exactly one attempt. The pre-#223 per-execute() budget gave
     * region 2 a fresh budget (three or four attempts), so the total call
     * count is the discriminator. The parallelised fan-out (issue #295)
     * still dispatches both regions, so both errors are collected in a
     * BatchPartialFailureException; deleteRange is idempotent.
     */
    public function testDeleteRangeSharesOneBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
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
            // Region 1 exhausts the shared budget after 3-4 attempts
            // (StaleCmd sleeps 1-2, 2-4, 4-8, 8-16 ms with equal jitter;
            // maxBackoffMs=10 is crossed by the third or fourth charge).
            // Region 2 then makes exactly one attempt: its first charge is
            // already above the shared budget. A per-region budget would
            // give it another 3-4 attempts, for 6-8 calls in total.
            $this->assertCount(2, $e->getRegionErrors());
            $this->assertGreaterThanOrEqual(4, $retryCount);
            $this->assertLessThanOrEqual(5, $retryCount, 'Both regions must share one 10 ms budget');
        }
    }

    /**
     * A multi-region checksum shares one backoff budget across its regions
     * (issue #223), exactly like deleteRange: region 1 exhausts
     * maxBackoffMs, region 2's first retryable failure is charged against
     * the same exhausted budget and gets one attempt. The parallelised
     * fan-out (issue #295) still collects both errors in a
     * BatchPartialFailureException.
     */
    public function testChecksumSharesOneBackoffBudgetAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
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
            // Same shared-budget arithmetic as deleteRange: region 1 makes
            // 3-4 attempts, region 2 exactly one. A per-region budget would
            // land at 6-8 calls.
            $this->assertCount(2, $e->getRegionErrors());
            $this->assertGreaterThanOrEqual(4, $retryCount);
            $this->assertLessThanOrEqual(5, $retryCount, 'Both regions must share one 10 ms budget');
        }
    }

    /**
     * A multi-region reverseScan walks its regions sequentially from the
     * upper bound: the first region exhausts the shared maxBackoffMs and the
     * scan aborts before the lower region is scanned (issue #223). The
     * shared budget does not change the abort point here; the test pins
     * that a range operation still stops on budget exhaustion.
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
            fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
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

        $this->assertLessThanOrEqual(4, $retryCount, 'First region exhausts the shared budget');
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
     * A multi-region scan shares one backoff budget across all its regions
     * (issue #223). Each of the four regions needs two StaleCmd retries to
     * succeed; the first region's spend fits in maxBackoffMs=6, but the
     * second region's retries are charged against the same budget and the
     * scan aborts. A per-execute() budget (the pre-#223 behaviour) lets all
     * four regions retry twice and succeed, which is what this test rejects.
     */
    public function testScanSharesOneBackoffBudgetAcrossRegions(): void
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
                KeyOrder::lt($key, 'b') => $region1,
                KeyOrder::lt($key, 'c') => $region2,
                KeyOrder::lt($key, 'd') => $region3,
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
                KeyOrder::lt($key, 'b') => 0,
                KeyOrder::lt($key, 'c') => 1,
                KeyOrder::lt($key, 'd') => 2,
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

        $caught = null;
        try {
            $scanner->scan('a', 'z', 100, false);
        } catch (TiKvException $e) {
            $caught = $e;
        }

        $this->assertInstanceOf(TiKvException::class, $caught);
        $this->assertSame('StaleCommand', $caught->getMessage());
        $this->assertSame(2, $failuresPerRegion[0], 'Region 1 fits its two retries in the shared budget');
        $this->assertGreaterThanOrEqual(
            1,
            $failuresPerRegion[1] + $failuresPerRegion[2],
            'The shared budget is exhausted by the second or third region, not refilled per region',
        );
        $this->assertSame(0, $failuresPerRegion[3], 'The scan aborts before the fourth region');
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
