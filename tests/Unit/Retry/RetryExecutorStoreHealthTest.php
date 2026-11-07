<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Cache\StoreHealthTracker;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #424 (GAP-10): a store that keeps failing stops receiving requests
 * for the length of its cooldown, and a successful half-open probe recovers
 * it. The whole flow runs through the real RetryExecutor and RegionResolver.
 */
class RetryExecutorStoreHealthTest extends TestCase
{
    private const KEY = 'key';
    private const STORE_ID = 1;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private InMemoryMetrics $metrics;

    private RegionCache $regionCache;

    private StoreHealthTracker $tracker;

    private RegionResolver $resolver;

    private int $now = 0;

    private int $rpcCalls = 0;

    private int $attempts = 0;

    private int $regionLookups = 0;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->metrics = new InMemoryMetrics();
        $this->regionCache = new RegionCache(metrics: $this->metrics);
        $this->regionCache->put($this->region(42, '', 'm'));
        // A second region on the same store proves the invalidation is
        // store-wide, not just the region the failing key resolved through.
        $this->regionCache->put($this->region(99, 'm', ''));

        $this->tracker = new StoreHealthTracker(
            metrics: $this->metrics,
            clockMs: fn (): int => $this->now,
        );
        $this->resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            $this->metrics,
            logger: new NullLogger(),
            storeCache: new StoreCache(),
            storeHealth: $this->tracker,
        );

        $store = new Store();
        $store->setId(self::STORE_ID);
        $store->setAddress('store1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturnCallback(
            function (string $key): RegionInfo {
                $this->regionLookups++;

                return $this->region(42, '', 'm');
            },
        );
    }

    public function testStoreGoingDownStopsRoutingDuringCooldown(): void
    {
        $executor = $this->executor(maxAttempts: 6);

        try {
            $executor->execute(
                self::KEY,
                $this->failingOperation(0),
                $this->zeroSleepClassifier(),
                static fn (): int => self::STORE_ID,
            );
            $this->fail('Expected the retry attempt cap to be exhausted');
        } catch (RetryBudgetExhaustedException) {
        }

        // Three RPC failures reach the threshold; the remaining attempts are
        // rejected locally by resolveStoreAddress() without an RPC.
        $this->assertSame(3, $this->rpcCalls);
        $this->assertSame(6, $this->attempts);
        // The three failing RPCs each invalidate the region (existing #245
        // behaviour), so PD is asked once per failure. The cooldown probes
        // then reuse the cached entry: StoreUnavailableException is not a
        // routing error, so attempts 4-6 add no PD lookup and no invalidation.
        $this->assertSame(3, $this->regionLookups);
        $this->assertSame(3, $this->metrics->getInvalidations('retry_region_error'));
        $this->assertFalse($this->tracker->isAvailable(self::STORE_ID));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(self::STORE_ID));
        $this->assertSame(0, $this->metrics->getStoreRecoveredCount(self::STORE_ID));
        $this->assertNull($this->regionCache->getById(99));
        $this->assertSame(1, $this->metrics->getInvalidations('store_unhealthy'));
    }

    public function testStoreRecoversAfterCooldown(): void
    {
        $executor = $this->executor(maxAttempts: 10);

        $result = $executor->execute(
            self::KEY,
            $this->operationFailingFirst(3),
            $this->zeroSleepClassifier(),
            static fn (): int => self::STORE_ID,
        );

        $this->assertSame('ok', $result);
        $this->assertSame(4, $this->rpcCalls);
        $this->assertSame(6, $this->attempts);
        $this->assertTrue($this->tracker->isHealthy(self::STORE_ID));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(self::STORE_ID));
        $this->assertSame(1, $this->metrics->getStoreRecoveredCount(self::STORE_ID));
        $this->assertTrue($this->metrics->isStoreHealthy(self::STORE_ID));
    }

    public function testFailedHalfOpenProbeKeepsStoreDownWithoutDuplicateMetric(): void
    {
        $executor = $this->executor(maxAttempts: 12);

        try {
            $executor->execute(
                self::KEY,
                $this->failingOperation(400),
                $this->zeroSleepClassifier(),
                static fn (): int => self::STORE_ID,
            );
            $this->fail('Expected the retry attempt cap to be exhausted');
        } catch (RetryBudgetExhaustedException) {
        }

        // The store is probed once per cooldown and fails each time: the
        // cooldown restarts without a second healthy-to-unhealthy metric.
        $this->assertGreaterThan(3, $this->rpcCalls);
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(self::STORE_ID));
        $this->assertSame(0, $this->metrics->getStoreRecoveredCount(self::STORE_ID));
        $this->assertFalse($this->tracker->isAvailable(self::STORE_ID));
    }

    public function testFatalTransportStatusDoesNotDemoteTheStore(): void
    {
        $executor = $this->executor(maxAttempts: 1);
        $thrown = 0;

        for ($i = 0; $i < 5; $i++) {
            try {
                $executor->execute(
                    self::KEY,
                    function (): string {
                        $region = $this->resolver->getRegionInfo(self::KEY);
                        $address = $this->resolver->resolveStoreAddress($region->leaderStoreId);

                        throw new GrpcException(
                            'unauthenticated',
                            GrpcStatusCode::Unauthenticated->value,
                            $address,
                        );
                    },
                    null,
                    static fn (): int => self::STORE_ID,
                );
            } catch (GrpcException) {
                $thrown++;
            }
        }

        // A fatal status is a client/request problem, not a dead store: it
        // must not accumulate failures toward the unhealthy threshold.
        $this->assertSame(5, $thrown);
        $this->assertSame(0, $this->metrics->getStoreUnhealthyCount(self::STORE_ID));
        $this->assertTrue($this->tracker->isAvailable(self::STORE_ID));
    }

    private function executor(int $maxAttempts): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: $this->resolver,
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
            deadlineMs: 0,
        );
    }

    /**
     * @return \Closure(): string
     */
    private function failingOperation(int $advanceMs): \Closure
    {
        return function () use ($advanceMs): string {
            $this->attempts++;
            $this->now += $advanceMs;
            $region = $this->resolver->getRegionInfo(self::KEY);
            $address = $this->resolver->resolveStoreAddress($region->leaderStoreId);
            $this->rpcCalls++;

            throw new GrpcException('unavailable', GrpcStatusCode::Unavailable->value, $address);
        };
    }

    /**
     * @return \Closure(): string
     */
    private function operationFailingFirst(int $failures): \Closure
    {
        return function () use ($failures): string {
            $this->attempts++;
            $this->now += 400;
            $region = $this->resolver->getRegionInfo(self::KEY);
            $address = $this->resolver->resolveStoreAddress($region->leaderStoreId);
            $this->rpcCalls++;

            if ($this->rpcCalls <= $failures) {
                throw new GrpcException('unavailable', GrpcStatusCode::Unavailable->value, $address);
            }

            return 'ok';
        };
    }

    /**
     * @return \Closure(TiKvException): BackoffType
     */
    private function zeroSleepClassifier(): \Closure
    {
        return static fn (TiKvException $e): BackoffType => BackoffType::None;
    }

    private function region(int $regionId, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $regionId,
            leaderStoreId: self::STORE_ID,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }
}
