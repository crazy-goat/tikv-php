<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\StoreHealthTracker;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
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
 * Issue #424: a failure is attributed to the store the failing RPC actually
 * targeted. PD and nested-RPC failures must not demote the outer store.
 */
class RetryExecutorStoreHealthAttributionTest extends TestCase
{
    private const KEY = 'key';
    private const OUTER_STORE = 1;
    private const NESTED_STORE = 2;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private InMemoryMetrics $metrics;

    private RegionCache $regionCache;

    private StoreHealthTracker $tracker;

    private RegionResolver $resolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->metrics = new InMemoryMetrics();
        $this->regionCache = new RegionCache(metrics: $this->metrics);
        $this->regionCache->put($this->region(self::OUTER_STORE));

        $this->tracker = new StoreHealthTracker(metrics: $this->metrics);
        $this->resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            $this->metrics,
            logger: new NullLogger(),
            pdEndpoints: ['pd:2379'],
            storeHealth: $this->tracker,
        );

        $this->pdClient->method('getRegion')->willReturn($this->region(self::OUTER_STORE));
    }

    public function testPdTransportFailureDoesNotDemoteAStore(): void
    {
        $this->pdClient->method('getStore')->willThrowException(
            new GrpcException('pd down', GrpcStatusCode::Unavailable->value, 'pd:2379'),
        );

        $executor = $this->executor(maxAttempts: 5);

        try {
            $executor->execute(
                self::KEY,
                function (): string {
                    $region = $this->resolver->getRegionInfo(self::KEY);
                    $this->resolver->resolveStoreAddress($region->leaderStoreId);

                    return 'unreachable';
                },
                $this->zeroSleepClassifier(),
                static fn (): int => self::OUTER_STORE,
            );
        } catch (RetryBudgetExhaustedException) {
        }

        $this->assertSame(0, $this->metrics->getStoreUnhealthyCount(self::OUTER_STORE));
        $this->assertTrue($this->tracker->isAvailable(self::OUTER_STORE));
    }

    public function testNestedRpcFailureDemotesTheNestedStoreNotTheOuterStore(): void
    {
        $this->pdClient->method('getStore')->willReturnCallback(
            fn (int $storeId): Store => $this->store($storeId),
        );

        $executor = $this->executor(maxAttempts: 3);

        try {
            $executor->execute(
                self::KEY,
                function (): string {
                    $region = $this->resolver->getRegionInfo(self::KEY);
                    $this->resolver->resolveStoreAddress($region->leaderStoreId);
                    $nestedAddress = $this->resolver->resolveStoreAddress(self::NESTED_STORE);

                    throw new GrpcException(
                        'unavailable',
                        GrpcStatusCode::Unavailable->value,
                        $nestedAddress,
                    );
                },
                $this->zeroSleepClassifier(),
                static fn (): int => self::OUTER_STORE,
            );
        } catch (RetryBudgetExhaustedException) {
        }

        $this->assertSame(0, $this->metrics->getStoreUnhealthyCount(self::OUTER_STORE));
        $this->assertTrue($this->tracker->isAvailable(self::OUTER_STORE));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(self::NESTED_STORE));
        $this->assertFalse($this->tracker->isAvailable(self::NESTED_STORE));
    }

    public function testCasIndeterminateTransportFailureDemotesTheStore(): void
    {
        $this->pdClient->method('getStore')->willReturnCallback(
            fn (int $storeId): Store => $this->store($storeId),
        );

        $executor = $this->executor(maxAttempts: 1);
        $thrown = 0;

        for ($i = 0; $i < 3; $i++) {
            try {
                $executor->execute(
                    self::KEY,
                    function (): string {
                        $region = $this->resolver->getRegionInfo(self::KEY);
                        $address = $this->resolver->resolveStoreAddress($region->leaderStoreId);

                        throw new CasIndeterminateException(
                            'indeterminate',
                            new GrpcException('unavailable', GrpcStatusCode::Unavailable->value, $address),
                        );
                    },
                    null,
                    static fn (): int => self::OUTER_STORE,
                );
            } catch (CasIndeterminateException) {
                $thrown++;
            }
        }

        $this->assertSame(3, $thrown);
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(self::OUTER_STORE));
        $this->assertFalse($this->tracker->isAvailable(self::OUTER_STORE));
    }

    public function testUnknownTargetAddressIsNotGuessed(): void
    {
        $executor = $this->executor(maxAttempts: 1);
        $thrown = 0;

        for ($i = 0; $i < 3; $i++) {
            try {
                $executor->execute(
                    self::KEY,
                    function (): string {
                        $this->resolver->getRegionInfo(self::KEY);

                        throw new GrpcException(
                            'unavailable',
                            GrpcStatusCode::Unavailable->value,
                            'unknown:20160',
                        );
                    },
                    $this->zeroSleepClassifier(),
                    static fn (): int => self::OUTER_STORE,
                );
            } catch (RetryBudgetExhaustedException) {
                $thrown++;
            }
        }

        $this->assertSame(3, $thrown);
        $this->assertSame(0, $this->metrics->getStoreUnhealthyCount(self::OUTER_STORE));
        $this->assertTrue($this->tracker->isAvailable(self::OUTER_STORE));
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
     * @return \Closure(TiKvException): BackoffType
     */
    private function zeroSleepClassifier(): \Closure
    {
        return static fn (TiKvException $e): BackoffType => BackoffType::None;
    }

    private function region(int $leaderStoreId): RegionInfo
    {
        return new RegionInfo(
            regionId: 42,
            leaderPeerId: 42,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
    }

    private function store(int $storeId): Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress(sprintf('store%d:20160', $storeId));

        return $store;
    }
}
