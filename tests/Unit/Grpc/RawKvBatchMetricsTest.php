<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #273: the V1 batch fan-out goes through `GrpcClient::callAsync()`, so
 * its unary calls are metered like every other RPC and the client's closed
 * state is honoured.
 *
 * Every RPC targets a port nothing listens on: the sends succeed and the
 * waits fail with a transport error, so each call settles as a failure.
 */
final class RawKvBatchMetricsTest extends TestCase
{
    use GrpcExtensionGate;

    private const OPERATION = 'tikvpb.Tikv/RawBatchGet';

    private const DEAD_ADDRESS = '127.0.0.1:1';

    private const OTHER_DEAD_ADDRESS = '127.0.0.1:2';

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testBatchGetMetersEverySubBatchExactlyOnce(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(metrics: $metrics);

        try {
            try {
                $this->createBatch($grpc)->batchGet(['k1', 'k2'], $this->createRetryExecutor($grpc));
                $this->fail('Expected BatchPartialFailureException from the dead endpoints');
            } catch (BatchPartialFailureException) {
                // Two regions, so two sub-batches.
            }

            $this->assertSame(2, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(2, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    public function testCancelledFutureRecordsOneFailedCompletion(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(metrics: $metrics);

        try {
            $request = new RawBatchGetRequest();
            $request->setKeys(['k1']);
            $future = $grpc->callAsync(
                self::DEAD_ADDRESS,
                'tikvpb.Tikv',
                'RawBatchGet',
                $request,
                RawBatchGetResponse::class,
                timeoutMs: 500,
            );
            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcFailed(self::OPERATION));

            $future->cancel();
            $future->cancel();

            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(1, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    public function testBatchOnClosedClientFailsBecauseTheClientIsClosed(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(metrics: $metrics);
        $grpc->close();

        try {
            $this->createBatch($grpc)->batchGet(['k1'], $this->createRetryExecutor($grpc));
            $this->fail('Expected BatchPartialFailureException from a closed client');
        } catch (BatchPartialFailureException $e) {
            // The fan-out reports the per-region failure; the cause is the
            // closed client, and no RPC was started or metered.
            $this->assertStringContainsString('gRPC client is closed', $e->getMessage());
            $this->assertInstanceOf(InvalidStateException::class, $e->getFirstRegionError());
            $this->assertSame(0, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcFailed(self::OPERATION));
        }
    }

    public function testCallAsyncOnClosedClientThrowsInvalidStateException(): void
    {
        $grpc = new GrpcClient();
        $grpc->close();

        $this->expectException(InvalidStateException::class);
        $grpc->callAsync(
            self::DEAD_ADDRESS,
            'tikvpb.Tikv',
            'RawBatchGet',
            new RawBatchGetRequest(),
            RawBatchGetResponse::class,
        );
    }

    public function testSendFailureCompletesTheRpcAsFailedExactlyOnce(): void
    {
        // Plaintext is refused, so createChannel() throws after the closed
        // check and rpcStarted(): no future exists to settle the RPC.
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(allowInsecure: false, metrics: $metrics);

        try {
            $grpc->callAsync(
                self::DEAD_ADDRESS,
                'tikvpb.Tikv',
                'RawBatchGet',
                new RawBatchGetRequest(),
                RawBatchGetResponse::class,
            );
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(1, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    public function testNonPositiveTimeoutIsRejectedBeforeAnyRpcIsMetered(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(metrics: $metrics);

        try {
            $grpc->callAsync(
                self::DEAD_ADDRESS,
                'tikvpb.Tikv',
                'RawBatchGet',
                new RawBatchGetRequest(),
                RawBatchGetResponse::class,
                timeoutMs: 0,
            );
            $this->fail('Expected InvalidArgumentException for a zero timeout');
        } catch (InvalidArgumentException) {
            $this->assertSame(0, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(0, $grpc->getChannelCount());
        } finally {
            $grpc->close();
        }
    }

    private function createBatch(GrpcClient $grpc): RawKvBatch
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);

        $first = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'k2',
        );
        $second = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'k2',
            endKey: '',
        );

        $regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): RegionInfo => $key === 'k1' ? $first : $second,
        );
        $pdClient->method('getStore')->willReturnCallback(
            static function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress($storeId === 1 ? self::DEAD_ADDRESS : self::OTHER_DEAD_ADDRESS);

                return $store;
            },
        );

        return new RawKvBatch(
            $grpc,
            new RegionResolver($pdClient, $regionCache),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function createRetryExecutor(GrpcClient $grpc): RetryExecutor
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);

        return new RetryExecutor(
            100,
            1000,
            $regionCache,
            $grpc,
            new RegionResolver($pdClient, $regionCache),
            new NullLogger(),
            maxAttempts: 1,
        );
    }
}
