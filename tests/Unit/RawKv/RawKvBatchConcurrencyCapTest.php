<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #264: batchGet/batchPut/batchDelete fan out one RPC per sub-batch;
 * the fan-out must be dispatched in windows bounded by maxConcurrency so a
 * huge batch cannot open an unbounded thundering herd of in-flight gRPC
 * calls (memory + server-side overload).
 *
 * The send (dispatch) phase is observable at GrpcClientInterface::getChannel()
 * — every sub-batch callable asks for its channel exactly once when the RPC
 * is sent. The executor's windowing guarantees that no more than
 * maxConcurrency futures exist before a wait happens, so the number of
 * channels handed out before the first wait completes equals the window
 * size: the peak in-flight request count is bounded by the cap.
 */
final class RawKvBatchConcurrencyCapTest extends TestCase
{
    use GrpcExtensionGate;

    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;

    private int $inFlight = 0;
    private int $maxInFlight = 0;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();

        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $this->inFlight = 0;
        $this->maxInFlight = 0;
    }

    private function createBatch(int $maxConcurrency): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
            maxConcurrency: $maxConcurrency,
        );
    }

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    /**
     * One region for every key (so 1100 keys split into ceil(1100/512) = 3
     * sub-batch callables) and a real channel to a dead endpoint: the RPCs
     * are sent for real (observable in-flight accounting) and fail at wait
     * time with BatchPartialFailureException — which also asserts that
     * partial-failure semantics survive the windowed dispatch.
     */
    private function stubCluster(): void
    {
        $region = $this->defaultRegion();
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $this->regionCache->method('getByKey')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($store);

        $self = $this;
        $this->grpc->method('getChannel')->willReturnCallback(
            function (string $address) use ($self): \Grpc\Channel {
                $self->inFlight++;
                $self->maxInFlight = max($self->maxInFlight, $self->inFlight);

                return new \Grpc\Channel('127.0.0.1:1', [
                    'credentials' => \Grpc\ChannelCredentials::createInsecure(),
                ]);
            },
        );
    }

    /** @return string[] */
    private function manyKeys(int $count): array
    {
        $keys = [];
        for ($i = 0; $i < $count; $i++) {
            $keys[] = sprintf('key-%05d', $i);
        }

        return $keys;
    }

    public function testBatchGetFanOutNeverExceedsMaxConcurrency(): void
    {
        $this->stubCluster();

        try {
            $this->createBatch(2)->batchGet($this->manyKeys(1100), $this->createRetryExecutor());
            $this->fail('Expected BatchPartialFailureException from the dead-endpoint channel');
        } catch (BatchPartialFailureException) {
            // expected: the sends were issued, the waits failed
        }

        $this->assertLessThanOrEqual(2, $this->maxInFlight, 'Fan-out must never exceed the concurrency cap');
        $this->assertSame(2, $this->maxInFlight, 'Windows must actually saturate the cap (true fan-out)');
    }

    public function testBatchGetFanOutWithCapOneIsBoundedToOneInFlight(): void
    {
        $this->stubCluster();

        try {
            $this->createBatch(1)->batchGet($this->manyKeys(1100), $this->createRetryExecutor());
            $this->fail('Expected BatchPartialFailureException from the dead-endpoint channel');
        } catch (BatchPartialFailureException) {
        }

        $this->assertSame(1, $this->maxInFlight);
    }

    public function testBatchDeleteFanOutNeverExceedsMaxConcurrency(): void
    {
        $this->stubCluster();

        try {
            $this->createBatch(2)->batchDelete($this->manyKeys(1100), $this->createRetryExecutor());
            $this->fail('Expected BatchPartialFailureException from the dead-endpoint channel');
        } catch (BatchPartialFailureException) {
        }

        $this->assertLessThanOrEqual(2, $this->maxInFlight);
        $this->assertSame(2, $this->maxInFlight);
    }

    public function testBatchPutFanOutNeverExceedsMaxConcurrency(): void
    {
        $this->stubCluster();

        $values = [];
        foreach ($this->manyKeys(1100) as $key) {
            $values[$key] = 'v';
        }

        try {
            $this->createBatch(2)->batchPut($values, 0, $this->createRetryExecutor());
            $this->fail('Expected BatchPartialFailureException from the dead-endpoint channel');
        } catch (BatchPartialFailureException) {
        }

        $this->assertLessThanOrEqual(2, $this->maxInFlight);
        $this->assertSame(2, $this->maxInFlight);
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            100,
            1000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            // maxAttempts=1 disables the wait-phase re-dispatch introduced by
            // #183 so the getChannel() counter below measures only the
            // windowed dispatch, not sequential retries.
            maxAttempts: 1,
        );
    }
}
