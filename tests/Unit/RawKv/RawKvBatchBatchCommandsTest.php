<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsMultiplexer;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Tests\Unit\Batch\FakeBatchCommandsTransport;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Pure-PHP half of the RawKvBatch BatchCommands integration (issue #418):
 * the multiplexed path is exercised against the fake transport seam — no
 * ext-grpc involved, so this class runs in the Unit suite. The flag-off
 * identity and unary-fallback tests (which exercise the real unary
 * dispatch, Grpc\Call) live in RawKvBatchBatchCommandsGrpcTest (Grpc
 * suite, same rule as RawKvBatchTest — PR #484).
 */
final class RawKvBatchBatchCommandsTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function stubCluster(): void
    {
        // Region 1: ['', 'm'), region 2: ['m', '') — both led by store 1.
        $regions = [
            1 => $this->region(1, '', 'm'),
            2 => $this->region(2, 'm', ''),
        ];
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $regions[1] : $regions[2],
        );
        $this->pdClient->method('scanRegions')->willReturn(array_values($regions));
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
    }

    private function createRawKvBatch(?BatchCommandsMultiplexer $multiplexer): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
            batchCommands: $multiplexer,
        );
    }

    private function createRetryExecutor(int $maxAttempts = 1): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            $maxAttempts,
        );
    }

    public function testBatchGetMultiplexesOneRoundTripPerStore(): void
    {
        $this->stubCluster();
        // The multiplexed path never touches the unary gRPC layer.
        $this->grpc->expects($this->never())->method('getChannel');

        $fake = new FakeBatchCommandsTransport();
        $batch = $this->createRawKvBatch(new BatchCommandsMultiplexer($fake));

        // Two regions → two sub-batches, both on the same store: the AC is
        // ONE BatchCommands round trip instead of two unary RawBatchGet RPCs
        // (the wall-clock/RPC-count reduction pattern of the #295 fan-out
        // tests, counted at the transport seam).
        $results = $batch->batchGet(['a', 'z'], $this->createRetryExecutor());

        self::assertCount(1, $fake->exchanges, 'two sub-batches on one store must share one stream round trip');
        self::assertCount(2, $fake->exchanges[0]['ids']);
        self::assertSame('v:a', $results['a'], 'out-of-order responses must be correlated back to the right entry');
        self::assertSame('v:z', $results['z']);
    }

    public function testBatchPutMultiplexesWritesToOneRoundTrip(): void
    {
        $this->stubCluster();
        $this->grpc->expects($this->never())->method('getChannel');

        $fake = new FakeBatchCommandsTransport();
        $batch = $this->createRawKvBatch(new BatchCommandsMultiplexer($fake));

        $batch->batchPut(['a' => 'va', 'z' => 'vz'], 60, $this->createRetryExecutor());

        self::assertCount(1, $fake->exchanges, 'batchPut sub-batches on one store must share one stream round trip');
        self::assertCount(2, $fake->exchanges[0]['ids']);
    }
}
