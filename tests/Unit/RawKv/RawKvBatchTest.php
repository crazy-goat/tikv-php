<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\RawKv\RegionResolver;
use CrazyGoat\TiKV\Client\RawKv\RetryExecutor;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RawKvBatchTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;
    private RawKvBatch $batch;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->batch = new RawKvBatch(
            $this->grpc,
            $regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
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

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    public function testBatchPutThrowsOnTtlCountMismatch(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TTL array count (1) must match key-value pairs count (2)');

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], [60], $retryExecutor);
    }

    public function testBatchGetEmptyReturnsEmpty(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->assertSame([], $this->batch->batchGet([], $retryExecutor));
    }

    public function testBatchPutEmptyReturnsEarly(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->grpc->expects($this->never())->method('getChannel');
        $this->batch->batchPut([], 0, $retryExecutor);
    }

    public function testBatchDeleteEmptyReturnsEarly(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->grpc->expects($this->never())->method('getChannel');
        $this->batch->batchDelete([], $retryExecutor);
    }

    public function testBatchPutWithIntTtl(): void
    {
        if (!class_exists('Grpc\Timeval')) {
            $this->markTestSkipped('Requires grpc extension (Grpc\Timeval)');
        }

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], 60, $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testBatchPutWithAssociativeTtlArray(): void
    {
        if (!class_exists('Grpc\Timeval')) {
            $this->markTestSkipped('Requires grpc extension (Grpc\Timeval)');
        }

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], ['k1' => 60, 'k2' => 120], $retryExecutor);
        $this->addToAssertionCount(1);
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
        );
    }
}
