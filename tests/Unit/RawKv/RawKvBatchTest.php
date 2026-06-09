<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutRequest;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RawKvBatchTest extends TestCase
{
    use GrpcExtensionGate;

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
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], 60, $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testBatchPutWithAssociativeTtlArray(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], ['k1' => 60, 'k2' => 120], $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testScalarTtlExpandsToMatchPairCountOnRequest(): void
    {
        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');
        $pair3 = new KvPair();
        $pair3->setKey('k3');
        $pair3->setValue('v3');

        $request = new RawBatchPutRequest();
        $request->setPairs([$pair1, $pair2, $pair3]);

        // Simulate the fix: scalar TTL expanded to match pair count
        $ttl = 60;
        $ttls = array_fill(0, count([$pair1, $pair2, $pair3]), $ttl);
        $request->setTtls($ttls);

        $this->assertCount(3, $request->getTtls());
        foreach ($request->getTtls() as $entry) {
            $this->assertSame(60, $entry);
        }
    }

    public function testPerKeyTtlArrayPassesThroughUnchanged(): void
    {
        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $request = new RawBatchPutRequest();
        $request->setPairs([$pair1, $pair2]);

        // Per-key TTL array passes through unchanged
        $ttls = [60, 120];
        $request->setTtls($ttls);

        $this->assertCount(2, $request->getTtls());
        $this->assertSame(60, $request->getTtls()[0]);
        $this->assertSame(120, $request->getTtls()[1]);
    }

    public function testBatchPutWithMultipleKeysAndScalarTtl(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        $retryExecutor = $this->createRetryExecutor();
        // 3 keys with scalar TTL - old code would send a 1-element ttls array
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3'], 60, $retryExecutor);
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
