<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #275 (GRPC-16): `RawKvBatch` hands the configured batch deadline to
 * the executor, and the executor enforces it across all dispatch windows.
 * The tests go through the public `batchGet()`; the two keys live in two
 * regions and the window size is 1, so the batch needs two windows.
 */
final class RawKvBatchDeadlineTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
    }

    public function testBatchGetEnforcesTheConfiguredBatchDeadline(): void
    {
        $this->grpc->method('callAsync')->willReturnOnConsecutiveCalls(
            GrpcFuture::fromWaiter(static function (): Message {
                usleep(60_000); // well past the 20 ms batch deadline

                return new RawBatchGetResponse();
            }),
            GrpcFuture::fromWaiter(static fn(): Message => new RawBatchGetResponse()),
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 20));

        $this->expectException(BatchDeadlineExceededException::class);

        $batch->batchGet(['a', 'b'], $this->retryExecutor());
    }

    public function testBatchGetWithTheDeadlineDisabledRunsEveryWindow(): void
    {
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn(): GrpcFuture => GrpcFuture::fromWaiter(static function (): Message {
                usleep(30_000);

                return new RawBatchGetResponse();
            }),
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 0));

        self::assertSame(['a' => null, 'b' => null], $batch->batchGet(['a', 'b'], $this->retryExecutor()));
    }

    private function timeoutConfig(int $batchDeadlineMs): TimeoutConfig
    {
        // Every per-call timeout is tiny so a tiny batch deadline passes the
        // constructor check (the deadline may not be below a per-call one).
        return new TimeoutConfig(
            readTimeoutMs: 1,
            writeTimeoutMs: 1,
            batchReadTimeoutMs: 1,
            batchWriteTimeoutMs: 1,
            scanTimeoutMs: 1,
            deleteRangeTimeoutMs: 1,
            checksumTimeoutMs: 1,
            ingestTimeoutMs: 1,
            batchDeadlineMs: $batchDeadlineMs,
        );
    }

    private function batch(TimeoutConfig $timeoutConfig): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            $this->resolver(),
            $timeoutConfig,
            new NullLogger(),
            maxConcurrency: 1,
        );
    }

    private function resolver(): RegionResolver
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturnCallback($this->regionFor(...));

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getRegion')->willReturnCallback($this->regionFor(...));
        $pdClient->method('getStore')->willReturn($this->store());

        return new RegionResolver($pdClient, $regionCache);
    }

    private function retryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->createMock(RegionCacheInterface::class),
            $this->grpc,
            $this->resolver(),
            new NullLogger(),
        );
    }

    private function regionFor(string $key): RegionInfo
    {
        $regionId = $key === 'a' ? 1 : 2;

        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $regionId,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $key === 'a' ? '' : 'b',
            endKey: $key === 'a' ? 'b' : '',
        );
    }

    private function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        return $store;
    }
}
