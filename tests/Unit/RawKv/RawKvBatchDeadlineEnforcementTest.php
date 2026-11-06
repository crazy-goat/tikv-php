<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
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
final class RawKvBatchDeadlineEnforcementTest extends TestCase
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

    /**
     * A positive deadline is a ceiling, not a minimum: a batch that answers
     * inside the budget returns its results unchanged.
     */
    public function testBatchGetWithAPositiveDeadlineThatFinishesInTimeIsUnaffected(): void
    {
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn(): GrpcFuture => GrpcFuture::fromWaiter(static function (): Message {
                usleep(1_000);

                return new RawBatchGetResponse();
            }),
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 5000));

        self::assertSame(['a' => null, 'b' => null], $batch->batchGet(['a', 'b'], $this->retryExecutor()));
    }

    /**
     * `batchPut` fans out through the same executor and must pass the
     * configured deadline, not the executor default (issue #341).
     */
    public function testBatchPutEnforcesTheConfiguredBatchDeadline(): void
    {
        $this->grpc->method('callAsync')->willReturnOnConsecutiveCalls(
            GrpcFuture::fromWaiter(static function (): Message {
                usleep(500_000);

                return new RawBatchPutResponse();
            }),
            GrpcFuture::fromWaiter(static fn(): Message => new RawBatchPutResponse()),
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 200));

        $this->expectException(BatchDeadlineExceededException::class);

        $batch->batchPut(['a' => '1', 'b' => '2'], 0, $this->retryExecutor());
    }

    /**
     * `batchDelete` fans out through the same executor and must pass the
     * configured deadline, not the executor default (issue #341).
     */
    public function testBatchDeleteEnforcesTheConfiguredBatchDeadline(): void
    {
        $this->grpc->method('callAsync')->willReturnOnConsecutiveCalls(
            GrpcFuture::fromWaiter(static function (): Message {
                usleep(500_000);

                return new RawBatchDeleteResponse();
            }),
            GrpcFuture::fromWaiter(static fn(): Message => new RawBatchDeleteResponse()),
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 200));

        $this->expectException(BatchDeadlineExceededException::class);

        $batch->batchDelete(['a', 'b'], $this->retryExecutor());
    }

    /**
     * A windowed write fan-out shares one budget: with three regions and a
     * window size of two, the first window's wait exhausts the deadline and
     * the third region's window is never dispatched (issue #341).
     */
    public function testWindowedBatchPutSharesTheDeadlineAcrossWindows(): void
    {
        $calls = 0;
        $this->grpc->expects(self::exactly(2))->method('callAsync')->willReturnCallback(
            static function () use (&$calls): GrpcFuture {
                ++$calls;
                if ($calls === 1) {
                    return GrpcFuture::fromWaiter(static function (): Message {
                        usleep(1_000_000);

                        return new RawBatchPutResponse();
                    });
                }

                return GrpcFuture::fromWaiter(static fn(): Message => new RawBatchPutResponse());
            },
        );

        $batch = $this->batch($this->timeoutConfig(batchDeadlineMs: 500), maxConcurrency: 2);

        try {
            $batch->batchPut(['a' => '1', 'b' => '2', 'c' => '3'], 0, $this->retryExecutor());
            self::fail('the deadline must stop the write fan-out between windows');
        } catch (BatchDeadlineExceededException $e) {
            self::assertSame(500, $e->getDeadlineMs());
            self::assertGreaterThanOrEqual(500, $e->getElapsedMs());
            self::assertSame([0, 1], $e->getContext()['dispatchedRegions']);
            self::assertSame([0, 1, 2], $e->getContext()['pendingRegions']);
            self::assertSame(2, $calls, 'the third region must not be dispatched');
        }
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

    public function testUnboundedScanSharesOneDeadlineAcrossWindows(): void
    {
        $this->grpc->expects(self::never())->method('call');
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn(): GrpcFuture => GrpcFuture::fromWaiter(static function (): Message {
                usleep(30_000); // one window already uses more than the 20 ms deadline

                return new RawScanResponse();
            }),
        );

        try {
            $this->scanner(maxConcurrency: 1)->scan('a', 'z', 0, false);
            self::fail('the exhausted deadline must stop the scan between windows');
        } catch (BatchDeadlineExceededException $e) {
            self::assertSame(20, $e->getDeadlineMs());
            self::assertGreaterThanOrEqual(20, $e->getElapsedMs());
            self::assertSame([0], $e->getContext()['dispatchedRegions']);
            self::assertSame([1, 2], $e->getContext()['pendingRegions']);
        }
    }

    public function testUnboundedScanDoesNotFallBackToTheSequentialPathOnADeadline(): void
    {
        // Two regions in the first window: the deadline runs out inside it,
        // between the two waits. The generic TiKvException fallback would
        // re-run the scan on the sequential path, which has no deadline.
        $this->grpc->expects(self::never())->method('call');
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn(): GrpcFuture => GrpcFuture::fromWaiter(static function (): Message {
                usleep(30_000);

                return new RawScanResponse();
            }),
        );

        try {
            $this->scanner(maxConcurrency: 2)->scan('a', 'z', 0, false);
            self::fail('the deadline must not be swallowed by the sequential fallback');
        } catch (BatchDeadlineExceededException $e) {
            self::assertSame(20, $e->getDeadlineMs());
            self::assertSame([0, 1, 2], $e->getContext()['pendingRegions']);
        }
    }

    private function scanner(int $maxConcurrency): RawKvScanner
    {
        $regions = [
            new RegionInfo(
                regionId: 1,
                leaderPeerId: 1,
                leaderStoreId: 1,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: 'a',
                endKey: 'h',
            ),
            new RegionInfo(
                regionId: 2,
                leaderPeerId: 2,
                leaderStoreId: 1,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: 'h',
                endKey: 'p',
            ),
            new RegionInfo(
                regionId: 3,
                leaderPeerId: 3,
                leaderStoreId: 1,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: 'p',
                endKey: 'z',
            ),
        ];
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getRegionsInRange')->willReturn($regions);
        $regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($regions): RegionInfo {
                foreach ($regions as $region) {
                    if (strcmp($key, $region->endKey) < 0) {
                        return $region;
                    }
                }

                return $regions[2];
            },
        );
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getStore')->willReturn($this->store());

        return new RawKvScanner(
            $pdClient,
            $this->grpc,
            new RegionResolver($pdClient, $regionCache),
            $this->timeoutConfig(batchDeadlineMs: 20),
            20000,
            600000,
            $regionCache,
            new NullLogger(),
            maxConcurrency: $maxConcurrency,
        );
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

    private function batch(TimeoutConfig $timeoutConfig, int $maxConcurrency = 1): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            $this->resolver(),
            $timeoutConfig,
            new NullLogger(),
            maxConcurrency: $maxConcurrency,
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
        $regionId = match ($key) {
            'a' => 1,
            'b' => 2,
            default => 3,
        };

        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $regionId,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: match ($regionId) {
                1 => '',
                2 => 'b',
                default => 'c',
            },
            endKey: match ($regionId) {
                1 => 'b',
                2 => 'c',
                default => '',
            },
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
