<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Google\Protobuf\Internal\Message;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * The slow log must be wired into the batch read path (issue #340, TEST-20).
 *
 * The other tests of the issue drive the private
 * `RawKvBatch::wrapWithLogging()` seam, which pins the wrapper but not the
 * wiring: deleting the call site in `RawKvBatch::batchGetWithRetry()`
 * would leave them green while the slow log silently stopped working for
 * batch reads. This test goes through the public `batchGet()` instead, on
 * the API V2 transport — the one path that uses the mockable `callAsync()`
 * instead of the hand-rolled `new \Grpc\Call(...)`. It still needs the
 * gRPC extension (`RawKvBatch::deadlineFor()` builds a `\Grpc\Timeval`
 * before the API V2 branch), so it lives in the `Grpc` suite.
 */
class RawKvBatchSlowLogWiringTest extends TestCase
{
    private GrpcClientInterface&MockObject $inner;

    protected function setUp(): void
    {
        $this->inner = $this->createMock(GrpcClientInterface::class);
    }

    public function testBatchReadIsSlowLoggedThroughThePublicApi(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        $this->inner->method('callAsync')->willReturn(GrpcFuture::fromWaiter(
            static function (): Message {
                usleep(20_000); // 20 ms — above the 1 ms threshold

                return new RawBatchGetResponse();
            },
        ));

        $batch = $this->batchWithSlowLog($logger);
        $key = 'user_email:alice@example.com';

        $result = $batch->batchGet([$key], $this->retryExecutor());

        self::assertSame([$key => null], $result);

        $slowRecords = array_values(array_filter(
            $handler->getRecords(),
            static fn(LogRecord $record): bool => $record->message === 'Slow TiKV operation',
        ));

        self::assertCount(1, $slowRecords, 'the batch read was not slow-logged');
        $record = $slowRecords[0];
        self::assertSame(Level::Warning, $record->level);
        self::assertSame('batch_read', $record->context['operation']);
        self::assertSame(1, $record->context['threshold_ms']);
        self::assertGreaterThan(1, $record->context['duration_ms']);
        self::assertSame(KeyRedactor::redact($key), $record->context['key']);
    }

    public function testBatchReadIsNotSlowLoggedBelowTheThreshold(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        $this->inner->method('callAsync')->willReturn(GrpcFuture::fromWaiter(
            static fn(): Message => new RawBatchGetResponse(),
        ));

        $batch = $this->batchWithSlowLog($logger, new SlowLogConfig(batchReadThresholdMs: 60_000));

        $batch->batchGet(['k1'], $this->retryExecutor());

        $slowRecords = array_filter(
            $handler->getRecords(),
            static fn(LogRecord $record): bool => $record->message === 'Slow TiKV operation',
        );

        self::assertSame([], $slowRecords);
    }

    private function batchWithSlowLog(
        Logger $logger,
        ?SlowLogConfig $slowLogConfig = null,
    ): RawKvBatch {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn($this->region());

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getRegion')->willReturn($this->region());
        $pdClient->method('getStore')->willReturn($this->store());

        $regionResolver = new RegionResolver($pdClient, $regionCache);

        $apiV2 = new ApiV2GrpcClient(
            $this->inner,
            new CodecV2(Mode::Raw, 42, 'tenant-a'),
            Mode::Raw,
        );

        return new RawKvBatch(
            $apiV2,
            $regionResolver,
            new TimeoutConfig(),
            $logger,
            $slowLogConfig ?? new SlowLogConfig(batchReadThresholdMs: 1),
        );
    }

    private function retryExecutor(): RetryExecutor
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn($this->region());

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getRegion')->willReturn($this->region());
        $pdClient->method('getStore')->willReturn($this->store());

        return new RetryExecutor(
            20000,
            600000,
            $regionCache,
            $this->inner,
            new RegionResolver($pdClient, $regionCache),
            new NullLogger(),
        );
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
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
