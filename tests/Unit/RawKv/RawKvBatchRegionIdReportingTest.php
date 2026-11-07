<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchCallId;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #285 (GRPC-26): a RawKvBatch failure must name the real TiKV region
 * ID, not the zero-based fan-out slot. A region split into several
 * sub-batches must report each sub-batch separately without collisions.
 */
final class RawKvBatchRegionIdReportingTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;
    private TestHandler $logs;
    private Logger $logger;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->logs = new TestHandler();
        $this->logger = new Logger('test', [$this->logs]);
    }

    public function testFailureReportsTheRealRegionIdNotTheFanOutSlot(): void
    {
        $region4 = $this->region(4, leaderStoreId: 1, startKey: '', endKey: 'm');
        $region11 = $this->region(11, leaderStoreId: 2, startKey: 'm', endKey: '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region4 : $region11,
        );
        $this->pdClient->method('getStore')->willReturnCallback(
            fn(int $storeId): Store => $this->store($storeId),
        );
        $this->grpc->method('callAsync')->willReturnCallback(
            static function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ): GrpcFuture {
                self::assertInstanceOf(RawBatchGetRequest::class, $request);
                if (in_array('z', iterator_to_array($request->getKeys()), true)) {
                    return GrpcFuture::fromWaiter(static function (): Message {
                        throw new GrpcException('connection refused', 14);
                    });
                }

                return GrpcFuture::completed(new RawBatchGetResponse());
            },
        );

        try {
            $this->batch()->batchGet(['a', 'z'], $this->retryExecutor());
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            // Slot 1 failed; the region is 11. The old code reported "1".
            self::assertSame([1], array_keys($e->getRegionErrors()));
            self::assertSame([11], $e->getFailedRegionIds());
            self::assertSame(11, $e->getFailedCallIds()[1]->regionId);
        }

        $records = $this->failureRecords();
        self::assertCount(1, $records);
        self::assertSame(11, $records[0]->context['regionId']);
        self::assertSame(0, $records[0]->context['subBatchIndex']);
    }

    public function testTwoSubBatchesOfOneRegionAreBothReportedWithoutCollision(): void
    {
        $region = $this->region(7);

        $this->regionCache->method('getByKey')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->store(1));
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn(
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ): GrpcFuture => GrpcFuture::fromWaiter(static function (): Message {
                throw new GrpcException('connection refused', 14);
            }),
        );

        // 600 keys in one region -> 2 sub-batches at MAX_BATCH_LIMIT = 512.
        $keys = array_map(static fn(int $i): string => sprintf('key-%05d', $i), range(0, 599));

        try {
            $this->batch()->batchGet($keys, $this->retryExecutor());
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            self::assertCount(2, $e->getRegionErrors());
            self::assertSame([7], $e->getFailedRegionIds(), 'one region, reported once');
            $callIds = $e->getFailedCallIds();
            self::assertSame([0, 1], array_keys($callIds));
            self::assertSame([7, 7], array_map(
                static fn(BatchCallId $callId): int => $callId->regionId,
                $callIds,
            ));
            self::assertSame([0, 1], array_map(
                static fn(BatchCallId $callId): int => $callId->subBatchIndex,
                $callIds,
            ));
        }

        $records = $this->failureRecords();
        self::assertCount(2, $records);
        self::assertSame(0, $records[0]->context['subBatchIndex']);
        self::assertSame(1, $records[1]->context['subBatchIndex']);
    }

    private function batch(): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            $this->logger,
        );
    }

    private function retryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            $this->logger,
            1,
        );
    }

    private function region(
        int $regionId,
        int $leaderStoreId = 1,
        string $startKey = '',
        string $endKey = '',
    ): RegionInfo {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: $regionId,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function store(int $storeId): Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress('tikv1:20160');

        return $store;
    }

    /**
     * @return list<LogRecord>
     */
    private function failureRecords(): array
    {
        $records = [];
        foreach ($this->logs->getRecords() as $record) {
            if ($record->message === 'Region failed') {
                $records[] = $record;
            }
        }

        return $records;
    }
}
