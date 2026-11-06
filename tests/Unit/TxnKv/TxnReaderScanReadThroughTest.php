<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #425: a transactional scan reads the region cache first and asks PD
 * only for the gap. The test drives `TxnReader::scan()` directly, because that
 * is the path the issue names: it used to call `PdClient::scanRegions()` for
 * every page even when the cache already held the whole chain.
 */
final class TxnReaderScanReadThroughTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private RegionCache $cache;

    private InMemoryMetrics $metrics;

    private RegionResolver $regionResolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->cache = new RegionCache();
        $this->metrics = new InMemoryMetrics();
        $this->regionResolver = new RegionResolver($this->pdClient, $this->cache);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
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

    private function reader(): TxnReader
    {
        return new TxnReader(
            startTs: 1000,
            grpc: $this->grpc,
            pdClient: $this->pdClient,
            regionResolver: $this->regionResolver,
            timeoutConfig: new TimeoutConfig(),
            lockResolver: new LockResolver(
                $this->grpc,
                $this->regionResolver,
                $this->cache,
                $this->pdClient,
                1000,
            ),
            regionCache: $this->cache,
            metrics: $this->metrics,
        );
    }

    private function executor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            60000,
            $this->cache,
            $this->grpc,
            $this->regionResolver,
            new NullLogger(),
        );
    }

    /**
     * Answer every KvScan with the key of the region that owns its start key.
     */
    private function stubScan(): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                ScanRequest $request,
            ): ScanResponse {
                $response = new ScanResponse();
                $pair = new KvPair();
                $pair->setKey($request->getStartKey() . '-row');
                $pair->setValue('v');
                $response->setPairs([$pair]);

                return $response;
            },
        );
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    private function scanAll(): array
    {
        return $this->reader()->scan(
            'a',
            'z',
            10,
            new TransactionState(),
            $this->executor(),
            static fn (): ?BackoffType => null,
        );
    }

    public function testWarmCacheServesTheScanWithoutPd(): void
    {
        $this->cache->put($this->region(1, 'a', 'z'));
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->stubScan();

        $result = $this->scanAll();

        $this->assertSame(['a-row'], array_column($result, 'key'));
        $this->assertSame(1, $this->metrics->getCacheHits('txn_scan'));
        $this->assertSame(0, $this->metrics->getCacheMisses('txn_scan'));
    }

    public function testColdCacheQueriesPdOnlyForTheGapAndCachesIt(): void
    {
        $this->cache->put($this->region(1, 'a', 'm'));
        $tail = [$this->region(2, 'm', 'z')];

        $calls = [];
        $this->pdClient->method('scanRegions')->willReturnCallback(
            function (string $startKey, string $endKey, int $limit) use (&$calls, $tail): array {
                $calls[] = [$startKey, $endKey, $limit];

                return $tail;
            },
        );
        $this->stubScan();

        $result = $this->scanAll();

        // Rows from the cached region and from the fetched tail.
        $this->assertSame(['a-row', 'm-row'], array_column($result, 'key'));
        // PD was asked only for the uncovered window, starting at the gap.
        $this->assertSame([['m', 'z', 10]], $calls);
        $this->assertNotNull($this->cache->getById(2));
        $this->assertSame(1, $this->metrics->getCacheHits('txn_scan'));
        $this->assertSame(1, $this->metrics->getCacheMisses('txn_scan'));

        // The next scan over the same range is served from the cache.
        $this->scanAll();
        $this->assertCount(1, $calls);
        $this->assertSame(2, $this->metrics->getCacheHits('txn_scan'));
    }

    public function testSplitBetweenPopulationAndUseIsRefetchedAndCached(): void
    {
        // The cache was populated before the split; the stale entry was
        // dropped when the client saw the EpochNotMatch.
        $this->cache->put($this->region(1, 'a', 'z'));
        $this->cache->invalidate(1);

        $split = [$this->region(2, 'a', 'm'), $this->region(3, 'm', 'z')];
        $calls = 0;
        $this->pdClient->method('scanRegions')->willReturnCallback(
            function () use (&$calls, $split): array {
                $calls++;

                return $split;
            },
        );
        $this->stubScan();

        $result = $this->scanAll();

        $this->assertSame(['a-row', 'm-row'], array_column($result, 'key'));
        $this->assertSame(1, $calls);
        $this->assertNotNull($this->cache->getById(2));
        $this->assertNotNull($this->cache->getById(3));

        // The split layout is read from the cache from now on.
        $this->scanAll();
        $this->assertSame(1, $calls);
    }
}
