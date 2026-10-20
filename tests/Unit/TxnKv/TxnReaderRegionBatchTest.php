<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\RegionScanBatch;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for the bounded region list a transactional scan asks PD
 * for (issue #282).
 *
 * `TxnReader::scan()` used to ask PD for **every** region covering the
 * requested range (`limit = 0`), even though the scan is capped at a few
 * thousand rows and normally stops after the first region or two. The tests
 * below drive the reader against a PD double that answers like PD does (the
 * regions intersecting the range, up to the requested limit) and assert that
 * the limit is positive and bounded, that a scan satisfied by the first
 * region asks PD exactly once, and that a scan spanning more regions than
 * one batch asks PD again with an advancing cursor.
 */
final class TxnReaderRegionBatchTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    /** Number of regions in the test layout — more than one batch holds. */
    private const REGION_COUNT = 40;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;

    /** @var list<array{0: string, 1: string, 2: int}> the recorded scanRegions() calls */
    private array $scanRegionsCalls = [];

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->scanRegionsCalls = [];

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);

        $this->stubPdScanRegions();
        $this->stubRegionCache();
        $this->stubScan();
    }

    /**
     * The region layout: region $i covers ["k%03d", "k%03d+1") and holds
     * exactly one key. The key's suffix starts with a byte below "1", so it
     * sorts before the next region's boundary bytewise.
     *
     * @return list<RegionInfo>
     */
    private function regions(): array
    {
        $regions = [];
        for ($index = 0; $index < self::REGION_COUNT; $index++) {
            $regions[] = new RegionInfo(
                regionId: $index + 1,
                leaderPeerId: 1,
                leaderStoreId: 1,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: $this->boundary($index),
                endKey: $this->boundary($index + 1),
            );
        }

        return $regions;
    }

    private function boundary(int $index): string
    {
        return $index >= self::REGION_COUNT ? '' : sprintf('k%03d', $index);
    }

    private function keyOf(int $index): string
    {
        return sprintf('k%03d-00', $index);
    }

    /**
     * Answer `scanRegions()` the way PD does: the regions intersecting the
     * half-open range, in ascending order, up to the requested limit.
     */
    private function stubPdScanRegions(): void
    {
        $regions = $this->regions();

        $this->pdClient->method('scanRegions')->willReturnCallback(
            function (string $startKey, string $endKey, int $limit = 0) use ($regions): array {
                $this->scanRegionsCalls[] = [$startKey, $endKey, $limit];

                $matches = [];
                foreach ($regions as $region) {
                    if (!$this->intersects($region, $startKey, $endKey)) {
                        continue;
                    }

                    $matches[] = $region;
                    if ($limit > 0 && count($matches) >= $limit) {
                        break;
                    }
                }

                return $matches;
            },
        );
    }

    /**
     * Whether the region [start, end) intersects the requested range.
     */
    private function intersects(RegionInfo $region, string $startKey, string $endKey): bool
    {
        if ($region->startKey === $region->endKey) {
            return false;
        }

        if ($region->endKey !== '' && KeyOrder::lte($region->endKey, $startKey)) {
            return false;
        }

        return $endKey === '' || KeyOrder::lt($region->startKey, $endKey);
    }

    /**
     * A cold region cache: it resolves a key to the region that contains it.
     */
    private function stubRegionCache(): void
    {
        $regions = $this->regions();

        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($regions): ?RegionInfo {
                foreach ($regions as $region) {
                    if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                        return $region;
                    }
                }

                return null;
            },
        );
        $this->regionCache->method('put');
    }

    /**
     * Emulate a TiKV scan: answer with the key of the region that owns the
     * requested range.
     */
    private function stubScan(): void
    {
        $regions = $this->regions();

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                ScanRequest $request,
            ) use ($regions): ScanResponse {
                $response = new ScanResponse();

                foreach ($regions as $index => $region) {
                    if (!KeyOrder::inRange($request->getStartKey(), $region->startKey, $region->endKey)) {
                        continue;
                    }

                    $pair = new KvPair();
                    $pair->setKey($this->keyOf($index));
                    $pair->setValue('v-' . $this->keyOf($index));
                    $response->setPairs([...$response->getPairs(), $pair]);
                    break;
                }

                return $response;
            },
        );
    }

    private function transaction(): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-region-batch',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
        );
    }

    public function testScanAsksPdForABoundedRegionLimit(): void
    {
        $this->transaction()->scan('k000', '', 100);

        $this->assertNotSame([], $this->scanRegionsCalls);

        foreach ($this->scanRegionsCalls as [, $endKey, $limit]) {
            $this->assertSame('', $endKey);
            // A positive, bounded limit — never the "every region" 0.
            $this->assertGreaterThan(0, $limit);
            $this->assertLessThanOrEqual(RegionScanBatch::MAX_REGIONS, $limit);
        }

        $this->assertSame('k000', $this->scanRegionsCalls[0][0]);
        $this->assertSame(RegionScanBatch::forRowBudget(100), $this->scanRegionsCalls[0][2]);
    }

    public function testScanSatisfiedByTheFirstRegionAsksPdExactlyOnce(): void
    {
        $result = $this->transaction()->scan('k000', '', 1);

        $this->assertSame([$this->keyOf(0)], array_column($result, 'key'));
        $this->assertCount(1, $this->scanRegionsCalls);
    }

    public function testScanSpanningMoreRegionsThanOneBatchLoadsTheNextBatchAtTheCursor(): void
    {
        $result = $this->transaction()->scan('k000', '', 1000);

        $expected = [];
        for ($index = 0; $index < self::REGION_COUNT; $index++) {
            $expected[] = $this->keyOf($index);
        }
        $this->assertSame($expected, array_column($result, 'key'));

        // The layout spans more regions than one batch holds, so PD is asked
        // again — with the last region's end key as the new cursor.
        $this->assertCount(2, $this->scanRegionsCalls);

        [$firstStart, $firstEnd, $firstLimit] = $this->scanRegionsCalls[0];
        $this->assertSame('k000', $firstStart);
        $this->assertSame('', $firstEnd);
        $this->assertSame(RegionScanBatch::MAX_REGIONS, $firstLimit);

        [$secondStart, $secondEnd, $secondLimit] = $this->scanRegionsCalls[1];
        $this->assertSame($this->boundary(RegionScanBatch::MAX_REGIONS), $secondStart);
        $this->assertSame('', $secondEnd);
        $this->assertSame(RegionScanBatch::MAX_REGIONS, $secondLimit);
    }
}
