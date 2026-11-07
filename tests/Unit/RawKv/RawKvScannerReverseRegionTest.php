<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\RegionScanBatch;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Regression tests for the bounded, top-down region enumeration of a reverse
 * scan (issue #198, RAW-13).
 *
 * A reverse scan used to collect every region of the remaining range before
 * walking it from the top, so a narrow `reverseScan()` over a wide keyspace
 * paid for thousands of region descriptors it never read. The scanner now
 * resolves one region at a time from the upper bound: the region cache
 * answers the common case and a miss uses `GetRegion`/`GetPrevRegion`. A
 * scan satisfied by the first batch never resolves the rest of the range.
 *
 * The tests below use a layout of {@see self::REGION_COUNT} single-row
 * regions. A cold-cache PD double answers like PD does (the region
 * containing a key, and the adjacent region below a boundary); a warm-cache
 * test runs the real `RegionCache` and proves no PD lookup happens at all.
 */
final class RawKvScannerReverseRegionTest extends TestCase
{
    /** Number of regions in the layout — more than one row batch holds. */
    private const REGION_COUNT = 40;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->method('call')->willReturnCallback(
            fn(string $address, string $service, string $method, RawScanRequest $request): RawScanResponse
                => $this->scanResponse($request),
        );
    }

    /**
     * Region $i covers ["k%03d", "k%03d+1") and holds one key.
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

    /** An exclusive upper bound above the last region's only key. */
    private function upperBound(): string
    {
        return sprintf('k%03d', self::REGION_COUNT);
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        return $store;
    }

    /**
     * A cold cache: every lookup misses, so the scanner must resolve the
     * region through PD.
     */
    private function stubColdCache(): void
    {
        $this->regionCache->method('getRegionBelow')->willReturn(null);
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
    }

    /**
     * Answer `GetRegion` like PD: the region containing the key, or the
     * region starting at the key when the key sits on a boundary (that
     * region lies above a reverse scan's upper bound).
     */
    private function stubPdRegionLookups(): void
    {
        $regions = $this->regions();

        $this->pdClient->method('getRegion')->willReturnCallback(
            static function (string $key) use ($regions): RegionInfo {
                foreach ($regions as $region) {
                    if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                        return $region;
                    }
                }
                foreach ($regions as $region) {
                    if ($region->startKey === $key) {
                        return $region;
                    }
                }

                throw new \RuntimeException('no region for key ' . bin2hex($key));
            },
        );

        // PD's GetPrevRegion returns the adjacent region whose end key is the
        // start key of the region containing the key.
        $this->pdClient->method('getPrevRegion')->willReturnCallback(
            static function (string $key) use ($regions): ?RegionInfo {
                foreach ($regions as $region) {
                    if ($region->endKey !== '' && KeyOrder::eq($region->endKey, $key)) {
                        return $region;
                    }
                }

                return null;
            },
        );
    }

    /**
     * Emulate a TiKV raw scan: one key per region, honouring the wire range.
     */
    private function scanResponse(RawScanRequest $request): RawScanResponse
    {
        $response = new RawScanResponse();
        if (!$request->getReverse()) {
            return $response;
        }

        $lower = $request->getEndKey();
        $upper = $request->getStartKey();

        foreach ($this->regions() as $index => $region) {
            if (!KeyOrder::inRange($lower, $region->startKey, $region->endKey)) {
                continue;
            }

            $key = $this->keyOf($index);
            if (KeyOrder::gte($key, $lower) && ($upper === '' || KeyOrder::lt($key, $upper))) {
                $pair = new KvPair();
                $pair->setKey($key);
                $pair->setValue('v-' . $key);
                $response->setKvs([$pair]);
            }

            break;
        }

        return $response;
    }

    private function scanner(): RawKvScanner
    {
        return new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );
    }

    public function testNarrowReverseScanResolvesOnlyTheTopRegion(): void
    {
        $this->stubColdCache();
        $this->stubPdRegionLookups();

        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->never())->method('getPrevRegion');

        $result = $this->scanner()->reverseScan($this->upperBound(), $this->boundary(0), 1, false);

        $this->assertSame([$this->keyOf(self::REGION_COUNT - 1)], array_column($result, 'key'));
    }

    public function testReverseScanSatisfiedByTheFirstRegionDoesNotResolveTheNextOne(): void
    {
        $this->stubColdCache();
        $this->stubPdRegionLookups();

        // Two regions, each holding one row: a limit of two needs the top
        // region and exactly one step down, never the whole range.
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->exactly(1))->method('getPrevRegion');

        $result = $this->scanner()->reverseScan($this->upperBound(), $this->boundary(0), 2, false);

        $this->assertSame(
            [$this->keyOf(self::REGION_COUNT - 1), $this->keyOf(self::REGION_COUNT - 2)],
            array_column($result, 'key'),
        );
    }

    /**
     * The row budget is larger than one region batch (32), so the scan must
     * continue into the next batch and return the complete descending set
     * without enumerating the range up front.
     */
    public function testReverseScanContinuesIntoTheNextBatchAndReturnsTheCompleteOrderedResultSet(): void
    {
        $this->stubColdCache();
        $this->stubPdRegionLookups();

        $this->pdClient->expects($this->never())->method('scanRegions');
        // One step down per region below the top region.
        $this->pdClient->expects($this->exactly(self::REGION_COUNT - 1))->method('getPrevRegion');

        $result = $this->scanner()->reverseScan(
            $this->upperBound(),
            $this->boundary(0),
            self::REGION_COUNT,
            false,
        );

        $expected = [];
        for ($index = self::REGION_COUNT - 1; $index >= 0; $index--) {
            $expected[] = $this->keyOf($index);
        }

        $this->assertSame($expected, array_column($result, 'key'));
        $this->assertCount(
            count($expected),
            array_unique(array_column($result, 'key')),
            'the reverse scan returned a row twice',
        );
    }

    public function testWarmCacheReverseScanResolvesNoRegionThroughPd(): void
    {
        $cache = new RegionCache();
        foreach ($this->regions() as $region) {
            $cache->put($region);
        }

        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->never())->method('getRegion');
        $this->pdClient->expects($this->never())->method('getPrevRegion');

        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            new RegionResolver($this->pdClient, $cache),
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $cache,
            logger: new NullLogger(),
        );

        $result = $scanner->reverseScan($this->upperBound(), $this->boundary(0), 3, false);

        $this->assertSame(
            [
                $this->keyOf(self::REGION_COUNT - 1),
                $this->keyOf(self::REGION_COUNT - 2),
                $this->keyOf(self::REGION_COUNT - 3),
            ],
            array_column($result, 'key'),
        );
    }

    public function testRegionBatchSizeForASmallReverseLimitIsTheRowBudget(): void
    {
        // The reverse walk asks for at most the rows it can return, capped by
        // the shared region batch ceiling.
        $this->assertSame(1, RegionScanBatch::forRowBudget(1));
        $this->assertSame(5, RegionScanBatch::forRowBudget(5));
        $this->assertSame(RegionScanBatch::MAX_REGIONS, RegionScanBatch::forRowBudget(self::REGION_COUNT));
    }
}
