<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
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
 * Regression tests for the bounded region list a scan asks PD for
 * (issue #282).
 *
 * A scan used to ask PD for **every** region covering the requested range
 * (`limit = 0`), even though it is capped at a few thousand rows and
 * normally stops after the first region or two. On a large cluster that
 * answer is tens of megabytes — enough to exceed the gRPC receive limit —
 * and it is paid for a query that reads one region.
 *
 * The tests below drive the scanner against a PD double that answers like
 * PD does (the regions intersecting the range, up to the requested limit)
 * and assert three things: the limit passed to PD is positive and bounded,
 * a scan satisfied by the first region asks PD exactly once, and a scan
 * that spans more regions than one batch asks PD again with an advancing
 * cursor and returns the complete, ordered result set.
 */
final class RawKvScannerRegionBatchTest extends TestCase
{
    /** Number of regions in the test layout — more than one batch holds. */
    private const REGION_COUNT = 40;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;

    /** @var list<array{0: string, 1: string, 2: int}> the recorded scanRegions() calls */
    private array $scanRegionsCalls = [];

    /**
     * Per-call region answers, overriding the generic one. A test seam for
     * the layouts PD does not produce on its own (a merged region).
     *
     * @var list<list<RegionInfo>>|null
     */
    private ?array $regionBatchAnswers = null;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->scanRegionsCalls = [];
        $this->regionBatchAnswers = null;

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

                if ($this->regionBatchAnswers !== null) {
                    return $this->regionBatchAnswers[count($this->scanRegionsCalls) - 1] ?? [];
                }

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
     * A cold region cache: it holds no chain (so every page asks PD) and
     * resolves a key to the region that contains it.
     */
    private function stubRegionCache(): void
    {
        $regions = $this->regions();

        $this->regionCache->method('getRegionsInRange')->willReturn([]);
        $this->regionCache->method('put');
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
    }

    /**
     * Emulate a TiKV raw scan: answer with the key of the region that owns
     * the requested range, honouring the request's limit.
     */
    private function stubScan(): void
    {
        $regions = $this->regions();

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                RawScanRequest $request,
            ) use ($regions): RawScanResponse {
                $response = new RawScanResponse();

                // A forward scan resolves on its start key, a reverse scan on
                // its end key (see RawKvScanner::executeReverseScanForSubRange()).
                $resolutionKey = $request->getReverse() ? $request->getEndKey() : $request->getStartKey();

                foreach ($regions as $index => $region) {
                    if (!KeyOrder::inRange($resolutionKey, $region->startKey, $region->endKey)) {
                        continue;
                    }

                    $pair = new KvPair();
                    $pair->setKey($this->keyOf($index));
                    $pair->setValue('v-' . $this->keyOf($index));
                    $response->setKvs([...$response->getKvs(), $pair]);
                    break;
                }

                return $response;
            },
        );
    }

    private function scanner(): RawKvScanner
    {
        return new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 60000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );
    }

    public function testScanAsksPdForABoundedRegionLimit(): void
    {
        $this->scanner()->scan('k000', '', 100, false);

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

    public function testScanNarrowLimitAsksForNoMoreRegionsThanItCanUse(): void
    {
        $this->scanner()->scan('k000', '', 3, false);

        $this->assertCount(1, $this->scanRegionsCalls);
        $this->assertSame(3, $this->scanRegionsCalls[0][2]);
    }

    public function testScanSatisfiedByTheFirstRegionAsksPdExactlyOnce(): void
    {
        $result = $this->scanner()->scan('k000', '', 1, false);

        $this->assertSame([$this->keyOf(0)], array_column($result, 'key'));
        $this->assertCount(1, $this->scanRegionsCalls);
    }

    public function testScanSpanningMoreRegionsThanOneBatchLoadsTheNextBatchAtTheCursor(): void
    {
        $result = $this->scanner()->scan('k000', '', 1000, false);

        // Every region of the layout was read, in order.
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

    public function testReverseScanAsksPdForABoundedRegionLimit(): void
    {
        $result = $this->scanner()->reverseScan('k040', 'k000', 1, false);

        $this->assertSame([$this->keyOf(self::REGION_COUNT - 1)], array_column($result, 'key'));
        $this->assertNotSame([], $this->scanRegionsCalls);

        foreach ($this->scanRegionsCalls as [, , $limit]) {
            $this->assertGreaterThan(0, $limit);
            $this->assertLessThanOrEqual(RegionScanBatch::MAX_REGIONS, $limit);
        }
    }

    /**
     * The reverse path is the one that deviates most from the forward path:
     * it collects the whole chain of the range in bounded batches and then
     * walks it downwards, so a wrong cursor would silently drop or duplicate
     * rows. The range below spans more regions than one batch holds, so the
     * chain is loaded in several PD calls.
     */
    public function testReverseScanSpanningMoreRegionsThanOneBatchReturnsTheCompleteOrderedResultSet(): void
    {
        $result = $this->scanner()->reverseScan('k040', 'k000', 1000, false);

        // Every region of the layout was read, in descending order.
        $expected = [];
        for ($index = self::REGION_COUNT - 1; $index >= 0; $index--) {
            $expected[] = $this->keyOf($index);
        }

        $keys = array_column($result, 'key');
        $this->assertSame($expected, $keys);
        $this->assertCount(count($expected), array_unique($keys), 'the reverse scan returned a row twice');

        // Every PD response is bounded — never the "every region" 0.
        foreach ($this->scanRegionsCalls as [, , $limit]) {
            $this->assertGreaterThan(0, $limit);
            $this->assertLessThanOrEqual(RegionScanBatch::MAX_REGIONS, $limit);
        }

        // The layout spans more regions than one batch holds, so PD is asked
        // again — with the last region's end key as the new cursor.
        $this->assertGreaterThan(1, count($this->scanRegionsCalls));
        $this->assertSame($this->boundary(0), $this->scanRegionsCalls[0][0]);
        $this->assertSame($this->boundary(RegionScanBatch::MAX_REGIONS), $this->scanRegionsCalls[1][0]);
    }

    public function testRegionMergedBetweenTwoBatchesIsNotReadTwice(): void
    {
        $regions = $this->regions();

        // The first batch enumerates the regions up to "k032". A merge then
        // produces one region that starts *below* the cursor — the part
        // below it was read by the first batch already and must not be read
        // a second time.
        $merged = new RegionInfo(
            regionId: 1000,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $this->boundary(31),
            endKey: $this->boundary(33),
        );

        $this->regionBatchAnswers = [
            array_slice($regions, 0, RegionScanBatch::MAX_REGIONS),
            [$merged],
        ];

        $result = $this->scanner()->scan('k000', '', 1000, false);

        $expected = [];
        for ($index = 0; $index <= RegionScanBatch::MAX_REGIONS; $index++) {
            $expected[] = $this->keyOf($index);
        }

        $this->assertSame($expected, array_column($result, 'key'));
        $this->assertCount(2, $this->scanRegionsCalls);
        $this->assertSame($this->boundary(RegionScanBatch::MAX_REGIONS), $this->scanRegionsCalls[1][0]);
    }
}
