<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RawKvScannerTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private RawKvScanner $scanner;

    private function defaultRegion(
        string $startKey = '',
        string $endKey = '',
        int $regionId = 1,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function defaultStore(int $storeId = 1): Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress('tikv' . $storeId . ':20160');
        return $store;
    }

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
        );
    }

    /**
     * Simulate the region cache being populated with the given regions
     * (scan() caches the scanRegions() result), so the retried closure
     * resolves regions through the cache like in production.
     *
     * @param RegionInfo[] $regions regions sorted by startKey
     */
    private function stubRegionLookup(array $regions): void
    {
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): ?RegionInfo => self::findRegion($regions, $key),
        );
    }

    /**
     * @param RegionInfo[] $regions
     */
    private static function findRegion(array $regions, string $key): ?RegionInfo
    {
        foreach ($regions as $region) {
            if ($region->startKey <= $key && ($region->endKey === '' || $key < $region->endKey)) {
                return $region;
            }
        }

        return null;
    }

    // ========================================================================
    // scan() – multi-region correctness
    // ========================================================================

    public function testScanSingleRegionReturnsAllPairs(): void
    {
        $region = $this->defaultRegion('a', 'z');
        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair1 = new KvPair();
        $pair1->setKey('key1');
        $pair1->setValue('val1');

        $pair2 = new KvPair();
        $pair2->setKey('key2');
        $pair2->setValue('val2');

        $response = new RawScanResponse();
        $response->setKvs([$pair1, $pair2]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(2, $result);
        $this->assertSame('key1', $result[0]['key']);
        $this->assertSame('val1', $result[0]['value']);
        $this->assertSame('key2', $result[1]['key']);
        $this->assertSame('val2', $result[1]['value']);
    }

    public function testScanMultipleRegionsMergesResultsInOrder(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairR1a = new KvPair();
        $pairR1a->setKey('key_a');
        $pairR1a->setValue('val_a');

        $pairR1b = new KvPair();
        $pairR1b->setKey('key_b');
        $pairR1b->setValue('val_b');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pairR1a, $pairR1b]);

        $pairR2a = new KvPair();
        $pairR2a->setKey('key_m');
        $pairR2a->setValue('val_m');

        $pairR2b = new KvPair();
        $pairR2b->setKey('key_n');
        $pairR2b->setValue('val_n');

        $response2 = new RawScanResponse();
        $response2->setKvs([$pairR2a, $pairR2b]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2);

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(4, $result);
        $this->assertSame('key_a', $result[0]['key']);
        $this->assertSame('key_b', $result[1]['key']);
        $this->assertSame('key_m', $result[2]['key']);
        $this->assertSame('key_n', $result[3]['key']);
    }

    public function testScanMultipleRegionsRespectsLimit(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs1 = [];
        for ($i = 0; $i < 3; $i++) {
            $pair = new KvPair();
            $pair->setKey('r1_key' . $i);
            $pair->setValue('r1_val' . $i);
            $pairs1[] = $pair;
        }
        $response1 = new RawScanResponse();
        $response1->setKvs($pairs1);

        $pairs2 = [];
        for ($i = 0; $i < 5; $i++) {
            $pair = new KvPair();
            $pair->setKey('r2_key' . $i);
            $pair->setValue('r2_val' . $i);
            $pairs2[] = $pair;
        }
        $response2 = new RawScanResponse();
        $response2->setKvs($pairs2);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2);

        $result = $this->scanner->scan('a', 'z', 3, false);

        $this->assertCount(3, $result);
        $this->assertSame('r1_key0', $result[0]['key']);
        $this->assertSame('r1_key1', $result[1]['key']);
        $this->assertSame('r1_key2', $result[2]['key']);
    }

    public function testScanMultipleRegionsAggregatesAcrossRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs1 = [];
        for ($i = 0; $i < 2; $i++) {
            $pair = new KvPair();
            $pair->setKey('r1_key' . $i);
            $pair->setValue('r1_val' . $i);
            $pairs1[] = $pair;
        }
        $response1 = new RawScanResponse();
        $response1->setKvs($pairs1);

        $pairs2 = [];
        for ($i = 0; $i < 3; $i++) {
            $pair = new KvPair();
            $pair->setKey('r2_key' . $i);
            $pair->setValue('r2_val' . $i);
            $pairs2[] = $pair;
        }
        $response2 = new RawScanResponse();
        $response2->setKvs($pairs2);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2);

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(5, $result);
        $this->assertSame('r1_key0', $result[0]['key']);
        $this->assertSame('r1_key1', $result[1]['key']);
        $this->assertSame('r2_key0', $result[2]['key']);
        $this->assertSame('r2_key1', $result[3]['key']);
        $this->assertSame('r2_key2', $result[4]['key']);
    }

    public function testScanEmptyRegionIsSkipped(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $emptyResponse = new RawScanResponse();
        $emptyResponse->setKvs([]);

        $pair = new KvPair();
        $pair->setKey('key_from_r2');
        $pair->setValue('val_from_r2');

        $response2 = new RawScanResponse();
        $response2->setKvs([$pair]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($emptyResponse, $response2);

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('key_from_r2', $result[0]['key']);
    }

    public function testScanKeyOnlyReturnsNullValues(): void
    {
        $region = $this->defaultRegion('a', 'z');
        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scan('a', 'z', 100, true);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertNull($result[0]['value']);
    }

    // ========================================================================
    // reverseScan() – multi-region correctness
    // ========================================================================

    public function testReverseScanMultipleRegionsMergesResultsInOrder(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairR2a = new KvPair();
        $pairR2a->setKey('key_y');
        $pairR2a->setValue('val_y');

        $pairR2b = new KvPair();
        $pairR2b->setKey('key_x');
        $pairR2b->setValue('val_x');

        $response2 = new RawScanResponse();
        $response2->setKvs([$pairR2a, $pairR2b]);

        $pairR1a = new KvPair();
        $pairR1a->setKey('key_l');
        $pairR1a->setValue('val_l');

        $pairR1b = new KvPair();
        $pairR1b->setKey('key_k');
        $pairR1b->setValue('val_k');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pairR1a, $pairR1b]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response2, $response1);

        $result = $this->scanner->reverseScan('z', 'a', 100, false);

        $this->assertCount(4, $result);
        $this->assertSame('key_y', $result[0]['key']);
        $this->assertSame('key_x', $result[1]['key']);
        $this->assertSame('key_l', $result[2]['key']);
        $this->assertSame('key_k', $result[3]['key']);
    }

    // ========================================================================
    // scanPrefix() – delegates to scan()
    // ========================================================================

    public function testScanPrefixDelegatesToScanWithCorrectRange(): void
    {
        $region = $this->defaultRegion('prefix', "prefix\xFF");
        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('prefix_key1');
        $pair->setValue('v1');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scanPrefix('prefix', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('prefix_key1', $result[0]['key']);
    }

    // ========================================================================
    // scanLimit validation
    // ========================================================================

    public function testScanLimitZeroReturnsMaxScanLimit(): void
    {
        $region = $this->defaultRegion('a', 'z');
        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs = [];
        for ($i = 0; $i < RawKvScanner::MAX_SCAN_LIMIT; $i++) {
            $pair = new KvPair();
            $pair->setKey('k' . $i);
            $pair->setValue('v' . $i);
            $pairs[] = $pair;
        }

        $response = new RawScanResponse();
        $response->setKvs($pairs);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scan('a', 'z', 0, false);

        $this->assertCount(RawKvScanner::MAX_SCAN_LIMIT, $result);
    }

    public function testScanLimitExceedingMaxThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit (10241) exceeds maximum allowed scan limit of 10240');

        $this->scanner->scan('a', 'z', RawKvScanner::MAX_SCAN_LIMIT + 1, false);
    }

    // ========================================================================
    // scan() – empty end key (unbounded)
    // ========================================================================

    public function testScanWithEmptyEndKeyScansThroughAllRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);
        $region3 = $this->defaultRegion('z', '', regionId: 3);

        $this->stubRegionLookup([$region1, $region2, $region3]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2, $region3]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairR1 = new KvPair();
        $pairR1->setKey('key_a');
        $pairR1->setValue('val_a');

        $pairR2 = new KvPair();
        $pairR2->setKey('key_m');
        $pairR2->setValue('val_m');

        $pairR3 = new KvPair();
        $pairR3->setKey('key_z');
        $pairR3->setValue('val_z');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pairR1]);

        $response2 = new RawScanResponse();
        $response2->setKvs([$pairR2]);

        $response3 = new RawScanResponse();
        $response3->setKvs([$pairR3]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2, $response3);

        // End key '' means unbounded → should scan all regions
        $result = $this->scanner->scan('a', '', 100, false);

        $this->assertCount(3, $result);
        $this->assertSame('key_a', $result[0]['key']);
        $this->assertSame('key_m', $result[1]['key']);
        $this->assertSame('key_z', $result[2]['key']);
    }

    // ========================================================================
    // scan() – limit spanning three regions
    // ========================================================================

    public function testScanLimitSpanningThreeRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);
        $region3 = $this->defaultRegion('z', '', regionId: 3);

        $this->stubRegionLookup([$region1, $region2, $region3]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2, $region3]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs1 = [];
        for ($i = 0; $i < 5; $i++) {
            $pair = new KvPair();
            $pair->setKey("r1_key{$i}");
            $pair->setValue("r1_val{$i}");
            $pairs1[] = $pair;
        }
        $response1 = new RawScanResponse();
        $response1->setKvs($pairs1);

        $pairs2 = [];
        for ($i = 0; $i < 3; $i++) {
            $pair = new KvPair();
            $pair->setKey("r2_key{$i}");
            $pair->setValue("r2_val{$i}");
            $pairs2[] = $pair;
        }
        $response2 = new RawScanResponse();
        $response2->setKvs($pairs2);

        // Region 3: remaining limit is 2, so only return 2 pairs (TiKV would enforce the limit)
        $pairs3 = [];
        for ($i = 0; $i < 2; $i++) {
            $pair = new KvPair();
            $pair->setKey("r3_key{$i}");
            $pair->setValue("r3_val{$i}");
            $pairs3[] = $pair;
        }
        $response3 = new RawScanResponse();
        $response3->setKvs($pairs3);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2, $response3);

        // Limit 10: take 5 from r1, 3 from r2, 2 from r3
        $result = $this->scanner->scan('a', '', 10, false);

        $this->assertCount(10, $result);
        // r1 results
        $this->assertSame('r1_key0', $result[0]['key']);
        $this->assertSame('r1_key4', $result[4]['key']);
        // r2 results
        $this->assertSame('r2_key0', $result[5]['key']);
        $this->assertSame('r2_key2', $result[7]['key']);
        // r3 results (only 2 of 4 in real TiKV, but mock returns exactly 2)
        $this->assertSame('r3_key0', $result[8]['key']);
        $this->assertSame('r3_key1', $result[9]['key']);
    }

    // ========================================================================
    // scan() – non-aligned key range clipping
    // ========================================================================

    public function testScanWithNonAlignedRangeClipsToRegionBoundaries(): void
    {
        $region1 = $this->defaultRegion('b', 'n', regionId: 1);
        $region2 = $this->defaultRegion('n', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // scan from 'a' to 'z' — region1 starts at 'b', so scanStart should be 'b' (region->startKey)
        // region1 ends at 'n', region2 starts at 'n', so second region scanStart = 'n'
        $pairR1 = new KvPair();
        $pairR1->setKey('key_b');
        $pairR1->setValue('val_b');

        $pairR2 = new KvPair();
        $pairR2->setKey('key_n');
        $pairR2->setValue('val_n');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pairR1]);

        $response2 = new RawScanResponse();
        $response2->setKvs([$pairR2]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2);

        // startKey 'a' is before region1->startKey 'b', so scanStart becomes 'b'
        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(2, $result);
        $this->assertSame('key_b', $result[0]['key']);
        $this->assertSame('key_n', $result[1]['key']);
    }

    public function testScanWithStartKeyInsideRegionClipsStartCorrectly(): void
    {
        $region = $this->defaultRegion('a', 'z', regionId: 1);

        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // scan from 'm' to 'z' — startKey 'm' > region->startKey 'a', so scanStart = 'm'
        $pair = new KvPair();
        $pair->setKey('key_m');
        $pair->setValue('val_m');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scan('m', 'z', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('key_m', $result[0]['key']);
    }

    public function testScanWithEndKeyInsideRegionClipsEndCorrectly(): void
    {
        $region = $this->defaultRegion('a', 'z', regionId: 1);

        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // scan from 'a' to 'm' — endKey 'm' < region->endKey 'z', so scanEnd = 'm'
        $pair = new KvPair();
        $pair->setKey('key_a');
        $pair->setValue('val_a');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->scanner->scan('a', 'm', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('key_a', $result[0]['key']);
    }

    // ========================================================================
    // reverseScan() – limit spanning regions
    // ========================================================================

    public function testReverseScanWithLimitSpanningRegions(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs2 = [];
        for ($i = 0; $i < 3; $i++) {
            $pair = new KvPair();
            $pair->setKey("r2_key{$i}");
            $pair->setValue("r2_val{$i}");
            $pairs2[] = $pair;
        }
        $response2 = new RawScanResponse();
        $response2->setKvs($pairs2);

        $pairs1 = [];
        for ($i = 0; $i < 2; $i++) {
            $pair = new KvPair();
            $pair->setKey("r1_key{$i}");
            $pair->setValue("r1_val{$i}");
            $pairs1[] = $pair;
        }
        $response1 = new RawScanResponse();
        $response1->setKvs($pairs1);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response2, $response1);

        // Reverse scan from 'z' to 'a', limit 5: take 3 from r2, 2 from r1
        $result = $this->scanner->reverseScan('z', 'a', 5, false);

        $this->assertCount(5, $result);
        $this->assertSame('r2_key0', $result[0]['key']);
        $this->assertSame('r2_key2', $result[2]['key']);
        $this->assertSame('r1_key0', $result[3]['key']);
        $this->assertSame('r1_key1', $result[4]['key']);
    }

    public function testReverseScanLimitStopsWithinFirstRegion(): void
    {
        $region1 = $this->defaultRegion('a', 'm', regionId: 1);
        $region2 = $this->defaultRegion('m', 'z', regionId: 2);

        $this->stubRegionLookup([$region1, $region2]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // Limit 2, all satisfied from r2 (first region in reverse order).
        // Mock returns only 2 pairs because TiKV would enforce the limit.
        $pairs2 = [];
        for ($i = 0; $i < 2; $i++) {
            $pair = new KvPair();
            $pair->setKey("r2_key{$i}");
            $pair->setValue("r2_val{$i}");
            $pairs2[] = $pair;
        }
        $response2 = new RawScanResponse();
        $response2->setKvs($pairs2);

        $this->grpc->method('call')->willReturn($response2);

        // Limit 2, all satisfied from r2 (first region in reverse order)
        $result = $this->scanner->reverseScan('z', 'a', 2, false);

        $this->assertCount(2, $result);
        $this->assertSame('r2_key0', $result[0]['key']);
        $this->assertSame('r2_key1', $result[1]['key']);
    }

    // ========================================================================
    // scanPrefix() – all-0xFF prefix
    // ========================================================================

    public function testScanPrefixWithAllFFPrefixProducesEmptyEndKey(): void
    {
        // All-0xFF prefix → calculatePrefixEndKey returns ''
        // This exercises the scan('prefix', '', ...) path with unbounded end key
        $region = $this->defaultRegion('a', '', regionId: 1);

        $this->stubRegionLookup([$region]);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('some_key');
        $pair->setValue('some_val');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        // Prefix consisting entirely of 0xFF bytes → endKey becomes ''
        $result = $this->scanner->scanPrefix("\xff\xff\xff", 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('some_key', $result[0]['key']);
    }

    // ========================================================================
    // scan() – region re-resolution inside the retried closure (issue #267)
    // ========================================================================

    public function testScanRetriesOnNotLeaderAndResolvesFreshRegion(): void
    {
        $oldRegion = $this->defaultRegion('a', 'z', regionId: 1);
        $newRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 30,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
        );

        // The cache serves the old leader once; after the retry executor
        // switches the leader it serves the region with the new leader.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($oldRegion, $newRegion);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->once())->method('switchLeader')->willReturn(true);

        $this->pdClient->method('scanRegions')->willReturn([$oldRegion]);
        $this->pdClient->method('getStore')->willReturnCallback(
            fn(int $storeId): Store => $this->defaultStore($storeId),
        );

        $leader = new Peer();
        $leader->setId(30);
        $leader->setStoreId(2);
        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);
        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');
        $cleanResponse = new RawScanResponse();
        $cleanResponse->setKvs([$pair]);

        $addresses = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (string $address) use (
                &$callCount,
                &$addresses,
                $error,
                $cleanResponse,
            ): Message {
                $callCount++;
                $addresses[] = $address;

                return $callCount === 1 ? $this->responseWithRegionError($error) : $cleanResponse;
            },
        );

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame(2, $callCount);
        // The retry must target the NEW leader's store: proof that the
        // closure re-resolved the region after the leader switch.
        $this->assertSame(['tikv1:20160', 'tikv2:20160'], $addresses);
    }

    public function testScanRetriesOnEpochNotMatchWithNarrowerRange(): void
    {
        $preSplit = $this->defaultRegion('a', 'z', regionId: 1);
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        // The cache serves the stale region for the first resolution and the
        // retry executor's invalidation lookup, then misses so the retry
        // falls back to PD for the post-split region.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        /** @var list<Message> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
                $pair,
            ): Message {
                $callCount++;
                $capturedRequests[] = $request;

                if ($callCount === 1) {
                    return $this->responseWithRegionError($error);
                }

                // The continuation over [k,z) returns no keys.
                $response = new RawScanResponse();
                /** @var RawScanRequest $request */
                if ($request->getStartKey() !== 'k') {
                    $response->setKvs([$pair]);
                }

                return $response;
            },
        );

        $result = $this->scanner->scan('a', 'z', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertCount(3, $capturedRequests);

        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[1]);
        $secondRequest = $capturedRequests[1];
        // The retry must use the post-split region and re-clip the range.
        $context = $secondRequest->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(2, $regionId);
        $this->assertSame('a', $secondRequest->getStartKey());
        $this->assertSame('k', $secondRequest->getEndKey());

        // A retry clipped to the fresh region must not silently drop the
        // remainder: the rest of the range is scanned afterwards.
        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[2]);
        $thirdRequest = $capturedRequests[2];
        $context = $thirdRequest->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('k', $thirdRequest->getStartKey());
        $this->assertSame('z', $thirdRequest->getEndKey());
    }

    public function testScanContinuesAfterSplitToCoverRemainder(): void
    {
        // The outer region enumeration is stale (it ran before the split):
        // the first attempt against the stale [a,z) fails with
        // EpochNotMatch, the retry resolves the post-split [a,k), and the
        // continuation must then scan [k,z) so keys of the original range
        // are not silently dropped.
        $preSplit = $this->defaultRegion('a', 'z', regionId: 1);
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');

        $k1 = new KvPair();
        $k1->setKey('k1');
        $k1->setValue('v1');
        $k2 = new KvPair();
        $k2->setKey('k2');
        $k2->setValue('v2');
        $k3 = new KvPair();
        $k3->setKey('k3');
        $k3->setValue('v3');

        /** @var list<Message> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
                $k1,
                $k2,
                $k3,
            ): Message {
                $callCount++;
                $capturedRequests[] = $request;

                if ($callCount === 1) {
                    return $this->responseWithRegionError($error);
                }

                $response = new RawScanResponse();
                /** @var RawScanRequest $request */
                if ($request->getStartKey() === 'k') {
                    $response->setKvs([$k3]);
                } else {
                    $response->setKvs([$k1, $k2]);
                }

                return $response;
            },
        );

        $result = $this->scanner->scan('a', 'z', 100, false);

        // All three keys of the original range are returned, in order.
        $this->assertSame(['k1', 'k2', 'k3'], array_column($result, 'key'));
        $this->assertCount(3, $capturedRequests);

        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[2]);
        $continuation = $capturedRequests[2];
        $context = $continuation->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('k', $continuation->getStartKey());
        $this->assertSame('z', $continuation->getEndKey());
    }

    public function testReverseScanRetriesOnEpochNotMatchAndClipsStartKey(): void
    {
        $preSplit = $this->defaultRegion('a', 'z', regionId: 1);
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        // The cache serves the stale region for the first resolution and the
        // retry executor's invalidation lookup, then misses so the retry
        // falls back to PD for the post-split region.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        /** @var list<Message> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
                $pair,
            ): Message {
                $callCount++;
                $capturedRequests[] = $request;

                if ($callCount === 1) {
                    return $this->responseWithRegionError($error);
                }

                // Only the first (clipped) reverse batch returns keys; the
                // higher remainder [k,z) is scanned but empty here.
                $response = new RawScanResponse();
                /** @var RawScanRequest $request */
                if ($request->getStartKey() !== 'z') {
                    $response->setKvs([$pair]);
                }

                return $response;
            },
        );

        $result = $this->scanner->reverseScan('z', 'a', 100, false);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertCount(3, $capturedRequests);

        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[1]);
        $secondRequest = $capturedRequests[1];
        // Reverse scans resolve the region on the range end ('a') and clip
        // the wire start (upper) key down to the post-split region's end.
        $context = $secondRequest->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(2, $regionId);
        $this->assertSame('k', $secondRequest->getStartKey());
        $this->assertSame('a', $secondRequest->getEndKey());
        $this->assertTrue($secondRequest->getReverse());

        // The remainder [k,z) must still be scanned, not silently dropped.
        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[2]);
        $thirdRequest = $capturedRequests[2];
        $context = $thirdRequest->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('z', $thirdRequest->getStartKey());
        $this->assertSame('k', $thirdRequest->getEndKey());
        $this->assertTrue($thirdRequest->getReverse());
    }

    public function testReverseScanContinuesAfterSplitToCoverRemainder(): void
    {
        // Stale outer enumeration: the first reverse-scan attempt against
        // [a,z) fails with EpochNotMatch; the retry resolves the post-split
        // [a,k) and the remainder [k,z) is scanned afterwards. Because the
        // scan is reverse, the remainder's (higher) keys must come FIRST.
        $preSplit = $this->defaultRegion('a', 'z', regionId: 1);
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');

        $k1 = new KvPair();
        $k1->setKey('k1');
        $k1->setValue('v1');
        $k2 = new KvPair();
        $k2->setKey('k2');
        $k2->setValue('v2');
        $k3 = new KvPair();
        $k3->setKey('k3');
        $k3->setValue('v3');

        /** @var list<Message> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
                $k1,
                $k2,
                $k3,
            ): Message {
                $callCount++;
                $capturedRequests[] = $request;

                if ($callCount === 1) {
                    return $this->responseWithRegionError($error);
                }

                $response = new RawScanResponse();
                /** @var RawScanRequest $request */
                if ($request->getStartKey() === 'z') {
                    // Higher remainder [k,z), scanned by the continuation.
                    $response->setKvs([$k3]);
                } else {
                    // Clipped lower part [a,k).
                    $response->setKvs([$k1, $k2]);
                }

                return $response;
            },
        );

        $result = $this->scanner->reverseScan('z', 'a', 100, false);

        // Reverse order: the higher remainder keys come first.
        $this->assertSame(['k3', 'k1', 'k2'], array_column($result, 'key'));
        $this->assertCount(3, $capturedRequests);

        $this->assertInstanceOf(RawScanRequest::class, $capturedRequests[2]);
        $continuation = $capturedRequests[2];
        $context = $continuation->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('z', $continuation->getStartKey());
        $this->assertSame('k', $continuation->getEndKey());
        $this->assertTrue($continuation->getReverse());
    }

    public function testScanWithPermanentRegionErrorStopsAtAttemptCap(): void
    {
        $region = $this->defaultRegion('a', 'z');

        // The cache keeps serving the same stale region: invalidation
        // cannot help, so the retry loop must terminate at the attempt cap
        // instead of looping forever.
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');

        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function () use (&$callCount, $error): Message {
                $callCount++;

                return $this->responseWithRegionError($error);
            },
        );

        $this->expectException(RetryBudgetExhaustedException::class);
        $this->expectExceptionMessage('Retry attempt cap');

        try {
            $this->scanner->scan('a', 'z', 100, false);
        } finally {
            $this->assertSame(RetryExecutor::DEFAULT_MAX_ATTEMPTS, $callCount);
        }
    }

    private function responseWithRegionError(Error $error): RawScanResponse
    {
        $response = new RawScanResponse();
        $response->setRegionError($error);

        return $response;
    }
}
