<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
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

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
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

    // ========================================================================
    // scan() – multi-region correctness
    // ========================================================================

    public function testScanSingleRegionReturnsAllPairs(): void
    {
        $region = $this->defaultRegion('a', 'z');
        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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
        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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
        $this->regionCache->method('getByKey')->willReturn(null);
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
        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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

        $this->regionCache->method('getByKey')->willReturn(null);
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
}
