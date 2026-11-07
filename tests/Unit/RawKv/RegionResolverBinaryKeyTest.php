<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\Attributes\CoversMethod;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * batchResolveRegions() must assign numeric-string and binary keys to regions
 * using TiKV byte order, not PHP numeric-string comparison.
 */
#[CoversMethod(RegionResolver::class, 'batchResolveRegions')]
final class RegionResolverBinaryKeyTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $resolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->resolver = new RegionResolver($this->pdClient, $this->regionCache);
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: $id,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    public function testBatchResolveUsesByteOrderNotNumericOrder(): void
    {
        $regions = [
            $this->region(1, '', '100'),
            $this->region(2, '100', ''),
        ];

        $this->pdClient->method('scanRegions')->with('0999', "9\x00")->willReturn($regions);
        $this->regionCache->method('put');

        $result = $this->resolver->batchResolveRegions(['9', '0999', '20']);

        foreach (['9' => 2, '0999' => 1, '20' => 2] as $key => $expectedRegionId) {
            $region = $result[$key] ?? null;
            self::assertNotNull($region);
            self::assertSame($expectedRegionId, $region->regionId);
        }
    }

    public function testBatchResolveAssignsTheIssueVectorBytewise(): void
    {
        // The issue's own vector. Bytewise "10" < "100" < "9", so "9" belongs
        // to the last region; a numeric comparison (9 < 10 < 100) would route
        // it to the first one and TiKV would reject the request with
        // KeyNotInRegion (issue #220, TXN-15).
        $regions = [
            $this->region(1, '', '10'),
            $this->region(2, '10', '100'),
            $this->region(3, '100', ''),
        ];

        $this->pdClient->method('scanRegions')->with('10', "9\x00")->willReturn($regions);
        $this->regionCache->method('put');

        $result = $this->resolver->batchResolveRegions(['9', '10', '100']);

        foreach (['9' => 3, '10' => 2, '100' => 3] as $key => $expectedRegionId) {
            $region = $result[$key] ?? null;
            self::assertNotNull($region);
            self::assertSame($expectedRegionId, $region->regionId);
        }
    }

    public function testBatchResolveHandlesBinaryBoundaries(): void
    {
        $regions = [
            $this->region(1, '', "\x00\xff"),
            $this->region(2, "\x00\xff", "\xff\xff"),
            $this->region(3, "\xff\xff", ''),
        ];

        $this->pdClient->method('scanRegions')->willReturn($regions);
        $this->regionCache->method('put');

        $result = $this->resolver->batchResolveRegions([
            "\x00",
            "\x00\xfe",
            "\x00\xff", // inclusive start of region 2
            "\xff\xfe",
            "\xff\xff", // inclusive start of region 3
            "\xff\xff\x00",
        ]);

        self::assertSame(1, $result["\x00"]->regionId);
        self::assertSame(1, $result["\x00\xfe"]->regionId);
        self::assertSame(2, $result["\x00\xff"]->regionId);
        self::assertSame(2, $result["\xff\xfe"]->regionId);
        self::assertSame(3, $result["\xff\xff"]->regionId);
        self::assertSame(3, $result["\xff\xff\x00"]->regionId);
    }

    public function testBatchResolveEmptyEndKeyIsPlusInfinity(): void
    {
        $regions = [
            $this->region(1, '', '100'),
            $this->region(2, '100', ''),
        ];

        $this->pdClient->method('scanRegions')->willReturn($regions);
        $this->regionCache->method('put');

        $result = $this->resolver->batchResolveRegions(['9', "\xff\xff"]);

        foreach (['9' => 2, "\xff\xff" => 2] as $key => $expectedRegionId) {
            $region = $result[$key] ?? null;
            self::assertNotNull($region);
            self::assertSame($expectedRegionId, $region->regionId);
        }
    }
}
