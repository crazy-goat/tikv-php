<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionScanBatch;
use PHPUnit\Framework\TestCase;

/**
 * The cursor-trimming helpers of {@see RegionScanBatch}.
 *
 * A scan loads its regions in bounded batches, so a split or merge between
 * two batches can make a later region overlap the part of the range the
 * previous batch already read. The forward path trims the region's start to
 * the cursor; the reverse path (issue #198) trims its end.
 */
final class RegionScanBatchTest extends TestCase
{
    private function region(string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: 7,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    public function testForRowBudgetNeverRequestsZeroRegions(): void
    {
        $this->assertSame(1, RegionScanBatch::forRowBudget(0));
        $this->assertSame(1, RegionScanBatch::forRowBudget(-5));
    }

    public function testForRowBudgetIsCappedAtTheBatchCeiling(): void
    {
        $this->assertSame(5, RegionScanBatch::forRowBudget(5));
        $this->assertSame(RegionScanBatch::MAX_REGIONS, RegionScanBatch::forRowBudget(RegionScanBatch::MAX_REGIONS));
        $this->assertSame(RegionScanBatch::MAX_REGIONS, RegionScanBatch::forRowBudget(10_000));
    }

    public function testTrimEndToCursorClampsARegionThatExtendsAboveTheCursor(): void
    {
        $trimmed = RegionScanBatch::trimEndToCursor($this->region('a', 'z'), 'm');

        $this->assertSame('a', $trimmed->startKey);
        $this->assertSame('m', $trimmed->endKey);
        $this->assertSame(7, $trimmed->regionId);
    }

    public function testTrimEndToCursorClampsAnUnboundedRegion(): void
    {
        $trimmed = RegionScanBatch::trimEndToCursor($this->region('a', ''), 'm');

        $this->assertSame('m', $trimmed->endKey);
    }

    public function testTrimEndToCursorLeavesARegionAtOrBelowTheCursorAlone(): void
    {
        $region = $this->region('a', 'm');

        $this->assertSame($region, RegionScanBatch::trimEndToCursor($region, 'm'));
        $this->assertSame($region, RegionScanBatch::trimEndToCursor($region, 'z'));
    }

    public function testTrimToCursorStillTrimsTheForwardPath(): void
    {
        $trimmed = RegionScanBatch::trimToCursor([$this->region('a', 'z')], 'm');

        $this->assertCount(1, $trimmed);
        $this->assertSame('m', $trimmed[0]->startKey);
        $this->assertSame('z', $trimmed[0]->endKey);
    }
}
