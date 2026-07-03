<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use PHPUnit\Framework\TestCase;

/**
 * @covers \CrazyGoat\TiKV\Client\Region\RegionRangeClipper
 */
final class RegionRangeClipperTest extends TestCase
{
    private RegionRangeClipper $clipper;

    protected function setUp(): void
    {
        $this->clipper = new RegionRangeClipper();
    }

    // ========================================================================
    // clipForward – basic
    // ========================================================================

    public function testClipForwardSingleRegion(): void
    {
        $regions = [$this->region('a', 'z')];
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', 'z'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('a', $start);
        $this->assertSame('z', $end);
    }

    public function testClipForwardMultipleRegions(): void
    {
        $regions = [
            $this->region('a', 'm', regionId: 1),
            $this->region('m', 'z', regionId: 2),
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', 'z'));

        $this->assertCount(2, $results);

        [$r1, $s1, $e1] = $results[0];
        $this->assertSame(1, $r1->regionId);
        $this->assertSame('a', $s1);
        $this->assertSame('m', $e1);

        [$r2, $s2, $e2] = $results[1];
        $this->assertSame(2, $r2->regionId);
        $this->assertSame('m', $s2);
        $this->assertSame('z', $e2);
    }

    public function testClipForwardStartKeyBeforeRegion(): void
    {
        $regions = [$this->region('b', 'z')];
        // startKey 'a' is before region->startKey 'b' → scanStart becomes 'b'
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', 'z'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('b', $start);
        $this->assertSame('z', $end);
    }

    public function testClipForwardStartKeyInsideRegion(): void
    {
        $regions = [$this->region('a', 'z')];
        // startKey 'm' > region->startKey 'a' → scanStart becomes 'm'
        $results = iterator_to_array($this->clipper->clipForward($regions, 'm', 'z'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('m', $start);
        $this->assertSame('z', $end);
    }

    public function testClipForwardEndKeyInsideRegion(): void
    {
        $regions = [$this->region('a', 'z')];
        // endKey 'm' < region->endKey 'z' → scanEnd becomes 'm'
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', 'm'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('a', $start);
        $this->assertSame('m', $end);
    }

    public function testClipForwardEmptyEndKeyReturnsRegionEndKey(): void
    {
        $regions = [$this->region('a', 'z')];
        // endKey '' (unbounded) → scanEnd becomes region->endKey 'z'
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', ''));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('a', $start);
        $this->assertSame('z', $end);
    }

    public function testClipForwardEmptyEndKeyOnLastRegionWithInfiniteEnd(): void
    {
        $regions = [
            $this->region('a', 'm'),
            $this->region('m', ''),  // +infinity end
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', ''));

        $this->assertCount(2, $results);

        [$r1, $s1, $e1] = $results[0];
        $this->assertSame('a', $s1);
        $this->assertSame('m', $e1);

        [$r2, $s2, $e2] = $results[1];
        $this->assertSame('m', $s2);
        $this->assertSame('', $e2);  // end is infinity
    }

    public function testClipForwardRangeOutsideRegionSkipped(): void
    {
        // Single region covers [b, c). Request [a, b) should be skipped.
        $regions = [$this->region('b', 'c')];
        $results = iterator_to_array($this->clipper->clipForward($regions, 'a', 'b'));

        $this->assertCount(0, $results);
    }

    public function testClipForwardRequestStartEqualsRegionEnd(): void
    {
        // Region [a, m). Request [m, z) should be skipped for this region.
        $regions = [$this->region('a', 'm')];
        $results = iterator_to_array($this->clipper->clipForward($regions, 'm', 'z'));

        $this->assertCount(0, $results);
    }

    public function testClipForwardEmptyRegionsArray(): void
    {
        $results = iterator_to_array($this->clipper->clipForward([], 'a', 'z'));
        $this->assertCount(0, $results);
    }

    // ========================================================================
    // clipReverse – basic
    // ========================================================================

    public function testClipReverseSingleRegion(): void
    {
        $regions = [$this->region('a', 'z')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'z', 'a'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('z', $start);
        $this->assertSame('a', $end);
    }

    public function testClipReverseMultipleRegionsDescending(): void
    {
        // Regions in descending order (as they come from reverse scan)
        $regions = [
            $this->region('m', 'z', regionId: 2),
            $this->region('a', 'm', regionId: 1),
        ];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'z', 'a'));

        $this->assertCount(2, $results);

        [$r1, $s1, $e1] = $results[0];
        $this->assertSame(2, $r1->regionId);
        $this->assertSame('z', $s1);
        $this->assertSame('m', $e1);

        [$r2, $s2, $e2] = $results[1];
        $this->assertSame(1, $r2->regionId);
        $this->assertSame('m', $s2);
        $this->assertSame('a', $e2);
    }

    public function testClipReverseStartBeforeRegionEnd(): void
    {
        // Region [a, m). startKey 'l' < region->endKey 'm' → scanStart = 'l'
        $regions = [$this->region('a', 'm')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'l', 'a'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('l', $start);
        $this->assertSame('a', $end);
    }

    public function testClipReverseStartAtRegionEnd(): void
    {
        // Region [a, m). startKey 'm' is NOT < region->endKey 'm' → scanStart = region->endKey 'm'.
        // The range [a, m) is yielded; the caller's scan will get empty results from TiKV
        // because 'm' is the exclusive region boundary.
        $regions = [$this->region('a', 'm')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'm', 'a'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('m', $start);
        $this->assertSame('a', $end);
    }

    public function testClipReverseEndBeforeRegionStart(): void
    {
        // Region [b, z). endKey 'a' < region->startKey 'b' → scanEnd = region->startKey 'b'
        // This should give range ['z, 'b) which is fine for reverse.
        $regions = [$this->region('b', 'z')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'z', 'a'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('z', $start);
        $this->assertSame('b', $end);
    }

    public function testClipReverseEmptyEndKey(): void
    {
        // Region ['m', ''). startKey 'z' < region->endKey '' (infinity) → scanStart = 'z'
        $regions = [$this->region('m', '')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'z', 'm'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame('z', $start);
        $this->assertSame('m', $end);
    }

    public function testClipReverseEmptyRegionsArray(): void
    {
        $results = iterator_to_array($this->clipper->clipReverse([], 'z', 'a'));
        $this->assertCount(0, $results);
    }

    public function testClipReverseRangeOutsideRegionSkipped(): void
    {
        // Region [b, c). Reverse scan from 'b' to 'a' is outside region (b is the start).
        $regions = [$this->region('b', 'c')];
        $results = iterator_to_array($this->clipper->clipReverse($regions, 'b', 'a'));

        $this->assertCount(0, $results);
    }

    // ========================================================================
    // Helper
    // ========================================================================

    private function region(string $startKey, string $endKey, int $regionId = 1): RegionInfo
    {
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
}
