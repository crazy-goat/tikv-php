<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(RegionRangeClipper::class)]
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
    // Byte-order semantics (region boundaries must compare in byte order,
    // never PHP numeric-string order)
    // ========================================================================

    public function testClipForwardUsesByteOrderNotNumericOrder(): void
    {
        // r1 = ['', '100'), r2 = ['100', ''). In byte order '9' > '100', so the
        // [9, +inf) range starts inside r2; the sub-range for r1 is reversed
        // and MUST be skipped, not yielded.
        $regions = [
            $this->region('', '100', regionId: 1),
            $this->region('100', '', regionId: 2),
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, '9', ''));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame(2, $region->regionId);
        $this->assertSame('9', $start);
        $this->assertSame('', $end);
    }

    public function testClipForwardSkippedNotReversedSubRange(): void
    {
        // The reproduced bug: clipForward([r1,r2], '9', '') yielded a reversed
        // sub-range (r1, start='9', end='100') that should have been skipped.
        // After the fix the first region yields nothing.
        $regions = [
            $this->region('', '100', regionId: 1),
            $this->region('100', '', regionId: 2),
        ];

        $it = $this->clipper->clipForward($regions, '9', '');

        $first = $it->current();
        $this->assertSame(2, $first[0]->regionId);
    }

    public function testClipReverseUsesByteOrderNotNumericOrder(): void
    {
        // Reverse scan from ('', '9'] (startKey '9', endKey '' = unbounded
        // lower bound) over regions in descending byte order:
        //   r2 = ['100', '')   r1 = ['', '100')
        // In byte order '9' > '100', so the range ('', '9'] spans:
        //   '100'..'9'  -> r2  (scan from '9' down to '100')
        //   '' .. '100' -> r1  (scan from '100' down to '')
        // Both regions must be yielded, each clipped to its byte-order share.
        $regions = [
            $this->region('100', '', regionId: 2),
            $this->region('', '100', regionId: 1),
        ];
        $results = iterator_to_array($this->clipper->clipReverse($regions, '9', ''));

        $this->assertCount(2, $results);

        [$r2, $s2, $e2] = $results[0];
        $this->assertSame(2, $r2->regionId);
        $this->assertSame('9', $s2);
        $this->assertSame('100', $e2);

        [$r1, $s1, $e1] = $results[1];
        $this->assertSame(1, $r1->regionId);
        $this->assertSame('100', $s1);
        $this->assertSame('', $e1);
    }

    // ========================================================================
    // Numeric-boundary vectors (issue #232)
    //
    // The vectors above use 'a'/'m'/'z', ASCII from the middle of the byte
    // range, where PHP's numeric-string comparison and byte order happen to
    // agree — which is why the whole class of bug went undetected. The
    // boundaries below are decimal strings, so the two orderings disagree.
    // ========================================================================

    public function testClipForwardYieldsThreeSubRangesForNumericBoundaries(): void
    {
        // The issue #232 vector: three regions byte-sorted as
        // '' < '1000' < '999' ('1' = 0x31 < '9' = 0x39), clipped forward
        // from '0' to +infinity. Pre-fix region 2 was dropped without a
        // trace, because PHP read '1000' >= '999' as 1000 >= 999 and called
        // its sub-range empty — a deleteRange() over this layout then
        // reported success without ever touching region 2.
        $regions = [
            $this->region('', '1000', regionId: 1),
            $this->region('1000', '999', regionId: 2),
            $this->region('999', '', regionId: 3),
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, '0', ''));

        $this->assertCount(3, $results);

        // '0' > '' so region 1 is clipped forward to its start: ['0', '1000').
        [$r1, $s1, $e1] = $results[0];
        $this->assertSame(1, $r1->regionId);
        $this->assertSame('0', $s1);
        $this->assertSame('1000', $e1);

        // '0' < '1000' bytewise, so region 2 keeps its own bounds. The range
        // is non-empty: '1000' < '999' ('1' < '9'), the byte order that PHP
        // inverts.
        [$r2, $s2, $e2] = $results[1];
        $this->assertSame(2, $r2->regionId);
        $this->assertSame('1000', $s2);
        $this->assertSame('999', $e2);

        // The unbounded last region is clipped only at its start; '' stays +infinity.
        [$r3, $s3, $e3] = $results[2];
        $this->assertSame(3, $r3->regionId);
        $this->assertSame('999', $s3);
        $this->assertSame('', $e3);
    }

    public function testClipForwardKeepsEveryRegionOfTheNumericLayoutWhenClippedToASubRange(): void
    {
        // The six-region layout of issue #232 — byte-sorted as
        // '' < '1000' < '2000' < '30' < '400' < '999', numerically
        // backwards from '2000' on — clipped forward to ['100', '9995').
        // Every region contributes exactly one sub-range, and they are
        // contiguous, i.e. no region is skipped in the middle.
        $regions = [
            $this->region('', '1000', regionId: 1),
            $this->region('1000', '2000', regionId: 2),
            $this->region('2000', '30', regionId: 3),
            $this->region('30', '400', regionId: 4),
            $this->region('400', '999', regionId: 5),
            $this->region('999', '', regionId: 6),
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, '100', '9995'));

        //   region      sub-range        why
        //   1 ["", "1000")    ['100', '1000')   '100' is inside it
        //   2 ['1000', "2000") ['1000', '2000') '100' < '1000' bytewise
        //   3 ['2000', '30')  ['2000', '30')    '2000' < '30' bytewise ('2' < '3'),
        //                                        so this sub-range is valid although
        //                                        2000 > 30 numerically — pre-fix PHP
        //                                        dropped it
        //   4 ['30', '400')   ['30', '400')     fully inside the request
        //   5 ['400', '999')  ['400', '999')    fully inside the request
        //   6 ['999', '')     ['999', '9995')   unbounded end clipped to the request
        $expected = [
            [1, '100', '1000'],
            [2, '1000', '2000'],
            [3, '2000', '30'],
            [4, '30', '400'],
            [5, '400', '999'],
            [6, '999', '9995'],
        ];

        $this->assertCount(count($expected), $results);
        foreach ($expected as $index => [$regionId, $start, $end]) {
            [$region, $actualStart, $actualEnd] = $results[$index];
            $this->assertSame($regionId, $region->regionId);
            $this->assertSame($start, $actualStart);
            $this->assertSame($end, $actualEnd);
        }
    }

    public function testClipForwardKeepsTheDeleteRangeStartKeyInAnUnboundedRegion(): void
    {
        // deleteRange("20", "300") against the single region ["100", "").
        // Bytewise '20' > '100' ('2' = 0x32 > '1' = 0x31), so the request
        // starts inside the region and the clipped range must start at '20'.
        // Pre-fix PHP compared 20 > 100 and kept the region's own start key,
        // so the delete silently began at '100' and every key in
        // ['20', '100') — '250', '299' — was never deleted.
        $results = iterator_to_array($this->clipper->clipForward(
            [$this->region('100', '', regionId: 1)],
            '20',
            '300',
        ));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame(1, $region->regionId);
        $this->assertSame('20', $start);
        $this->assertSame('300', $end);
    }

    public function testClipForwardYieldsOnlyTheRegionThatContainsTheNumericRange(): void
    {
        // Request ['3', '9') over ["", "20") and ['20', ""). '3' > '20' and
        // '9' > '20' bytewise ('3' = 0x33 > '2' = 0x32), so the whole range
        // sits in the second region: the first must be skipped, not yielded
        // with inverted bounds. PHP reads 3 < 20 and 9 < 20 and routes the
        // range to the first region instead.
        $regions = [
            $this->region('', '20', regionId: 1),
            $this->region('20', '', regionId: 2),
        ];
        $results = iterator_to_array($this->clipper->clipForward($regions, '3', '9'));

        $this->assertCount(1, $results);
        [$region, $start, $end] = $results[0];
        $this->assertSame(2, $region->regionId);
        $this->assertSame('3', $start);
        $this->assertSame('9', $end);
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
