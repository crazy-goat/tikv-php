<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use InvalidArgumentException;

/**
 * The canonical byte-order key and region-boundary vectors of issue #180, and
 * the `strcmp` reference every key-ordering component is differentially checked
 * against in `tests/Unit/Util/KeyOrderDifferentialTest.php`.
 *
 * #186, #232 and #261 each shipped their own copy of "a keyspace where PHP's
 * numeric order and TiKV's byte order disagree", and it took three issues to fix
 * one root cause — the argument for ONE fixture every component is measured
 * against instead of one fixture per finding.
 *
 * The class has two halves and they are not equally independent. "Shares no code"
 * decides whether a differential is worth anything, so say which half means it:
 *
 * 1. **The ordering primitives are independent** — `referenceSign()`,
 *    `referenceInRange()`, `referenceRegionId()` and the pair table are plain
 *    `strcmp()`, deliberately *not* {@see \CrazyGoat\TiKV\Client\Util\KeyOrder}.
 *    Their expectations are *derived*, never asserted: a pair enters
 *    `discriminatingPairs()` only when the fixture itself asks PHP what `$a < $b`
 *    says and gets a different answer (`isDiscriminating()`), so the table cannot
 *    rot into pairs the pre-#186 comparison passes. `phpAgreesWithByteOrder()`
 *    is the control for that filter.
 * 2. **The clipping half is a restatement** — `overlappingRegions()`,
 *    `referenceForwardClip()` and `maximalIntervals()` re-derive
 *    `RegionRangeClipper`'s `max`/`min` and its `''`-as-+infinity algebra
 *    condition for condition, because the production-shaped input (only the
 *    regions that overlap the request) has to be built somewhere. It catches
 *    transcription errors, not design errors. The independent check for the
 *    clipper is the **tiling property** in `KeyOrderDifferentialTest`
 *    (`testClippedSubRangesTileTheRequestWithoutGapOrOverlap()`): a statement
 *    about no gap and no overlap, not a second run of the clipping formula.
 *
 * Deliberately NOT named `*Test.php`, so PHPUnit's `Unit` suite does not collect
 * it — the same reason `tests/Unit/Phpstan/KeyOrderRuleFixture.php` is not a test.
 */
final class BinaryKeyVectors
{
    /**
     * #186/#261's two-region layout: bytewise `'9' > '100'` while PHP reads
     * `9 < 100`, so every `"9"`/`"99"`/`"20"`-shaped key routes differently.
     */
    public const TWO_REGION = 'two-region';

    /**
     * #232's three-region layout: bytewise `'1000' < '999'` but numerically not,
     * so the pre-fix clipper dropped the middle region's sub-range silently.
     */
    public const THREE_REGION = 'three-region';

    /** #232's six-region layout: byte-sorted, numerically backwards from region 3 on. */
    public const SIX_REGION = 'six-region';

    /**
     * #261's five-region layout: byte-sorted, but read as *numbers* the boundaries
     * collapse (`0100 == 100`, `99 < 100 < 1e3 == 1000`).
     */
    public const FOUR_BOUNDARY = 'four-boundary';

    /**
     * The pure-binary layout: 0x00 is the lowest and 0xFF the highest byte, so it
     * pins inclusive starts, exclusive ends and the `"\x00"`-bounded byte-range end.
     */
    public const BINARY = 'binary';

    /** PHP's own verdict for a pair: `$left < $right` holds. */
    public const PHP_LESS = 'less';

    /** PHP's own verdict for a pair: `$left > $right` holds. */
    public const PHP_GREATER = 'greater';

    /**
     * PHP's own verdict for a pair: neither operator holds, i.e. PHP reads the two as
     * numerically equal ("1e3"/"1000") or byte-identical. TiKV still has an order.
     */
    public const PHP_EQUAL = 'equal';

    /**
     * Memoised derived data: `maximalIntervals()` asks for the region lookup once per
     * interval, so rebuilding these on every call is what made the differential
     * O(intervals x regions). Nothing mutates a `RegionInfo` in place (a leader switch
     * writes `RegionEntry`, not the region), so the shared instances are safe.
     *
     * @var array<string, list<RegionInfo>>
     */
    private static array $regionsByLayout = [];

    /** @var array<string, list<string>> */
    private static array $probeKeysByLayout = [];

    /**
     * Every layout name, in the order the differential test walks them.
     *
     * @return list<string>
     */
    public static function layoutNames(): array
    {
        return [self::TWO_REGION, self::THREE_REGION, self::SIX_REGION, self::FOUR_BOUNDARY, self::BINARY];
    }

    /**
     * The layouts as raw `[startKey, endKey)` pairs — data only, so a caller can
     * build its own `RegionInfo` list with its own peer/epoch values
     * (`RegionRangeClipperTest` does; the rest take `layoutRegions()`).
     *
     * @return list<array{string, string}>
     */
    public static function layoutBounds(string $layout): array
    {
        return match ($layout) {
            self::TWO_REGION => [['', '100'], ['100', '']],
            self::THREE_REGION => [['', '1000'], ['1000', '999'], ['999', '']],
            self::SIX_REGION => [
                ['', '1000'], ['1000', '2000'], ['2000', '30'], ['30', '400'], ['400', '999'], ['999', ''],
            ],
            self::FOUR_BOUNDARY => [['', '0100'], ['0100', '100'], ['100', '1e3'], ['1e3', '99'], ['99', '']],
            self::BINARY => [['', "\x00\xff"], ["\x00\xff", "\xff\xff"], ["\xff\xff", '']],
            default => throw new InvalidArgumentException('unknown key-vector layout: ' . $layout),
        };
    }

    /**
     * The layout as a `RegionInfo` list, region id = 1-based position. The other
     * fields are irrelevant to every comparison under test, so they are fixed at 1.
     *
     * @return list<RegionInfo>
     */
    public static function layoutRegions(string $layout): array
    {
        if (isset(self::$regionsByLayout[$layout])) {
            return self::$regionsByLayout[$layout];
        }

        $regions = [];
        foreach (self::layoutBounds($layout) as $index => [$startKey, $endKey]) {
            $id = $index + 1;
            $regions[] = new RegionInfo(
                regionId: $id,
                leaderPeerId: $id,
                leaderStoreId: $id,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: $startKey,
                endKey: $endKey,
            );
        }

        return self::$regionsByLayout[$layout] = $regions;
    }

    /**
     * The non-empty boundaries of a layout, in byte order — the cut points of the
     * keyspace. `''` is excluded: it is the keyspace start (and the +infinity end of
     * the last region), not a boundary *between* two regions.
     *
     * @return list<string>
     */
    public static function layoutBoundaries(string $layout): array
    {
        $boundaries = [];
        foreach (self::layoutBounds($layout) as [$startKey, $endKey]) {
            foreach ([$startKey, $endKey] as $key) {
                if ($key !== '') {
                    $boundaries[] = $key;
                }
            }
        }
        $boundaries = array_values(array_unique($boundaries));
        sort($boundaries, SORT_STRING);

        return $boundaries;
    }

    /**
     * Every key the differential test probes in a layout, byte-sorted and de-duplicated
     * so a caller can compare it against a byte-sorted result directly: the boundaries
     * themselves, the key as low as possible inside the region each boundary opens (its
     * immediate byte successor), the key as high as possible below it and that key's byte
     * predecessor; every candidate pair's two keys; the pure binary keys, so the ends of
     * the byte range and the fact that a key may contain NUL and 0xFF at all are pinned;
     * and a deterministic decimal stream plus the same stream zero-padded to five digits
     * — the leading-zero shape is what reaches the regions below the first non-empty
     * boundary, and PHP merges it hardest ("0500" == "500").
     *
     * @return list<string>
     */
    public static function probeKeys(string $layout): array
    {
        if (isset(self::$probeKeysByLayout[$layout])) {
            return self::$probeKeysByLayout[$layout];
        }

        $decimal = self::decimalKeys(200, 99999);
        $keys = $decimal;
        foreach ($decimal as $key) {
            $keys[] = str_pad($key, 5, '0', STR_PAD_LEFT);
        }
        $keys = array_merge($keys, [
            "\x00", "\x00\x00", "\x00\xff", "\x00\xfe", "\x01", "\x7f",
            "\x80", "\xfe\xff\x00", "\xff", "\xff\xff", "\xff\xff\x00", "\xff\xff\xff",
        ]);
        foreach (self::candidatePairs() as [$left, $right]) {
            $keys[] = $left;
            $keys[] = $right;
        }
        foreach (self::layoutBoundaries($layout) as $boundary) {
            $keys[] = $boundary;
            $keys[] = $boundary . "\x00";
            $keys[] = $boundary . "\xff";
            // The key just below the boundary bytewise: last byte decremented, or the
            // boundary minus its last byte when that byte is already 0x00.
            $last = ord($boundary[strlen($boundary) - 1]);
            $keys[] = $last === 0 ? substr($boundary, 0, -1) : substr($boundary, 0, -1) . chr($last - 1);
        }

        $keys = array_values(array_unique($keys));
        sort($keys, SORT_STRING);

        return self::$probeKeysByLayout[$layout] = $keys;
    }

    /**
     * The probe keys of a layout grouped by the region the reference lookup assigns
     * them to — the explicit form of "which regions are hit by which keys".
     *
     * @return array<int, list<string>>
     */
    public static function probeKeysByRegion(string $layout): array
    {
        $byRegion = [];
        foreach (self::probeKeys($layout) as $key) {
            $regionId = self::referenceRegionId($layout, $key);
            if ($regionId !== null) {
                $byRegion[$regionId][] = $key;
            }
        }
        ksort($byRegion);

        return $byRegion;
    }

    // ========================================================================
    // The ordering reference: straight on strcmp(), sharing no code with KeyOrder
    // ========================================================================

    /**
     * The reference region lookup: a plain linear scan over the layout's half-open
     * `[startKey, endKey)` bounds, like the naive implementation the pre-#186 code
     * approximated with a binary search and PHP operators. Deliberately O(n) and
     * deliberately duplicated logic — a differential test against a *different*
     * algorithm catches what a shared helper cannot.
     */
    public static function referenceRegionId(string $layout, string $key): ?int
    {
        return self::referenceRegionIdIn(self::layoutRegions($layout), $key);
    }

    /**
     * {@see referenceRegionId()} over a region list the caller already has, so a test
     * that builds its own `RegionInfo` objects still uses the one reference.
     *
     * @param list<RegionInfo> $regions in ascending byte order of startKey
     */
    public static function referenceRegionIdIn(array $regions, string $key): ?int
    {
        foreach ($regions as $region) {
            if (self::referenceInRange($key, $region->startKey, $region->endKey)) {
                return $region->regionId;
            }
        }

        return null;
    }

    /**
     * Half-open `[start, end)` membership straight on `strcmp()`, with an empty `$end`
     * meaning +infinity — the range contract every component implements.
     */
    public static function referenceInRange(string $key, string $start, string $end): bool
    {
        return strcmp($key, $start) >= 0 && ($end === '' || strcmp($key, $end) < 0);
    }

    /**
     * Byte-order sign of two keys: negative when `$left` sorts before `$right`, 0 when
     * equal, positive when after.
     */
    public static function referenceSign(string $left, string $right): int
    {
        $comparison = strcmp($left, $right);

        return $comparison === 0 ? 0 : ($comparison < 0 ? -1 : 1);
    }

    // ========================================================================
    // The clipping contract: a restatement of RegionRangeClipper's conventions
    // ========================================================================

    /**
     * The regions of a layout that share at least one byte with the request
     * `[startKey, endKey)` — in layout order, which is what
     * `RegionRangeClipper::clipForward()` is documented to receive and what
     * `PdClient::scanRegions($startKey, $endKey)` returns for it.
     *
     * A request that is empty bytewise (`['20', '100')`: `'2'` = 0x32 > `'1'`) is
     * non-empty numerically but overlaps *nothing*, so this returns an empty list and
     * the clipper must yield nothing for it. Handing the clipper regions it was not
     * asked about is out of contract — `clipForward()` clips a request to the regions
     * it is given and does not reject a region the request does not touch — which is
     * why this method, not the whole layout, is what the differential test hands over.
     *
     * @return list<RegionInfo>
     */
    public static function overlappingRegions(string $layout, string $startKey, string $endKey): array
    {
        $overlapping = [];
        foreach (self::layoutRegions($layout) as $region) {
            $low = strcmp($startKey, $region->startKey) > 0 ? $startKey : $region->startKey;
            $high = self::lowerBound($endKey, $region->endKey);
            if ($high !== '' && strcmp($low, $high) >= 0) {
                continue;
            }
            $overlapping[] = $region;
        }

        return $overlapping;
    }

    /**
     * The reference forward clip: every region that overlaps the request, intersected
     * with it as `[max(startKey, region start), min(endKey, region end))`, in ascending
     * region order. An empty end key is +infinity on both sides, so the unbounded last
     * region keeps `''` and a request that ends inside a region is clipped to the end.
     *
     * @return list<array{int, string, string}> [regionId, startKey, endKey]
     */
    public static function referenceForwardClip(string $layout, string $startKey, string $endKey): array
    {
        $clips = [];
        foreach (self::overlappingRegions($layout, $startKey, $endKey) as $region) {
            $clips[] = [
                $region->regionId,
                strcmp($startKey, $region->startKey) > 0 ? $startKey : $region->startKey,
                self::lowerBound($endKey, $region->endKey),
            ];
        }

        return $clips;
    }

    /**
     * The lower of two end keys, where an empty key is +infinity — so an unbounded
     * request keeps a bounded region's end, an unbounded region keeps a bounded
     * request's end, and two unbounded ends stay `''`.
     */
    private static function lowerBound(string $requestEnd, string $regionEnd): string
    {
        if ($requestEnd === '') {
            return $regionEnd;
        }

        return ($regionEnd === '' || strcmp($requestEnd, $regionEnd) < 0) ? $requestEnd : $regionEnd;
    }

    /**
     * The maximal byte intervals of `[startKey, endKey)`: the coarsest partition of the
     * request in which no bound of a region it touches falls. Each entry is a
     * `[representative key, owner region id]` pair, the representative being the
     * interval's own lower bound — inside the interval by construction (a non-empty
     * `[low, high)` contains `low`), and shared by no two intervals, so a key can never
     * count for two of them.
     *
     * A partition element is what makes "no gap, no overlap" expressible: a clipped
     * sub-range list is correct exactly when it covers every interval once and only
     * once, and both the intervals and the count come from the vectors, never from the
     * production code being checked. An interval no region owns is outside the layout's
     * coverage and is left out, as is every interval of a bytewise-empty request — the
     * correct answer for those is no sub-range at all.
     *
     * @return list<array{string, int}>
     */
    public static function maximalIntervals(string $layout, string $startKey, string $endKey): array
    {
        if ($endKey !== '' && strcmp($startKey, $endKey) >= 0) {
            return [];
        }

        // Cut the request at the bounds of the regions that overlap it. `''` is
        // +infinity, never a cut point, and the last interval runs to the request's end.
        $cuts = [$startKey];
        foreach (self::overlappingRegions($layout, $startKey, $endKey) as $region) {
            foreach ([$region->startKey, $region->endKey] as $bound) {
                $inside = $bound !== '' && strcmp($bound, $startKey) > 0;
                if ($inside && ($endKey === '' || strcmp($bound, $endKey) <= 0)) {
                    $cuts[] = $bound;
                }
            }
        }
        $cuts = array_values(array_unique($cuts));
        sort($cuts, SORT_STRING);

        $intervals = [];
        for ($index = 0, $count = count($cuts); $index < $count; ++$index) {
            $low = $cuts[$index];
            $high = $cuts[$index + 1] ?? $endKey;
            if ($high !== '' && strcmp($low, $high) >= 0) {
                continue;
            }
            // A key no region of this layout owns is outside the layout's coverage and
            // is not part of the tiling either.
            $regionId = self::referenceRegionId($layout, $low);
            if ($regionId !== null) {
                $intervals[] = [$low, $regionId];
            }
        }

        return $intervals;
    }

    // ========================================================================
    // The discriminating pairs
    // ========================================================================

    /**
     * The candidate key pairs, each one a pair on which PHP's own relational operators
     * and TiKV's byte order can disagree. Which of them actually disagree is *not*
     * asserted here — `isDiscriminating()` computes it, so a pair PHP happens to order
     * correctly is filtered out rather than silently passing a mutation.
     *
     * @return list<array{string, string}>
     */
    public static function candidatePairs(): array
    {
        return [
            // Leading zeros ("007" < "7" bytewise, == numerically) and exponent notation
            // ("1e3" > "1000" bytewise, == numerically): PHP cannot order either pair at
            // all, while byte order is decisive.
            ['007', '7'],
            ['7', '007'],
            ['1e3', '1000'],
            ['1000', '1e3'],
            // Multi-digit decimals whose numeric order is the reverse of their byte order
            // ('1' = 0x31 < '2' = 0x32 < '3' = 0x33 < '9' = 0x39).
            ['20', '100'],
            ['3', '20'],
            ['199', '20'],
            ['9', '10'],
            ['9', '11'],
            ['2000', '30'],
            // Signed decimals: -6 > -5 bytewise, -6 < -5 numerically, so this is the one
            // region bound PHP reads backwards in both directions.
            ['-6', '-5'],
            ['-5', '-6'],
            // The two boundary pairs #261 splits a region on: "0100" == "100" numerically
            // while sorting before it bytewise, and "99" > "100".
            ['0100', '100'],
            ['0999', '100'],
            ['0100', '99'],
            ['99', '100'],
        ];
    }

    /**
     * The candidate pairs PHP actually mis-orders, i.e. the ones a pre-#186 comparison
     * cannot pass. Empty output means the table has rotted and the differential test has
     * nothing left to prove.
     *
     * @return list<array{string, string}>
     */
    public static function discriminatingPairs(): array
    {
        return array_values(array_filter(
            self::candidatePairs(),
            static fn (array $pair): bool => self::isDiscriminating($pair[0], $pair[1]),
        ));
    }

    /**
     * The control for `isDiscriminating()`: pairs PHP orders exactly like byte order, so
     * no byte-wise implementation can tell them apart and they must be rejected. `'30' <
     * '400'` is the obvious one — first byte `'3'` (0x33) below `'4'` (0x34) *and* 30
     * below 400. `'a' < 'b'` is the case every test here used before #232. `'15' < '20'`
     * is the instructive one: the surviving key of #186's `deleteRange("20", "300")`
     * reproduction, agreed under both orders, so it carries no signal — only its
     * neighbour `'199'` does.
     *
     * @return list<array{string, string}>
     */
    public static function phpAgreesWithByteOrder(): array
    {
        return [['30', '400'], ['400', '999'], ['1000', '2000'], ['15', '20'], ['a', 'b'], ['key1', 'key2']];
    }

    /**
     * Whether PHP's own relational operators order a pair differently from byte order —
     * either on `<` or on `>`, or by refusing to order it at all (numerically equal, like
     * "1e3" vs "1000", where both operators are false and only `strcmp()` knows).
     *
     * The parameters are deliberately named `$left`/`$right` and not `$key`/`$otherKey`:
     * the #186 PHPStan rule (`tikv.keyOrder.relationOnStrings`) reports relational
     * operators between two *key-like* names, and this method exists precisely to ask PHP
     * what that operator does to keys. Same reason as `RawKvBatchKeyOrderTest::phpVerdict()`.
     */
    public static function isDiscriminating(string $left, string $right): bool
    {
        $php = self::phpVerdict($left, $right);
        $byte = self::referenceSign($left, $right);

        return match ($byte) {
            -1 => $php !== self::PHP_LESS,
            1 => $php !== self::PHP_GREATER,
            default => $php !== self::PHP_EQUAL,
        };
    }

    /**
     * What PHP itself says about two strings — the pre-#186 comparison, asked rather
     * than simulated, so the expectation of the differential test is the interpreter's
     * own verdict and not a re-derivation of it. PHP 8 compares two strings bytewise
     * *except* when both are numeric strings, in which case it compares them numerically,
     * and it reports numeric equality with both `<` and `>` false. Both cases are bugs
     * against TiKV, which is the entire subject of issue #180.
     *
     * @return self::PHP_* the verdict
     */
    public static function phpVerdict(string $left, string $right): string
    {
        if ($left < $right) {
            return self::PHP_LESS;
        }
        if ($left > $right) {
            return self::PHP_GREATER;
        }

        return self::PHP_EQUAL;
    }

    // ========================================================================
    // Deterministic key stream
    // ========================================================================

    /**
     * A deterministic xorshift32 stream rendered as decimal keys — no `rand()`/
     * `mt_rand()`, so a failure is reproducible from the seed alone, and `(string) (int)`
     * so the keys are exactly the decimal bytes TiKV stores.
     *
     * @return list<string>
     */
    public static function decimalKeys(int $count, int $max): array
    {
        $keys = [];
        $state = 0x5EED1234;
        for ($i = 0; $i < $count; ++$i) {
            $state ^= ($state << 13) & 0xFFFFFFFF;
            $state ^= $state >> 17;
            $state ^= ($state << 5) & 0xFFFFFFFF;
            $keys[] = (string) ($state % ($max + 1));
        }

        return $keys;
    }

    // ========================================================================
    // Request ranges
    // ========================================================================

    /**
     * The `[startKey, endKey)` requests the clipper is measured with, derived from a
     * layout: the whole keyspace, the span from and to each boundary, every single
     * region, and both directions of every candidate pair — the bytewise-valid one,
     * which must produce sub-ranges, and the bytewise empty one (PHP calls it
     * non-empty), which must produce none.
     *
     * @return list<array{string, string}>
     */
    public static function requestRanges(string $layout): array
    {
        $boundaries = self::layoutBoundaries($layout);
        $ranges = [['', '']];

        foreach ($boundaries as $boundary) {
            $ranges[] = ['', $boundary];
            $ranges[] = [$boundary, ''];
        }

        for ($index = 0, $count = count($boundaries) - 1; $index < $count; ++$index) {
            $ranges[] = [$boundaries[$index], $boundaries[$index + 1]];
        }

        foreach (self::candidatePairs() as [$left, $right]) {
            $comparison = strcmp($left, $right);
            if ($comparison === 0) {
                continue;
            }
            $ranges[] = $comparison < 0 ? [$left, $right] : [$right, $left];
            $ranges[] = $comparison < 0 ? [$right, $left] : [$left, $right];
        }

        $unique = [];
        foreach ($ranges as $range) {
            $unique[$range[0] . "\x00" . $range[1]] = $range;
        }

        return array_values($unique);
    }
}
