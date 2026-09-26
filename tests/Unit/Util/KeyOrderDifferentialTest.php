<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Util;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Tests\Unit\Support\BinaryKeyVectors;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The cross-implementation differential that closes out issue #180.
 *
 * #186, #232 and #261 fixed the same root cause at three different sites and each
 * shipped its own vectors, so the remaining risk is not a known site but a *class*.
 * Nothing in the per-finding tests answers that — they pin their own site and
 * nothing else. This test runs the shared vectors of {@see BinaryKeyVectors}
 * through every key-ordering component and compares the answer against the
 * reference: `KeyOrder` against PHP's *own* verdict (the interpreter is the
 * reference, not a re-derivation of it), `RegionCache::getByKey()` fully seeded
 * and holding one region at a time, `RegionRangeClipper::clipForward()` against
 * the reference intersection, `clipForward()`/`clipReverse()` against the **no-gap,
 * no-overlap tiling property**, and `ScanIterator` pagination.
 *
 * The tiling property is the load-bearing check for the clipper, and the one that
 * is not a second run of the formula under test: a dropped region leaves a hole, an
 * over-wide clip makes two sub-ranges cover one interval, a mis-ordered one shows
 * up as a region id in the wrong place, and it holds even when a clipper bug is
 * injected into the production code *and* the fixture's own interval derivation.
 * That is also why `clipReverse()` gets no reference-intersection check of its own:
 * `BinaryKeyVectors::referenceReverseClip()` was a condition-for-condition
 * transliteration of `clipReverse()` and so only caught transcription errors.
 *
 * Layouts and pairs live in {@see BinaryKeyVectors}; add a boundary there and every
 * component below is measured against it at once.
 */
#[CoversClass(KeyOrder::class)]
#[CoversClass(RegionCache::class)]
#[CoversClass(RegionRangeClipper::class)]
#[CoversClass(ScanIterator::class)]
final class KeyOrderDifferentialTest extends TestCase
{
    /** Number of RPCs the fake scan in {@see self::drain()} issued. */
    private int $scanCalls = 0;

    private RegionRangeClipper $clipper;

    protected function setUp(): void
    {
        $this->clipper = new RegionRangeClipper();
    }

    // ========================================================================
    // The table itself — self-verifying, so a rotten table fails loudly
    // ========================================================================

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function discriminatingPairs(): iterable
    {
        foreach (BinaryKeyVectors::discriminatingPairs() as $index => [$left, $right]) {
            yield $index . ': "' . $left . '" / "' . $right . '"' => [$left, $right];
        }
    }

    /**
     * Every pair in the table is one PHP actually mis-orders, and `KeyOrder` orders it
     * the other way. PHP's verdict is *asked*, not reimplemented: `phpVerdict()`
     * evaluates `$left < $right` itself, so the expectation is the interpreter's
     * behaviour on this PHP build rather than a claim about it.
     */
    #[DataProvider('discriminatingPairs')]
    public function testKeyOrderDisagreesWithPhpOnEveryDiscriminatingPair(string $left, string $right): void
    {
        $byteSign = BinaryKeyVectors::referenceSign($left, $right);
        $phpVerdict = BinaryKeyVectors::phpVerdict($left, $right);
        $phpSign = match ($phpVerdict) {
            BinaryKeyVectors::PHP_LESS => -1,
            BinaryKeyVectors::PHP_GREATER => 1,
            default => 0,
        };

        self::assertSame($byteSign < 0, KeyOrder::lt($left, $right));
        self::assertSame($byteSign > 0, KeyOrder::gt($left, $right));
        self::assertSame($byteSign <= 0, KeyOrder::lte($left, $right));
        self::assertSame($byteSign >= 0, KeyOrder::gte($left, $right));
        self::assertSame($byteSign === 0, KeyOrder::eq($left, $right));
        self::assertSame($byteSign, KeyOrder::cmp($left, $right) <=> 0);
        // The point of the whole table: PHP reaches a different verdict. For
        // "1e3" / "1000" it reaches *no* verdict — the two are numerically equal, so
        // both operators are false and only strcmp() knows the answer.
        self::assertNotSame(
            $byteSign,
            $phpSign,
            sprintf(
                'the pair "%s" / "%s" must be one PHP mis-orders (PHP says %s, byte order says %d)',
                $left,
                $right,
                $phpVerdict,
                $byteSign,
            ),
        );
    }

    /**
     * The table cannot rot into a set of pairs that pass under the pre-#186
     * comparison: a candidate pair PHP happens to order correctly is dropped from
     * {@see BinaryKeyVectors::discriminatingPairs()} instead of being tested
     * vacuously, and this fails the moment one is.
     */
    public function testEveryCandidatePairIsDiscriminating(): void
    {
        $rejected = [];
        foreach (BinaryKeyVectors::candidatePairs() as [$left, $right]) {
            if (!BinaryKeyVectors::isDiscriminating($left, $right)) {
                $rejected[] = self::render($left) . ' / ' . self::render($right);
            }
        }

        self::assertSame([], $rejected, 'these candidate pairs no longer discriminate — drop them from '
            . 'BinaryKeyVectors::candidatePairs() instead of letting them test nothing');
        self::assertGreaterThanOrEqual(
            8,
            count(BinaryKeyVectors::discriminatingPairs()),
            'the table must keep at least the eight pairs issue #180 names',
        );
    }

    /**
     * The control for the filter above: the pairs whose two orders agree are rejected,
     * so the differential is never accidentally vacuous.
     */
    public function testPairsPhpOrdersLikeByteOrderAreRejected(): void
    {
        foreach (BinaryKeyVectors::phpAgreesWithByteOrder() as [$left, $right]) {
            self::assertFalse(
                BinaryKeyVectors::isDiscriminating($left, $right),
                self::render($left) . ' / ' . self::render($right) . ' is ordered the same way by both',
            );
        }
    }

    // ========================================================================
    // KeyOrder's range and successor contracts, against the reference
    // ========================================================================

    /**
     * `inRange()` against the reference for every probe key of every layout: against
     * the whole keyspace, against each region's own bounds with the key as one bound
     * (the inclusive start and the exclusive end, the two shapes a pre-#186 `>=` / `<`
     * gets wrong), against each region bound paired with the key's other side, and
     * against the degenerate `[key, key)`.
     */
    public function testInRangeAgreesWithTheReferenceForEveryProbeKeyOfEveryLayout(): void
    {
        $mismatches = [];
        $checked = 0;

        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            $bounds = BinaryKeyVectors::layoutBounds($layout);
            foreach (BinaryKeyVectors::probeKeys($layout) as $key) {
                $candidates = [['', ''], ['', $key], [$key, ''], [$key, $key]];
                foreach ($bounds as [$startKey, $endKey]) {
                    $candidates[] = [$startKey, $endKey];
                    $candidates[] = [$startKey, $key];
                    $candidates[] = [$key, $endKey];
                }

                foreach ($candidates as [$startKey, $endKey]) {
                    $expected = BinaryKeyVectors::referenceInRange($key, $startKey, $endKey);
                    ++$checked;
                    if (KeyOrder::inRange($key, $startKey, $endKey) === $expected) {
                        continue;
                    }
                    $mismatches[] = sprintf(
                        'inRange(%s, %s, %s) is %s, the reference says %s',
                        self::render($key),
                        self::render($startKey),
                        self::render($endKey),
                        var_export(!$expected, true),
                        var_export($expected, true),
                    );
                }
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, $checked));
    }

    /**
     * The immediate-successor contract on every probe key of every layout:
     * `[key, successor(key))` contains exactly the key, which is what makes an
     * inclusive region bound expressible as a half-open one and what keeps a scan
     * cursor from repeating its last row.
     */
    public function testSuccessorIsTheImmediateSuccessorOfEveryProbeKey(): void
    {
        $mismatches = [];
        $checked = 0;

        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            foreach (BinaryKeyVectors::probeKeys($layout) as $key) {
                $successor = KeyOrder::successor($key);
                ++$checked;
                $inRange = static fn (string $candidate): bool
                    => BinaryKeyVectors::referenceInRange($candidate, $key, $successor);
                $properties = [
                    'the successor is greater than the key'
                        => BinaryKeyVectors::referenceSign($successor, $key) > 0,
                    '[key, successor) contains the key' => $inRange($key),
                    '[key, successor) excludes the successor' => !$inRange($successor),
                    '[key, successor) excludes any extension' => !$inRange($key . '!'),
                    'the empty range [key, key) is empty'
                        => !BinaryKeyVectors::referenceInRange($key, $key, $key),
                ];
                foreach ($properties as $property => $holds) {
                    if (!$holds) {
                        $mismatches[] = sprintf(
                            '%s is false for key %s (successor %s)',
                            $property,
                            self::render($key),
                            self::render($successor),
                        );
                    }
                }
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, $checked));
    }

    // ========================================================================
    // RegionCache::getByKey()
    // ========================================================================

    public function testRegionCacheRoutesEveryProbeKeyLikeTheReference(): void
    {
        $mismatches = [];
        $checked = 0;

        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            $cache = new RegionCache();
            foreach (BinaryKeyVectors::layoutRegions($layout) as $region) {
                $cache->put($region);
            }

            foreach (BinaryKeyVectors::probeKeys($layout) as $key) {
                $expected = BinaryKeyVectors::referenceRegionId($layout, $key);
                $actual = $cache->getByKey($key)?->regionId;
                ++$checked;
                if ($actual === $expected) {
                    continue;
                }
                $mismatches[] = sprintf(
                    'getByKey(%s) is %s, the reference says %s',
                    self::render($key),
                    var_export($actual, true),
                    var_export($expected, true),
                );
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, $checked));
    }

    /**
     * The same routing, but from a cache holding only a *prefix* of the layout: a
     * partial view of the keyspace must still answer for every key it does hold, and
     * must report a miss only for keys below the split. This is the false-negative side
     * of the pre-#186 walk (issue #261's deviation, recorded in
     * docs/helpers/decisions.md): a numeric predecessor search found no region and cost
     * a PD round trip for a key the client already had.
     */
    public function testRegionCacheServesKeysFromAPartialLayout(): void
    {
        $mismatches = [];

        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            $regions = BinaryKeyVectors::layoutRegions($layout);
            // Derived once per layout, not once per held region: the grouping
            // re-derives, de-duplicates and re-sorts the whole key set, so calling it
            // inside the loop below made this O(regions x keys log keys).
            $byRegion = BinaryKeyVectors::probeKeysByRegion($layout);
            foreach ($regions as $held) {
                $cache = new RegionCache();
                $cache->put($held);

                foreach ($byRegion as $regionId => $keys) {
                    foreach ($keys as $key) {
                        $expected = $regionId === $held->regionId ? $held->regionId : null;
                        $actual = $cache->getByKey($key)?->regionId;
                        if ($actual === $expected) {
                            continue;
                        }
                        $mismatches[] = sprintf(
                            'holding only region %d: getByKey(%s) is %s, expected %s',
                            $held->regionId,
                            self::render($key),
                            var_export($actual, true),
                            var_export($expected, true),
                        );
                    }
                }
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, 0));
    }

    // ========================================================================
    // RegionRangeClipper
    // ========================================================================

    /**
     * Every request of every layout, 170 in total, in both directions: the whole
     * keyspace, every boundary span and single region, and both directions of every
     * candidate pair. The tiling property runs over all of them, because a request
     * that is empty bytewise forward is the bytewise-valid one *in reverse* — dropping
     * it from this provider would delete 65 real reverse checks, not vacuous ones.
     *
     * @return iterable<string, array{string, string, string}>
     */
    public static function clipRanges(): iterable
    {
        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            foreach (BinaryKeyVectors::requestRanges($layout) as $index => [$startKey, $endKey]) {
                $name = sprintf('%s [%s, %s) #%d', $layout, self::render($startKey), self::render($endKey), $index);
                yield $name => [$layout, $startKey, $endKey];
            }
        }
    }

    /**
     * The 105 of those 170 that are non-empty bytewise in the forward direction. The
     * other 65 — the direction of a candidate pair that byte order calls empty — overlap
     * no region at all, so {@see BinaryKeyVectors::overlappingRegions()} hands the
     * clipper an empty list, the clipper correctly yields nothing and the reference
     * yields nothing: the differential would be asserting `[] === []`. That class of
     * request is proven where it can actually fail, by
     * {@see self::testScanIteratorRefusesRangesThatAreOnlyValidNumerically()} and by
     * the byte-order tests in `RegionRangeClipperTest`.
     *
     * @return iterable<string, array{string, string, string}>
     */
    public static function forwardClipRanges(): iterable
    {
        foreach (self::clipRanges() as $name => [$layout, $startKey, $endKey]) {
            if ($endKey === '' || strcmp($startKey, $endKey) < 0) {
                yield $name => [$layout, $startKey, $endKey];
            }
        }
    }

    #[DataProvider('forwardClipRanges')]
    public function testClipForwardMatchesTheReferenceIntersection(
        string $layout,
        string $startKey,
        string $endKey,
    ): void {
        // Production-shaped input: the regions that overlap the request, which is what
        // PdClient::scanRegions($startKey, $endKey) hands the clipper.
        $actual = $this->clip($this->clipper->clipForward(
            BinaryKeyVectors::overlappingRegions($layout, $startKey, $endKey),
            $startKey,
            $endKey,
        ));

        self::assertSame(BinaryKeyVectors::referenceForwardClip($layout, $startKey, $endKey), $actual);
    }

    /**
     * The tiling property, in both directions, for every layout and every request range:
     * the clipped sub-ranges cover the request exactly once.
     *
     * Derived from the vectors, not from the clipper. The request is cut at the bounds
     * of the regions it touches into maximal byte intervals, and each interval's own
     * lower bound is a key inside it. The sub-ranges are correct exactly when every one
     * of those keys falls in exactly one sub-range, and that sub-range is the region
     * the `strcmp` reference says owns the key. This is the assertion that would have
     * caught #186 even if every individual site had looked right in isolation, and the
     * only one here that is not a restatement of the formula under test.
     */
    #[DataProvider('clipRanges')]
    public function testClippedSubRangesTileTheRequestWithoutGapOrOverlap(
        string $layout,
        string $startKey,
        string $endKey,
    ): void {
        $forward = $this->clip($this->clipper->clipForward(
            BinaryKeyVectors::overlappingRegions($layout, $startKey, $endKey),
            $startKey,
            $endKey,
        ));
        $this->assertTiles($layout, $startKey, $endKey, $forward, 'clipForward', false);

        if ($this->reverseRangeIsEmpty($startKey, $endKey)) {
            return;
        }

        $reverse = $this->clip($this->clipper->clipReverse(
            array_reverse(BinaryKeyVectors::overlappingRegions($layout, $endKey, $startKey)),
            $startKey,
            $endKey,
        ));
        $this->assertTiles($layout, $endKey, $startKey, $reverse, 'clipReverse', true);
    }

    /**
     * `clipReverse()` over a request that is empty bytewise yields one degenerate
     * sub-range for the region that starts at the keyspace start, because the skip test
     * carries a `$scanEnd !== ''` guard (an empty lower bound means "the whole
     * keyspace", not "an empty range"). Remove that guard and the sub-range is skipped,
     * so a reverse scan of a whole layout silently loses its first region.
     *
     * The sub-range it yields is `('', '']` — and it is NOT empty and NOT
     * unobservable. On the wire `RawKvScanner::executeReverseScanForSubRange()` sets
     * `start_key = ''` and omits `end_key` entirely (under `if ($endKey !== '')`), and
     * an empty start key in a reverse `RawScan` means the whole keyspace: one unbounded
     * reverse `RawScan` against region 1's leader, in place of the per-region loop with
     * its per-region error handling and limit accounting. The rows it returns are the
     * right rows, so this is not data loss — but it is one RPC over the whole keyspace
     * where the loop asked for one bounded sub-range, and `reverseScan('', '')` reaches
     * it from the public API with no guard at all. Changing that is a `src/` change and
     * belongs in its own PR; the unbounded reverse `RawScan` is tracked separately. This
     * test exists for the other half of the statement: if the `$scanEnd !== ''` guard
     * ever goes, *this* test must fail, and nothing else in the repository does.
     */
    public function testClipReverseOverAnEmptyReverseRequest(): void
    {
        $regions = BinaryKeyVectors::layoutRegions(BinaryKeyVectors::SIX_REGION);

        $actual = $this->clip($this->clipper->clipReverse(array_reverse($regions), '', ''));

        self::assertSame([[1, '', '']], $actual);
    }

    /**
     * @param list<array{int, string, string}> $clips [regionId, startKey, endKey] as
     *        yielded: for clipForward() that triple is the byte interval
     *        [$startKey, $endKey), for clipReverse() it is the same interval with the
     *        two bounds swapped, because a reverse triple covers (endKey, startKey]
     */
    private function assertTiles(
        string $layout,
        string $startKey,
        string $endKey,
        array $clips,
        string $method,
        bool $reverse,
    ): void {
        $intervals = BinaryKeyVectors::maximalIntervals($layout, $startKey, $endKey);

        self::assertCount(
            count($intervals),
            $clips,
            sprintf(
                '%s over [%s, %s): one sub-range per maximal interval, no more',
                $method,
                self::render($startKey),
                self::render($endKey),
            ),
        );

        $mismatches = [];
        foreach ($intervals as [$key, $owner]) {
            $covering = [];
            foreach ($clips as [$regionId, $clipStart, $clipEnd]) {
                $low = $reverse ? $clipEnd : $clipStart;
                $high = $reverse ? $clipStart : $clipEnd;
                if (BinaryKeyVectors::referenceInRange($key, $low, $high)) {
                    $covering[] = $regionId;
                }
            }
            if ($covering === [$owner]) {
                continue;
            }
            $mismatches[] = sprintf(
                '%s is owned by region %d but is covered by [%s]',
                self::render($key),
                $owner,
                $covering === [] ? 'nothing' : implode(', ', $covering),
            );
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, count($intervals)));
    }

    // ========================================================================
    // ScanIterator pagination
    // ========================================================================

    /**
     * A scan iterator over a layout's key set must return every key inside the
     * requested range exactly once, in byte order, across however many pages it takes,
     * and must not spend more RPCs than that needs. The cursor is the last key's
     * immediate successor, so this is where a numeric comparison of `currentStartKey`
     * against `endKey` shows up: it either stops before the range ends (keys missing)
     * or never stops.
     */
    public function testScanIteratorReturnsEveryKeyOfEveryLayoutExactlyOnceInByteOrder(): void
    {
        $mismatches = [];
        $checked = 0;

        foreach (BinaryKeyVectors::layoutNames() as $layout) {
            $keys = BinaryKeyVectors::probeKeys($layout);
            $requests = [['', ''], ...BinaryKeyVectors::requestRanges($layout)];
            foreach ($requests as [$startKey, $endKey]) {
                foreach ([1, 2, 7, 1000] as $batchSize) {
                    $expected = array_values(array_filter(
                        $keys,
                        static fn (string $key): bool => BinaryKeyVectors::referenceInRange($key, $startKey, $endKey),
                    ));
                    $seen = $this->drain($keys, $startKey, $endKey, $batchSize);
                    $calls = $this->scanCalls;
                    // One page per batch, plus the empty page that ends a range whose
                    // row count is a multiple of the page size.
                    $expectedCalls = max(1, (int) ceil(count($expected) / $batchSize));
                    $range = sprintf('[%s, %s)', self::render($startKey), self::render($endKey));
                    ++$checked;

                    // Before the `continue` below, not after it: a cursor that pages
                    // wrongly while still returning the right keys in the right order is
                    // never flagged otherwise.
                    if ($calls > $expectedCalls + 1) {
                        $mismatches[] = sprintf(
                            'page size %d, %s: %d scans for %d keys, the cursor does not advance',
                            $batchSize,
                            $range,
                            $calls,
                            count($expected),
                        );
                    }

                    if ($seen === $expected) {
                        continue;
                    }
                    $mismatches[] = sprintf(
                        'page size %d, %s: got %d keys, expected %d (%s)',
                        $batchSize,
                        $range,
                        count($seen),
                        count($expected),
                        $this->firstDifference($seen, $expected),
                    );
                }
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, $checked));
    }

    /**
     * A range that is only valid numerically is empty bytewise and must be refused
     * before a single RPC — the mirror of the previous case, and the shape
     * `ScanIteratorTest` pins for one range. Every direction of every candidate pair is
     * checked, so the filter holds for the whole table; this is also what proves the 65
     * byte-empty forward requests excluded from {@see self::forwardClipRanges()}.
     */
    public function testScanIteratorRefusesRangesThatAreOnlyValidNumerically(): void
    {
        $mismatches = [];
        $checked = 0;

        foreach (BinaryKeyVectors::candidatePairs() as [$left, $right]) {
            foreach ([[$left, $right], [$right, $left]] as [$startKey, $endKey]) {
                if (strcmp($startKey, $endKey) < 0) {
                    continue;
                }
                // Keys that both orders consider inside the range, so a numeric
                // termination check would return them.
                $seen = $this->drain(['20', '3', '007'], $startKey, $endKey, 10);
                $calls = $this->scanCalls;
                ++$checked;
                if ($seen === [] && $calls === 0) {
                    continue;
                }
                $mismatches[] = sprintf(
                    '[%s, %s) is empty bytewise but PHP calls it a range: %d scans, %d keys',
                    self::render($startKey),
                    self::render($endKey),
                    $calls,
                    count($seen),
                );
            }
        }

        self::assertSame([], $mismatches, $this->summarize($mismatches, $checked));
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /**
     * A stand-in for the raw scan RPC: the keys of the range that fall inside the
     * requested bounds, in byte order, capped at `$limit`. `strcmp` only — a fake that
     * used PHP's own operators would be the bug under test.
     *
     * @param list<string> $keys
     * @return \Closure(string, string, int, bool, string): list<array{key: string, value: string}>
     */
    private function scanFn(array $keys): \Closure
    {
        // A pagination cursor that does not advance would page forever, so the fake
        // gives up after far more scans than any correct cursor needs. The cap turns a
        // hang into a key-count failure instead.
        $maxScans = (count($keys) + 1) * 8;

        return function (string $startKey, string $endKey, int $limit) use ($keys, $maxScans): array {
            ++$this->scanCalls;
            if ($this->scanCalls > $maxScans) {
                return [];
            }

            $page = [];
            foreach ($keys as $key) {
                if (strcmp($key, $startKey) < 0) {
                    continue;
                }
                if ($endKey !== '' && strcmp($key, $endKey) >= 0) {
                    break;
                }
                $page[] = ['key' => $key, 'value' => 'v'];
                if (count($page) >= $limit) {
                    break;
                }
            }

            return $page;
        };
    }

    /**
     * Iterate a scan iterator over $keys to exhaustion, collecting the keys through
     * `key()` rather than `iterator_to_array()`: a canonical decimal TiKV key is coerced
     * to an `int` array key (issue #261), which would make the comparison depend on
     * PHP's array-key rules instead of the byte order under test. Sets
     * {@see self::$scanCalls} to the number of RPCs issued.
     *
     * @param list<string> $keys
     * @return list<string>
     */
    private function drain(array $keys, string $startKey, string $endKey, int $batchSize): array
    {
        $this->scanCalls = 0;
        $iterator = new ScanIterator($this->scanFn($keys), $startKey, $endKey, $batchSize);

        $seen = [];
        foreach ($iterator as $_) {
            $seen[] = $iterator->key();
        }

        return $seen;
    }

    /**
     * @param \Generator<int, array{RegionInfo, string, string}> $results
     * @return list<array{int, string, string}>
     */
    private function clip(\Generator $results): array
    {
        $clips = [];
        foreach ($results as [$region, $startKey, $endKey]) {
            $clips[] = [$region->regionId, $startKey, $endKey];
        }

        return $clips;
    }

    /**
     * Whether the reverse request (endKey, startKey] holds no key at all. Two different
     * things answer true here and they are NOT equally unobservable:
     *
     * - the *byte-empty direction of a candidate pair*, e.g. `('20', '100']`: the wire
     *   request is `start='20' end='100' reverse=true`, and TiKV's scanner returns
     *   nothing for inverted reverse bounds, so `clipReverse()` yields no sub-range, no
     *   RPC is issued and the difference is genuinely unobservable. That is 90 of the
     *   170 requests.
     * - `reverseScan('', '')`, whose `('', '']` sub-range *is* issued, as one unbounded
     *   reverse `RawScan` — observable, and pinned apart in
     *   {@see self::testClipReverseOverAnEmptyReverseRequest()}.
     */
    private function reverseRangeIsEmpty(string $startKey, string $endKey): bool
    {
        return strcmp($endKey, $startKey) >= 0;
    }

    /**
     * A key as a test failure should show it: printable ASCII as-is, anything else as
     * hex, so a NUL or 0xFF byte is visible in the message.
     */
    private static function render(string $key): string
    {
        return preg_match('/^[\x20-\x7e]*$/', $key) === 1 ? '"' . $key . '"' : '"0x' . bin2hex($key) . '"';
    }

    /**
     * @param list<string> $mismatches
     */
    private function summarize(array $mismatches, int $checked): string
    {
        return sprintf(
            'byte order disagrees with the reference on %d of %d checks, first: %s',
            count($mismatches),
            $checked,
            implode('; ', array_slice($mismatches, 0, 3)),
        );
    }

    /**
     * @param list<string> $actual
     * @param list<string> $expected
     */
    private function firstDifference(array $actual, array $expected): string
    {
        foreach ($expected as $index => $key) {
            if (($actual[$index] ?? null) !== $key) {
                return sprintf(
                    'position %d is %s, expected %s',
                    $index,
                    isset($actual[$index]) ? self::render($actual[$index]) : 'missing',
                    self::render($key),
                );
            }
        }

        return sprintf('%d extra keys', count($actual) - count($expected));
    }
}
