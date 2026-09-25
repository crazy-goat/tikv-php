<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

/**
 * The numeric-boundary vectors of issue #232, run against the real
 * RegionCache.
 *
 * #186 routed every key comparison through `KeyOrder`, but the region-cache
 * tests that came with it (like every one before them) used `'a'`, `'m'`,
 * `'key1'` — ASCII from the middle of the byte range, exactly where PHP's
 * numeric-string comparison and TiKV's byte order agree, so the bug class
 * stayed invisible. The layout below is byte-sorted yet numerically
 * *inverted*, which is the only shape on which the two orderings disagree.
 */
#[CoversClass(RegionCache::class)]
final class RegionCacheNumericBoundaryTest extends TestCase
{
    private function region(int $id, string $start, string $end): RegionInfo
    {
        return new RegionInfo($id, $id, $id, 1, 1, $start, $end);
    }

    /**
     * Issue #232's layout, a valid binary-sorted keyspace: the boundaries
     * sort as '' < '1000' < '2000' < '30' < '400' < '999' by first byte
     * ('1' = 0x31 < '2' = 0x32 < '3' = 0x33 < '4' = 0x34 < '9' = 0x39).
     * Read as *numbers* they run the other way round for the multi-digit
     * boundaries, which is what makes the layout discriminating.
     *
     * @return list<RegionInfo>
     */
    private function numericLayout(): array
    {
        return [
            $this->region(1, '', '1000'),
            $this->region(2, '1000', '2000'),
            $this->region(3, '2000', '30'),
            $this->region(4, '30', '400'),
            $this->region(5, '400', '999'),
            $this->region(6, '999', ''),
        ];
    }

    private function seededCache(): RegionCache
    {
        $cache = new RegionCache();
        foreach ($this->numericLayout() as $region) {
            $cache->put($region);
        }

        return $cache;
    }

    public function testGetByKeyRoutesTheNumericBoundarySetToByteOrderedRegions(): void
    {
        $cache = $this->seededCache();

        // '0500' starts with '0' (0x30) < '1' (0x31), so it sorts before the
        // first boundary and falls in region 1 ["", "1000").
        self::assertSame(1, $cache->getByKey('0500')?->regionId);

        // '1000' IS region 1's exclusive end key, so it is region 2's
        // inclusive start key ["1000", "2000").
        self::assertSame(2, $cache->getByKey('1000')?->regionId);

        // '1500': second byte '5' > '0' puts it after '1000', first byte
        // '1' < '2' puts it before '2000' → region 2.
        self::assertSame(2, $cache->getByKey('1500')?->regionId);

        // '203' extends '200' with a last byte '3' > '0', so it sorts after
        // '2000', and its first byte '2' < '3' puts it before '30'. Region 3
        // owns exactly that byte-valid, numerically "impossible" range
        // ['2000', '30'); PHP's numeric comparison calls it empty.
        self::assertSame(3, $cache->getByKey('203')?->regionId);

        // '92': '9' > '4' puts it after '400', and '92' < '999' because the
        // second byte '2' < '9' → region 5 ['400', '999').
        self::assertSame(5, $cache->getByKey('92')?->regionId);

        // '999' is region 5's exclusive end and region 6's inclusive start.
        // Numerically it is the largest key in the layout, so a numeric
        // comparison puts it past '400' and '999' at once and cannot place it
        // where it belongs — between them.
        self::assertSame(6, $cache->getByKey('999')?->regionId);

        // '9995' extends '999', so it sorts after it and lands in the
        // unbounded last region ['999', '').
        self::assertSame(6, $cache->getByKey('9995')?->regionId);
    }

    public function testGetByKeyTreats1e3And1000AsDistinctKeys(): void
    {
        // PHP reads these two as the same number: "1e3" == "1000" is true,
        // and neither "1e3" < "1000" nor "1e3" > "1000" holds, so PHP has no
        // way to order them at all. TiKV sees two distinct keys, because
        // 'e' = 0x65 > '0' = 0x30 at the second byte, so '1e3' sorts after
        // '1000' and before '2000'.
        //
        // The layout therefore needs a boundary between them; the six-region
        // layout above cannot show the difference, since both keys fall in
        // ["1000", "2000") and a cache that merged them would answer with
        // that same region. Here the middle region ends where '1e3' starts,
        // so the two keys are separated only if the cache keeps them apart.
        $cache = new RegionCache();
        $cache->put($this->region(1, '', '1000'));
        $cache->put($this->region(2, '1000', '1e3'));
        $cache->put($this->region(3, '1e3', ''));

        self::assertSame(2, $cache->getByKey('1000')?->regionId);
        self::assertSame(3, $cache->getByKey('1e3')?->regionId);
    }

    /**
     * Issue #261's step 4/6: with the keyspace split at "100", the lookup of
     * "99" must be answered by the region that actually contains it.
     *
     * Bytewise "99" > "100" ('9' = 0x39 > '1' = 0x31), so ["100", +inf) is the
     * region that owns it and ["", "100") is not. A numeric comparison picked
     * the predecessor by 100 <= 99, landed on ["", "100"), and then let it
     * through the end check because "99" >= "100" also reads false
     * numerically — the cache served a region that does not contain the key,
     * which is the silent `not_found`/`KeyNotInRegion` churn of the issue.
     */
    public function testGetByKeyRoutes99ToTheRegionThatContainsIt(): void
    {
        $cache = new RegionCache();
        $cache->put($this->region(1, '', '100'));
        $cache->put($this->region(2, '100', ''));

        self::assertSame(2, $cache->getByKey('99')?->regionId);
        // The neighbouring boundaries still route where they belong: a leading
        // '0' sorts before the split, and the split key itself is region 2's
        // inclusive start.
        self::assertSame(1, $cache->getByKey('099')?->regionId);
        self::assertSame(2, $cache->getByKey('100')?->regionId);
    }

    /**
     * The same "99" against a cache holding ONLY ["100", +inf).
     *
     * The cache is a partial view of the keyspace: it knows one region, and
     * "99" is inside that region bytewise, so the byte-ordered predecessor
     * walk must find it. A numeric walk compares 100 <= 99, finds no
     * predecessor, and answers a miss — the false-negative side of the same
     * bug class, which costs a PD round trip on every lookup of a key the
     * client already has.
     *
     * NOTE: issue #261's acceptance criteria word this case as
     * "getByKey('99') returns null (a miss) for a cached region ['100', '')".
     * That expectation is inverted: bytewise "99" is *inside* ["100", +inf),
     * so returning the region is the correct answer and returning null is the
     * behaviour from before #321 (PR #462) — not from before #186, which is
     * what this docblock used to claim. #321 already routed the
     * `RegionCache` predecessor walk through `strcmp()`, and it is an ancestor
     * of #186, so the numeric `getByKey()` that answered a miss here was
     * already gone when #186 landed. Verified against `aaadc4c^` (7017c28):
     * that tree's numeric `binarySearch()` returns null for this case and
     * serves the WRONG region (`["", "100")`) in the two-region case above,
     * while `2ad8236^` (a4f898b, the tip before #186) answers both correctly.
     * Pinning null here would lock the bug in. The wrong-region case above is
     * the one the criteria are really after. The same deviation is recorded in
     * docs/helpers/decisions.md.
     */
    public function testGetByKeyServes99FromTheOnlyCachedRegion(): void
    {
        $cache = new RegionCache();
        $cache->put($this->region(1, '100', ''));

        self::assertSame(1, $cache->getByKey('99')?->regionId);
    }

    public function testGetByKeyAgreesWithAStrcmpReferenceForEveryDecimalKey(): void
    {
        // The issue's own differential check (step 3): decimal keys in
        // [0, 99999] against a strcmp-based reference. Pre-fix the auditor
        // measured ok=49 wrongRegion=5 miss=2946 of 3000 lookups.
        $regions = $this->numericLayout();
        $cache = $this->seededCache();

        $plain = $this->decimalKeys(3000, 99999);
        // The same 500 keys zero-padded to five digits ('500' → '0500'). A
        // (string) cast of an int can never contain a leading zero, yet that
        // is exactly the shape that moves a key across a boundary ('0500'
        // lands in region 1, '500' in region 2) and that PHP merges hardest:
        // "0500" == "500". Without this stream the differential run never
        // reaches region 1 at all — among the keys of [0, 99999] only '0',
        // '1', '10' and '100' (the prefixes of the '1000' boundary) sort
        // below it.
        $padded = array_map(
            static fn(string $key): string => str_pad($key, 5, '0', STR_PAD_LEFT),
            array_slice($plain, 0, 500),
        );

        $agreements = 0;
        $mismatches = [];
        foreach ([$plain, $padded] as $stream) {
            [$streamAgreements, $streamMismatches] = $this->compareWithReference($cache, $regions, $stream);
            $agreements += $streamAgreements;
            $mismatches = [...$mismatches, ...$streamMismatches];
        }

        self::assertSame(
            [],
            $mismatches,
            sprintf(
                'RegionCache::getByKey() must route every decimal key exactly like the '
                . 'strcmp reference; agreed on %d of %d keys, first disagreements: %s',
                $agreements,
                count($plain) + count($padded),
                implode('; ', array_slice($mismatches, 0, 5)),
            ),
        );
    }

    /**
     * Route every key through both implementations and report the
     * disagreements.
     *
     * @param list<RegionInfo> $regions
     * @param list<string> $keys
     * @return array{int, list<string>} the number of agreements and the
     *     human-readable disagreements
     */
    private function compareWithReference(RegionCache $cache, array $regions, array $keys): array
    {
        $agreements = 0;
        $mismatches = [];
        foreach ($keys as $key) {
            $expected = $this->referenceRegionId($regions, $key);
            $actual = $cache->getByKey($key)?->regionId;
            if ($actual === $expected) {
                ++$agreements;
                continue;
            }
            $mismatches[] = sprintf(
                'key=%s expected region %s, cache returned %s',
                $key,
                var_export($expected, true),
                var_export($actual, true),
            );
        }

        return [$agreements, $mismatches];
    }

    /**
     * The strcmp-based reference: the owning region of $key is the first one
     * in the byte-sorted layout that contains it. Deliberately a plain linear
     * scan over half-open [start, end) bounds, with no shared code with
     * RegionCache beyond the layout itself.
     *
     * @param list<RegionInfo> $regions
     */
    private function referenceRegionId(array $regions, string $key): ?int
    {
        foreach ($regions as $region) {
            $atOrAfterStart = $region->startKey === '' || strcmp($key, $region->startKey) >= 0;
            $beforeEnd = $region->endKey === '' || strcmp($key, $region->endKey) < 0;
            if ($atOrAfterStart && $beforeEnd) {
                return $region->regionId;
            }
        }

        return null;
    }

    /**
     * A deterministic xorshift32 stream rendered as decimal keys — no
     * rand()/mt_rand(), so a failure is reproducible from the seed alone.
     *
     * @return list<string>
     */
    private function decimalKeys(int $count, int $max): array
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
}
