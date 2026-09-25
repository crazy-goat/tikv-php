<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Util;

use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * KeyOrder is the byte-wise key comparison seam (issue #186). Every pair in
 * the data provider is one on which PHP's own relational operators give the
 * wrong answer, because PHP switches to a numeric comparison when both
 * operands are numeric strings while TiKV always compares bytewise.
 */
class KeyOrderTest extends TestCase
{
    /**
     * Byte-wise [a, b] pairs, with the sign strcmp() must return.
     *
     * @return iterable<string, array{string, string, int}>
     */
    public static function divergentPairs(): iterable
    {
        // "20" is bytewise AFTER "100" ('2' > '1'), but 20 < 100 numerically.
        yield 'decimal 20 vs 100' => ['20', '100', 1];
        yield 'decimal 100 vs 20' => ['100', '20', -1];
        yield 'decimal 3 vs 20' => ['3', '20', 1];
        // Numerically equal, but different bytes: two distinct TiKV keys.
        yield 'exponent 1e3 vs 1000' => ['1e3', '1000', 1];
        yield 'leading zeros 007 vs 7' => ['007', '7', -1];
        yield 'mixed width 0999 vs 100' => ['0999', '100', -1];
        yield 'mixed width 0100 vs 99' => ['0100', '99', -1];
    }

    #[DataProvider('divergentPairs')]
    public function testCmpMatchesByteOrderWherePhpOperatorsDisagree(string $a, string $b, int $expected): void
    {
        $this->assertSame($expected < 0, KeyOrder::lt($a, $b));
        $this->assertSame($expected > 0, KeyOrder::gt($a, $b));
        $this->assertSame($expected <= 0, KeyOrder::lte($a, $b));
        $this->assertSame($expected >= 0, KeyOrder::gte($a, $b));
        $this->assertFalse(KeyOrder::eq($a, $b));

        $comparison = KeyOrder::cmp($a, $b);
        $this->assertSame($expected < 0 ? -1 : 1, $comparison < 0 ? -1 : 1);

        // The whole point of the helper: PHP reaches a different verdict than
        // the byte order on at least one of < / > for every one of these
        // pairs ("1e3" and "1000" are even numerically equal in PHP, i.e.
        // neither less nor greater, while they are distinct TiKV keys).
        $phpLess = $a < $b;
        $phpGreater = $a > $b;
        $this->assertTrue(
            $phpLess !== KeyOrder::lt($a, $b) || $phpGreater !== KeyOrder::gt($a, $b),
            sprintf('the pair "%s" / "%s" must be one PHP mis-orders', $a, $b),
        );
    }

    public function testCmpReturnsZeroOnlyForIdenticalBytes(): void
    {
        $this->assertSame(0, KeyOrder::cmp('100', '100'));
        $this->assertSame(0, KeyOrder::cmp('', ''));
        $this->assertTrue(KeyOrder::eq('100', '100'));
        $this->assertTrue(KeyOrder::eq('', ''));

        // Equal numerically, different bytes: two distinct TiKV keys.
        $this->assertNotSame(0, KeyOrder::cmp('1e3', '1000'));
        $this->assertNotSame(0, KeyOrder::cmp('007', '7'));
        $this->assertFalse(KeyOrder::eq('1e3', '1000'));
        $this->assertFalse(KeyOrder::eq('007', '7'));
    }

    public function testCmpSignFollowsStrcmp(): void
    {
        foreach ([['a', 'b'], ['b', 'a'], ['a', 'a'], ['20', '100'], ["\xff", "\x00"]] as [$a, $b]) {
            $expected = strcmp($a, $b);
            $actual = KeyOrder::cmp($a, $b);
            $this->assertSame(
                $expected === 0 ? 0 : ($expected < 0 ? -1 : 1),
                $actual === 0 ? 0 : ($actual < 0 ? -1 : 1),
            );
        }
    }

    public function testBinaryKeysWithNulAndHighBytes(): void
    {
        // 0x00 is the lowest byte and 0xff the highest, so the order is
        // 0x00 < 0x01 < ... < 0x7f < 0x80 < ... < 0xff.
        $this->assertTrue(KeyOrder::lt("\x00", 'a'));
        $this->assertTrue(KeyOrder::lt('a', "\xff"));
        $this->assertTrue(KeyOrder::lt("\x01", "\xff"));
        $this->assertTrue(KeyOrder::lt("\x7f", "\x80"));
        $this->assertTrue(KeyOrder::gte("\xff", "\xfe"));
        $this->assertFalse(KeyOrder::lt("\xff", "\xfe"));

        $this->assertTrue(KeyOrder::eq("\x00\xff", "\x00\xff"));
        $this->assertFalse(KeyOrder::eq("\x00\xff", "\x00\xfe"));
        $this->assertTrue(KeyOrder::inRange("\x80", "\x7f", "\xff"));
        $this->assertFalse(KeyOrder::inRange("\xff", "\x7f", "\xff"));
    }

    public function testInRangeUsesHalfOpenBounds(): void
    {
        // Inclusive start.
        $this->assertTrue(KeyOrder::inRange('b', 'b', 'd'));
        $this->assertTrue(KeyOrder::inRange('c', 'b', 'd'));

        // Exclusive end.
        $this->assertFalse(KeyOrder::inRange('d', 'b', 'd'));
        $this->assertFalse(KeyOrder::inRange('e', 'b', 'd'));

        // Outside on the low side.
        $this->assertFalse(KeyOrder::inRange('a', 'b', 'd'));

        // The empty range [x, x) contains nothing.
        $this->assertFalse(KeyOrder::inRange('b', 'b', 'b'));
    }

    public function testInRangeWithEmptyEndMeansInfinity(): void
    {
        $this->assertTrue(KeyOrder::inRange('a', '', ''));
        $this->assertTrue(KeyOrder::inRange('zzz', 'b', ''));
        $this->assertFalse(KeyOrder::inRange('a', 'b', ''));

        // Bytewise, not numeric: "100" sorts BEFORE "20" ('1' < '2'), so it
        // is outside ["20", +inf) — PHP's numeric comparison puts it inside.
        $this->assertTrue(KeyOrder::inRange('20', '20', ''));
        $this->assertFalse(KeyOrder::inRange('100', '20', ''));
        $this->assertTrue(KeyOrder::inRange('20', '100', ''));

        // "007" sorts BEFORE "7" bytewise even though PHP considers the two
        // numerically equal (and therefore neither less nor greater).
        $this->assertFalse(KeyOrder::inRange('007', '7', ''));
        $this->assertTrue(KeyOrder::inRange('7', '007', ''));
    }

    public function testInRangeOnDivergentNumericKeys(): void
    {
        // The deleteRange("20", "300") shape from issue #186. Bytewise the
        // order is "15" < "199" < "20" < "300" — a leading '1' sorts before
        // '2' — so only "20" and keys starting with "300" are inside. PHP's
        // numeric comparison claims 199 is inside [20, 300), which is how the
        // pre-#186 clipping deleted keys outside the requested range.
        $this->assertFalse(KeyOrder::inRange('15', '20', '300'));
        $this->assertFalse(KeyOrder::inRange('199', '20', '300'));
        $this->assertTrue(KeyOrder::inRange('20', '20', '300'));
        $this->assertTrue(KeyOrder::inRange('300', '20', '3000'));
        $this->assertFalse(KeyOrder::inRange('300', '20', '300'));
        // The verdict PHP gives for "199" against the same bounds (20 <= 199
        // and 199 < 300) is the opposite: it reads the key as inside.

        // Bytewise "0100" < "99" (leading '0' sorts first) while PHP compares
        // them as the numbers 100 and 99.
        $this->assertTrue(KeyOrder::inRange('0100', '0099', '0101'));
        $this->assertFalse(KeyOrder::inRange('99', '0100', '0101'));
        $this->assertTrue(KeyOrder::inRange('99', '0100', ''));
    }

    public function testSuccessorIsTheImmediateByteSuccessor(): void
    {
        $this->assertSame("a\x00", KeyOrder::successor('a'));
        $this->assertSame("\x00", KeyOrder::successor(''));
        $this->assertSame("20\x00", KeyOrder::successor('20'));

        // Strictly greater than the key itself, and lower than every
        // extension of it. "a\x00" is in the list on purpose: TiKV does not
        // forbid NUL bytes in keys, so the successor of a NUL-bearing key has
        // to be its immediate successor too (KeyOrder::successor()'s
        // docblock proves it for arbitrary byte strings).
        foreach (['20', '100', 'a', "\xff", '', "a\x00", "\x00"] as $key) {
            $successor = KeyOrder::successor($key);
            $this->assertTrue(KeyOrder::gt($successor, $key), 'successor must be greater than the key');
            $this->assertTrue(KeyOrder::lt($successor, $key . '!'), 'successor must be below any extension');
            $this->assertTrue(KeyOrder::lt($successor, $key . "\x01"));

            // The immediate-successor property: [key, successor(key)) contains
            // the key and nothing else — the reason an inclusive region bound
            // can be made half-open this way.
            $this->assertTrue(KeyOrder::inRange($key, $key, $successor));
            $this->assertFalse(KeyOrder::inRange($successor, $key, $successor));
            $this->assertFalse(KeyOrder::inRange($key . '!', $key, $successor));
        }

        $this->assertSame("a\x00\x00", KeyOrder::successor("a\x00"));
    }

    public function testSuccessorMakesAHalfOpenBoundInclusive(): void
    {
        // scanRegions($max, successor($max)) must include the region that
        // begins exactly at $max (issue #244), and [k, successor(k))
        // contains exactly k — no other key can fit between them.
        $this->assertTrue(KeyOrder::gte(KeyOrder::successor('k'), 'k'));
        $this->assertTrue(KeyOrder::inRange('k', 'k', KeyOrder::successor('k')));
        $this->assertFalse(KeyOrder::inRange('k!', 'k', KeyOrder::successor('k')));
        $this->assertFalse(KeyOrder::inRange('k', 'k', 'k'));
    }
}
