<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Util;

/**
 * Byte-wise ordering for TiKV keys — the single comparison seam of this
 * client (issue #186).
 *
 * ## Why PHP's relational operators must never touch keys
 *
 * TiKV sorts keys by **unsigned byte value** (client-go: `bytes.Compare`;
 * client-rust: the `Ord` impl on `Vec<u8>`). PHP 8 does not always compare
 * strings that way: when *both* operands of `<`, `<=`, `>` or `>=` are
 * "numeric strings" — strings that parse as numbers per PHP's own grammar —
 * PHP switches from byte comparison to **numeric** comparison. The two
 * orderings disagree exactly where real applications store keys:
 *
 * - `"20" > "100"` is **false** in PHP (20 vs 100) but **true** bytewise
 *   (`'2'` = 0x32 > `'1'` = 0x31). A scan that stops on the first "key past
 *   the end" therefore stops in the wrong place.
 * - `"1e3" == "1000"` and `"007" == "7"` are **true** in PHP, but they are
 *   two distinct TiKV keys: region routing splits them apart, and an
 *   `in`-style test merges them.
 *
 * The consequences are silent data-level bugs, not exceptions:
 *
 * - **wrong region routing** — a key is looked up in a region that does not
 *   contain it, so PD/TiKV answer with the wrong (or no) region;
 * - **skipped regions in scans** — pagination resumes from the last returned
 *   key, so a mis-ordered comparison ends the scan early (or loops) and keys
 *   in between are never returned;
 * - **`deleteRange()` clipping bounds outward** — the clipped range becomes
 *   wider than requested, so keys *outside* `[startKey, endKey)` are deleted.
 *
 * Every key comparison in this library therefore goes through this class.
 * `cmp()` delegates to `strcmp()`, which is byte-wise and locale-independent
 * by definition; the named helpers exist so the call sites read as intent
 * ("is this key before the region end?") instead of as a three-way sign
 * test. A custom PHPStan rule
 * (`CrazyGoat\TiKV\Phpstan\KeyOrderComparisonRule`) reports relational
 * operators applied to two key-like strings, so a bare `<`/`>` cannot come
 * back unnoticed.
 *
 * Ranges are always **half-open `[start, end)`** — the same convention TiKV
 * uses for region bounds — and an **empty end key means +infinity**, which
 * is how the last region of the keyspace is represented.
 */
final class KeyOrder
{
    /**
     * Byte-wise three-way comparison.
     *
     * @return int negative when $a sorts before $b, 0 when equal, positive
     *     when $a sorts after $b
     */
    public static function cmp(string $a, string $b): int
    {
        return strcmp($a, $b);
    }

    /** True when $a sorts strictly before $b bytewise. */
    public static function lt(string $a, string $b): bool
    {
        return strcmp($a, $b) < 0;
    }

    /** True when $a sorts before or equal to $b bytewise. */
    public static function lte(string $a, string $b): bool
    {
        return strcmp($a, $b) <= 0;
    }

    /** True when $a sorts strictly after $b bytewise. */
    public static function gt(string $a, string $b): bool
    {
        return strcmp($a, $b) > 0;
    }

    /** True when $a sorts after or equal to $b bytewise. */
    public static function gte(string $a, string $b): bool
    {
        return strcmp($a, $b) >= 0;
    }

    /** True when $a and $b are the same byte string. */
    public static function eq(string $a, string $b): bool
    {
        return strcmp($a, $b) === 0;
    }

    /**
     * Whether $key falls inside the half-open range [start, end) — the
     * membership test for a region or a clipped sub-range. An empty $end
     * means +infinity, so the last region contains every key from $start on.
     */
    public static function inRange(string $key, string $start, string $end): bool
    {
        return self::gte($key, $start) && ($end === '' || self::lt($key, $end));
    }

    /**
     * The immediate byte successor of $key: the smallest key strictly
     * greater than $key.
     *
     * Appending a NUL byte works for *every* byte string, including one that
     * already ends in (or contains) a NUL — TiKV does not forbid NUL bytes in
     * keys and this client does not reject them either, so the argument must
     * not assume it. Let X be any key with X > $key. Either
     *
     *  - X extends $key, and its first extra byte is at least 0x00 (the
     *    lowest byte value), so X >= $key . "\x00"; or
     *  - X differs from $key at an earlier position, where it has the
     *    greater byte. $key . "\x00" shares the prefix $key with X and has
     *    $key's byte at that position, so X > $key . "\x00" as well.
     *
     * So no key lies strictly between $key and $key . "\x00", and the
     * half-open range [$key, $key . "\x00") contains exactly $key. All three
     * call sites depend on precisely that: the inclusive region bound
     * (`RegionResolver::batchResolveRegions()` passes the successor of the
     * batch's maximum key to `scanRegions()`, or the region beginning exactly
     * at that key is excluded — issue #244), and the two forward-pagination
     * cursors of a scan (`RawKvScanner`, `ScanIterator`), which must resume
     * after the last key returned without repeating it.
     */
    public static function successor(string $key): string
    {
        return $key . "\x00";
    }
}
