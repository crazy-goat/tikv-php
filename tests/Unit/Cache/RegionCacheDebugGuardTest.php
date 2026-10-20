<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;

/**
 * Guard for issue #300 (PERF-13): the region cache must not build a log
 * context — and must not run `KeyRedactor::redact()` — for a logger that
 * discards the record.
 *
 * The behavioural tests of `RegionCacheDebugLoggingTest` prove the property
 * for the paths they drive; this one states the rule over the whole file, so
 * a debug call site added later cannot come back unguarded.
 */
class RegionCacheDebugGuardTest extends TestCase
{
    /**
     * The property that decides whether the cache emits debug records.
     */
    private const GUARD_PROPERTY = 'debugEnabled';

    public function testEveryDebugCallIsGuarded(): void
    {
        $file = __DIR__ . '/../../../src/Client/Cache/RegionCache.php';
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        self::assertStringContainsString(
            '$' . self::GUARD_PROPERTY,
            $contents,
            'The debug guard property is gone — update this test.',
        );

        $tokens = token_get_all($contents);
        $calls = $this->debugCalls($tokens);

        // A scan that finds nothing would pass vacuously.
        self::assertGreaterThan(0, count($calls), 'The scan found no debug call at all.');

        $guardRanges = $this->guardRanges($tokens);
        $unguarded = [];
        foreach ($calls as $index => $line) {
            if (!$this->isInside($index, $guardRanges)) {
                $unguarded[] = sprintf('line %d', $line);
            }
        }

        self::assertSame(
            [],
            $unguarded,
            "Every debug call must sit behind the '\$this->" . self::GUARD_PROPERTY . "' guard:\n"
            . implode("\n", $unguarded),
        );
    }

    /**
     * The token indexes of the debug calls, mapped to their line number.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return array<int, int>
     */
    private function debugCalls(array $tokens): array
    {
        $count = count($tokens);
        $calls = [];
        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || $token[0] !== T_STRING || $token[1] !== 'debug') {
                continue;
            }
            if (!$this->isMethodCall($tokens, $i)) {
                continue;
            }
            $calls[$i] = $token[2];
        }

        return $calls;
    }

    /**
     * The token ranges of the blocks that are guarded by the debug flag.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return list<array{int, int}>
     */
    private function guardRanges(array $tokens): array
    {
        $count = count($tokens);
        $ranges = [];
        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || $token[0] !== T_IF) {
                continue;
            }
            $range = $this->guardBlockRange($tokens, $i);
            if ($range !== null) {
                $ranges[] = $range;
            }
        }

        return $ranges;
    }

    /**
     * Whether the token at the given index is inside one of the ranges.
     *
     * @param list<array{int, int}> $ranges
     */
    private function isInside(int $index, array $ranges): bool
    {
        foreach ($ranges as [$start, $end]) {
            if ($index > $start && $index < $end) {
                return true;
            }
        }

        return false;
    }

    /**
     * Whether the token at the given index is a method call — `->debug(`.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function isMethodCall(array $tokens, int $index): bool
    {
        $count = count($tokens);

        $next = $index + 1;
        while ($next < $count && is_array($tokens[$next]) && $tokens[$next][0] === T_WHITESPACE) {
            $next++;
        }
        if ($next >= $count || $tokens[$next] !== '(') {
            return false;
        }

        $previous = $index - 1;
        while ($previous >= 0 && is_array($tokens[$previous]) && $tokens[$previous][0] === T_WHITESPACE) {
            $previous--;
        }

        return $previous >= 0
            && is_array($tokens[$previous])
            && $tokens[$previous][0] === T_OBJECT_OPERATOR;
    }

    /**
     * The token range of the block of the `if` statement at the given index,
     * or null when the statement is not a guard.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return array{int, int}|null
     */
    private function guardBlockRange(array $tokens, int $ifIndex): ?array
    {
        $count = count($tokens);

        $open = $ifIndex + 1;
        while ($open < $count && is_array($tokens[$open]) && $tokens[$open][0] === T_WHITESPACE) {
            $open++;
        }
        if ($open >= $count || $tokens[$open] !== '(') {
            return null;
        }

        $depth = 0;
        $isGuard = false;
        $conditionEnd = null;
        for ($i = $open; $i < $count; $i++) {
            $token = $tokens[$i];
            if ($token === '(') {
                $depth++;
                continue;
            }
            if ($token === ')') {
                $depth--;
                if ($depth === 0) {
                    $conditionEnd = $i;
                    break;
                }
                continue;
            }
            if (is_array($token) && $this->isGuardProperty($token)) {
                $isGuard = true;
            }
        }
        if (!$isGuard || $conditionEnd === null) {
            return null;
        }

        $blockStart = $conditionEnd + 1;
        while ($blockStart < $count && is_array($tokens[$blockStart]) && $tokens[$blockStart][0] === T_WHITESPACE) {
            $blockStart++;
        }
        if ($blockStart >= $count || $tokens[$blockStart] !== '{') {
            return null;
        }

        $depth = 0;
        for ($i = $blockStart; $i < $count; $i++) {
            if ($tokens[$i] === '{') {
                $depth++;
                continue;
            }
            if ($tokens[$i] === '}') {
                $depth--;
                if ($depth === 0) {
                    return [$blockStart, $i];
                }
            }
        }

        return null;
    }

    /**
     * Whether the token names the guard property — `$this->debugEnabled` or
     * a bare `$debugEnabled`.
     *
     * @param array{int, string, int} $token
     */
    private function isGuardProperty(array $token): bool
    {
        if ($token[0] === T_STRING) {
            return $token[1] === self::GUARD_PROPERTY;
        }
        if ($token[0] === T_VARIABLE) {
            return $token[1] === '$' . self::GUARD_PROPERTY;
        }

        return false;
    }
}
