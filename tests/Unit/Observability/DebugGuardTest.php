<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use PHPUnit\Framework\TestCase;

/**
 * Guard for issue #715 (PERF): the debug-level probe must cover every hot
 * path, not only the region cache.
 *
 * The behavioural tests of `RegionCacheDebugLoggingTest` prove the property
 * for the cache's own paths; this test states the rule over the source tree:
 *
 * - no `debug()` / `SafeLogging::log(..., 'debug', ...)` call that redacts a
 *   key may sit on an unguarded path, so a call site added later cannot
 *   reintroduce the per-attempt `hash_hmac`;
 * - every component that decides the answer at construction keeps its
 *   `debugEnabled` flag and keeps every debug record behind it.
 *
 * The scan is token-based, so a comment or a docblock mentioning `debug()`
 * does not count as a call. It matches the redactor by its short or its
 * fully-qualified name, and it accepts only the positive guard form:
 * `if ($this->debugEnabled)`, never `if (!$this->debugEnabled)` or a
 * comparison to `false`.
 *
 * A redaction assigned to a local variable and passed to the logger later
 * is outside a token scan of the call's own arguments; no call site does
 * that today.
 */
final class DebugGuardTest extends TestCase
{
    /**
     * The property every guarded component decides once at construction.
     */
    private const GUARD_PROPERTY = 'debugEnabled';

    /**
     * Short name of the class whose `redact()` produces a key-dependent
     * string; matched in short and fully-qualified form.
     */
    private const REDACTOR_CLASS = 'KeyRedactor';

    /**
     * Components that decide at construction whether they emit debug records.
     *
     * @var list<string>
     */
    private const GUARDED_FILES = [
        'src/Client/Batch/BatchAsyncExecutor.php',
        'src/Client/Cache/RegionCache.php',
        'src/Client/Cache/StoreCache.php',
        'src/Client/Connection/PdClient.php',
        'src/Client/Connection/SafePointCache.php',
        'src/Client/Connection/TimestampOracle.php',
        'src/Client/Grpc/GrpcClient.php',
        'src/Client/RawKv/SstIngestor.php',
        'src/Client/Region/RegionCacheWarmer.php',
        'src/Client/TxnKv/LockResolver.php',
        'src/Client/TxnKv/TxnReader.php',
        'src/Client/TxnKv/TwoPhaseCommitter.php',
    ];

    public function testNoRedactedDebugCallSitsOnAnUnguardedPath(): void
    {
        $redacted = 0;
        $unguarded = [];
        foreach ($this->clientFiles() as $file) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            $scan = $this->scanRedactedDebugCalls($contents);
            $redacted += $scan['total'];
            foreach ($scan['unguarded'] as $line) {
                $unguarded[] = $this->relative($file) . ':' . $line;
            }
        }

        // A scan that finds nothing would pass vacuously.
        self::assertGreaterThan(0, $redacted, 'The scan found no redacted debug call at all.');
        self::assertSame(
            [],
            $unguarded,
            'Every debug call that redacts a key must sit behind the $this->' . self::GUARD_PROPERTY . " guard:\n"
            . implode("\n", $unguarded),
        );
    }

    public function testFullyQualifiedRedactionIsDetected(): void
    {
        $scan = $this->scanRedactedDebugCalls(
            '<?php $this->logger->debug("x", ["key" => '
            . '\CrazyGoat\TiKV\Client\Util\KeyRedactor::redact("k")]);',
        );

        self::assertSame(1, $scan['total']);
        self::assertSame([1], $scan['unguarded']);
    }

    public function testSafeLoggingDebugCallIsScannedOutsideTheGuardedList(): void
    {
        $scan = $this->scanRedactedDebugCalls(
            '<?php SafeLogging::log($this->logger, \'debug\', ["key" => KeyRedactor::redact("k")]);',
        );

        self::assertSame(1, $scan['total']);
        self::assertSame([1], $scan['unguarded']);
    }

    public function testInvertedGuardDoesNotCountAsAGuard(): void
    {
        $guarded = $this->scanRedactedDebugCalls(
            '<?php if (!$this->debugEnabled) { $this->logger->debug("x", '
            . '["key" => KeyRedactor::redact("k")]); }',
        );
        $compared = $this->scanRedactedDebugCalls(
            '<?php if ($this->debugEnabled === false) { $this->logger->debug("x", '
            . '["key" => KeyRedactor::redact("k")]); }',
        );

        self::assertSame([1], $guarded['unguarded']);
        self::assertSame([1], $compared['unguarded']);
    }

    public function testPositiveGuardIsAccepted(): void
    {
        $scan = $this->scanRedactedDebugCalls(
            '<?php if ($this->debugEnabled) { $this->logger->debug("x", '
            . '["key" => KeyRedactor::redact("k")]); }',
        );

        self::assertSame(1, $scan['total']);
        self::assertSame([], $scan['unguarded']);
    }

    public function testEveryGuardedComponentKeepsEveryDebugCallGuarded(): void
    {
        foreach (self::GUARDED_FILES as $relativePath) {
            $file = dirname(__DIR__, 3) . '/' . $relativePath;
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            self::assertStringContainsString(
                '$' . self::GUARD_PROPERTY,
                $contents,
                "{$relativePath} no longer has the debug guard property — update this test.",
            );
            self::assertStringContainsString(
                'DebugLogging::handlesDebug',
                $contents,
                "{$relativePath} no longer asks the logger at construction — update this test.",
            );

            $tokens = $this->tokens($file);
            $calls = $this->debugCalls($tokens) + $this->safeDebugCalls($tokens);
            self::assertGreaterThan(0, count($calls), "The scan found no debug call in {$relativePath}.");

            $guardRanges = $this->guardRanges($tokens);
            $unguarded = [];
            foreach ($calls as $index => $line) {
                if (!$this->isInside($index, $guardRanges)) {
                    $unguarded[] = sprintf('%s: line %d', $relativePath, $line);
                }
            }

            self::assertSame(
                [],
                $unguarded,
                "Every debug call in {$relativePath} must sit behind the \$this->"
                . self::GUARD_PROPERTY . " guard:\n"
                . implode("\n", $unguarded),
            );
        }
    }

    /**
     * Every PHP file of the client library.
     *
     * @return list<string>
     */
    private function clientFiles(): array
    {
        $root = dirname(__DIR__, 3) . '/src/Client';
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
        );

        $files = [];
        foreach ($iterator as $entry) {
            if (!$entry instanceof \SplFileInfo || $entry->getExtension() !== 'php') {
                continue;
            }
            $files[] = $entry->getPathname();
        }

        self::assertNotSame([], $files, 'The scan found no client source file.');

        return $files;
    }

    /**
     * Count the debug records whose arguments redact a key, and list the
     * lines of those that do not sit behind a positive guard.
     *
     * `debug()` and `SafeLogging::log(..., 'debug', ...)` are both scanned,
     * so a file outside {@see self::GUARDED_FILES} is not exempt.
     *
     * @return array{total: int, unguarded: list<int>}
     */
    private function scanRedactedDebugCalls(string $source): array
    {
        $tokens = token_get_all($source);
        $guardRanges = $this->guardRanges($tokens);

        $total = 0;
        $unguarded = [];
        foreach ($this->debugCalls($tokens) + $this->safeDebugCalls($tokens) as $index => $line) {
            if (!$this->callArgumentsContainClassName($tokens, $index, self::REDACTOR_CLASS)) {
                continue;
            }
            $total++;
            if (!$this->isInside($index, $guardRanges)) {
                $unguarded[] = $line;
            }
        }

        return ['total' => $total, 'unguarded' => $unguarded];
    }

    /**
     * @return array<int, array{int, string, int}|string>
     */
    private function tokens(string $file): array
    {
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return token_get_all($contents);
    }

    /**
     * The token indexes of the `->debug(` calls, mapped to their line number.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return array<int, int>
     */
    private function debugCalls(array $tokens): array
    {
        $calls = [];
        foreach ($tokens as $index => $token) {
            if (!is_array($token) || $token[0] !== T_STRING || $token[1] !== 'debug') {
                continue;
            }
            if (!$this->isMethodCall($tokens, $index)) {
                continue;
            }
            $calls[$index] = $token[2];
        }

        return $calls;
    }

    /**
     * The token indexes of the `SafeLogging::log(..., 'debug', ...)` calls,
     * mapped to their line number.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return array<int, int>
     */
    private function safeDebugCalls(array $tokens): array
    {
        $calls = [];
        foreach ($tokens as $index => $token) {
            if (!is_array($token) || $token[0] !== T_STRING || $token[1] !== 'log') {
                continue;
            }
            if (!$this->isStaticCall($tokens, $index)) {
                continue;
            }
            if (!$this->callArgumentTokensContain($tokens, $index, "'debug'")) {
                continue;
            }
            $calls[$index] = $token[2];
        }

        return $calls;
    }

    /**
     * The token ranges of the blocks guarded by the debug flag.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @return list<array{int, int}>
     */
    private function guardRanges(array $tokens): array
    {
        $ranges = [];
        foreach ($tokens as $index => $token) {
            if (!is_array($token) || $token[0] !== T_IF) {
                continue;
            }
            $range = $this->guardBlockRange($tokens, $index);
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

        $next = $this->nextMeaningful($tokens, $index + 1);
        if ($next >= $count || $tokens[$next] !== '(') {
            return false;
        }

        $previous = $this->previousMeaningful($tokens, $index - 1);

        return $previous >= 0
            && is_array($tokens[$previous])
            && $tokens[$previous][0] === T_OBJECT_OPERATOR;
    }

    /**
     * Whether the token at the given index is a static call — `::log(`.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function isStaticCall(array $tokens, int $index): bool
    {
        $count = count($tokens);

        $next = $this->nextMeaningful($tokens, $index + 1);
        if ($next >= $count || $tokens[$next] !== '(') {
            return false;
        }

        $previous = $this->previousMeaningful($tokens, $index - 1);

        return $previous >= 0
            && is_array($tokens[$previous])
            && $tokens[$previous][0] === T_DOUBLE_COLON;
    }

    /**
     * Whether the argument list of the call starting at the given index
     * contains the given token text.
     *
     * The text is matched against `T_CONSTANT_ENCAPSED_STRING` literals
     * (`'debug'`).
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function callArgumentTokensContain(array $tokens, int $index, string $needle): bool
    {
        return $this->callArgumentsContain($tokens, $index, static fn (array $token): bool => $token[1] === $needle);
    }

    /**
     * Whether the argument list of the call starting at the given index
     * contains the class name, in short (`KeyRedactor`) or fully-qualified
     * (`\CrazyGoat\TiKV\Client\Util\KeyRedactor`) form.
     *
     * PHP 8 tokenises a fully-qualified name as one
     * `T_NAME_FULLY_QUALIFIED` token, so comparing the short name alone
     * would miss that form.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function callArgumentsContainClassName(array $tokens, int $index, string $shortName): bool
    {
        return $this->callArgumentsContain(
            $tokens,
            $index,
            static function (array $token) use ($shortName): bool {
                if (!in_array($token[0], [T_STRING, T_NAME_QUALIFIED, T_NAME_FULLY_QUALIFIED], true)) {
                    return false;
                }
                $name = ltrim($token[1], '\\');

                return $name === $shortName || str_ends_with($name, '\\' . $shortName);
            },
        );
    }

    /**
     * Whether any argument token of the call starting at the given index
     * satisfies the matcher.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     * @param callable(array{int, string, int}): bool  $matches
     */
    private function callArgumentsContain(array $tokens, int $index, callable $matches): bool
    {
        $count = count($tokens);
        $open = $this->nextMeaningful($tokens, $index + 1);
        if ($open >= $count || $tokens[$open] !== '(') {
            return false;
        }

        $depth = 0;
        for ($i = $open; $i < $count; $i++) {
            $token = $tokens[$i];
            if ($token === '(') {
                $depth++;
                continue;
            }
            if ($token === ')') {
                $depth--;
                if ($depth === 0) {
                    return false;
                }
                continue;
            }
            if (is_array($token) && $matches($token)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function nextMeaningful(array $tokens, int $index): int
    {
        $count = count($tokens);
        while ($index < $count && is_array($tokens[$index]) && $tokens[$index][0] === T_WHITESPACE) {
            $index++;
        }

        return $index;
    }

    /**
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function previousMeaningful(array $tokens, int $index): int
    {
        while ($index >= 0 && is_array($tokens[$index]) && $tokens[$index][0] === T_WHITESPACE) {
            $index--;
        }

        return $index;
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

        $open = $this->nextMeaningful($tokens, $ifIndex + 1);
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
        if (!$this->guardConditionIsPositive($tokens, $open, $conditionEnd)) {
            return null;
        }

        $blockStart = $this->nextMeaningful($tokens, $conditionEnd + 1);
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
     * Whether the `if` condition accepts the guard property in its positive
     * form — `if ($this->debugEnabled)`, optionally compounded with `&&`.
     *
     * An inverted condition (`!$this->debugEnabled`, `=== false`,
     * `!== true`) logs exactly when debug is off, so it must not count as a
     * guard.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function guardConditionIsPositive(array $tokens, int $open, int $conditionEnd): bool
    {
        $first = $this->nextMeaningful($tokens, $open + 1);
        if ($first < $conditionEnd && $tokens[$first] === '!') {
            return false;
        }

        for ($i = $open + 1; $i < $conditionEnd; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || !$this->isGuardProperty($token)) {
                continue;
            }
            if ($this->guardPropertyIsNegated($tokens, $i, $open, $conditionEnd)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Whether a `!` or a comparison to a boolean literal inverts the guard
     * property at the given index.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function guardPropertyIsNegated(array $tokens, int $index, int $open, int $conditionEnd): bool
    {
        $previous = $this->previousMeaningful($tokens, $index - 1);
        if ($previous > $open && $tokens[$previous] === '!') {
            return true;
        }
        $literalBefore = $this->previousMeaningful($tokens, $previous - 1);
        if ($this->comparisonInvertsGuard($tokens, $previous, $literalBefore, $open, $conditionEnd)) {
            return true;
        }

        $next = $this->nextMeaningful($tokens, $index + 1);
        $literalAfter = $this->nextMeaningful($tokens, $next + 1);

        return $this->comparisonInvertsGuard($tokens, $next, $literalAfter, $open, $conditionEnd);
    }

    /**
     * Whether the comparison at the given token indexes inverts the guard.
     *
     * `=== false` and `== false` (or `!== true` / `!= true`) hold exactly
     * when debug is disabled. Either operand order is checked, and a
     * non-comparison or a non-boolean literal is not a guard concern.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function comparisonInvertsGuard(
        array $tokens,
        int $comparatorIndex,
        int $literalIndex,
        int $open,
        int $conditionEnd,
    ): bool {
        if ($comparatorIndex <= $open || $comparatorIndex >= $conditionEnd) {
            return false;
        }
        if ($literalIndex <= $open || $literalIndex >= $conditionEnd) {
            return false;
        }

        $comparator = $tokens[$comparatorIndex];
        $literal = $tokens[$literalIndex];
        if (!is_array($comparator) || !is_array($literal)) {
            return false;
        }

        $comparisons = [T_IS_IDENTICAL, T_IS_NOT_IDENTICAL, T_IS_EQUAL, T_IS_NOT_EQUAL];
        if (!in_array($comparator[0], $comparisons, true)) {
            return false;
        }

        $value = strtolower($literal[1]);
        if ($value !== 'true' && $value !== 'false') {
            return false;
        }

        $negatedComparison = $comparator[0] === T_IS_NOT_IDENTICAL || $comparator[0] === T_IS_NOT_EQUAL;

        return $negatedComparison ? $value === 'true' : $value === 'false';
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

    private function relative(string $file): string
    {
        return str_replace(dirname(__DIR__, 3) . '/', '', $file);
    }
}
