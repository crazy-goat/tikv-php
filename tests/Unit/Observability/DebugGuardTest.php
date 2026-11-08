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
 * - no `debug()` call that redacts a key may sit on an unguarded path, so a
 *   call site added later cannot reintroduce the per-attempt `hash_hmac`;
 * - every component that decides the answer at construction keeps its
 *   `debugEnabled` flag and keeps every debug record behind it.
 *
 * The scan is token-based, so a comment or a docblock mentioning `debug()`
 * does not count as a call.
 */
final class DebugGuardTest extends TestCase
{
    /**
     * The property every guarded component decides once at construction.
     */
    private const GUARD_PROPERTY = 'debugEnabled';

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
        'src/Client/TxnKv/LockResolver.php',
        'src/Client/TxnKv/TxnReader.php',
        'src/Client/TxnKv/TwoPhaseCommitter.php',
    ];

    public function testNoRedactedDebugCallSitsOnAnUnguardedPath(): void
    {
        $redacted = 0;
        $unguarded = [];
        foreach ($this->clientFiles() as $file) {
            $tokens = $this->tokens($file);
            $guardRanges = $this->guardRanges($tokens);

            foreach ($this->debugCalls($tokens) as $index => $line) {
                if (!$this->callArgumentTokensContain($tokens, $index, 'KeyRedactor')) {
                    continue;
                }
                $redacted++;
                if (!$this->isInside($index, $guardRanges)) {
                    $unguarded[] = $this->relative($file) . ':' . $line;
                }
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
     * The text is matched against `T_STRING` names (`KeyRedactor`) and
     * `T_CONSTANT_ENCAPSED_STRING` literals (`'debug'`).
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function callArgumentTokensContain(array $tokens, int $index, string $needle): bool
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
            if (is_array($token) && $token[1] === $needle) {
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
