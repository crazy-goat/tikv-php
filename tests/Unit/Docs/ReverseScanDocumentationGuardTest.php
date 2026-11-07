<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #201 (RAW-16): every documented `reverseScan()`
 * example must pass the exclusive upper bound first.
 *
 * `reverseScan($startKey, $endKey)` scans `[$endKey, $startKey)` in descending
 * order, so the first argument has to sort after the second. The two examples
 * in `docs/operations.md` and the one in `README.md` once had them the other
 * way round; copied verbatim they returned `[]` without an error, while the
 * prose directly above them was correct — the worst possible teaching aid for
 * the most error-prone part of the RawKV API.
 *
 * The check is deliberately narrow and static:
 *
 *  - only fenced code blocks are read, so prose such as
 *    `reverseScan('', $lower)` cannot trip it;
 *  - only calls whose first two arguments are plain string literals are
 *    compared, because an expression such as `$newest . "\x00"` cannot be
 *    evaluated here without running the snippet;
 *  - equal bounds are allowed: `reverseScan('', '')` is the documented empty
 *    range, and the client itself only rejects `startKey < endKey`.
 *
 * The two remaining assertions keep the guard from passing vacuously and pin
 * the other half of the fix: at least one example demonstrates the `"\x00"`
 * suffix for an inclusive upper bound, and the reverse-scan documentation
 * points prefix-bounded reads at `RawKvSplitter::calculatePrefixEndKey()`.
 */
final class ReverseScanDocumentationGuardTest extends TestCase
{
    private const REVERSE_SCAN_HEADING = "\n### Reverse Scan\n";

    private const PREFIX_END_KEY_HELPER = 'calculatePrefixEndKey';

    public function testEveryLiteralReverseScanExampleOrdersItsBounds(): void
    {
        $violations = [];
        $checked = 0;

        foreach ($this->documentationFiles() as $file) {
            foreach ($this->fencedCodeBlocks($file) as $block) {
                foreach ($this->reverseScanCalls($block) as $arguments) {
                    $startKey = $this->stringLiteral($arguments[0] ?? null);
                    $endKey = $this->stringLiteral($arguments[1] ?? null);

                    if ($startKey === null || $endKey === null) {
                        continue;
                    }

                    $checked++;

                    if (strcmp($startKey, $endKey) < 0) {
                        $violations[] = sprintf(
                            "%s: reverseScan('%s', '%s') — the upper bound sorts before the lower one",
                            $this->relative($file),
                            $startKey,
                            $endKey,
                        );
                    }
                }
            }
        }

        // A guard that checks nothing is worse than no guard: it reports green
        // while the examples it exists for could be inverted again.
        self::assertGreaterThan(
            0,
            $checked,
            'No reverseScan() example with two string literals was found in README.md '
            . 'or docs/; the bounds-order guard has nothing to check.',
        );

        self::assertSame(
            [],
            $violations,
            "Documented reverseScan() examples with inverted bounds (the first argument "
            . "is the exclusive UPPER bound and must sort after the second):\n"
            . implode("\n", $violations),
        );
    }

    public function testAtLeastOneExampleShowsTheInclusiveUpperBoundIdiom(): void
    {
        $found = false;

        foreach ($this->documentationFiles() as $file) {
            foreach ($this->fencedCodeBlocks($file) as $block) {
                if (str_contains($block, 'reverseScan(') && str_contains($block, '\\x00')) {
                    $found = true;
                }
            }
        }

        self::assertTrue(
            $found,
            'No reverseScan() example demonstrates the "\\x00" suffix that makes the '
            . 'exclusive upper bound inclusive of one key (issue #201).',
        );
    }

    public function testReverseScanDocsReferenceThePrefixEndKeyHelper(): void
    {
        $section = $this->section($this->read('docs/operations.md'), self::REVERSE_SCAN_HEADING);

        self::assertStringContainsString(
            self::PREFIX_END_KEY_HELPER,
            $section,
            'The Reverse Scan section of docs/operations.md must point prefix-bounded '
            . 'reverse scans at RawKvSplitter::calculatePrefixEndKey(), which also handles '
            . 'a trailing 0xFF (issue #201).',
        );
    }

    /**
     * Every reverseScan() call in a code block, as a list of its top-level
     * arguments. A call that is not closed inside the block is ignored: it is
     * an elided snippet, not an example with bounds to check.
     *
     * @return list<list<string>>
     */
    private function reverseScanCalls(string $code): array
    {
        $calls = [];
        $needle = 'reverseScan(';
        $offset = 0;

        while (($position = strpos($code, $needle, $offset)) !== false) {
            $start = $position + strlen($needle);
            $end = $this->closingParenthesis($code, $start);

            if ($end === null) {
                break;
            }

            $calls[] = $this->splitArguments(substr($code, $start, $end - $start));
            $offset = $end + 1;
        }

        return $calls;
    }

    /**
     * The offset of the `)` matching the `(` just before $start, skipping
     * parentheses inside string literals.
     */
    private function closingParenthesis(string $code, int $start): ?int
    {
        $depth = 1;
        $quote = null;
        $length = strlen($code);

        for ($index = $start; $index < $length; $index++) {
            $character = $code[$index];

            if ($quote !== null) {
                if ($character === '\\') {
                    $index++;
                    continue;
                }

                if ($character === $quote) {
                    $quote = null;
                }

                continue;
            }

            if ($character === "'" || $character === '"') {
                $quote = $character;
                continue;
            }

            if ($character === '(') {
                $depth++;
                continue;
            }

            if ($character === ')') {
                $depth--;

                if ($depth === 0) {
                    return $index;
                }
            }
        }

        return null;
    }

    /**
     * Split an argument list on its top-level commas, keeping quoted strings
     * and nested brackets intact.
     *
     * @return list<string>
     */
    private function splitArguments(string $arguments): array
    {
        $parts = [];
        $current = '';
        $depth = 0;
        $quote = null;
        $length = strlen($arguments);

        for ($index = 0; $index < $length; $index++) {
            $character = $arguments[$index];

            if ($quote !== null) {
                $current .= $character;

                if ($character === '\\' && $index + 1 < $length) {
                    $current .= $arguments[++$index];
                    continue;
                }

                if ($character === $quote) {
                    $quote = null;
                }

                continue;
            }

            if ($character === "'" || $character === '"') {
                $quote = $character;
                $current .= $character;
                continue;
            }

            if ($character === '(' || $character === '[') {
                $depth++;
                $current .= $character;
                continue;
            }

            if ($character === ')' || $character === ']') {
                $depth--;
                $current .= $character;
                continue;
            }

            if ($character === ',' && $depth === 0) {
                $parts[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $character;
        }

        if (trim($current) !== '') {
            $parts[] = trim($current);
        }

        return $parts;
    }

    /**
     * The bytes of a single-quoted or double-quoted PHP string literal, or
     * null when the argument is not a plain literal (a variable, a method
     * call, a concatenation, …).
     */
    private function stringLiteral(?string $argument): ?string
    {
        if ($argument === null) {
            return null;
        }

        $argument = preg_replace('/^[A-Za-z_][A-Za-z0-9_]*\s*:\s*/', '', trim($argument));
        if ($argument === null) {
            return null;
        }

        $matches = [];

        if (preg_match("/^'((?:[^'\\\\]|\\\\.)*)'$/s", $argument, $matches) === 1) {
            // Single-quoted strings only recognise \\ and \'.
            return strtr($matches[1], ['\\\\' => '\\', "\\'" => "'"]);
        }

        if (preg_match('/^"((?:[^"\\\\]|\\\\.)*)"$/s', $argument, $matches) === 1) {
            return stripcslashes($matches[1]);
        }

        return null;
    }

    /**
     * Every fenced code block in a markdown file, with the block body only
     * (the fence lines are dropped). Indented blocks are not read: no shipped
     * reverse-scan example uses one.
     *
     * @return list<string>
     */
    private function fencedCodeBlocks(string $file): array
    {
        $contents = $this->read($file);
        $lines = preg_split('/\R/', $contents);
        self::assertNotFalse($lines, "Unable to split {$file} into lines");

        $blocks = [];
        $body = [];
        $fence = null;

        foreach ($lines as $line) {
            $trimmed = trim($line);

            if ($fence === null) {
                if (preg_match('/^(`{3,}|~{3,})/', $trimmed, $matches) === 1) {
                    $fence = $matches[1];
                    $body = [];
                }

                continue;
            }

            if (
                str_starts_with($trimmed, $fence)
                && trim(substr($trimmed, strlen($fence))) === ''
            ) {
                $blocks[] = implode("\n", $body);
                $fence = null;
                continue;
            }

            $body[] = $line;
        }

        self::assertNull($fence, "Unterminated code fence in {$file}");

        return $blocks;
    }

    /**
     * The text of a markdown section, from its heading up to the next heading
     * of the same or a shallower level.
     */
    private function section(string $contents, string $heading): string
    {
        $start = strpos($contents, $heading);
        self::assertNotFalse($start, sprintf('No "%s" section found.', trim($heading)));

        $level = 1;
        if (preg_match('/^#+/', trim($heading), $matches) === 1) {
            $level = strlen($matches[0]);
        }

        $rest = substr($contents, $start + strlen($heading));
        $end = strlen($rest);
        if (preg_match('/\n#{1,' . $level . '} /', $rest, $matches, PREG_OFFSET_CAPTURE) === 1) {
            $end = $matches[0][1];
        }

        return substr($rest, 0, $end);
    }

    /**
     * Every markdown file a reader may copy from: the whole docs/ tree plus
     * the README. Recursed so a new docs subdirectory is covered without
     * touching this test.
     *
     * @return list<string>
     */
    private function documentationFiles(): array
    {
        $repoRoot = dirname(__DIR__, 3);
        $files = [$repoRoot . '/README.md'];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($repoRoot . '/docs', \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'md') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }

    private function read(string $file): string
    {
        $path = str_starts_with($file, '/') ? $file : dirname(__DIR__, 3) . '/' . $file;
        $contents = file_get_contents($path);
        self::assertNotFalse($contents, "Unable to read {$path}");

        return $contents;
    }

    private function relative(string $file): string
    {
        $repoRoot = dirname(__DIR__, 3) . '/';

        return str_starts_with($file, $repoRoot) ? substr($file, strlen($repoRoot)) : $file;
    }
}
