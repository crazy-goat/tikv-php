<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #367: every documented `compareAndSwap()` /
 * `putIfAbsent()` call site must show how atomic mode gets enabled, and every
 * documented quote of the resulting exception message must still be the one
 * the client throws.
 *
 * Both methods throw `InvalidStateException` unless
 * `setAtomicForCAS(true)` was called on the client, and the flag defaults to
 * `false` — so a snippet that shows the call without the prerequisite is
 * broken code that an engineer copies into production. Nothing else in the
 * test suite can catch that: the E2E suite turns atomic mode on for every
 * test in setUp() (tests/E2E/RawKvE2ETest.php), so the only cheap,
 * cluster-free check left is to read the documentation itself — the same
 * pragmatic stand-in as ExceptionMessageRedactionGuardTest for issue #269.
 *
 * The message matters as much as the call: docs/operations.md,
 * docs/error-handling.md and docs/troubleshooting.md quote the
 * `InvalidStateException` text verbatim so that grepping a production log for
 * it lands on the section explaining the fix. A quote that has drifted stops
 * matching the log, which is the one job it has, so the literal is compared
 * byte for byte against the one the client throws.
 *
 * The prerequisite rule is deliberately blunt: a code block that *calls* an
 * atomic operation must mention `setAtomicForCAS(` somewhere in that same
 * block (in code or in a comment). Which of the two it gets follows where the
 * code sits, not what it does:
 *
 *  - top-level driver code (the `// Usage` block that builds the example) gets
 *    a real call, because that code owns the client it configures;
 *  - code inside a function that *receives* the client as a parameter gets a
 *    comment instead, because reconfiguring a caller's client is a surprising
 *    side effect to bury in a helper. docs/operations.md's `incrementCounter()`
 *    and `acquireLock()`, and the classes in docs/advanced.md, are the shipped
 *    examples of that shape — and the `// Usage` block under each of them
 *    still has to make the real call.
 *
 * Two limits of that bluntness are known and accepted; making the check
 * clever enough to catch them is not worth the false positives:
 *
 *  - it is receiver-blind: a snippet that enables atomic mode on a *different*
 *    client than the one it then calls `compareAndSwap()` on passes;
 *  - it does not run code: a comment-only enable, or one inside a branch that
 *    never executes, satisfies it just the same.
 */
class AtomicModeDocumentationGuardTest extends TestCase
{
    /** Method calls that throw unless atomic mode is enabled. */
    private const ATOMIC_CALLS = ['->compareAndSwap(', '->putIfAbsent('];

    /**
     * The call that lifts the guard. Matched without its argument so a
     * snippet is free to pass a variable; every shipped snippet uses the
     * `setAtomicForCAS(true)` form the docs instruct readers to copy.
     */
    private const ENABLE_CALL = 'setAtomicForCAS(';

    /**
     * The opening words of the message, used to *find* the quotes in the docs.
     * Deliberately not the whole literal: a drifted quote has to be found in
     * order to be reported, so the marker only has to be stable. If the
     * client ever rewords the message, the assertion that this is still a
     * prefix of it fires here rather than letting the search go blind.
     */
    private const MESSAGE_MARKER = 'CompareAndSwap requires atomic mode';

    public function testEveryDocumentedAtomicCallSiteEnablesAtomicMode(): void
    {
        $violations = [];

        foreach ($this->documentationFiles() as $file) {
            $violations = array_merge($violations, $this->violationsInMarkdown($file));
        }

        foreach ($this->exampleFiles() as $file) {
            $violation = $this->violationInPhpFile($file);
            if ($violation !== null) {
                $violations[] = $violation;
            }
        }

        self::assertSame(
            [],
            $violations,
            "Documented atomic operations without an atomic-mode prerequisite "
            . "(every one of these throws InvalidStateException when copied):\n"
            . implode("\n", $violations),
        );
    }

    /**
     * The docs quote the InvalidStateException message verbatim so a reader can
     * grep a log for it; the quote is read out of the client, never restated
     * here, so a rewording in src/ cannot be papered over by this test.
     */
    public function testDocumentedAtomicModeMessageMatchesTheClient(): void
    {
        $expected = $this->atomicModeMessageFromClient();
        self::assertStringStartsWith(
            self::MESSAGE_MARKER,
            $expected,
            self::MESSAGE_MARKER . ' no longer opens the message thrown by '
            . 'RawKvClient::compareAndSwap(); update the marker in this test to '
            . 'match the new wording, then re-check every quote below.',
        );

        $drift = [];
        $quotes = 0;

        foreach ($this->documentationFiles() as $file) {
            foreach ($this->documentedAtomicModeMessages($file) as [$line, $quote]) {
                $quotes++;
                if ($quote !== $expected) {
                    $drift[] = sprintf(
                        "%s:%d quotes \"%s\", the client throws \"%s\"",
                        $this->relative($file),
                        $line,
                        $quote,
                        $expected,
                    );
                }
            }
        }

        // Without this the test would also pass if the docs stopped quoting the
        // message altogether, which loses the log-search affordance silently.
        self::assertGreaterThan(
            0,
            $quotes,
            'No documentation quotes the atomic-mode message any more, so grepping '
            . 'a log for it finds nothing. Quote it verbatim in docs/operations.md, '
            . 'docs/error-handling.md and docs/troubleshooting.md.',
        );

        self::assertSame(
            [],
            $drift,
            "Documented atomic-mode messages that no longer match the one thrown:\n"
            . implode("\n", $drift),
        );
    }

    /**
     * Markdown documentation: every code block that calls an atomic operation
     * has to mention the enabling call in that same block.
     *
     * @return list<string>
     */
    private function violationsInMarkdown(string $file): array
    {
        $violations = [];

        foreach ($this->codeBlocks($file) as $startLine => $body) {
            $violation = $this->violationInSnippet(
                sprintf('%s:%d', $this->relative($file), $startLine),
                $body,
            );
            if ($violation !== null) {
                $violations[] = $violation;
            }
        }

        return $violations;
    }

    /**
     * Every code block in a markdown file, keyed by the 1-based line the block
     * starts on.
     *
     * Both CommonMark block forms count, because either one can hide a broken
     * snippet: fenced blocks (three or more backticks *or* tildes, closing only
     * on a run of the same character at least as long with nothing else after
     * it — which is what lets a 4-backtick fence document a 3-backtick sample)
     * and 4-space-indented blocks (only when they follow a blank line, since
     * CommonMark does not let an indented run interrupt a paragraph).
     *
     * @return array<int, string>
     */
    private function codeBlocks(string $file): array
    {
        $lines = file($file, FILE_IGNORE_NEW_LINES);
        self::assertNotFalse($lines, "Unable to read {$file}");

        $blocks = [];
        $body = [];
        $blockStart = 0;
        $fenceCharacter = null;
        $fenceLength = 0;
        $indented = false;
        // An indented run can only be a code block when nothing but whitespace
        // precedes it; otherwise it is a lazy continuation of the paragraph.
        $afterBlankLine = true;

        foreach ($lines as $index => $line) {
            // Fences are indented when the block sits inside a list item.
            $trimmed = trim($line);
            $lineNumber = $index + 1;
            $fenceRun = $this->fenceRun($trimmed);

            if ($fenceCharacter !== null) {
                if (
                    $fenceRun !== null
                    && $fenceRun[0] === $fenceCharacter
                    && strlen($fenceRun) >= $fenceLength
                    // An info string is allowed on the opening fence only.
                    && $fenceRun === $trimmed
                ) {
                    $blocks[$blockStart] = implode("\n", $body);
                    $fenceCharacter = null;
                    $body = [];
                } else {
                    $body[] = $line;
                }

                $afterBlankLine = false;
                continue;
            }

            if ($fenceRun !== null) {
                $fenceCharacter = $fenceRun[0];
                $fenceLength = strlen($fenceRun);
                $blockStart = $lineNumber;
                $body = [];
                $afterBlankLine = false;
                continue;
            }

            if (str_starts_with($line, '    ') || str_starts_with($line, "\t")) {
                if (!$indented) {
                    if (!$afterBlankLine) {
                        continue;
                    }

                    $indented = true;
                    $blockStart = $lineNumber;
                    $body = [];
                }

                $body[] = $line;
                continue;
            }

            if ($indented) {
                $blocks[$blockStart] = implode("\n", $body);
                $indented = false;
                $body = [];
            }

            $afterBlankLine = $trimmed === '';
        }

        self::assertNull($fenceCharacter, "Unterminated code fence in {$file}");

        if ($indented) {
            $blocks[$blockStart] = implode("\n", $body);
        }

        return $blocks;
    }

    /**
     * The run of backticks or tildes with which a line opens or closes a
     * fenced block, or null when the line is not a fence at all. CommonMark
     * asks for three; the length is returned as well because only a run at
     * least as long as the opening one closes the block again.
     */
    private function fenceRun(string $trimmedLine): ?string
    {
        $matches = [];

        if (preg_match('/^(?<run>`{3,}|~{3,})/', $trimmedLine, $matches) !== 1) {
            return null;
        }

        return $matches['run'];
    }

    /**
     * The message literal as RawKvClient::compareAndSwap() throws it, read out
     * of src/ rather than restated here — a copy in this file could agree with
     * a reworded client while every quote in the docs kept the old text.
     */
    private function atomicModeMessageFromClient(): string
    {
        $file = dirname(__DIR__, 3) . '/src/Client/RawKv/RawKvClient.php';
        $source = file_get_contents($file);
        self::assertNotFalse($source, "Unable to read {$file}");

        $matches = [];
        $found = preg_match(
            "/throw new InvalidStateException\\('(?<message>[^']*atomic mode[^']*)'\\)/",
            $source,
            $matches,
        );

        self::assertSame(
            1,
            $found,
            "Unable to read the atomic-mode InvalidStateException message out of {$file}",
        );

        // The pattern has a named group, so preg_match always fills it in — and
        // the assertion above has already failed the test if it did not match.
        return $matches['message'];
    }

    /**
     * Every quote of the atomic-mode message in a markdown file, as
     * (line, quoted text) pairs.
     *
     * A quote runs to the closing backtick, double quote or end of line, which
     * is the only way the docs quote it. Anything else that opens with the
     * message's first words is reported as drift: a partial quote is exactly
     * what a log search cannot match.
     *
     * @return list<array{int, string}>
     */
    private function documentedAtomicModeMessages(string $file): array
    {
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        $quotes = [];
        $offset = 0;

        while (($start = strpos($contents, self::MESSAGE_MARKER, $offset)) !== false) {
            $length = strcspn($contents, "`\"\n", $start);
            $quotes[] = [substr_count($contents, "\n", 0, $start) + 1, substr($contents, $start, $length)];
            $offset = $start + 1;
        }

        return $quotes;
    }

    /**
     * Runnable examples: a file that calls an atomic operation has to enable
     * atomic mode (these are executed, not copied).
     */
    private function violationInPhpFile(string $file): ?string
    {
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return $this->violationInSnippet($this->relative($file), $contents);
    }

    private function violationInSnippet(string $where, string $code): ?string
    {
        $callsAtomic = false;
        foreach (self::ATOMIC_CALLS as $call) {
            if (str_contains($code, $call)) {
                $callsAtomic = true;
                break;
            }
        }

        if (!$callsAtomic || str_contains($code, self::ENABLE_CALL)) {
            return null;
        }

        return sprintf('%s calls compareAndSwap()/putIfAbsent() without setAtomicForCAS(true)', $where);
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

    /**
     * Runnable examples: a file that calls an atomic operation has to enable
     * atomic mode (these are executed, not copied).
     *
     * @return list<string>
     */
    private function exampleFiles(): array
    {
        $repoRoot = dirname(__DIR__, 3);
        $files = glob($repoRoot . '/examples/*.php') ?: [];

        return array_values(array_filter($files, is_file(...)));
    }

    private function relative(string $file): string
    {
        $repoRoot = dirname(__DIR__, 3) . '/';

        return str_starts_with($file, $repoRoot) ? substr($file, strlen($repoRoot)) : $file;
    }
}
