<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use PHPUnit\Framework\TestCase;

/**
 * Issue #187 (RAW-02): the client's own invariant is that a key is either
 * routed to a region or the call fails. `RegionResolver::batchResolveRegions()`
 * fails closed (issue #244), so a grouping loop that finds no region for a
 * key it submitted is looking at a broken invariant — and the ways to react to
 * that are not equivalent:
 *
 *  - **throw** (what every loop does now, through
 *    {@see \CrazyGoat\TiKV\Client\Region\RegionGrouper::resolvedRegion()}):
 *    the caller learns the batch was not applied;
 *  - **skip**: `batchPut()`/`batchDelete()`/`ingest()` return `void` and
 *    report success for a write that was never sent, and `batchGet()` hands
 *    back a `null` indistinguishable from a missing key. This is the bug;
 *  - **log**: same data loss, one grep deeper.
 *
 * The loop-level throw is unreachable through the public API (`RegionResolver`
 * is `final`, and a partial map cannot be produced), so nothing in the
 * behavioural suite can stop a future edit from turning it back into a
 * `continue`. This test is the machine-enforced half of the fix, the same
 * pragmatic stand-in as `ExceptionMessageRedactionGuardTest` (issue #269) and
 * `AtomicModeDocumentationGuardTest` (issue #367): it reads the sources and
 * fails if the shape comes back.
 *
 * ## Scope: deliberately broad, with a reviewed allowlist
 *
 * The rule is "no `if (<null check>) { continue; }` anywhere in `src/Client`",
 * *not* "no `$region === null`", so a new grouping loop in a new file is
 * caught whatever its variable is called — a rule keyed on the variable name
 * is defeated by `$r`, which is what three of the five sites used.
 *
 * Five legitimate sites remain and are allowlisted by **count**, each
 * discarding an *internal bookkeeping entry* (a cache node, a malformed
 * configured endpoint) rather than a key the caller asked to write — which is
 * the whole distinction. Counting rather than matching the condition text is
 * deliberate: the count is invariant under reformatting and comment edits, and
 * it forces a *conscious* review — adding or removing one of these sites fails
 * the test until the allowlist and its reason below are updated — while a
 * new null-checked `continue` anywhere still fails with the site's source in
 * the message.
 */
class NoSilentRegionDropGuardTest extends TestCase
{
    private const CLIENT_SOURCE_DIR = __DIR__ . '/../../../src/Client';

    /**
     * Reviewed exceptions, as `file (relative to src/Client) => how many
     * null-checked `continue`s that file may contain`.
     *
     *  - `Cache/RegionCache.php` (2): `sweepExpired()` pops a heap node and
     *    discards it when the entry it described no longer matches, and
     *    `rebuildExpiryHeap()` skips an entry whose version is gone. Both
     *    discard a cache node; nothing is written to TiKV.
     *  - `Region/RegionResolver.php` (3): the host-policy helpers iterate the
     *    *configured* PD endpoints and skip one that is not a bare
     *    `host:port`. Skipping a malformed endpoint from a config list is the
     *    documented behaviour, and there is no key in those loops to drop.
     *
     * @var array<string, int>
     */
    private const ALLOWED = [
        'Cache/RegionCache.php' => 2,
        'Region/RegionResolver.php' => 3,
    ];

    public function testNoLoopInTheClientSkipsWorkOnANullCheck(): void
    {
        $violations = [];

        foreach ($this->phpFilesUnder(self::CLIENT_SOURCE_DIR) as [$file, $relative]) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            $found = $this->nullCheckedContinues($contents);
            if ($found === []) {
                continue;
            }

            $expected = self::ALLOWED[$relative] ?? 0;
            if (count($found) !== $expected) {
                $violations[] = sprintf(
                    "%s: %d null-checked continue(s), %d reviewed (issue #187)\n    %s",
                    $relative,
                    count($found),
                    $expected,
                    implode("\n    ", $found),
                );
            }
        }

        self::assertSame(
            [],
            $violations,
            "A value is silently skipped on a null check. A key whose region cannot be "
            . "resolved must fail the call loudly instead — see "
            . "RegionGrouper::resolvedRegion() and issue #187. If a site here is "
            . "legitimate, review it and update the ALLOWED map with its reason.\n"
            . implode("\n", $violations),
        );
    }

    /**
     * Locates every `if (<cond comparing something against null by identity>)
     * { continue; }` and returns the conditions for the failure message.
     *
     * Tokenised rather than regexed: the condition can contain parentheses
     * and arbitrary nesting, and `$region === null`, `null === $region` and
     * `$r !== null` must all be recognised whatever the spacing. Braceless
     * `if (…) continue;` is accepted too, so the check cannot be evaded by
     * dropping the braces.
     *
     * @return list<string>
     */
    private function nullCheckedContinues(string $code): array
    {
        $tokens = $this->significantTokens($code);
        $count = count($tokens);
        $conditions = [];

        for ($i = 0; $i < $count; $i++) {
            $if = $tokens[$i];
            if (!is_array($if) || $if[0] !== T_IF || ($tokens[$i + 1] ?? null) !== '(') {
                continue;
            }

            $close = $this->matchingParenthesis($tokens, $i + 1);
            if ($close === null) {
                continue;
            }

            $condition = array_slice($tokens, $i + 2, $close - $i - 2);
            if (!$this->comparesAgainstNull($condition)) {
                continue;
            }

            $body = $tokens[$close + 1] ?? null;
            if ($body === '{') {
                $body = $tokens[$close + 2] ?? null;
            }
            if (is_array($body) && $body[0] === T_CONTINUE) {
                $conditions[] = sprintf('line %d: %s', $if[2], $this->render($condition));
            }
        }

        return $conditions;
    }

    /**
     * `token_get_all()`'s array tokens are `[id, text, line]`.
     *
     * @return list<array{int, string, int}|string>
     */
    private function significantTokens(string $code): array
    {
        $kept = [];
        foreach (token_get_all($code) as $token) {
            if (
                is_array($token)
                && in_array($token[0], [T_WHITESPACE, T_COMMENT, T_DOC_COMMENT], true)
            ) {
                continue;
            }
            $kept[] = $token;
        }

        return $kept;
    }

    /**
     * Index of the `)` closing the `(` at $open, or null when unbalanced.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function matchingParenthesis(array $tokens, int $open): ?int
    {
        $depth = 0;
        $count = count($tokens);

        for ($i = $open; $i < $count; $i++) {
            if ($tokens[$i] === '(') {
                $depth++;
            } elseif ($tokens[$i] === ')') {
                $depth--;
                if ($depth === 0) {
                    return $i;
                }
            }
        }

        return null;
    }

    /**
     * @param array<int, array{int, string, int}|string> $condition
     */
    private function comparesAgainstNull(array $condition): bool
    {
        $identityCompared = false;
        $mentionsNull = false;

        foreach ($condition as $token) {
            if (!is_array($token)) {
                $mentionsNull = $mentionsNull || $token === 'null';
                continue;
            }
            $identityCompared = $identityCompared
                || in_array($token[0], [T_IS_IDENTICAL, T_IS_NOT_IDENTICAL], true);
            $mentionsNull = $mentionsNull || $token[1] === 'null';
        }

        return $identityCompared && $mentionsNull;
    }

    /**
     * Tokens concatenated verbatim. Only used for the failure message, so it
     * favours determinism over prettiness.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function render(array $tokens): string
    {
        $text = '';
        foreach ($tokens as $token) {
            $text .= is_array($token) ? $token[1] : $token;
        }

        return $text;
    }

    /**
     * @return list<array{string, string}> absolute path and path relative to $dir
     */
    private function phpFilesUnder(string $dir): array
    {
        $root = (string) realpath($dir);
        $files = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $files[] = [$entry->getPathname(), substr($entry->getPathname(), strlen($root) + 1)];
            }
        }

        usort($files, static fn (array $a, array $b): int => $a[1] <=> $b[1]);

        return $files;
    }
}
