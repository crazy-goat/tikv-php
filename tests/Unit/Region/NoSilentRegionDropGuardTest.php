<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use PHPUnit\Framework\Attributes\DataProvider;
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
 * The rule is "no `if (<absence check>) { continue; }` anywhere in
 * `src/Client`", *not* "no `$region === null`", so a new grouping loop in a
 * new file is caught whatever its variable is called — a rule keyed on the
 * variable name is defeated by `$r`, which is what three of the five sites
 * used. It is not keyed on the *spelling* of the check either: a rule keyed
 * on `=== null` alone is defeated by `if (!isset($resolved[$key])) { continue; }`
 * and by `if (!array_key_exists($key, $resolved)) { continue; }`, which are
 * the same silent drop written the way PHP makes it easy to write. That
 * evasion was measured, not assumed: both spellings at
 * `RegionGrouper::groupItemsByRegion()` and
 * `groupKeysByRegionBatch()` — the two loops a `set(K); commit()` pair
 * reaches — passed the whole 1648-test `Unit` suite before the rule was
 * broadened (issue #181).
 *
 * The rule deliberately **over-matches**: a *positive* `isset($resolved[$key])`
 * keeps the entry rather than dropping it, and is flagged anyway. Erring that
 * way costs one reviewed allowlist row and makes the allowlist the single
 * place a judgement about a loop lives; {@see testTheRuleRecognisesEveryAbsenceCheckSpelling()}
 * pins the recognised shapes so the broadening cannot be narrowed back
 * silently.
 *
 * Six legitimate sites remain and are allowlisted by **count**, each
 * discarding an *internal bookkeeping entry* (a cache node, a malformed
 * configured endpoint, a configuration key the caller did not supply) rather
 * than a key the caller asked to write — which is the whole distinction.
 * Counting rather than matching the condition text is deliberate: the count is
 * invariant under reformatting and comment edits, and it forces a
 * *conscious* review — adding or removing one of these sites fails the test
 * until the allowlist and its reason below are updated — while a
 * new absence-checked `continue` anywhere still fails with the site's source
 * in the message.
 */
class NoSilentRegionDropGuardTest extends TestCase
{
    private const CLIENT_SOURCE_DIR = __DIR__ . '/../../../src/Client';

    /**
     * Reviewed exceptions, as `file (relative to src/Client) => how many
     * absence-checked `continue`s that file may contain`.
     *
     *  - `Cache/RegionCache.php` (2): `sweepExpired()` pops a heap node and
     *    discards it when the entry it described no longer matches, and
     *    `rebuildExpiryHeap()` skips an entry whose version is gone. Both
     *    discard a cache node; nothing is written to TiKV.
     *  - `Connection/ConnectionFactory.php` (1):
     *    `resolveGrpcChannelArgs()` iterates the *known* gRPC option names and
     *    skips one the caller did not supply, leaving that channel argument at
     *    its documented default. It iterates a config list, not a key set.
     *  - `Region/RegionResolver.php` (3): the host-policy helpers iterate the
     *    *configured* PD endpoints and skip one that is not a bare
     *    `host:port`. Skipping a malformed endpoint from a config list is the
     *    documented behaviour, and there is no key in those loops to drop.
     *
     * @var array<string, int>
     */
    private const ALLOWED = [
        'Cache/RegionCache.php' => 2,
        'Connection/ConnectionFactory.php' => 1,
        'Region/RegionResolver.php' => 3,
    ];

    public function testNoLoopInTheClientSkipsWorkOnAnAbsenceCheck(): void
    {
        $violations = [];

        foreach ($this->phpFilesUnder(self::CLIENT_SOURCE_DIR) as [$file, $relative]) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            $found = $this->absenceCheckedContinues($contents);
            if ($found === []) {
                continue;
            }

            $expected = self::ALLOWED[$relative] ?? 0;
            if (count($found) !== $expected) {
                $violations[] = sprintf(
                    "%s: %d absence-checked continue(s), %d reviewed (issue #187)\n    %s",
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
            "A value is silently skipped because it is absent. A key whose region cannot be "
            . "resolved must fail the call loudly instead — see "
            . "RegionGrouper::resolvedRegion() and issue #187. If a site here is "
            . "legitimate, review it and update the ALLOWED map with its reason.\n"
            . implode("\n", $violations),
        );
    }

    /**
     * The rule must recognise an absence test under every spelling, so that
     * rewriting a guarded `continue` in a different one does not quietly
     * return the tree to the state #187 removed.
     *
     * Driven through {@see absenceCheckedContinues()} by reflection because it
     * is the rule's own implementation — the same reflection-over-a-private-seam
     * pattern `GrpcClientChannelArgsTest` uses. The negative rows matter as much
     * as the positive ones: a rule that flagged every `if` in the tree would
     * still fail a violation and be useless as a guard, because the allowlist
     * would be unmaintainable.
     */
    #[DataProvider('absenceCheckShapes')]
    public function testTheRuleRecognisesEveryAbsenceCheckSpelling(string $condition, bool $expected): void
    {
        $code = sprintf('<?php foreach ($keys as $key) { if (%s) { continue; } }', $condition);

        $detector = new \ReflectionMethod(self::class, 'absenceCheckedContinues');

        self::assertSame($expected, $detector->invoke($this, $code) !== [], $condition);
    }

    /**
     * @return iterable<string, array{string, bool}>
     */
    public static function absenceCheckShapes(): iterable
    {
        yield 'identity against null' => ['$resolved[$key] === null', true];
        yield 'reversed identity against null' => ['null === $resolved[$key]', true];
        yield 'not-identical against null' => ['$resolved[$key] !== null', true];
        yield 'negated isset' => ['!isset($resolved[$key])', true];
        yield 'negated array_key_exists' => ['!array_key_exists($key, $resolved)', true];
        yield 'empty' => ['empty($resolved[$key])', true];
        yield 'isset compared to false' => ['isset($resolved[$key]) === false', true];
        yield 'isset not-true' => ['isset($resolved[$key]) !== true', true];
        yield 'nested parentheses' => ['($region ?? $resolved[$key]) === null', true];
        yield 'a compound condition' => ['$resolved[$key] === null || $flag', true];
        // Over-matched on purpose: a *positive* isset keeps the entry rather
        // than dropping it, and is flagged anyway so the allowlist stays the
        // one place a judgement about a loop lives.
        yield 'positive isset (over-matched by design)' => ['isset($resolved[$key])', true];
        // Not absence checks: a value- or key-based condition, and a `break`
        // (which ends the loop instead of skipping anything).
        yield 'a value comparison' => ['$key === ""', false];
        yield 'a counter condition' => ['$index > 3', false];
    }

    public function testABreakIsNotAFlaggedSkip(): void
    {
        $detector = new \ReflectionMethod(self::class, 'absenceCheckedContinues');

        self::assertSame([], $detector->invoke(
            $this,
            '<?php foreach ($keys as $key) { if ($resolved[$key] === null) { break; } }',
        ));
    }

    /**
     * Locates every `if (<cond testing a value's absence>) { continue; }` and
     * returns the conditions for the failure message.
     *
     * Tokenised rather than regexed: the condition can contain parentheses
     * and arbitrary nesting, and `$region === null`, `null === $region`,
     * `$r !== null` and `!isset($resolved[$key])` must all be recognised
     * whatever the spacing. Braceless `if (…) continue;` is accepted too, so
     * the check cannot be evaded by dropping the braces.
     *
     * @return list<string>
     */
    private function absenceCheckedContinues(string $code): array
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
            if (!$this->testsAbsence($condition)) {
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
     * True when the condition tests a value's *absence*, in any of the
     * spellings PHP offers:
     *
     *  - an identity comparison against `null` (`$region === null`,
     *    `null === $region`, `$r !== null`, …), optionally nested in
     *    parentheses or combined with `||`/`&&`; or
     *  - an existence test — `isset()`, `empty()` or `array_key_exists()` —
     *    anywhere in the condition.
     *
     * The second clause deliberately ignores *polarity*, so a positive
     * `isset($resolved[$key])` is flagged too. That is over-matching on
     * purpose (see {@see testTheRuleRecognisesEveryAbsenceCheckSpelling()}):
     * the cost is one reviewed allowlist row, and the alternative is a rule
     * whose evasion is a two-character edit.
     *
     * @param array<int, array{int, string, int}|string> $condition
     */
    private function testsAbsence(array $condition): bool
    {
        $identityCompared = false;
        $mentionsNull = false;
        $mentionsExistenceTest = false;

        foreach ($condition as $token) {
            if (!is_array($token)) {
                $mentionsNull = $mentionsNull || $token === 'null';
                continue;
            }
            $identityCompared = $identityCompared
                || in_array($token[0], [T_IS_IDENTICAL, T_IS_NOT_IDENTICAL], true);
            $mentionsNull = $mentionsNull || $token[1] === 'null';
            // `isset` and `empty` are their own token types; `array_key_exists`
            // comes through as T_STRING, so its *text* is what identifies it.
            $mentionsExistenceTest = $mentionsExistenceTest
                || in_array($token[0], [T_ISSET, T_EMPTY], true)
                || $token[1] === 'array_key_exists';
        }

        return ($identityCompared && $mentionsNull) || $mentionsExistenceTest;
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
