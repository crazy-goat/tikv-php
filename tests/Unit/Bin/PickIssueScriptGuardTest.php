<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Bin;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #461: `bin/pick-issue.sh` must not use process
 * substitution, and the per-issue work must not fork.
 *
 * The script wedged four times at 100 % CPU for 21 h on a macOS machine.
 * The root cause was a bash 3.2 footgun, not the GitHub API: a process
 * substitution used as a loop's input keeps its file descriptor open until
 * the *enclosing* loop ends, so a few hundred lookups exhausted
 * `RLIMIT_NOFILE` (256 by default) mid-run — and once the descriptor table
 * was full, the next `fork` wedged inside the macOS post-fork handler
 * (`xpc_atfork_child` -> `setenv` -> `malloc`), where `SIGTERM` is ignored.
 *
 * Nothing in the PHP suite can observe that: the script is not PHP, and the
 * failure mode is a hang, not a wrong value. The guard is therefore a source
 * scan of the script, the same pragmatic stand-in as
 * `ExceptionMessageRedactionGuardTest` (issue #269) and
 * `NoSilentRegionDropGuardTest` (issue #187). It states the rules the fix
 * established, so a future edit cannot bring the leak or the fork storm
 * back:
 *
 *  1. **No process substitution** (`<( ... )` / `>( ... )`) anywhere. That is
 *     the leak itself, and the script has no use for it: everything it reads
 *     comes from a file or from parameter expansion.
 *  2. **The label lookup is pure parameter expansion.** The old
 *     `has_label()` compared a needle against a here-string line by line;
 *     the `case` form needs no descriptor, no fork and no subshell.
 *  3. **The wall clock is read once for the whole run**, not once per issue.
 *     `date +%s` inside the scoring loop was a fork per issue.
 *  4. **The open-file limit is raised** to 4096 — defence in depth, so a
 *     descriptor that leaks anyway cannot exhaust the table.
 *  5. **The bash version guard is present.** bash 3.2 is the interpreter the
 *     leak happened on; the script warns on it instead of failing silently.
 *
 * The scan reads the script's *code* (comments stripped), so the prose that
 * documents the rule cannot trip the rule. The comment stripper is a
 * quote-aware approximation of the shell's own rule — a `#` starts a comment
 * when it is outside quotes and at the start of a word — which is exact for
 * this script (it contains no escaped quote inside a double-quoted string).
 */
class PickIssueScriptGuardTest extends TestCase
{
    private const SCRIPT_PATH = __DIR__ . '/../../../bin/pick-issue.sh';

    public function testScriptUsesNoProcessSubstitution(): void
    {
        $code = $this->codeWithoutComments();

        self::assertStringNotContainsString(
            '<(',
            $code,
            'bin/pick-issue.sh must not use process substitution: bash 3.2 leaks its '
            . 'file descriptors inside loops and wedged four runs at 100 % CPU (issue #461). '
            . 'Read the data from a file or expand it with parameter expansion instead.',
        );
        self::assertStringNotContainsString(
            '>(',
            $code,
            'bin/pick-issue.sh must not use process substitution (issue #461).',
        );
    }

    public function testLabelLookupIsPureParameterExpansion(): void
    {
        $body = $this->functionBody('has_label');

        self::assertStringContainsString(
            'case ',
            $body,
            'has_label() must match the label with a `case` pattern, not with a read loop.',
        );

        foreach (['read', 'while', 'for ', '$(', '<(', '<<<'] as $forbidden) {
            self::assertStringNotContainsString(
                $forbidden,
                $body,
                sprintf(
                    'has_label() must not contain "%s": it needs no descriptor, no fork and '
                    . 'no subshell (issue #461).',
                    $forbidden,
                ),
            );
        }
    }

    public function testWallClockIsReadOnceForTheWholeRun(): void
    {
        $code = $this->codeWithoutComments();

        self::assertSame(
            1,
            substr_count($code, 'date +%s'),
            'The wall clock must be read once for the whole run, not once per issue (issue #461).',
        );
        self::assertStringNotContainsString(
            'date',
            $this->functionBody('score_line'),
            'score_line() must not run a date command: the caller passes the wall clock in (issue #461).',
        );
    }

    public function testFileDescriptorLimitIsRaised(): void
    {
        $code = $this->codeWithoutComments();

        self::assertStringContainsString(
            'ulimit',
            $code,
            'bin/pick-issue.sh must raise the open-file limit as defence in depth (issue #461).',
        );
        self::assertStringContainsString(
            '4096',
            $code,
            'bin/pick-issue.sh must raise the open-file limit to 4096 (issue #461).',
        );
    }

    public function testBashVersionGuardIsPresent(): void
    {
        $code = $this->codeWithoutComments();

        self::assertStringContainsString(
            'BASH_VERSINFO',
            $code,
            'bin/pick-issue.sh must detect the bash version (issue #461).',
        );
        self::assertStringContainsString(
            '-lt 4',
            $code,
            'bin/pick-issue.sh must warn when the bash version is older than 4 (issue #461).',
        );
    }

    /**
     * The script's source with every comment removed.
     */
    private function codeWithoutComments(): string
    {
        $lines = explode("\n", $this->script());
        foreach ($lines as $index => $line) {
            $lines[$index] = $this->stripComment($line);
        }

        return implode("\n", $lines);
    }

    /**
     * The body of a shell function, without its `name() {` line and its
     * closing brace.
     */
    private function functionBody(string $name): string
    {
        $lines = explode("\n", $this->codeWithoutComments());
        $inside = false;
        $body = [];

        foreach ($lines as $line) {
            if (!$inside) {
                $inside = str_starts_with(trim($line), $name . '() {');
                continue;
            }
            if (trim($line) === '}') {
                return implode("\n", $body);
            }
            $body[] = $line;
        }

        self::fail(sprintf('Function %s() not found in bin/pick-issue.sh.', $name));
    }

    private function script(): string
    {
        $contents = file_get_contents(self::SCRIPT_PATH);
        self::assertNotFalse($contents, 'Unable to read ' . self::SCRIPT_PATH);

        return $contents;
    }

    /**
     * Cut a line at its comment: a `#` starts a comment when it is outside
     * quotes and at the start of a word.
     */
    private function stripComment(string $line): string
    {
        $length = strlen($line);
        $quote = null;

        for ($i = 0; $i < $length; $i++) {
            $char = $line[$i];

            if ($quote !== null) {
                if ($char === $quote) {
                    $quote = null;
                }
                continue;
            }

            if ($char === "'" || $char === '"') {
                $quote = $char;
                continue;
            }

            if ($char === '#' && ($i === 0 || $line[$i - 1] === ' ' || $line[$i - 1] === "\t")) {
                return substr($line, 0, $i);
            }
        }

        return $line;
    }
}
