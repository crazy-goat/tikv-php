<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #723: a worktree branch must be rebased onto the
 * default branch right before the pull request is opened.
 *
 * `bin/worktree.sh` fetches `origin/<default>` immediately before it creates
 * the branch, so the base is current at creation. The branch then ages while
 * it is coded and reviewed: three of the eight review rounds that motivated
 * the issue had to deal with commits that had landed after the worktree was
 * created, and two of them changed exactly what the branch documented (new
 * option keys, new transaction entry points). A worktree script alone cannot
 * prevent that, because creation and review are minutes or hours apart, so
 * the workflow has to rebase again in its pull-request step.
 *
 * Losing either half brings the stale base back: without the fetch the branch
 * starts from whatever `origin/<default>` happened to be, and without the
 * rebase it stays behind after creation. Both are pinned here rather than in
 * the shell script, because the repository's other process invariants are
 * already tests under `tests/Unit/Ci` and run in the unit matrix.
 */
final class WorktreeBaseGuardTest extends TestCase
{
    public function testTheWorktreeScriptFetchesBeforeItCreatesTheBranch(): void
    {
        $script = $this->read('bin/worktree.sh');

        $fetch = strpos($script, 'git -C "$root" fetch origin "$default"');
        self::assertNotFalse(
            $fetch,
            'bin/worktree.sh must fetch origin/<default> before it creates the worktree branch '
            . '(issue #723).',
        );

        $add = strpos($script, 'git -C "$root" worktree add -b "$branch"');
        self::assertNotFalse(
            $add,
            'bin/worktree.sh must create the worktree branch from origin/<default> (issue #723).',
        );

        self::assertLessThan(
            $add,
            $fetch,
            'bin/worktree.sh must fetch origin/<default> before "git worktree add", otherwise the '
            . 'branch starts from a stale base (issue #723).',
        );
    }

    public function testTheWorkflowRebasesBeforeThePullRequest(): void
    {
        $step = $this->section($this->read('docs/workflow.md'), '## 5. ', '## 6. ');

        $fetch = strpos($step, 'git fetch origin');
        self::assertNotFalse(
            $fetch,
            'The pull-request step of docs/workflow.md must fetch origin before rebasing '
            . '(issue #723).',
        );

        $rebase = strpos($step, 'git rebase origin/');
        self::assertNotFalse(
            $rebase,
            'The pull-request step of docs/workflow.md must rebase the branch onto origin/<default> '
            . 'right before the pull request is opened (issue #723).',
        );

        $push = strpos($step, 'git push -u origin HEAD');
        self::assertNotFalse(
            $push,
            'The pull-request step of docs/workflow.md must still push the branch (issue #723).',
        );

        self::assertLessThan(
            $push,
            $rebase,
            'docs/workflow.md must rebase before it pushes; rebasing after the pull request has '
            . 'been reviewed tests against a base that will never land (issue #723).',
        );
    }

    private function read(string $file): string
    {
        $content = file_get_contents(__DIR__ . '/../../../' . $file);
        self::assertIsString($content, sprintf('Could not read %s', $file));

        return $content;
    }

    private function section(string $markdown, string $start, string $end): string
    {
        $from = strpos($markdown, $start);
        self::assertNotFalse($from, sprintf('Missing section "%s" in docs/workflow.md', $start));

        $to = strpos($markdown, $end, $from);
        self::assertNotFalse($to, sprintf('Missing section "%s" in docs/workflow.md', $end));

        return substr($markdown, $from, $to - $from);
    }
}
