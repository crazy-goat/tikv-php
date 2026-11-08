<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Guard for the post-rebase verification step in `docs/workflow.md` (issue #678).
 *
 * A rebase onto a moving `master` damaged a branch twice: a sentence vanished
 * from the commit message, and a conflict resolution dropped the final newline
 * of `docs/helpers/faq.md`. Neither PHPCS nor PHPStan looks at Markdown, and
 * the workflow said nothing about re-reading the message or the file endings,
 * so the damage was caught only when the reviewer diffed both by hand.
 *
 * The guard pins the four checks the step requires, the checklist reminder and
 * the links from the push and conflict-resolution steps, so a future rewrite
 * of the workflow cannot quietly drop one of them again.
 */
final class PostRebaseVerificationGuardTest extends TestCase
{
    /**
     * The commands the step must show verbatim, in the order it lists them.
     *
     * @var list<string>
     */
    private const REQUIRED_COMMANDS = [
        'git log -1 --format=%B',
        "git diff origin/master...HEAD | grep -n '^\\\\ No newline at end of file'",
        'git diff --check origin/master...HEAD',
        "git grep -n '^<<<<<<<'",
        'git diff --stat origin/master...HEAD',
    ];

    private const ANCHOR = '#after-a-rebase-or-an-amend';

    private const STEP_HEADING = '### After a rebase or an amend';

    private const REVIEW_HEADING = '## 4. Review (review subagent)';

    private const PUSH_HEADING = '## 5. Push, open the pull request, wait for CI';

    private const MERGE_HEADING = '## 6. Merge';

    private const FOLLOW_UP_HEADING = '## 7. Follow-up findings';

    public function testWorkflowDocumentsEveryPostRebaseCheck(): void
    {
        $section = $this->section($this->workflow(), self::STEP_HEADING, self::REVIEW_HEADING);

        $previousPosition = -1;

        foreach (self::REQUIRED_COMMANDS as $command) {
            $position = strpos($section, $command);

            self::assertNotFalse(
                $position,
                sprintf(
                    'docs/workflow.md does not contain the post-rebase command "%s" (issue #678).',
                    $command,
                ),
            );

            self::assertGreaterThan(
                $previousPosition,
                $position,
                sprintf(
                    'docs/workflow.md lists "%s" out of order; the checks must follow '
                    . 'message, newline, conflict markers, diff stat (issue #678).',
                    $command,
                ),
            );

            $previousPosition = $position;
        }
    }

    public function testChecklistRemindsTheCoderToRunTheChecks(): void
    {
        self::assertMatchesRegularExpression(
            '/^- \[ \] After every rebase or amend: .*checked$/m',
            $this->workflow(),
            'docs/workflow.md checklist has no post-rebase verification item (issue #678).',
        );
    }

    public function testPushAndConflictStepsLinkToTheChecks(): void
    {
        $workflow = $this->workflow();

        self::assertStringContainsString(
            sprintf('[post-rebase checks](%s)', self::ANCHOR),
            $this->section($workflow, self::PUSH_HEADING, self::MERGE_HEADING),
            'The step 5 "Push" section does not link to "#after-a-rebase-or-an-amend" (issue #678).',
        );

        self::assertStringContainsString(
            sprintf('[post-rebase checks](%s)', self::ANCHOR),
            $this->section($workflow, self::MERGE_HEADING, self::FOLLOW_UP_HEADING),
            'The step 6 "Merge" conflict paragraph does not link to '
            . '"#after-a-rebase-or-an-amend" (issue #678).',
        );
    }

    private function section(string $workflow, string $heading, string $nextHeading): string
    {
        $start = strpos($workflow, $heading);
        self::assertNotFalse(
            $start,
            sprintf('docs/workflow.md has no "%s" heading (issue #678).', $heading),
        );

        $end = strpos($workflow, $nextHeading, $start + strlen($heading));
        self::assertNotFalse(
            $end,
            sprintf(
                'docs/workflow.md has no "%s" heading after "%s" (issue #678).',
                $nextHeading,
                $heading,
            ),
        );

        return substr($workflow, $start, $end - $start);
    }

    private function workflow(): string
    {
        $contents = file_get_contents(dirname(__DIR__, 3) . '/docs/workflow.md');
        self::assertNotFalse($contents, 'Unable to read docs/workflow.md.');

        return $contents;
    }
}
