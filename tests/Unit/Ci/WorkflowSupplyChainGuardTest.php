<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #316 (audit finding SEC-13): the CI workflows
 * must not execute code from a mutable ref.
 *
 * A major-version tag (`actions/checkout@v7`) and a branch (`@main`) can be
 * force-moved by whoever controls the upstream repository, so a compromised
 * action publisher would run its code on this project's runner. A full
 * commit SHA cannot be moved. The workflow is read as text because the Unit
 * suite runs without `ext-yaml` (the same reason `CiWorkflowGateTest` parses
 * it with regular expressions).
 *
 * Dependabot keeps the pins current: the `github-actions` ecosystem opens a
 * pull request when an action publishes a release, and it rewrites the SHA
 * and the trailing version comment together.
 */
final class WorkflowSupplyChainGuardTest extends TestCase
{
    public function testEveryActionIsPinnedToACommitSha(): void
    {
        $references = 0;

        foreach ($this->workflowFiles() as $file) {
            foreach ($this->usesReferences($file) as $line => $reference) {
                ++$references;

                self::assertSame(
                    1,
                    preg_match('/@[0-9a-f]{40}$/', $reference['uses']),
                    sprintf(
                        '%s:%d uses "%s", which is not a full 40-character commit SHA. A tag '
                        . 'or branch can be force-moved by the action publisher, so it can run '
                        . 'unreviewed code on the runner (issue #316, SEC-13).',
                        $file,
                        $line,
                        $reference['uses'],
                    ),
                );

                self::assertNotSame(
                    '',
                    $reference['comment'],
                    sprintf(
                        '%s:%d pins an action without a trailing version comment. The comment '
                        . 'is what Dependabot uses to refresh the SHA (issue #316, SEC-13).',
                        $file,
                        $line,
                    ),
                );
            }
        }

        self::assertGreaterThan(0, $references, 'No "uses:" reference was found in the workflows');
    }

    public function testTheCiWorkflowGrantsOnlyReadAccess(): void
    {
        $workflow = $this->read('.github/workflows/ci.yml');

        self::assertSame(
            'contents: read',
            $this->topLevelBlock($workflow, 'permissions'),
            'The CI workflow must declare a top-level "permissions: contents: read" block, so '
            . 'GITHUB_TOKEN does not inherit the repository default write scopes (issue #316, '
            . 'SEC-13).',
        );
    }

    public function testDependabotWatchesGitHubActions(): void
    {
        $dependabot = $this->read('.github/dependabot.yml');

        self::assertStringContainsString(
            'package-ecosystem: github-actions',
            $dependabot,
            'Dependabot must watch the github-actions ecosystem, otherwise the SHA pins are '
            . 'never refreshed and the workflows drift behind security releases (issue #316, '
            . 'SEC-13).',
        );
    }

    /**
     * Every workflow file, as a path relative to the repository root. The
     * list is discovered rather than hard-coded, so a workflow added later
     * cannot escape the guard.
     *
     * @return list<string>
     */
    private function workflowFiles(): array
    {
        $root = dirname(__DIR__, 3);
        $files = glob($root . '/.github/workflows/*.y*ml');
        self::assertIsArray($files, 'Unable to list the workflow files');
        self::assertNotEmpty(
            $files,
            'The guard found no workflow files, so it cannot check the pins (issue #316, SEC-13).',
        );

        $relative = array_map(
            static fn (string $file): string => substr($file, strlen($root) + 1),
            $files,
        );
        sort($relative);

        return $relative;
    }

    /**
     * The `uses:` values of a workflow, keyed by their 1-based line number.
     * Local actions (`./…`) and container images (`docker://…`) are not
     * repository references and are skipped.
     *
     * A line that mentions `uses:` but does not match the strict pattern is
     * collected as unparsed and fails the test: silently skipping it would
     * let an unpinned action hide behind a malformed trailing comment.
     *
     * @return array<int, array{uses: string, comment: string}>
     */
    private function usesReferences(string $file): array
    {
        $lines = preg_split('/\R/', $this->read($file));
        self::assertIsArray($lines, sprintf('Unable to split %s into lines', $file));

        $references = [];
        $unparsed = [];

        foreach ($lines as $index => $line) {
            if (preg_match('/^\s*(?:-\s+)?uses:/', $line) !== 1) {
                continue;
            }

            $matches = [];
            if (preg_match('/^\s*(?:-\s+)?uses:\s*(\S+)(?:\s+#\s*(.*?))?\s*$/', $line, $matches) !== 1) {
                $unparsed[] = sprintf('%s:%d %s', $file, $index + 1, trim($line));

                continue;
            }

            $uses = $matches[1];
            if (str_starts_with($uses, './') || str_starts_with($uses, 'docker://')) {
                continue;
            }

            $references[$index + 1] = [
                'uses' => $uses,
                'comment' => $matches[2] ?? '',
            ];
        }

        self::assertSame(
            [],
            $unparsed,
            'The guard could not parse these "uses:" lines, so an unpinned action could hide '
            . 'behind a malformed trailing comment (issue #316, SEC-13):',
        );

        return $references;
    }

    /**
     * The body of a top-level YAML key, with the indentation stripped and
     * empty lines removed, so the result can be compared literally.
     */
    private function topLevelBlock(string $workflow, string $key): string
    {
        $lines = preg_split('/\R/', $workflow);
        self::assertIsArray($lines, 'Unable to split the workflow into lines');

        $body = [];
        $inside = false;

        foreach ($lines as $line) {
            if (!$inside) {
                if ($line === $key . ':') {
                    $inside = true;
                }

                continue;
            }

            if ($line !== '' && ($line[0] !== ' ' && $line[0] !== '#')) {
                break;
            }

            $trimmed = trim($line);
            if ($trimmed !== '') {
                $body[] = $trimmed;
            }
        }

        self::assertTrue($inside, sprintf('The workflow has no top-level "%s:" key', $key));

        return implode("\n", $body);
    }

    private function read(string $path): string
    {
        $absolute = dirname(__DIR__, 3) . '/' . $path;
        self::assertFileExists($absolute, sprintf('Expected %s to exist', $absolute));

        $contents = file_get_contents($absolute);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $absolute));

        return $contents;
    }
}
