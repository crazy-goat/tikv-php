<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #365 (audit finding CI-08): the end-to-end
 * jobs must run on every code change, not only on a path filter.
 *
 * The two E2E jobs are the only place the client is exercised against a
 * real TiKV cluster, and they used to depend on a dedicated `e2e` output of
 * the `changes` job, which matched `src/**`, `tests/E2E/**`, the compose
 * files and a few others. A pull request that touched only `tests/Unit/**`
 * therefore got no E2E signal at all — the one place a regression in the
 * unit tests' own assumptions would be caught. #617 removed that filter and
 * moved every heavy job onto the shared `code` output, so any change that is
 * not documentation-only now runs the whole matrix.
 *
 * Nothing keeps the narrower filter from coming back: a job that adds
 * `if: needs.changes.outputs.e2e == 'true'` is still valid YAML and still
 * green on the pull requests it skips. These checks make that a failing
 * test instead:
 *
 *  1. the workflow has no `paths:` / `paths-ignore:` trigger filter;
 *  2. every heavy job is gated on `needs.changes.outputs.code`, not on a
 *     narrower output of the `changes` job;
 *  3. the required `ci-ok` check needs every heavy job and treats a skipped
 *     one as a failure for a code change;
 *  4. a change under `tests/Unit/**` (or `src/**`, or `tests/E2E/**`) is
 *     not classified as documentation-only by the `docs-regex`, so it does
 *     reach the E2E jobs.
 *
 * The workflow is read as text: the Unit suite runs without the project's
 * dev extensions, so `ext-yaml` is not available (the same reason
 * `TestSuiteSelectorGuardTest` parses `phpunit.xml` with a regular
 * expression). The job names, their `if:` conditions and the `docs-regex`
 * are read out of the file rather than restated, so a rename cannot leave
 * this test agreeing with a workflow that no longer exists.
 */
final class CiWorkflowGateTest extends TestCase
{
    /**
     * The jobs that carry the test signal. Every one of them is expensive
     * and every one of them is required for a code change; a path filter on
     * any of them hides a whole class of changes.
     *
     * @var list<string>
     */
    private const CODE_GATED_JOBS = [
        'lint',
        'unit-tests',
        'grpc-unit-tests',
        'integration-tests',
        'e2e-tests',
        'e2e-tests-apiv2',
    ];

    /**
     * The condition every heavy job has to carry.
     */
    private const CODE_GATE = "if: needs.changes.outputs.code == 'true'";

    /**
     * Changed paths that must reach the E2E jobs: a unit test, production
     * code and an E2E test itself. The unit test is the case CI-08 named.
     *
     * @var list<string>
     */
    private const CODE_PATHS = [
        'tests/Unit/Ci/CiWorkflowGateTest.php',
        'src/Client/RawKv/RawKvClient.php',
        'tests/E2E/RawKvE2ETest.php',
    ];

    public function testTheWorkflowHasNoPathFilter(): void
    {
        self::assertSame(
            0,
            preg_match('/^\s+paths(?:-ignore)?:/m', $this->workflow()),
            'The CI workflow has a paths/paths-ignore trigger filter, so a pull request outside '
            . 'the filter gets no CI signal at all. The heavy jobs are gated on the shared '
            . '"code" output instead (issue #365).',
        );
    }

    public function testEveryHeavyJobRunsOnEveryCodeChange(): void
    {
        $jobs = $this->jobs();

        foreach (self::CODE_GATED_JOBS as $job) {
            self::assertArrayHasKey($job, $jobs, sprintf('The CI workflow has no "%s" job', $job));

            self::assertStringContainsString(
                self::CODE_GATE,
                $jobs[$job],
                sprintf(
                    'The "%s" job is not gated on the shared "code" output. A narrower gate '
                    . '(for example "needs.changes.outputs.e2e") means a pull request that '
                    . 'touches only tests/Unit/** gets no signal from this job (issue #365).',
                    $job,
                ),
            );
        }
    }

    public function testNoJobGatesOnANarrowerChangesOutput(): void
    {
        $matches = [];
        preg_match_all('/needs\.changes\.outputs\.([A-Za-z0-9_-]+)/', $this->workflow(), $matches);

        $outputs = array_values(array_unique($matches[1]));
        self::assertSame(
            ['code'],
            $outputs,
            'A CI job reads an output of the "changes" job other than "code". The heavy jobs '
            . 'must run on every code change, not on a path-filtered subset (issue #365).',
        );
    }

    public function testTheRequiredCheckNeedsEveryHeavyJob(): void
    {
        $jobs = $this->jobs();
        self::assertArrayHasKey('ci-ok', $jobs, 'The CI workflow has no "ci-ok" job');

        $needs = $this->jobNeeds($jobs['ci-ok']);

        foreach (array_merge(self::CODE_GATED_JOBS, ['changes', 'docs']) as $job) {
            self::assertContains(
                $job,
                $needs,
                sprintf(
                    'The required "ci-ok" check does not need "%s", so that job can fail or be '
                    . 'skipped without blocking the merge (issue #365).',
                    $job,
                ),
            );
        }

        self::assertStringContainsString(
            '[ "$r" = "success" ]',
            $jobs['ci-ok'],
            'The "ci-ok" job no longer requires every needed job to be "success" for a code '
            . 'change, so a skipped E2E job would still report green (issue #365).',
        );
    }

    public function testAUnitOnlyChangeIsNotDocumentationOnly(): void
    {
        $matches = [];
        preg_match('/docs-regex:\s*[\'"]([^\'"]+)[\'"]/', $this->workflow(), $matches);
        $regex = $matches[1] ?? '';

        self::assertNotSame(
            '',
            $regex,
            'The CI workflow has no docs-regex for the changes job, so the classification of a '
            . 'changed path cannot be checked.',
        );

        foreach (self::CODE_PATHS as $path) {
            self::assertSame(
                0,
                preg_match('#' . str_replace('#', '\\#', $regex) . '#', $path),
                sprintf(
                    '%s is classified as documentation-only by the docs-regex of the changes '
                    . 'job, so a pull request touching only that path skips the E2E jobs '
                    . '(issue #365).',
                    $path,
                ),
            );
        }
    }

    private function workflow(): string
    {
        $path = dirname(__DIR__, 3) . '/.github/workflows/ci.yml';
        self::assertFileExists($path, sprintf('Expected %s to exist', $path));

        $contents = file_get_contents($path);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }

    /**
     * The job blocks of the workflow, keyed by job name. A block runs from
     * its two-space-indented name to the next job name (or to the next
     * top-level key).
     *
     * @return array<string, string>
     */
    private function jobs(): array
    {
        $lines = preg_split('/\R/', $this->workflow());
        self::assertIsArray($lines, 'Unable to split the CI workflow into lines');

        $jobs = [];
        $current = null;
        $inJobs = false;

        foreach ($lines as $line) {
            if ($line === 'jobs:') {
                $inJobs = true;

                continue;
            }

            if (!$inJobs) {
                continue;
            }

            if ($line !== '' && $line[0] !== ' ' && $line[0] !== '#') {
                break;
            }

            if (preg_match('/^  ([A-Za-z0-9_-]+):\s*$/', $line, $matches) === 1) {
                $current = $matches[1];
                $jobs[$current] = '';

                continue;
            }

            if ($current !== null) {
                $jobs[$current] .= $line . "\n";
            }
        }

        return $jobs;
    }

    /**
     * The job names in the `needs:` list of a job block.
     *
     * @return list<string>
     */
    private function jobNeeds(string $block): array
    {
        $matches = [];
        $found = preg_match('/^\s{4}needs:\s*\[([^\]]*)\]/m', $block, $matches);

        self::assertSame(1, $found, 'A job block has no inline "needs: [...]" list');

        return array_map(trim(...), explode(',', $matches[1]));
    }
}
