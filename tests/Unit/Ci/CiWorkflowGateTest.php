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
 *  1. the workflow has no `paths:` / `paths-ignore:` trigger filter, in
 *     either the block or the flow style;
 *  2. every heavy job is gated on exactly `needs.changes.outputs.code ==
 *     'true'` — an appended condition (`&& github.event_name == ...`)
 *     narrows the gate just as a different output would;
 *  3. the required `ci-ok` check runs unconditionally (`if: always()`),
 *     needs every heavy job, and fails for a code change unless every one
 *     of them reports `success`;
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
     * The condition every heavy job has to carry, as a whole line: a gate
     * that merely starts with this condition can still append a second one
     * (`&& github.event_name == 'pull_request'`) and skip a push to master.
     */
    private const CODE_GATE_PATTERN = '/^\s{4}if:\s*needs\.changes\.outputs\.code == \'true\'\s*$/m';

    /**
     * The exact lines of the `ci-ok` script that make a skipped or failed
     * job block the merge. A substring check would accept `|| true` or
     * `[ "$CODE" = "yes" ]`, which both leave the required check green.
     */
    private const CODE_OK_CONDITION = '/^\s*if \[ "\$CODE" = "true" \]; then\s*$/m';

    private const EVERY_RESULT_SUCCESS = '/^\s*\[ "\$r" = "success" \] \|\| exit 1\s*$/m';

    private const ALWAYS_RUN = '/^\s{4}if: always\(\)\s*$/m';

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
            preg_match('/\bpaths(?:-ignore)?\s*:/', $this->workflow()),
            'The CI workflow has a paths/paths-ignore trigger filter, in block or flow style, '
            . 'so a pull request outside the filter gets no CI signal at all. The heavy jobs '
            . 'are gated on the shared "code" output instead (issue #365).',
        );
    }

    public function testEveryHeavyJobRunsOnEveryCodeChange(): void
    {
        $jobs = $this->jobs();

        foreach (self::CODE_GATED_JOBS as $job) {
            self::assertArrayHasKey($job, $jobs, sprintf('The CI workflow has no "%s" job', $job));

            self::assertSame(
                1,
                preg_match(self::CODE_GATE_PATTERN, $jobs[$job]),
                sprintf(
                    'The "%s" job is not gated on exactly "needs.changes.outputs.code == '
                    . '\'true\'". A narrower gate (for example "needs.changes.outputs.e2e" or '
                    . 'an appended "&& github.event_name == \'pull_request\'") means a pull '
                    . 'request that touches only tests/Unit/** gets no signal from this job, '
                    . 'or a push to master skips it (issue #365).',
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

        self::assertSame(
            1,
            preg_match(self::ALWAYS_RUN, $jobs['ci-ok']),
            'The "ci-ok" job no longer runs with "if: always()", so GitHub skips it when a '
            . 'needed job fails or is skipped and the required check is never reported '
            . '(issue #365).',
        );

        self::assertSame(
            1,
            preg_match(self::CODE_OK_CONDITION, $jobs['ci-ok']),
            'The "ci-ok" job no longer checks that the change is a code change with '
            . '`if [ "$CODE" = "true" ]; then`, so the success loop can be skipped while the '
            . 'job still reports green (issue #365).',
        );

        self::assertSame(
            1,
            preg_match(self::EVERY_RESULT_SUCCESS, $jobs['ci-ok']),
            'The "ci-ok" job no longer fails with `[ "$r" = "success" ] || exit 1` for a code '
            . 'change, so a failed or skipped E2E job would still report green (issue #365).',
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
