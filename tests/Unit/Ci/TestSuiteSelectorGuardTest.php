<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #361: a test runner that selects nothing must
 * not be green.
 *
 * `composer test:e2e` used to select a test suite named `E2E`, which
 * `phpunit.xml` never declares — the end-to-end suites are `E2E-RawKV`,
 * `E2E-TxnKV` and `E2E-ApiV2`. PHPUnit answers an unknown selector with
 * "No tests executed!" and exit code 0, so the command was green having run
 * nothing at all. The same is true for every other runner that names a
 * suite: the shell scripts, the Makefile, the compose files and the CI
 * workflow.
 *
 * Five rules are pinned here, all of them cheap and cluster-free:
 *
 *  1. every `--testsuite` selector used in the repository names a suite
 *     that `phpunit.xml` declares;
 *  2. every PHPUnit invocation in the repository passes
 *     `--fail-on-empty-test-suite`, so an empty selection is a non-zero
 *     exit code instead of a silent success;
 *  3. every end-to-end suite is selected by some runner, so no suite is
 *     quietly forgotten;
 *  4. no single PHPUnit invocation selects two end-to-end suites: they
 *     need cluster modes of their own (V1TTL for RawKV, V1 for TxnKV, API
 *     V2 for ApiV2 — see AGENTS.md), so one cluster cannot serve two of
 *     them and such a run could never be green;
 *  5. a runner that selects the API V2 suite runs it with
 *     `TIKV_API_VERSION=2`: without it the suite skips every one of its
 *     tests, and a skipped test counts as a test, so the run would be
 *     green having tested nothing.
 *
 * The selectors and the suite names are read out of the files themselves
 * rather than restated here: a copy in this file could agree with a
 * renamed suite while the runner kept the old name.
 *
 * `phpunit.xml` is read with a regular expression instead of an XML parser
 * because the Unit suite has to run in an environment without the project's
 * dev extensions, and `ext-simplexml` and `ext-dom` are not part of a bare
 * PHP build. The pattern only matches a `<testsuite name="...">` element, not
 * the `<testsuites>` container that holds them.
 */
class TestSuiteSelectorGuardTest extends TestCase
{
    /**
     * The suites whose name marks them as end-to-end suites.
     */
    private const E2E_SUITE_PREFIX = 'E2E';

    public function testEveryTestSuiteSelectorNamesADeclaredSuite(): void
    {
        $declared = $this->declaredTestSuites();

        $violations = [];
        $selectors = 0;

        foreach ($this->runnerCommands() as $where => $commands) {
            foreach ($commands as $line => $command) {
                foreach ($this->suiteSelectorsIn($command) as $selector) {
                    $selectors++;

                    if (!in_array($selector, $declared, true)) {
                        $violations[] = sprintf(
                            '%s:%d selects the test suite "%s", which phpunit.xml does not declare',
                            $where,
                            $line,
                            $selector,
                        );
                    }
                }
            }
        }

        // Without this witness the test would pass vacuously if the scan
        // stopped finding anything at all.
        self::assertGreaterThan(
            0,
            $selectors,
            'No test suite selector was found in composer.json, the scripts, the Makefile, '
            . 'the compose files or the CI workflow, so this guard no longer guards anything.',
        );

        self::assertSame(
            [],
            $violations,
            "Test suite selectors that do not resolve to a suite:\n" . implode("\n", $violations),
        );
    }

    public function testEveryPhpunitInvocationFailsWhenItRunsNothing(): void
    {
        $violations = [];
        $invocations = 0;

        foreach ($this->runnerCommands() as $where => $commands) {
            foreach ($commands as $line => $command) {
                if (!$this->runsPhpunit($command)) {
                    continue;
                }

                $invocations++;

                if (!str_contains($command, '--fail-on-empty-test-suite')) {
                    $violations[] = sprintf(
                        '%s:%d runs PHPUnit without --fail-on-empty-test-suite',
                        $where,
                        $line,
                    );
                }
            }
        }

        // Without this witness the test would pass vacuously if no runner
        // called PHPUnit at all.
        self::assertGreaterThan(
            0,
            $invocations,
            'No PHPUnit invocation was found in composer.json, the scripts, the Makefile, '
            . 'the compose files or the CI workflow, so this guard no longer guards anything.',
        );

        self::assertSame(
            [],
            $violations,
            "PHPUnit invocations that can pass without running a single test:\n" . implode("\n", $violations),
        );
    }

    public function testEveryE2eSuiteIsSelectedBySomeRunner(): void
    {
        $e2eSuites = $this->e2eTestSuites();
        self::assertNotEmpty($e2eSuites, 'phpunit.xml declares no end-to-end test suite');

        $selected = [];
        foreach ($this->runnerCommands() as $commands) {
            foreach ($commands as $command) {
                foreach ($this->suiteSelectorsIn($command) as $selector) {
                    $selected[$selector] = true;
                }
            }
        }

        $violations = [];

        foreach ($e2eSuites as $suite) {
            if (!isset($selected[$suite])) {
                $violations[] = sprintf('no runner selects the "%s" suite, so it never runs', $suite);
            }
        }

        self::assertSame(
            [],
            $violations,
            "End-to-end suites that no runner selects:\n" . implode("\n", $violations),
        );
    }

    public function testNoRunnerSelectsTwoE2eSuitesAtOnce(): void
    {
        $violations = [];
        $e2eInvocations = 0;

        foreach ($this->runnerCommands() as $where => $commands) {
            foreach ($commands as $line => $command) {
                if (!$this->runsPhpunit($command)) {
                    continue;
                }

                $e2eSuites = array_values(array_filter(
                    $this->suiteSelectorsIn($command),
                    fn (string $suite): bool => str_starts_with($suite, self::E2E_SUITE_PREFIX),
                ));

                if ($e2eSuites === []) {
                    continue;
                }

                $e2eInvocations++;

                if (count($e2eSuites) < 2) {
                    continue;
                }

                $violations[] = sprintf(
                    '%s:%d selects the end-to-end suites %s in one run; every end-to-end suite '
                    . 'needs a cluster mode of its own, so one cluster cannot serve two of them. '
                    . 'Run them one after the other, each against its own cluster '
                    . '(scripts/test-e2e.sh does that).',
                    $where,
                    $line,
                    implode(', ', $e2eSuites),
                );
            }
        }

        // Without this witness the test would pass vacuously if no runner
        // selected an end-to-end suite at all.
        self::assertGreaterThan(
            0,
            $e2eInvocations,
            'No runner selects an end-to-end suite, so this guard no longer guards anything.',
        );

        self::assertSame(
            [],
            $violations,
            "Runs that mix end-to-end suites that need different cluster modes:\n"
            . implode("\n", $violations),
        );
    }

    /**
     * The API V2 lane must not be able to skip its whole suite.
     *
     * `tests/E2E/ApiV2E2ETest.php` skips every one of its tests unless
     * `TIKV_API_VERSION` is `2`, and a skipped test counts as a test, so
     * `--fail-on-empty-test-suite` does not fire: the lane would be green
     * having tested nothing. Every runner that selects the suite therefore
     * has to set the environment the suite needs.
     */
    public function testTheApiV2LaneCannotSkipEveryTest(): void
    {
        $violations = [];
        $runners = 0;

        foreach ($this->runnerSources() as $where => $source) {
            if (!in_array('E2E-ApiV2', $this->suiteSelectorsIn($source), true)) {
                continue;
            }

            $runners++;

            if (preg_match('/TIKV_API_VERSION[:=]+\s*-?2\b/', $source) !== 1) {
                $violations[] = sprintf(
                    '%s selects the "E2E-ApiV2" suite without running it with '
                    . 'TIKV_API_VERSION=2, so every one of its tests skips and the run is green '
                    . 'having tested nothing',
                    $where,
                );
            }
        }

        // Without this witness the test would pass vacuously if no runner
        // selected the suite at all.
        self::assertGreaterThan(
            0,
            $runners,
            'No runner selects the "E2E-ApiV2" suite, so this guard no longer guards anything.',
        );

        self::assertSame(
            [],
            $violations,
            "Runs of the API V2 suite that can skip every test:\n" . implode("\n", $violations),
        );
    }

    /**
     * The suite names declared in `phpunit.xml`.
     *
     * @return list<string>
     */
    private function declaredTestSuites(): array
    {
        $xml = $this->readFile($this->repoRoot() . '/phpunit.xml');

        $matches = [];
        $found = preg_match_all('/<testsuite\s+name="([^"]+)"/', $xml, $matches);
        self::assertNotFalse($found, 'Unable to scan phpunit.xml for test suite names');
        self::assertGreaterThan(0, $found, 'phpunit.xml declares no test suite');

        return $matches[1];
    }

    /**
     * The end-to-end suites declared in `phpunit.xml`.
     *
     * @return list<string>
     */
    private function e2eTestSuites(): array
    {
        return array_values(array_filter(
            $this->declaredTestSuites(),
            fn (string $suite): bool => str_starts_with($suite, self::E2E_SUITE_PREFIX),
        ));
    }

    /**
     * Every command of every runner in the repository, keyed by the file
     * (and the composer script) it comes from. The key of the inner array
     * is the line the command starts at.
     *
     * @return array<string, array<int, string>>
     */
    private function runnerCommands(): array
    {
        $commands = [];

        foreach ($this->composerScripts() as $name => $scriptCommands) {
            foreach ($scriptCommands as $index => $command) {
                $commands[sprintf('composer.json script "%s"', $name)][$index + 1] = $command;
            }
        }

        foreach ($this->runnerFiles() as $file) {
            foreach ($this->logicalLines($this->readFile($file)) as $line => $command) {
                $commands[$this->relative($file)][$line] = $command;
            }
        }

        return $commands;
    }

    /**
     * The composer scripts, each expanded into the command(s) it runs.
     *
     * @return array<string, list<string>>
     */
    private function composerScripts(): array
    {
        $decoded = json_decode($this->readFile($this->repoRoot() . '/composer.json'), true);
        self::assertIsArray($decoded, 'composer.json is not a JSON object');

        $scripts = $decoded['scripts'] ?? [];
        self::assertIsArray($scripts, 'composer.json has no "scripts" section');

        $result = [];

        foreach ($scripts as $name => $command) {
            $name = (string) $name;

            if (is_string($command)) {
                $result[$name] = [$command];

                continue;
            }

            if (!is_array($command)) {
                continue;
            }

            $commands = [];
            foreach ($command as $part) {
                if (is_string($part)) {
                    $commands[] = $part;
                }
            }

            $result[$name] = $commands;
        }

        return $result;
    }

    /**
     * The files that invoke PHPUnit or name a test suite.
     *
     * @return list<string>
     */
    private function runnerFiles(): array
    {
        $root = $this->repoRoot();
        $files = [$root . '/Makefile'];

        foreach (glob($root . '/scripts/*.sh') ?: [] as $script) {
            $files[] = $script;
        }

        foreach (glob($root . '/.github/workflows/*.y*ml') ?: [] as $workflow) {
            $files[] = $workflow;
        }

        foreach (glob($root . '/docker-compose*.yml') ?: [] as $compose) {
            $files[] = $compose;
        }

        sort($files);

        return $files;
    }

    /**
     * The text of every runner in the repository, keyed by its name.
     *
     * @return array<string, string>
     */
    private function runnerSources(): array
    {
        $sources = ['composer.json' => $this->readFile($this->repoRoot() . '/composer.json')];

        foreach ($this->runnerFiles() as $file) {
            $sources[$this->relative($file)] = $this->readFile($file);
        }

        return $sources;
    }

    /**
     * The logical lines of a runner file: a line that ends with a backslash
     * continues on the next one, so a command spread over several lines
     * counts as one.
     *
     * @return array<int, string> the line the command starts at => the command
     */
    private function logicalLines(string $text): array
    {
        $lines = preg_split('/\R/', $text);
        self::assertIsArray($lines, 'Unable to split a runner file into lines');

        $commands = [];
        $current = null;
        $start = 0;

        foreach ($lines as $index => $line) {
            $number = $index + 1;

            if ($current === null) {
                $current = $line;
                $start = $number;
            } else {
                $current .= ' ' . trim($line);
            }

            if (str_ends_with(rtrim($current), '\\')) {
                $current = rtrim(rtrim($current), '\\');

                continue;
            }

            $commands[$start] = $current;
            $current = null;
        }

        if ($current !== null) {
            $commands[$start] = $current;
        }

        return $commands;
    }

    /**
     * Every suite name selected by a `--testsuite` option in the given text.
     *
     * PHPUnit accepts a comma-separated list (`--testsuite A,B`) as well as
     * both the `=` and the space form of the option. A selector ends at the
     * first whitespace, quote or closing parenthesis, so a suite name inside
     * a shell array (`(--testsuite E2E-RawKV)`) is read as the suite name
     * and not as "E2E-RawKV)".
     *
     * @return list<string>
     */
    private function suiteSelectorsIn(string $text): array
    {
        $matches = [];
        $found = preg_match_all('/--testsuite[=\s]+([^\s\'")]+)/', $text, $matches);
        self::assertNotFalse($found, 'Unable to scan for test suite selectors');

        $selectors = [];

        foreach ($matches[1] as $value) {
            foreach (explode(',', $value) as $name) {
                $selectors[] = $name;
            }
        }

        return $selectors;
    }

    /**
     * Whether the command runs PHPUnit (and not, say, PHPStan or PHPCS).
     *
     * PHPUnit is called either by name (`phpunit`) or by path
     * (`vendor/bin/phpunit`), and both count.
     */
    private function runsPhpunit(string $command): bool
    {
        return preg_match('/(?:^|[\s\/])phpunit(?:\s|$)/', $command) === 1;
    }

    private function readFile(string $path): string
    {
        self::assertFileExists($path, sprintf('Expected %s to exist', $path));

        $contents = file_get_contents($path);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }

    private function repoRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    private function relative(string $file): string
    {
        $repoRoot = $this->repoRoot() . '/';

        return str_starts_with($file, $repoRoot) ? substr($file, strlen($repoRoot)) : $file;
    }
}
