<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Exit-code contract of `scripts/test.sh` (issue #727).
 *
 * The script runs the Unit and the Integration suites under `set -e`. It used
 * to call the Unit runner without capturing its status, so a failing Unit run
 * aborted the script before the summary block and its non-zero exit were ever
 * reached, and it ended the Integration runner with `|| true`, so a failing
 * Integration run exited 0 and the summary claimed that everything passed.
 *
 * These tests copy the real script into a temporary project with a fake
 * `vendor/bin/phpunit`, so they need neither the real suites nor a cluster:
 * the fake exits with the code each test asks for and logs that it ran. The
 * script itself is exercised end to end, including its `set -e` behaviour.
 */
final class TestScriptExitCodeTest extends TestCase
{
    public function testBothSuitesPassingExitsZero(): void
    {
        [$exitCode, $output] = $this->runScript(0, 0);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('Unit tests passed!', $output);
        self::assertStringContainsString('Integration tests passed!', $output);
        self::assertStringContainsString('fake phpunit: Unit suite', $output);
        self::assertStringContainsString('fake phpunit: Integration suite', $output);
    }

    /**
     * The case the summary block exists for: a failing Unit run must reach
     * the summary and exit non-zero instead of aborting at the PHPUnit line.
     */
    public function testAFailingUnitSuiteRunsTheIntegrationSuiteAndTheSummary(): void
    {
        [$exitCode, $output] = $this->runScript(1, 0);

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Unit tests failed!', $output);
        self::assertStringContainsString('Integration tests passed!', $output);
        self::assertStringContainsString(
            'fake phpunit: Integration suite',
            $output,
            'The Integration suite must still run when the Unit suite fails',
        );
    }

    /**
     * `|| true` used to hide this failure completely.
     */
    public function testAFailingIntegrationSuiteFailsTheScript(): void
    {
        [$exitCode, $output] = $this->runScript(0, 1);

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Unit tests passed!', $output);
        self::assertStringContainsString('Integration tests failed!', $output);
    }

    /**
     * The script must return the worse of the two codes, not the last one:
     * a failing Unit run followed by a passing Integration run used to exit
     * with the Integration status.
     */
    public function testTheWorseOfTheTwoExitCodesWins(): void
    {
        [$unitWorse, $unitWorseOutput] = $this->runScript(5, 2);

        self::assertSame(5, $unitWorse, 'A worse Unit code must not be replaced by the Integration code');
        self::assertStringContainsString('Unit tests failed!', $unitWorseOutput);
        self::assertStringContainsString('Integration tests failed!', $unitWorseOutput);

        [$integrationWorse, $integrationWorseOutput] = $this->runScript(2, 5);

        self::assertSame(5, $integrationWorse, 'A worse Integration code must not be replaced by the Unit code');
        self::assertStringContainsString('Unit tests failed!', $integrationWorseOutput);
        self::assertStringContainsString('Integration tests failed!', $integrationWorseOutput);
    }

    /**
     * Runs the real `scripts/test.sh` against a fake `vendor/bin/phpunit`
     * that exits with the requested code per suite.
     *
     * @return array{int, string} the exit code and the combined output
     */
    private function runScript(int $unitExit, int $integrationExit): array
    {
        $root = dirname(__DIR__, 3);
        $project = sys_get_temp_dir() . '/tikv-test-script-' . bin2hex(random_bytes(8));

        try {
            $this->createProject($root, $project);

            $command = sprintf(
                'cd %s && STUB_UNIT_EXIT=%d STUB_INTEGRATION_EXIT=%d bash test.sh 2>&1',
                escapeshellarg($project),
                $unitExit,
                $integrationExit,
            );

            $output = [];
            $exitCode = 0;
            exec($command, $output, $exitCode);

            return [$exitCode, implode("\n", $output)];
        } finally {
            exec('rm -rf ' . escapeshellarg($project));
        }
    }

    private function createProject(string $root, string $project): void
    {
        $bin = $project . '/vendor/bin';
        self::assertTrue(mkdir($bin, 0777, true), 'Unable to create the fake project directory');
        self::assertTrue(copy($root . '/scripts/test.sh', $project . '/test.sh'), 'Unable to copy scripts/test.sh');

        $composerJson = "{}\n";
        self::assertSame(strlen($composerJson), file_put_contents($project . '/composer.json', $composerJson));

        $phpunit = <<<'BASH'
#!/usr/bin/env bash
case " $* " in
    *" --testsuite Unit "*)
        echo "fake phpunit: Unit suite"
        exit "${STUB_UNIT_EXIT:-0}"
        ;;
    *" --testsuite Integration "*)
        echo "fake phpunit: Integration suite"
        exit "${STUB_INTEGRATION_EXIT:-0}"
        ;;
esac
echo "fake phpunit: unexpected invocation: $*" >&2
exit 99
BASH;

        $path = $bin . '/phpunit';
        self::assertSame(strlen($phpunit), file_put_contents($path, $phpunit));
        self::assertTrue(chmod($path, 0755), 'Unable to make the fake phpunit executable');
    }
}
