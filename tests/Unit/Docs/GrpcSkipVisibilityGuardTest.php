<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #284 (GRPC-25): a transport test must never
 * disappear from a run without a trace.
 *
 * `composer.json` has required `ext-grpc` since issue #408, so a deployment
 * without the extension fails at install time. The test suite is a different
 * question: `GrpcExtensionGate` skips a transport test when the extension is
 * not loaded, and PHPUnit's default output reduces that to one `S` and
 * "OK, but some tests were skipped!". A developer who runs a bare
 * `vendor/bin/phpunit` therefore sees a green run while the whole transport
 * layer went untested.
 *
 * Three things have to stay in step to keep that visible, and only a test can
 * hold them together:
 *
 *  - `phpunit.xml` lists skipped tests and warning details, and the gate
 *    emits one warning per skipped class so the TestDox output names it,
 *  - the gate's skip message names the class, the extension and how to
 *    install it,
 *  - the CI job that runs the transport suite sets `REQUIRE_GRPC_EXTENSION=1`,
 *    so a missing extension fails with a clear message instead of a skip.
 *
 * The `unit-tests` matrix job deliberately installs without the extension
 * (see `docs/helpers/faq.md`); its `--fail-on-skipped` turns any transport
 * test that leaks into the Unit suite into a failure, so it needs no gate.
 */
final class GrpcSkipVisibilityGuardTest extends TestCase
{
    public function testEveryRunListsTheSkippedTestsAndTheirReasons(): void
    {
        $configuration = $this->read('phpunit.xml');

        self::assertStringContainsString(
            'displayDetailsOnSkippedTests="true"',
            $configuration,
            'phpunit.xml must display the details of skipped tests, otherwise a missing '
            . 'gRPC extension reduces the transport suite to a single "S" (issue #284)',
        );
        self::assertStringContainsString(
            'displayDetailsOnTestsThatTriggerWarnings="true"',
            $configuration,
            'phpunit.xml must display warning details: the TestDox printer ignores '
            . 'displayDetailsOnSkippedTests, so the gate names the skipped class in a warning',
        );
    }

    public function testTheGateSkipMessageNamesTheClassAndHowToInstallTheExtension(): void
    {
        $gate = $this->read('tests/Unit/Grpc/GrpcExtensionGate.php');

        self::assertStringContainsString(
            'static::class',
            $gate,
            'the skip message must name the test class that did not run',
        );
        self::assertStringContainsString(
            'pecl install grpc',
            $gate,
            'the skip message must say how to install ext-grpc',
        );
        self::assertStringContainsString(
            'composer test:grpc',
            $gate,
            'the skip message must say how to rerun the transport suite',
        );
        self::assertStringContainsString(
            'trigger_error',
            $gate,
            'the gate must emit a warning so the skipped class appears in the run summary',
        );
        self::assertStringContainsString(
            'E_USER_WARNING',
            $gate,
            'the gate must emit a warning so the skipped class appears in the run summary',
        );
        self::assertStringNotContainsString(
            "markTestSkipped('gRPC extension not available')",
            $gate,
            'the bare skip message hides which tests were skipped and how to install the extension',
        );
    }

    public function testTheTransportCiJobRequiresTheExtension(): void
    {
        $workflow = $this->read('.github/workflows/ci.yml');
        $job = $this->job($workflow, 'grpc-unit-tests');

        self::assertStringContainsString(
            "REQUIRE_GRPC_EXTENSION: '1'",
            $job,
            'the grpc-unit-tests job must set REQUIRE_GRPC_EXTENSION=1 so the gate '
            . 'fails instead of skipping when ext-grpc is missing (issue #284)',
        );
        self::assertStringContainsString(
            '--fail-on-skipped',
            $job,
            'the grpc-unit-tests job must treat a skipped transport test as a failure',
        );
    }

    /**
     * The block of one job in the workflow, from its `  <name>:` line up to
     * the next top-level job.
     */
    private function job(string $workflow, string $name): string
    {
        $start = strpos($workflow, "\n  " . $name . ':');
        self::assertNotFalse($start, sprintf('the workflow has no "%s" job', $name));

        $next = preg_match(
            '/\n  [a-z][a-z0-9-]*:/',
            $workflow,
            $matches,
            PREG_OFFSET_CAPTURE,
            $start + 1,
        );
        self::assertSame(1, $next, sprintf('the "%s" job is not followed by another job', $name));

        $end = $matches[0][1];

        return substr($workflow, $start, $end - $start);
    }

    private function read(string $relativePath): string
    {
        $path = dirname(__DIR__, 3) . '/' . $relativePath;
        self::assertFileExists($path);

        $contents = file_get_contents($path);
        self::assertNotFalse($contents);

        return $contents;
    }
}
