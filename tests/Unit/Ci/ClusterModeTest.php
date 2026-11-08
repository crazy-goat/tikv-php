<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use CrazyGoat\TiKV\Tests\E2E\ClusterMode;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

/**
 * The cluster-mode check of the per-mode end-to-end runners (issue #730).
 *
 * Every end-to-end suite needs a cluster in its own storage mode, so a run
 * against the wrong one has to say "wrong cluster mode" instead of failing
 * with the TTL or API-version error from deep inside the client.
 *
 * The verdict and the message parsing are plain functions; the detection
 * itself needs a cluster and is exercised by the end-to-end runner. The
 * wiring of the runner is read out of the script, so the mode cannot drift
 * away from the suite it belongs to.
 */
final class ClusterModeTest extends TestCase
{
    public function testAnApiVersionMismatchNamesTheServersStorageMode(): void
    {
        self::assertSame(
            ClusterMode::V1TTL,
            ClusterMode::fromApiVersionMismatch(
                'BatchRequest failed: Api version in request does not match with TiKV storage, '
                . 'cmd: raw_get, storage: V1ttl, request: V2',
            ),
        );

        self::assertSame(
            ClusterMode::V1,
            ClusterMode::fromApiVersionMismatch(
                'BatchRequest failed: Api version in request does not match with TiKV storage, '
                . 'cmd: raw_get, storage: V1, request: V2',
            ),
        );

        self::assertSame(
            ClusterMode::APIV2,
            ClusterMode::fromApiVersionMismatch(
                'BatchRequest failed: Api version in request does not match with TiKV storage, '
                . 'cmd: raw_get, storage: V2, request: V1',
            ),
        );
    }

    public function testAnUnrelatedErrorDoesNotNameAMode(): void
    {
        self::assertNull(ClusterMode::fromApiVersionMismatch('PD is not reachable'));
        self::assertNull(ClusterMode::fromApiVersionMismatch(
            'Api version in request does not match with TiKV storage, cmd: raw_get, request: V2',
        ));
    }

    public function testTheExpectedModeIsAccepted(): void
    {
        $verdict = ClusterMode::verdict(ClusterMode::V1TTL, ClusterMode::V1TTL);

        self::assertTrue($verdict['ok']);
        self::assertStringContainsString('Cluster mode: V1TTL', $verdict['message']);
    }

    public function testADifferentModeIsRejected(): void
    {
        $verdict = ClusterMode::verdict(ClusterMode::V1, ClusterMode::V1TTL);

        self::assertFalse($verdict['ok']);
        self::assertStringContainsString('Wrong cluster mode', $verdict['message']);
        self::assertStringContainsString('V1TTL', $verdict['message']);
        self::assertStringContainsString('V1 ([storage] enable-ttl unset', $verdict['message']);
    }

    public function testAnUndeterminedModeContinuesWithAWarning(): void
    {
        $verdict = ClusterMode::verdict(ClusterMode::APIV2, null);

        self::assertTrue(
            $verdict['ok'],
            'A probe that cannot tell must not turn a working setup red; the cluster probe '
            . 'reports an unreachable cluster with a better message',
        );
        self::assertStringContainsString('Could not determine the cluster mode', $verdict['message']);
    }

    public function testDescribeRejectsAnUnknownMode(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ClusterMode::describe('v3');
    }

    /**
     * Each suite in the shared runner maps to the mode it needs, and the
     * check runs before PHPUnit starts.
     */
    public function testThePerModeRunnerChecksTheClusterModeBeforePhpunit(): void
    {
        $script = $this->readScript('scripts/test-e2e-suite.sh');

        self::assertMatchesRegularExpression(
            '/E2E-RawKV\)\s*\n\s*phpunit_args=\(--testsuite E2E-RawKV\)\s*\n\s*expected_mode=v1ttl/',
            $script,
        );
        self::assertMatchesRegularExpression(
            '/E2E-TxnKV\)\s*\n\s*phpunit_args=\(--testsuite E2E-TxnKV\)\s*\n\s*expected_mode=v1\b/',
            $script,
        );
        self::assertMatchesRegularExpression(
            '/E2E-ApiV2\).*expected_mode=apiv2/s',
            $script,
        );

        self::assertMatchesRegularExpression(
            '/bin\/cluster-mode\.php.*vendor\/bin\/phpunit/s',
            $script,
            'The cluster-mode check has to run before PHPUnit starts',
        );
        self::assertStringContainsString('Refusing to run $suite', $script);
    }

    /**
     * The CLI rejects an unknown mode without touching a cluster.
     */
    public function testTheCliRejectsAnUnknownMode(): void
    {
        $command = sprintf(
            '%s %s v3 2>&1',
            escapeshellarg(PHP_BINARY),
            escapeshellarg(dirname(__DIR__, 3) . '/bin/cluster-mode.php'),
        );

        $output = [];
        $exitCode = 0;
        exec($command, $output, $exitCode);

        self::assertSame(2, $exitCode);
        self::assertStringContainsString('Usage: php bin/cluster-mode.php', implode("\n", $output));
    }

    private function readScript(string $path): string
    {
        $contents = file_get_contents(dirname(__DIR__, 3) . '/' . $path);
        self::assertIsString($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }
}
