<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\E2E;

use CrazyGoat\TiKV\Tests\E2E\ClusterProbe;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The fail-fast contract of the end-to-end cluster probe (issue #730).
 *
 * `ClusterProbe::assertPdReachable()` was pinned only by a manual measurement
 * and exercised only through the end-to-end suites, which need a cluster. The
 * case that matters is a PD endpoint that does not resolve: the probe has to
 * give up in about a second with the setup hint, not spend the client's retry
 * budget (measured at 18.8 s before issue #361).
 *
 * `.invalid` is reserved by RFC 2606, so the resolver never answers it and no
 * cluster, network service or Docker daemon is involved. The class needs
 * ext-grpc (it builds a real channel), so it lives in the Grpc suite.
 */
final class ClusterProbeTest extends TestCase
{
    /**
     * A generous bound around the one-second promise: the probe makes three
     * attempts, and a slow resolver must still not turn this into a retry
     * budget.
     */
    private const MAX_FAILURE_MS = 1000.0;

    public function testAnUnresolvablePdEndpointFailsFastWithTheSetupHint(): void
    {
        $startedAt = hrtime(true);

        try {
            ClusterProbe::assertPdReachable(['pd.invalid:2379']);
            self::fail('ClusterProbe accepted an endpoint that cannot resolve');
        } catch (RuntimeException $e) {
            $elapsedMs = (hrtime(true) - $startedAt) / 1_000_000;

            self::assertLessThan(
                self::MAX_FAILURE_MS,
                $elapsedMs,
                sprintf(
                    'ClusterProbe took %.0f ms to reject an unresolvable endpoint; the whole point '
                    . 'is to fail in about a second instead of spending the retry budget',
                    $elapsedMs,
                ),
            );

            self::assertStringContainsString(
                'The TiKV cluster is not reachable at pd.invalid:2379',
                $e->getMessage(),
            );
            self::assertStringContainsString('PD_ENDPOINTS', $e->getMessage());
            self::assertStringContainsString(
                '${PD_PORT:-2379}',
                $e->getMessage(),
                'The host hint must use the configurable published PD port, not a hard-coded 2379 '
                . '(issue #730)',
            );
        }
    }
}
