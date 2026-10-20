<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * E2E round-trip test for service GC safe points (issue #499).
 *
 * Exercises the full cycle against a real PD cluster:
 * holdGcSafePoint() → getGCSafePoint() (min) → releaseGcSafePoint().
 *
 * Requires the TxnKV (V1) cluster: run via the E2E-TxnKV testsuite
 * (docker-compose.txnkv.yml / scripts/test-e2e.sh).
 */
final class GcSafePointE2ETest extends TestCase
{
    private const SERVICE_TTL_SECONDS = 600;

    private static PdClientInterface $pdClient;

    private static TxnKvClient $txnClient;

    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') !== false && getenv('PD_ENDPOINTS') !== ''
            ? explode(',', (string) getenv('PD_ENDPOINTS'))
            : ['pd:2379'];

        ClusterProbe::assertPdReachable($pdEndpoints);

        self::$txnClient = TxnKvClient::create($pdEndpoints);
        self::$pdClient = ConnectionFactory::create($pdEndpoints)->pdClient;
    }

    public static function tearDownAfterClass(): void
    {
        self::$txnClient->close();
    }

    private function uniqueServiceId(): string
    {
        return 'e2e-gc-safepoint-' . uniqid('', true);
    }

    private function currentTso(): int
    {
        return self::$pdClient->getTimestamp();
    }

    public function testServiceGcSafePointRoundTripThroughPdClient(): void
    {
        $serviceId = $this->uniqueServiceId();
        $baseline = self::$pdClient->getGCSafePoint();
        $this->assertGreaterThanOrEqual(0, $baseline);

        $safePoint = $this->currentTso();
        $this->assertGreaterThan($baseline, $safePoint);

        $gcWhileHeld = 0;
        try {
            $minWhileHeld = self::$pdClient->updateServiceGCSafePoint(
                $serviceId,
                $safePoint,
                self::SERVICE_TTL_SECONDS,
            );

            $this->assertNotNull($minWhileHeld, 'PD does not support service GC safe points');
            // The min safe point across all registered services can never
            // exceed our own registration.
            $this->assertLessThanOrEqual($safePoint, $minWhileHeld);
            $this->assertGreaterThanOrEqual(0, $minWhileHeld);

            // The cluster GC safe point is capped by the min service safe
            // point, so while we hold at $safePoint it cannot be above it.
            $gcWhileHeld = self::$pdClient->getGCSafePoint();
            $this->assertLessThanOrEqual($safePoint, $gcWhileHeld);
            $this->assertGreaterThanOrEqual(0, $gcWhileHeld);
        } finally {
            $minAfterRelease = self::$pdClient->updateServiceGCSafePoint($serviceId, 0, -1);
        }

        $this->assertNotNull($minAfterRelease);

        // The GC safe point is monotonic: releasing our hold must not move
        // it backwards.
        $gcAfterRelease = self::$pdClient->getGCSafePoint();
        $this->assertGreaterThanOrEqual($gcWhileHeld, $gcAfterRelease);
    }

    public function testTxnKvClientHoldAndReleaseGcSafePoint(): void
    {
        $safePoint = $this->currentTso();

        $minWhileHeld = self::$txnClient->holdGcSafePoint($safePoint, self::SERVICE_TTL_SECONDS);

        $this->assertNotNull($minWhileHeld, 'PD does not support service GC safe points');
        $this->assertLessThanOrEqual($safePoint, $minWhileHeld);

        try {
            $minAfterRelease = self::$txnClient->releaseGcSafePoint();
        } catch (\Throwable $e) {
            // Never leave the registration behind on an unexpected error.
            self::$txnClient->releaseGcSafePoint();
            throw $e;
        }

        $this->assertNotNull($minAfterRelease);
    }
}
