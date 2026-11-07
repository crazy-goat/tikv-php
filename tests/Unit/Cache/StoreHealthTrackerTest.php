<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\StoreHealthTracker;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use PHPUnit\Framework\TestCase;

/**
 * Issue #424 (GAP-10): the per-store circuit breaker behind store failover.
 */
class StoreHealthTrackerTest extends TestCase
{
    private InMemoryMetrics $metrics;

    protected function setUp(): void
    {
        $this->metrics = new InMemoryMetrics();
    }

    public function testFailuresBelowThresholdDoNotMarkStoreUnhealthy(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);

        $this->assertFalse($tracker->recordFailure(1));
        $this->assertFalse($tracker->recordFailure(1));

        $this->assertSame(2, $tracker->getConsecutiveFailures(1));
        $this->assertTrue($tracker->isAvailable(1));
        $this->assertTrue($tracker->isHealthy(1));
        $this->assertNull($this->metrics->isStoreHealthy(1));
    }

    public function testThresholdFailureMarksStoreUnhealthyAndReportsMetric(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);

        $tracker->recordFailure(1);
        $tracker->recordFailure(1);
        $this->assertTrue($tracker->recordFailure(1));

        $this->assertFalse($tracker->isAvailable(1));
        $this->assertFalse($tracker->isHealthy(1));
        $this->assertSame(1000, $tracker->getUnhealthyUntilMs(1));
        $this->assertFalse($this->metrics->isStoreHealthy(1));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(1));
        $this->assertSame(0, $this->metrics->getStoreRecoveredCount(1));
    }

    public function testCooldownBlocksRoutingThenOpensHalfOpen(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);
        $this->markUnhealthy($tracker);

        $this->assertFalse($tracker->isAvailable(1));

        $clock->nowMs = 999;
        $this->assertFalse($tracker->isAvailable(1));

        $clock->nowMs = 1000;
        $this->assertTrue($tracker->isAvailable(1));
        // Half-open is routable but not yet healthy: only a success is.
        $this->assertFalse($tracker->isHealthy(1));
    }

    public function testSuccessDuringHalfOpenRecoversStoreAndReportsMetric(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);
        $this->markUnhealthy($tracker);

        $clock->nowMs = 1000;
        $this->assertTrue($tracker->isAvailable(1));

        $tracker->recordSuccess(1);

        $this->assertTrue($tracker->isAvailable(1));
        $this->assertTrue($tracker->isHealthy(1));
        $this->assertSame(0, $tracker->getConsecutiveFailures(1));
        $this->assertNull($tracker->getUnhealthyUntilMs(1));
        $this->assertTrue($this->metrics->isStoreHealthy(1));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(1));
        $this->assertSame(1, $this->metrics->getStoreRecoveredCount(1));
    }

    public function testFailedHalfOpenProbeRestartsCooldownWithoutDuplicateMetric(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);
        $this->markUnhealthy($tracker);

        $clock->nowMs = 1000;
        $this->assertTrue($tracker->isAvailable(1));

        $this->assertFalse($tracker->recordFailure(1));
        $this->assertFalse($tracker->isAvailable(1));
        $this->assertSame(2000, $tracker->getUnhealthyUntilMs(1));
        $this->assertSame(1, $this->metrics->getStoreUnhealthyCount(1));
    }

    public function testSuccessBeforeThresholdResetsConsecutiveFailures(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);

        $tracker->recordFailure(1);
        $tracker->recordFailure(1);
        $tracker->recordSuccess(1);

        $this->assertSame(0, $tracker->getConsecutiveFailures(1));
        $this->assertFalse($tracker->recordFailure(1));
        $this->assertFalse($tracker->recordFailure(1));
        $this->assertTrue($tracker->isHealthy(1));
        $this->assertNull($this->metrics->isStoreHealthy(1));
    }

    public function testInvalidStoreIdIsIgnored(): void
    {
        $clock = new MutableClock();
        $tracker = $this->tracker($clock);

        $this->assertFalse($tracker->recordFailure(0));
        $this->assertFalse($tracker->recordFailure(-1));
        $tracker->recordSuccess(0);

        $this->assertTrue($tracker->isAvailable(0));
        $this->assertSame(0, $tracker->getConsecutiveFailures(0));
    }

    public function testConstructorRejectsInvalidConfiguration(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new StoreHealthTracker(failureThreshold: 0);
    }

    public function testConstructorRejectsNegativeCooldown(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new StoreHealthTracker(cooldownMs: -1);
    }

    private function markUnhealthy(StoreHealthTracker $tracker): void
    {
        $tracker->recordFailure(1);
        $tracker->recordFailure(1);
        $tracker->recordFailure(1);
    }

    private function tracker(MutableClock $clock, int $threshold = 3, int $cooldownMs = 1000): StoreHealthTracker
    {
        return new StoreHealthTracker(
            $threshold,
            $cooldownMs,
            $this->metrics,
            static fn (): int => $clock->nowMs,
        );
    }
}

final class MutableClock
{
    public int $nowMs = 0;
}
