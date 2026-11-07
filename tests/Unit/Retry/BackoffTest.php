<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Retry\Backoff;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use PHPUnit\Framework\TestCase;

class BackoffTest extends TestCase
{
    public function testExponentialAttemptZeroReturnsBase(): void
    {
        $this->assertSame(100, Backoff::exponential(100, 2000, 0));
    }

    public function testExponentialGrowsExponentially(): void
    {
        $this->assertSame(100, Backoff::exponential(100, 2000, 0));
        $this->assertSame(200, Backoff::exponential(100, 2000, 1));
        $this->assertSame(400, Backoff::exponential(100, 2000, 2));
        $this->assertSame(800, Backoff::exponential(100, 2000, 3));
        $this->assertSame(1600, Backoff::exponential(100, 2000, 4));
    }

    public function testExponentialCapsAtMax(): void
    {
        $this->assertSame(2000, Backoff::exponential(100, 2000, 5));
        $this->assertSame(2000, Backoff::exponential(100, 2000, 10));
        $this->assertSame(2000, Backoff::exponential(100, 2000, 100));
    }

    public function testExponentialWithEqualJitterInRange(): void
    {
        for ($i = 0; $i < 50; $i++) {
            $result = Backoff::exponential(100, 2000, 0, true);
            $this->assertGreaterThanOrEqual(50, $result);
            $this->assertLessThanOrEqual(100, $result);
        }
    }

    public function testExponentialWithEqualJitterCapped(): void
    {
        for ($i = 0; $i < 50; $i++) {
            $result = Backoff::exponential(100, 2000, 10, true);
            $this->assertGreaterThanOrEqual(1000, $result);
            $this->assertLessThanOrEqual(2000, $result);
        }
    }

    public function testExponentialSmallBaseAndCap(): void
    {
        $this->assertSame(2, Backoff::exponential(2, 500, 0));
        $this->assertSame(4, Backoff::exponential(2, 500, 1));
        $this->assertSame(256, Backoff::exponential(2, 500, 7));
        $this->assertSame(500, Backoff::exponential(2, 500, 8));
        $this->assertSame(500, Backoff::exponential(2, 500, 20));
    }

    /**
     * Issue #242 (REG-11): every backoff type must be jittered, because
     * fleet-correlated errors (NotLeader, RegionMiss, StaleCmd) would
     * otherwise make all clients retry in lockstep.
     */
    public function testAllBackoffTypesUseEqualJitter(): void
    {
        foreach (BackoffType::cases() as $type) {
            if ($type === BackoffType::None) {
                $this->assertSame(0, $type->sleepMs(0));
                continue;
            }
            $this->assertTrue($type->equalJitter(), (string) $type->name);
        }
    }

    public function testAllBackoffTypesStayWithinEqualJitterRange(): void
    {
        foreach (BackoffType::cases() as $type) {
            if ($type === BackoffType::None) {
                continue;
            }
            for ($attempt = 0; $attempt < 12; $attempt++) {
                $expo = min($type->capMs(), $type->baseMs() * (1 << $attempt));
                $sleep = $type->sleepMs($attempt);
                $this->assertGreaterThanOrEqual(intdiv($expo, 2), $sleep, "{$type->name} attempt {$attempt}");
                $this->assertLessThanOrEqual($expo, $sleep, "{$type->name} attempt {$attempt}");
            }
        }
    }

    public function testAllBackoffTypesProduceDifferingValuesForSameAttempt(): void
    {
        foreach (BackoffType::cases() as $type) {
            if ($type === BackoffType::None) {
                continue;
            }
            $values = [];
            for ($i = 0; $i < 50; $i++) {
                $values[] = $type->sleepMs(0);
            }
            $this->assertGreaterThan(1, count(array_unique($values)), "{$type->name} is deterministic");
        }
    }

    /**
     * Issue #296 (PERF-09): NotLeader is fleet-correlated — every worker
     * holding the region sees the same leader transfer — so its backoff must
     * be jittered, not the deterministic 2, 4, 8, ... ms schedule. The fix
     * shipped with #242 (equal jitter on every backoff type); this pins the
     * audit's acceptance criterion explicitly for NotLeader.
     */
    public function testNotLeaderSleepIsJitteredWithinEqualJitterRange(): void
    {
        $this->assertTrue(BackoffType::NotLeader->equalJitter());

        for ($attempt = 0; $attempt < 12; $attempt++) {
            $expo = min(BackoffType::NotLeader->capMs(), BackoffType::NotLeader->baseMs() * (1 << $attempt));
            $values = [];

            for ($i = 0; $i < 50; $i++) {
                $sleep = BackoffType::NotLeader->sleepMs($attempt);
                $this->assertGreaterThanOrEqual(intdiv($expo, 2), $sleep, "attempt {$attempt}");
                $this->assertLessThanOrEqual($expo, $sleep, "attempt {$attempt}");
                $values[$sleep] = true;
            }

            $this->assertGreaterThan(1, count($values), "attempt {$attempt} must not be deterministic");
        }
    }

    // Issue #241 (REG-10): EpochNotMatch must sleep a non-zero, jittered
    // interval — the same values as client-go's BoEpochNotMatch.

    public function testEpochNotMatchHasNonZeroBaseAndCap(): void
    {
        $this->assertSame(2, BackoffType::EpochNotMatch->baseMs());
        $this->assertSame(500, BackoffType::EpochNotMatch->capMs());
        $this->assertTrue(BackoffType::EpochNotMatch->equalJitter());
    }

    public function testEpochNotMatchSleepIsNonZeroAndJittered(): void
    {
        for ($attempt = 0; $attempt < 12; $attempt++) {
            $sleep = BackoffType::EpochNotMatch->sleepMs($attempt);
            $this->assertGreaterThan(0, $sleep, "attempt $attempt must sleep > 0 ms");
        }

        // Attempt 0 with equal jitter sleeps in [base/2, base] = [1, 2] ms.
        $seen = [];
        for ($i = 0; $i < 50; $i++) {
            $seen[BackoffType::EpochNotMatch->sleepMs(0)] = true;
        }
        foreach ($seen as $value => $_) {
            $this->assertGreaterThanOrEqual(1, $value);
            $this->assertLessThanOrEqual(2, $value);
        }
        $this->assertGreaterThan(1, count($seen), 'equal jitter must produce more than one value');
    }

    public function testEpochNotMatchSleepCapsAtCapMs(): void
    {
        $sleep = BackoffType::EpochNotMatch->sleepMs(100);
        // equal jitter: [cap/2, cap]
        $this->assertGreaterThanOrEqual(250, $sleep);
        $this->assertLessThanOrEqual(500, $sleep);
    }
}
