<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use PHPUnit\Framework\TestCase;

class MetricsTest extends TestCase
{
    public function testNoOpImplementsInterface(): void
    {
        $this->assertInstanceOf(MetricsInterface::class, new NoOpMetrics());
    }

    public function testNoOpNeverThrows(): void
    {
        $noop = new NoOpMetrics();

        // Same calls the library makes — must NOT throw.
        $noop->rpcStarted('tikvpb.Tikv/KvGet');
        $noop->rpcCompleted('tikvpb.Tikv/KvGet', 1.23, true);
        $noop->rpcCompleted('tikvpb.Tikv/KvGet', 5.67, false);
        $noop->retryAttempted('NotLeader');
        $noop->regionCacheHit('region_resolution');
        $noop->regionCacheMiss('region_resolution');
        $noop->regionInvalidated('not_leader');

        // Verify the no-op truly is a no-op (no side-effects to inspect).
        $this->addToAssertionCount(1);
    }

    public function testInMemoryImplementsInterface(): void
    {
        $this->assertInstanceOf(MetricsInterface::class, new InMemoryMetrics());
    }

    public function testInMemoryCountersStartAtZero(): void
    {
        $m = new InMemoryMetrics();
        $this->assertSame(0, $m->getRpcStarted('op'));
        $this->assertSame(0, $m->getRpcSucceeded('op'));
        $this->assertSame(0, $m->getRpcFailed('op'));
        $this->assertSame(0, $m->getRetries('op'));
        $this->assertSame(0, $m->getCacheHits('op'));
        $this->assertSame(0, $m->getCacheMisses('op'));
        $this->assertSame(0, $m->getInvalidations('reason'));
        $this->assertSame(0.0, $m->getMeanLatencyMs('op'));
    }

    public function testRpcCountersIncrementIndependently(): void
    {
        $m = new InMemoryMetrics();

        $m->rpcStarted('a');
        $m->rpcStarted('a');
        $m->rpcStarted('b');

        $m->rpcCompleted('a', 10.0, true);
        $m->rpcCompleted('a', 20.0, false);
        $m->rpcCompleted('b', 5.0, true);

        $this->assertSame(2, $m->getRpcStarted('a'));
        $this->assertSame(1, $m->getRpcStarted('b'));
        $this->assertSame(1, $m->getRpcSucceeded('a'));
        $this->assertSame(1, $m->getRpcFailed('a'));
        $this->assertSame(1, $m->getRpcSucceeded('b'));
        $this->assertSame(0, $m->getRpcFailed('b'));
    }

    public function testMeanLatencyMsComputesCorrectly(): void
    {
        $m = new InMemoryMetrics();

        $m->rpcCompleted('op', 10.0, true);
        $m->rpcCompleted('op', 20.0, true);
        $m->rpcCompleted('op', 30.0, true);

        $this->assertSame(20.0, $m->getMeanLatencyMs('op'));
    }

    public function testRetryCountersAreTaggedIndependently(): void
    {
        $m = new InMemoryMetrics();

        $m->retryAttempted('NotLeader');
        $m->retryAttempted('NotLeader');
        $m->retryAttempted('ServerBusy');

        $this->assertSame(2, $m->getRetries('NotLeader'));
        $this->assertSame(1, $m->getRetries('ServerBusy'));
        $this->assertSame(0, $m->getRetries('Unknown'));
    }

    public function testRegionCacheHitMissAreMutuallyExclusive(): void
    {
        $m = new InMemoryMetrics();

        $m->regionCacheHit('op');
        $m->regionCacheHit('op');
        $m->regionCacheMiss('op');

        $this->assertSame(2, $m->getCacheHits('op'));
        $this->assertSame(1, $m->getCacheMisses('op'));
    }

    public function testRegionInvalidationsAreTaggedByReason(): void
    {
        $m = new InMemoryMetrics();

        $m->regionInvalidated('not_leader');
        $m->regionInvalidated('not_leader');
        $m->regionInvalidated('retry_region_error');

        $this->assertSame(2, $m->getInvalidations('not_leader'));
        $this->assertSame(1, $m->getInvalidations('retry_region_error'));
    }

    public function testResetClearsAllCounters(): void
    {
        $m = new InMemoryMetrics();

        $m->rpcStarted('a');
        $m->rpcCompleted('a', 5.0, true);
        $m->retryAttempted('NotLeader');
        $m->regionCacheHit('op');
        $m->regionCacheMiss('op');
        $m->regionInvalidated('not_leader');

        $m->reset();

        $this->assertSame(0, $m->getRpcStarted('a'));
        $this->assertSame(0, $m->getRpcSucceeded('a'));
        $this->assertSame(0, $m->getRpcFailed('a'));
        $this->assertSame(0, $m->getRetries('NotLeader'));
        $this->assertSame(0, $m->getCacheHits('op'));
        $this->assertSame(0, $m->getCacheMisses('op'));
        $this->assertSame(0, $m->getInvalidations('not_leader'));
        $this->assertSame(0.0, $m->getMeanLatencyMs('a'));
    }
}
