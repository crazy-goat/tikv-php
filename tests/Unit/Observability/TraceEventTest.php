<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use CrazyGoat\TiKV\Client\Observability\InMemoryTracer;
use CrazyGoat\TiKV\Client\Observability\NoOpTracer;
use CrazyGoat\TiKV\Client\Observability\TraceCategory;
use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;
use PHPUnit\Framework\TestCase;

class TraceEventTest extends TestCase
{
    public function testNoOpTracerImplementsTheInterfaceAndNeverThrows(): void
    {
        $tracer = new NoOpTracer();
        $this->assertInstanceOf(TraceEventInterface::class, $tracer);

        $tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_started', ['operation' => 'op']);
        $tracer->traceEvent(TraceCategory::TXN_2PC, 'prewrite_completed', ['success' => true]);

        $this->addToAssertionCount(1);
    }

    public function testInMemoryTracerRecordsEventsInOrder(): void
    {
        $tracer = new InMemoryTracer();
        $tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_started', ['operation' => 'op']);
        $tracer->traceEvent(TraceCategory::TXN_2PC, 'prewrite_completed', ['success' => true, 'durationMs' => 1.5]);

        $events = $tracer->getEvents();

        $this->assertCount(2, $events);
        $this->assertSame(TraceCategory::KV_REQUEST, $events[0]['category']);
        $this->assertSame('rpc_started', $events[0]['event']);
        $this->assertSame(['operation' => 'op'], $events[0]['fields']);
        $this->assertSame(1.5, $events[1]['fields']['durationMs']);
    }

    public function testEventNamesCanBeFilteredByCategory(): void
    {
        $tracer = new InMemoryTracer();
        $tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_started');
        $tracer->traceEvent(TraceCategory::TXN_2PC, 'prewrite_started');
        $tracer->traceEvent(TraceCategory::KV_REQUEST, 'rpc_completed');

        $this->assertSame(['rpc_started', 'rpc_completed'], $tracer->eventNames(TraceCategory::KV_REQUEST));
        $this->assertSame(['prewrite_started'], $tracer->eventNames(TraceCategory::TXN_2PC));
        $this->assertSame(
            ['rpc_started', 'prewrite_started', 'rpc_completed'],
            $tracer->eventNames(),
        );
    }

    public function testResetClearsEvents(): void
    {
        $tracer = new InMemoryTracer();
        $tracer->traceEvent(TraceCategory::REGION_CACHE, 'cache_miss');

        $tracer->reset();

        $this->assertSame([], $tracer->getEvents());
    }

    public function testCategoriesMirrorClientGo(): void
    {
        $this->assertSame('txn_2pc', TraceCategory::TXN_2PC);
        $this->assertSame('txn_lock_resolve', TraceCategory::TXN_LOCK_RESOLVE);
        $this->assertSame('kv_request', TraceCategory::KV_REQUEST);
        $this->assertSame('region_cache', TraceCategory::REGION_CACHE);
    }
}
