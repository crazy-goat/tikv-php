<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use CrazyGoat\TiKV\Client\Observability\DetailedMetricsInterface;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\PrometheusMetrics;
use PHPUnit\Framework\TestCase;

class PrometheusMetricsTest extends TestCase
{
    public function testImplementsBothMetricsInterfaces(): void
    {
        $metrics = new PrometheusMetrics();

        $this->assertInstanceOf(MetricsInterface::class, $metrics);
        $this->assertInstanceOf(DetailedMetricsInterface::class, $metrics);
    }

    public function testRenderIsEmptyBeforeAnySample(): void
    {
        $this->assertSame('', (new PrometheusMetrics())->render());
    }

    public function testRpcCountersAndHistogramAreRendered(): void
    {
        $metrics = new PrometheusMetrics();
        $metrics->rpcStarted('tikvpb.Tikv/KvGet');
        $metrics->rpcCompleted('tikvpb.Tikv/KvGet', 1500.0, true);

        $rendered = $metrics->render();

        $this->assertStringContainsString(
            '# TYPE tikv_client_rpc_started_total counter',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_started_total{operation="tikvpb.Tikv/KvGet"} 1',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_completed_total{operation="tikvpb.Tikv/KvGet",status="success"} 1',
            $rendered,
        );
        $this->assertStringContainsString('# TYPE tikv_client_rpc_duration_seconds histogram', $rendered);
        $this->assertStringContainsString(
            'tikv_client_rpc_duration_seconds_bucket{operation="tikvpb.Tikv/KvGet",status="success",le="1"} 0',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_duration_seconds_bucket{operation="tikvpb.Tikv/KvGet",status="success",le="2.5"} 1',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_duration_seconds_bucket{operation="tikvpb.Tikv/KvGet",status="success",le="+Inf"} 1',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_duration_seconds_sum{operation="tikvpb.Tikv/KvGet",status="success"} 1.5',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_rpc_duration_seconds_count{operation="tikvpb.Tikv/KvGet",status="success"} 1',
            $rendered,
        );
    }

    public function testBaseCountersAreRendered(): void
    {
        $metrics = new PrometheusMetrics();
        $metrics->retryAttempted('NotLeader');
        $metrics->regionCacheHit('scan');
        $metrics->regionCacheMiss('scan');
        $metrics->regionInvalidated('not_leader');
        $metrics->storeHealthChanged(7, false);

        $rendered = $metrics->render();

        $this->assertStringContainsString('tikv_client_retries_total{backoff_type="NotLeader"} 1', $rendered);
        $this->assertStringContainsString('tikv_client_region_cache_hits_total{operation="scan"} 1', $rendered);
        $this->assertStringContainsString('tikv_client_region_cache_misses_total{operation="scan"} 1', $rendered);
        $this->assertStringContainsString('tikv_client_region_invalidations_total{reason="not_leader"} 1', $rendered);
        $this->assertStringContainsString(
            'tikv_client_store_health_transitions_total{store_id="7",state="unhealthy"} 1',
            $rendered,
        );
    }

    public function testDetailedSignalsAreRendered(): void
    {
        $metrics = new PrometheusMetrics();
        $metrics->backoffCompleted('ServerBusy', 2, 50.0);
        $metrics->storeAttemptCompleted(3, false, 'server_is_busy');
        $metrics->lockResolved('resolved', 2.0);
        $metrics->twoPhasePhaseCompleted('prewrite', 12.0, true);
        $metrics->twoPhasePhaseCompleted('commit', 4.0, false);

        $rendered = $metrics->render();

        $this->assertStringContainsString(
            'tikv_client_backoff_seconds_bucket{backoff_type="ServerBusy",le="0.05"} 1',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_store_attempts_total{store_id="3",outcome="error",error_kind="server_is_busy"} 1',
            $rendered,
        );
        $this->assertStringContainsString('tikv_client_lock_resolutions_total{result="resolved"} 1', $rendered);
        $this->assertStringContainsString(
            'tikv_client_two_phase_phase_seconds_bucket{phase="prewrite",outcome="success",le="0.025"} 1',
            $rendered,
        );
        $this->assertStringContainsString(
            'tikv_client_two_phase_phase_seconds_bucket{phase="commit",outcome="error",le="0.005"} 1',
            $rendered,
        );
    }

    public function testLabelValuesAreEscaped(): void
    {
        $metrics = new PrometheusMetrics();
        $metrics->rpcStarted("a\"b\\c\nd");

        $rendered = $metrics->render();

        $this->assertStringContainsString(
            'tikv_client_rpc_started_total{operation="a\"b\\\\c\\nd"} 1',
            $rendered,
        );
    }

    public function testResetClearsEverySample(): void
    {
        $metrics = new PrometheusMetrics();
        $metrics->rpcStarted('op');
        $metrics->twoPhasePhaseCompleted('prewrite', 1.0, true);

        $metrics->reset();

        $this->assertSame('', $metrics->render());
    }

    public function testCustomBucketsAreUsed(): void
    {
        $metrics = new PrometheusMetrics([0.5, 1.0]);
        $metrics->rpcCompleted('op', 750.0, true);

        $rendered = $metrics->render();

        $this->assertStringContainsString('le="0.5"} 0', $rendered);
        $this->assertStringContainsString('le="1"} 1', $rendered);
        $this->assertStringNotContainsString('le="2.5"', $rendered);
    }

    public function testRejectsEmptyBuckets(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new PrometheusMetrics([]);
    }

    public function testRejectsNonAscendingBuckets(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new PrometheusMetrics([1.0, 0.5]);
    }

    public function testContentTypeIsThePrometheusTextFormat(): void
    {
        $this->assertSame('text/plain; version=0.0.4; charset=utf-8', PrometheusMetrics::CONTENT_TYPE);
    }
}
