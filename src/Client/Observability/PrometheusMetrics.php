<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

/**
 * Dependency-free Prometheus adapter for {@see MetricsInterface} and
 * {@see DetailedMetricsInterface} (issue #434, GAP-20).
 *
 * The class keeps counters and histograms in process memory and renders the
 * Prometheus text exposition format through {@see self::render()}. It has no
 * dependency on a Prometheus client library and adds none to the package,
 * so exposing the client's metrics needs only an endpoint that returns
 * {@see self::CONTENT_TYPE} and the rendered body:
 *
 * <code>
 * $metrics = new PrometheusMetrics();
 * $client = RawKvClient::create($pd, options: ['metrics' => $metrics]);
 *
 * // In your HTTP metrics endpoint:
 * header('Content-Type: ' . PrometheusMetrics::CONTENT_TYPE);
 * echo $metrics->render();
 * </code>
 *
 * Like {@see InMemoryMetrics} it is single-process state: PHP-FPM workers do
 * not share it, so each worker exposes its own counters (Prometheus sums the
 * `_total` series across instances). For multi-process aggregation, scrape
 * every worker or implement {@see MetricsInterface} on top of a shared store.
 *
 * All durations are exposed in **seconds** (the Prometheus base unit), while
 * the interface receives milliseconds.
 */
final class PrometheusMetrics implements DetailedMetricsInterface
{
    /**
     * Content type of {@see self::render()}, including the text format
     * version Prometheus expects.
     */
    public const CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8';

    /**
     * Default histogram buckets, in seconds: 1 ms to 10 s. They cover a
     * point read (sub-millisecond to a few milliseconds), a retry backoff
     * (tens of milliseconds to seconds) and a slow multi-region commit
     * (seconds).
     *
     * @var list<float>
     */
    private const DEFAULT_DURATION_BUCKETS = [
        0.001,
        0.0025,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    ];

    /**
     * @var array<string, array{name: string, labels: array<string, string>, value: float}>
     */
    private array $counters = [];

    /**
     * @var array<string, array{
     *     name: string,
     *     labels: array<string, string>,
     *     bucketCounts: list<int>,
     *     sum: float,
     *     count: int
     * }>
     */
    private array $histograms = [];

    /** @var list<float> */
    private readonly array $durationBuckets;

    /**
     * @param list<float>|null $durationBuckets Ascending histogram bucket
     *        bounds in seconds. Null uses {@see self::DEFAULT_DURATION_BUCKETS}.
     */
    public function __construct(?array $durationBuckets = null)
    {
        $buckets = $durationBuckets ?? self::DEFAULT_DURATION_BUCKETS;
        if ($buckets === []) {
            throw new InvalidArgumentException('durationBuckets must not be empty');
        }

        $previous = -1.0;
        foreach ($buckets as $bucket) {
            if (!is_finite($bucket) || $bucket <= 0.0) {
                throw new InvalidArgumentException('durationBuckets must contain positive finite values');
            }
            if ($bucket <= $previous) {
                throw new InvalidArgumentException('durationBuckets must be strictly ascending');
            }
            $previous = $bucket;
        }

        $this->durationBuckets = $buckets;
    }

    public function rpcStarted(string $operation): void
    {
        $this->increment('tikv_client_rpc_started_total', ['operation' => $operation]);
    }

    public function rpcCompleted(string $operation, float $durationMs, bool $success): void
    {
        $status = $success ? 'success' : 'error';
        $this->increment('tikv_client_rpc_completed_total', [
            'operation' => $operation,
            'status' => $status,
        ]);
        $this->observe('tikv_client_rpc_duration_seconds', [
            'operation' => $operation,
            'status' => $status,
        ], $durationMs / 1000.0);
    }

    public function retryAttempted(string $operation): void
    {
        $this->increment('tikv_client_retries_total', ['backoff_type' => $operation]);
    }

    public function regionCacheHit(string $operation): void
    {
        $this->increment('tikv_client_region_cache_hits_total', ['operation' => $operation]);
    }

    public function regionCacheMiss(string $operation): void
    {
        $this->increment('tikv_client_region_cache_misses_total', ['operation' => $operation]);
    }

    public function regionInvalidated(string $reason): void
    {
        $this->increment('tikv_client_region_invalidations_total', ['reason' => $reason]);
    }

    public function storeHealthChanged(int $storeId, bool $healthy): void
    {
        $this->increment('tikv_client_store_health_transitions_total', [
            'store_id' => (string) $storeId,
            'state' => $healthy ? 'healthy' : 'unhealthy',
        ]);
    }

    public function backoffCompleted(string $backoffType, int $attempt, float $durationMs): void
    {
        $this->observe('tikv_client_backoff_seconds', [
            'backoff_type' => $backoffType,
        ], $durationMs / 1000.0);
    }

    public function storeAttemptCompleted(int $storeId, bool $success, ?string $errorKind = null): void
    {
        $this->increment('tikv_client_store_attempts_total', [
            'store_id' => (string) $storeId,
            'outcome' => $success ? 'success' : 'error',
            'error_kind' => $errorKind ?? '',
        ]);
    }

    public function lockResolved(string $result, float $durationMs): void
    {
        $this->increment('tikv_client_lock_resolutions_total', ['result' => $result]);
        $this->observe('tikv_client_lock_resolution_seconds', [
            'result' => $result,
        ], $durationMs / 1000.0);
    }

    public function twoPhasePhaseCompleted(string $phase, float $durationMs, bool $success): void
    {
        $this->observe('tikv_client_two_phase_phase_seconds', [
            'phase' => $phase,
            'outcome' => $success ? 'success' : 'error',
        ], $durationMs / 1000.0);
    }

    /**
     * Render every recorded sample in the Prometheus text exposition format.
     *
     * Output is deterministic: series are grouped by metric name and names
     * are sorted, so a test can assert on the whole body.
     */
    public function render(): string
    {
        $lines = [];
        $counterNames = [];
        foreach ($this->counters as $sample) {
            $counterNames[$sample['name']] = true;
        }
        ksort($counterNames);
        foreach (array_keys($counterNames) as $name) {
            $lines[] = '# HELP ' . $name . ' ' . $this->help($name);
            $lines[] = '# TYPE ' . $name . ' counter';
            foreach ($this->counters as $sample) {
                if ($sample['name'] === $name) {
                    $lines[] = $this->formatSample($name, $sample['labels'], $sample['value']);
                }
            }
        }

        $histogramNames = [];
        foreach ($this->histograms as $sample) {
            $histogramNames[$sample['name']] = true;
        }
        ksort($histogramNames);
        foreach (array_keys($histogramNames) as $name) {
            $lines[] = '# HELP ' . $name . ' ' . $this->help($name);
            $lines[] = '# TYPE ' . $name . ' histogram';
            foreach ($this->histograms as $sample) {
                if ($sample['name'] !== $name) {
                    continue;
                }
                $cumulative = 0;
                foreach ($this->durationBuckets as $index => $bucket) {
                    $cumulative += $sample['bucketCounts'][$index] ?? 0;
                    $bucketLabels = $sample['labels'];
                    $bucketLabels['le'] = $this->formatNumber($bucket);
                    $lines[] = $this->formatSample($name . '_bucket', $bucketLabels, $cumulative);
                }
                $bucketLabels = $sample['labels'];
                $bucketLabels['le'] = '+Inf';
                $lines[] = $this->formatSample($name . '_bucket', $bucketLabels, $sample['count']);
                $lines[] = $this->formatSample($name . '_sum', $sample['labels'], $sample['sum']);
                $lines[] = $this->formatSample($name . '_count', $sample['labels'], $sample['count']);
            }
        }

        return $lines === [] ? '' : implode("\n", $lines) . "\n";
    }

    /**
     * Drop every recorded sample. Intended for tests.
     */
    public function reset(): void
    {
        $this->counters = [];
        $this->histograms = [];
    }

    /**
     * @param array<string, string> $labels
     */
    private function increment(string $name, array $labels = []): void
    {
        $key = $this->key($name, $labels);
        $sample = $this->counters[$key] ?? [
            'name' => $name,
            'labels' => $labels,
            'value' => 0.0,
        ];
        $sample['value'] += 1.0;
        $this->counters[$key] = $sample;
    }

    /**
     * @param array<string, string> $labels
     */
    private function observe(string $name, array $labels, float $seconds): void
    {
        $value = is_finite($seconds) ? max(0.0, $seconds) : 0.0;
        $key = $this->key($name, $labels);
        $sample = $this->histograms[$key] ?? [
            'name' => $name,
            'labels' => $labels,
            'bucketCounts' => array_fill(0, count($this->durationBuckets), 0),
            'sum' => 0.0,
            'count' => 0,
        ];

        foreach ($this->durationBuckets as $index => $bucket) {
            if ($value <= $bucket) {
                $counts = $sample['bucketCounts'];
                $counts[$index] = ($counts[$index] ?? 0) + 1;
                $sample['bucketCounts'] = array_values($counts);
                break;
            }
        }
        $sample['sum'] += $value;
        $sample['count'] += 1;
        $this->histograms[$key] = $sample;
    }

    /**
     * @param array<string, string> $labels
     */
    private function key(string $name, array $labels): string
    {
        if ($labels === []) {
            return $name;
        }

        ksort($labels);
        $parts = [];
        foreach ($labels as $label => $value) {
            $parts[] = $label . '=' . $value;
        }

        return $name . '{' . implode(',', $parts) . '}';
    }

    /**
     * @param array<string, string> $labels
     */
    private function formatSample(string $name, array $labels, float|int $value): string
    {
        $rendered = '';
        if ($labels !== []) {
            $pairs = [];
            foreach ($labels as $label => $labelValue) {
                $pairs[] = $label . '="' . $this->escapeLabelValue($labelValue) . '"';
            }
            $rendered = '{' . implode(',', $pairs) . '}';
        }

        return $name . $rendered . ' ' . $this->formatNumber((float) $value);
    }

    private function escapeLabelValue(string $value): string
    {
        return str_replace(
            ["\\", '"', "\n"],
            ["\\\\", '\\"', '\\n'],
            $value,
        );
    }

    private function formatNumber(float $value): string
    {
        if ($value === 0.0) {
            return '0';
        }

        $formatted = sprintf('%.9F', $value);
        $formatted = rtrim(rtrim($formatted, '0'), '.');

        return $formatted === '' ? '0' : $formatted;
    }

    private function help(string $name): string
    {
        return match ($name) {
            'tikv_client_rpc_started_total' => 'Outbound gRPC calls dispatched.',
            'tikv_client_rpc_completed_total' => 'Completed gRPC calls by outcome.',
            'tikv_client_rpc_duration_seconds' => 'gRPC call duration in seconds.',
            'tikv_client_retries_total' => 'Retryable errors scheduled for another attempt.',
            'tikv_client_region_cache_hits_total' => 'Region lookups served from the client cache.',
            'tikv_client_region_cache_misses_total' => 'Region lookups that had to query PD.',
            'tikv_client_region_invalidations_total' => 'Regions dropped from the client cache.',
            'tikv_client_store_health_transitions_total' => 'Store health transitions.',
            'tikv_client_backoff_seconds' => 'Retry backoff duration in seconds.',
            'tikv_client_store_attempts_total' => 'RPC attempts attributed to a store.',
            'tikv_client_lock_resolutions_total' => 'Lock-resolution attempts by result.',
            'tikv_client_lock_resolution_seconds' => 'Lock-resolution duration in seconds.',
            'tikv_client_two_phase_phase_seconds' => 'Two-phase-commit phase duration in seconds.',
            default => 'TiKV client metric.',
        };
    }
}
