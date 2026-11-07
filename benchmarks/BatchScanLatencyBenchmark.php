<?php

/**
 * Benchmark for issue #433 (GAP-19): the latency of a multi-range scan with
 * the `RawBatchScan` RPC (shipped) versus the per-range loop it replaces.
 *
 * Run from the repository root with:
 *   php benchmarks/BatchScanLatencyBenchmark.php
 *
 * What is measured
 * ----------------
 *
 * The workload is {@see RANGES_PER_REGION} small ranges inside each of
 * {@see REGION_COUNT} regions: 64 disjoint ranges in total. Every range is
 * one round trip's worth of work for the per-range path, and every region
 * is one round trip's worth for `RawBatchScan`, which groups the ranges that
 * share a region into a single request.
 *
 * Two shapes are timed against the same fake store, which answers every RPC
 * after a fixed server-side latency:
 *
 *  1. `per-range loop` — `scan()` once per range, one after another. This is
 *     the shape the feature-gap audit describes: R ranges cost R `RawScan`
 *     round trips, even when several ranges live in the same region. (The
 *     client since #295 fans this loop out concurrently, which overlaps the
 *     waiting but still issues one `RawScan` per range and region.)
 *  2. `RawBatchScan` (shipped) — one request per region, all dispatched in
 *     the same burst, so the whole call costs one round trip for every
 *     region window instead of one per range.
 *
 * The RPC count is printed next to both columns: it is the part of the win
 * that does not depend on concurrency at all.
 *
 * The numbers are machine-dependent on purpose (never assert on them in the
 * test suite). A 2 ms per-RPC latency is a conservative LAN round trip; the
 * ratio between the columns is what the grouping buys.
 */

declare(strict_types=1);

use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

require dirname(__DIR__) . '/vendor/autoload.php';

/** Server-side latency of one store RPC, in milliseconds. */
const RPC_LATENCY_MS = 5.0;

/** Number of regions the ranges are spread over. */
const REGION_COUNT = 8;

/** Number of disjoint ranges inside each region. */
const RANGES_PER_REGION = 8;

/** Repetitions per measurement; the median is reported. */
const REPEATS = 3;

/** Rows requested per range (one is enough for the timing). */
const EACH_LIMIT = 10;

/**
 * A store that answers every RPC after a fixed latency.
 *
 * `call()` (the bounded `scan()` path) blocks for the round trip before it
 * returns, so a sequential loop pays one latency per call.
 *
 * `callAsync()` (the `RawBatchScan` path) returns immediately and its waiter
 * sleeps only until the latency measured from the dispatch has elapsed, so
 * requests issued in the same burst overlap.
 */
final class BenchStore implements GrpcClientInterface
{
    public int $rawScanCalls = 0;

    public int $rawBatchScanCalls = 0;

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        $this->rawScanCalls++;
        usleep((int) (RPC_LATENCY_MS * 1000));

        if ($method !== 'RawScan' || !$request instanceof RawScanRequest) {
            throw new RuntimeException("unexpected RPC {$method}");
        }

        $pair = new KvPair();
        $pair->setKey($request->getStartKey() . '-row');
        $pair->setValue('v');
        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        return $response;
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        if ($method !== 'RawBatchScan' || !$request instanceof RawBatchScanRequest) {
            throw new RuntimeException("unexpected async RPC {$method}");
        }

        $this->rawBatchScanCalls++;
        $dispatchAt = microtime(true);

        return GrpcFuture::fromWaiter(function () use ($request, $dispatchAt): Message {
            $remainingMs = RPC_LATENCY_MS - (microtime(true) - $dispatchAt) * 1000;
            if ($remainingMs > 0) {
                usleep((int) ($remainingMs * 1000));
            }

            $pairs = [];
            foreach ($request->getRanges() as $range) {
                /** @var KeyRange $range */
                $pair = new KvPair();
                $pair->setKey($range->getStartKey() . '-row');
                $pair->setValue('v');
                $pairs[] = $pair;
            }

            $response = new RawBatchScanResponse();
            $response->setKvs($pairs);

            return $response;
        });
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        iterable $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        throw new RuntimeException('callStreaming is not used by the benchmark');
    }

    public function close(): void
    {
    }

    public function closeChannel(string $address): void
    {
    }

    public function reapIdleChannels(): void
    {
    }

    public function getChannel(string $address): Channel
    {
        throw new RuntimeException('getChannel is not used by the benchmark');
    }
}

/**
 * A PD that answers from a pre-built region layout, so the benchmark measures
 * the scan path and not the metadata round trips.
 */
final class BenchPd implements PdClientInterface
{
    /**
     * @param list<RegionInfo> $regions
     */
    public function __construct(private readonly array $regions)
    {
    }

    public function getRegion(string $key, ?int $timeoutMs = null): RegionInfo
    {
        foreach ($this->regions as $region) {
            if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                return $region;
            }
        }

        throw new RuntimeException('no region for key ' . bin2hex($key));
    }

    public function getPrevRegion(string $key, ?int $timeoutMs = null): ?RegionInfo
    {
        return null;
    }

    public function getKeyspaceId(string $name): int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getStore(int $storeId): ?Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress('store-1:20160');

        return $store;
    }

    public function scanRegions(
        string $startKey,
        string $endKey,
        int $limit = 0,
        ?int $timeoutMs = null,
    ): array {
        $matches = [];
        foreach ($this->regions as $region) {
            if ($region->endKey !== '' && KeyOrder::lte($region->endKey, $startKey)) {
                continue;
            }
            if ($endKey !== '' && KeyOrder::gte($region->startKey, $endKey)) {
                continue;
            }
            $matches[] = $region;
            if ($limit > 0 && count($matches) >= $limit) {
                break;
            }
        }

        return $matches;
    }

    public function getAllStores(): array
    {
        return [];
    }

    public function getTimestamp(?int $timeoutMs = null): int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getLowResolutionTimestamp(?int $timeoutMs = null): int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getGCSafePoint(): int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function updateServiceGCSafePoint(string $serviceId, int $safePoint, int $ttlSeconds): ?int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getClusterId(): ?int
    {
        return null;
    }

    public function ping(): ?int
    {
        return null;
    }

    public function setClusterId(int $clusterId): void
    {
    }

    public function close(): void
    {
    }
}

/**
 * A layout of REGION_COUNT regions, each owning RANGES_PER_REGION disjoint
 * ranges. Keys are fixed-width so byte order and the visual order agree.
 *
 * @return array{regions: list<RegionInfo>, ranges: list<array{0: string, 1: string}>}
 */
function benchLayout(): array
{
    $regions = [];
    $ranges = [];
    for ($region = 0; $region < REGION_COUNT; $region++) {
        $start = sprintf('r%02d', $region);
        $end = $region + 1 === REGION_COUNT ? '' : sprintf('r%02d', $region + 1);
        $regions[] = new RegionInfo(
            regionId: $region + 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $start,
            endKey: $end,
        );

        for ($range = 0; $range < RANGES_PER_REGION; $range++) {
            $ranges[] = [
                sprintf('%s-%02d', $start, $range),
                sprintf('%s-%02d', $start, $range + 1),
            ];
        }
    }

    return ['regions' => $regions, 'ranges' => $ranges];
}

/**
 * @param list<RegionInfo> $regions
 */
function benchScanner(array $regions, BenchStore $store): RawKvScanner
{
    $pdClient = new BenchPd($regions);
    $regionCache = new RegionCache();
    foreach ($regions as $region) {
        $regionCache->put($region);
    }

    return new RawKvScanner(
        $pdClient,
        $store,
        new RegionResolver($pdClient, $regionCache),
        new TimeoutConfig(),
        maxBackoffMs: 20000,
        serverBusyBudgetMs: 60000,
        regionCache: $regionCache,
        logger: new Psr\Log\NullLogger(),
    );
}

/**
 * @param list<array{0: string, 1: string}> $ranges
 * @param list<RegionInfo> $regions
 */
function benchPerRange(array $ranges, array $regions, BenchStore $store): float
{
    $scanner = benchScanner($regions, $store);
    $startNs = hrtime(true);
    foreach ($ranges as [$startKey, $endKey]) {
        $scanner->scan($startKey, $endKey, EACH_LIMIT, false);
    }

    return (hrtime(true) - $startNs) / 1_000_000;
}

/**
 * @param list<array{0: string, 1: string}> $ranges
 * @param list<RegionInfo> $regions
 */
function benchBatchScan(array $ranges, array $regions, BenchStore $store): float
{
    $scanner = benchScanner($regions, $store);
    $startNs = hrtime(true);
    $scanner->batchScan($ranges, EACH_LIMIT, false);

    return (hrtime(true) - $startNs) / 1_000_000;
}

/**
 * @param list<float> $samples
 */
function benchMedian(array $samples): float
{
    sort($samples);

    return $samples[intdiv(count($samples), 2)];
}

$layout = benchLayout();
$ranges = $layout['ranges'];
$regions = $layout['regions'];
$rangeCount = count($ranges);

$perRangeSamples = [];
$batchScanSamples = [];
for ($repeat = 0; $repeat < REPEATS; $repeat++) {
    $perRangeStore = new BenchStore();
    $perRangeSamples[] = benchPerRange($ranges, $regions, $perRangeStore);
    $perRangeCalls = $perRangeStore->rawScanCalls;

    $batchScanStore = new BenchStore();
    $batchScanSamples[] = benchBatchScan($ranges, $regions, $batchScanStore);
    $batchScanCalls = $batchScanStore->rawBatchScanCalls;
}

$perRangeMedian = benchMedian($perRangeSamples);
$batchScanMedian = benchMedian($batchScanSamples);

printf(
    "batchScan of %d disjoint ranges across %d regions (eachLimit %d, %.1f ms per RPC)\n",
    $rangeCount,
    REGION_COUNT,
    EACH_LIMIT,
    RPC_LATENCY_MS,
);
printf("  per-range loop (sequential): %3d RawScan RPCs,     median %8.2f ms\n", $perRangeCalls, $perRangeMedian);
printf("  RawBatchScan:                %3d RawBatchScan RPCs, median %8.2f ms\n", $batchScanCalls, $batchScanMedian);
printf("  RPC reduction:   %.1fx\n", $perRangeCalls / max(1, $batchScanCalls));
printf("  latency speedup: %.1fx\n", $perRangeMedian / max(0.001, $batchScanMedian));
