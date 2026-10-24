<?php

/**
 * Benchmark for issue #431 (GAP-17): the commit latency of a multi-region
 * transaction, with the two-phase-commit RPCs fanned out across regions
 * (#291, PERF-04) versus the sequential shape the fan-out replaced.
 *
 * Run from the repository root with:
 *   php benchmarks/TxnCommitLatencyBenchmark.php
 *
 * What is measured
 * ----------------
 *
 * A transaction that writes one key per region (R regions, R keys) is
 * committed against a fake store that answers every RPC after a fixed
 * server-side latency. The same client code path is measured twice:
 *
 *  1. `sequential` — the transport answers each RPC *before* the client
 *     continues, so the client issues one RPC at a time. This is the shape
 *     of the client before the fan-out (#291): R prewrite round trips, then
 *     R commit round trips, i.e. `2R x RTT` of pure waiting.
 *  2. `fan-out` (shipped) — the transport answers the RPC after the latency
 *     has elapsed, so every region's send is in flight at the same time.
 *     The prewrite phase costs one round trip for the primary plus one
 *     overlapped round trip for the secondaries, and the commit phase the
 *     same: `2 x RTT` of pure waiting, independent of R.
 *
 * The RPC count is printed next to both columns: the fan-out changes WHEN
 * the requests are sent, never HOW MANY are sent.
 *
 * The numbers are machine-dependent on purpose (never assert on them in the
 * test suite). The fake models a LAN store: 5 ms per RPC is a conservative
 * round trip; a real cluster is usually faster per call and slower under
 * load, and the ratio between the two columns is what the fan-out buys.
 */

declare(strict_types=1);

use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

require dirname(__DIR__) . '/vendor/autoload.php';

/** Server-side latency of one store RPC, in milliseconds. */
const RPC_LATENCY_MS = 5.0;

/** Region counts to sweep. */
const REGION_COUNTS = [1, 2, 5, 10, 20];

/** Repetitions per measurement; the median is reported. */
const REPEATS = 5;

/**
 * A store that answers every RPC after a fixed latency.
 *
 * `sequential` models the pre-#291 client: the answer is already there
 * when `callAsync()` returns, so the client continues only after the
 * round trip is over and the next request is issued afterwards.
 *
 * The non-sequential mode models the fan-out: the send returns immediately
 * and the response becomes available `RPC_LATENCY_MS` later, so requests
 * issued in the same burst overlap.
 */
final class BenchStore implements GrpcClientInterface
{
    public int $callCount = 0;

    /** @var array<string, int> */
    public array $callsByMethod = [];

    public function __construct(private readonly bool $sequential)
    {
    }

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        $this->callCount++;
        usleep((int) (RPC_LATENCY_MS * 1000));

        return $this->respond($method);
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        $this->callCount++;
        $this->callsByMethod[$method] = ($this->callsByMethod[$method] ?? 0) + 1;
        $dispatchAt = microtime(true);

        if ($this->sequential) {
            // The server answered before the client could issue the next
            // request: one round trip at a time.
            usleep((int) (RPC_LATENCY_MS * 1000));

            return GrpcFuture::fromWaiter(fn (): Message => $this->respond($method));
        }

        return GrpcFuture::fromWaiter(function () use ($method, $dispatchAt): Message {
            $remainingMs = RPC_LATENCY_MS - (microtime(true) - $dispatchAt) * 1000;
            if ($remainingMs > 0) {
                usleep((int) ($remainingMs * 1000));
            }

            return $this->respond($method);
        });
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
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

    private function respond(string $method): Message
    {
        return match ($method) {
            'KvPrewrite' => new PrewriteResponse(),
            'KvCommit' => new CommitResponse(),
            default => throw new RuntimeException("unexpected RPC {$method}"),
        };
    }
}

/**
 * A PD that answers from a pre-built region layout, so the benchmark
 * measures the commit path and not the metadata round trips.
 */
final class BenchPd implements PdClientInterface
{
    /**
     * @param list<RegionInfo> $regions
     */
    public function __construct(private readonly array $regions)
    {
    }

    public function getRegion(string $key): RegionInfo
    {
        foreach ($this->regions as $region) {
            if ($region->startKey === $key) {
                return $region;
            }
        }

        throw new RuntimeException("no region for key {$key}");
    }

    public function getKeyspaceId(string $name): int
    {
        throw new RuntimeException('not used by the benchmark');
    }

    public function getStore(int $storeId): ?Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress("store-{$storeId}:20160");

        return $store;
    }

    public function scanRegions(string $startKey, string $endKey, int $limit = 0): array
    {
        return $this->regions;
    }

    public function getAllStores(): array
    {
        return [];
    }

    public function getTimestamp(?int $timeoutMs = null): int
    {
        return 9001;
    }

    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array
    {
        return array_fill(0, $count, 9001);
    }

    public function getLowResolutionTimestamp(?int $timeoutMs = null): int
    {
        return 9001;
    }

    public function getGCSafePoint(): int
    {
        return 0;
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
 * R regions, one key each: region i owns ["key{i}", "key{i}\xff").
 *
 * The key is zero-padded to a fixed width so the regions never overlap:
 * with a variable width, "key1" and "key10" would place "key10" inside
 * region 1's range, and the region cache would drop one of them.
 *
 * @return list<RegionInfo>
 */
function benchLayout(int $regionCount): array
{
    $regions = [];
    for ($i = 0; $i < $regionCount; $i++) {
        $key = sprintf('key%03d', $i);
        $regions[] = new RegionInfo(
            regionId: 100 + $i,
            leaderPeerId: 1,
            leaderStoreId: 1000 + $i,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $key,
            endKey: $key . "\xff",
            peers: [],
        );
    }

    return $regions;
}

/**
 * Commit one transaction that writes one key per region and return the
 * elapsed wall-clock time in milliseconds.
 *
 * @param list<RegionInfo> $regions
 */
function benchCommit(array $regions, BenchStore $store): float
{
    $pdClient = new BenchPd($regions);
    $regionCache = new RegionCache();
    foreach ($regions as $region) {
        $regionCache->put($region);
    }

    $regionResolver = new RegionResolver($pdClient, $regionCache);
    $lockResolver = new LockResolver(
        $store,
        $regionResolver,
        $regionCache,
        $pdClient,
        1000,
    );

    $transaction = new Transaction(
        txnId: 'bench-txn',
        startTs: 1000,
        pessimistic: false,
        priority: 0,
        pdClient: $pdClient,
        grpc: $store,
        regionCache: $regionCache,
        lockResolver: $lockResolver,
        regionResolver: $regionResolver,
    );

    foreach ($regions as $region) {
        $transaction->set($region->startKey, 'v');
    }

    $startNs = hrtime(true);
    $transaction->commit();

    return (hrtime(true) - $startNs) / 1_000_000;
}

/**
 * @param list<float> $samples
 */
function benchMedian(array $samples): float
{
    sort($samples);
    $middle = intdiv(count($samples), 2);

    return count($samples) % 2 === 1
        ? $samples[$middle]
        : ($samples[$middle - 1] + $samples[$middle]) / 2;
}

/**
 * @param list<RegionInfo> $regions
 * @return array{samples: list<float>, rpcs: int, byMethod: array<string, int>}
 */
function benchSamples(array $regions, bool $sequential): array
{
    $samples = [];
    $rpcs = 0;
    $byMethod = [];
    for ($i = 0; $i < REPEATS; $i++) {
        $store = new BenchStore($sequential);
        $samples[] = benchCommit($regions, $store);
        $rpcs = $store->callCount;
        $byMethod = $store->callsByMethod;
    }

    return ['samples' => $samples, 'rpcs' => $rpcs, 'byMethod' => $byMethod];
}

printf(
    "TxnKV commit latency, one key per region, %.1f ms per store RPC (median of %d):\n",
    RPC_LATENCY_MS,
    REPEATS,
);
printf(
    "  %-8s %14s %14s %10s %12s\n",
    'regions',
    'sequential',
    'fan-out',
    'speedup',
    'RPCs',
);

$breakdown = [];
foreach (REGION_COUNTS as $regionCount) {
    $regions = benchLayout($regionCount);

    $sequential = benchSamples($regions, true);
    $fanOut = benchSamples($regions, false);

    $sequentialMs = benchMedian($sequential['samples']);
    $fanOutMs = benchMedian($fanOut['samples']);

    printf(
        "  %-8d %11.2f ms %11.2f ms %9.2fx %12s\n",
        $regionCount,
        $sequentialMs,
        $fanOutMs,
        $sequentialMs / max($fanOutMs, 0.001),
        sprintf('%d/%d', $fanOut['rpcs'], $sequential['rpcs']),
    );

    $breakdown = $fanOut['byMethod'];
}

printf(
    "\n  RPC breakdown of the widest run (%d regions): %s\n",
    max(REGION_COUNTS),
    implode(', ', array_map(
        static fn (string $method, int $count): string => "{$method}={$count}",
        array_keys($breakdown),
        $breakdown,
    )),
);
echo "  The RPC column is fan-out/sequential: the fan-out changes WHEN the requests are\n";
echo "  sent, never HOW MANY. The sequential column is the shape of the client before\n";
echo "  the two-phase-commit fan-out (#291, PERF-04) — one round trip per region and\n";
echo "  per phase, so its waiting time grows linearly with the region count while the\n";
echo "  fan-out's stays at one round trip per phase.\n";
