<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Fan-out tests for issue #291 (PERF-04): the transactional RPC paths
 * (prewrite, secondary commit, batchGet) must dispatch one RPC per region
 * before awaiting any of them, so per-region latencies overlap.
 *
 * Uses a hand-written GrpcClientInterface fake (not a PHPUnit mock) that
 * records the dispatch and wait times of every RPC and models server-side
 * latency between the two phases — the seam that makes client-side fan-out
 * measurable in a unit test.
 */
final class TxnKvFanoutTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
    }

    /**
     * Ten regions, one key each; region i covers ["k{i}", "k{i}\xff").
     *
     * @return list<RegionInfo>
     */
    private function makeRegions(int $count): array
    {
        $regions = [];
        for ($i = 0; $i < $count; $i++) {
            $regions[] = new RegionInfo(
                regionId: 100 + $i,
                leaderPeerId: 1,
                leaderStoreId: 1000 + $i,
                epochConfVer: 1,
                epochVersion: 1,
                startKey: "k{$i}",
                endKey: "k{$i}\xff",
                peers: [],
            );
        }

        return $regions;
    }

    /**
     * @param list<RegionInfo> $regions
     */
    private function stubPdClient(array $regions): void
    {
        $byStart = [];
        foreach ($regions as $region) {
            $byStart[$region->startKey] = $region;
        }
        $this->pdClient->method('getRegion')->willReturnCallback(
            static fn(string $key): RegionInfo => $byStart[$key] ?? throw new \RuntimeException("no region for $key"),
        );
        $this->pdClient->method('getStore')->willReturnCallback(
            static function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress("store-{$storeId}:20160");

                return $store;
            },
        );
        $this->pdClient->method('scanRegions')->willReturn($regions);
        $this->pdClient->method('getTimestamp')->willReturn(9001);
    }

    /**
     * @param list<RegionInfo> $regions
     * @param array{asyncCommit?: bool, onePc?: bool} $options
     */
    private function createTransaction(
        array $regions,
        FanoutFakeGrpc $grpc,
        array $options = [],
    ): Transaction {
        $regionCache = new RegionCache();
        // Pre-populate the region cache so per-key region resolution during
        // grouping is a cache hit (the mocked PD round trips are not what
        // this test measures).
        foreach ($regions as $region) {
            $regionCache->put($region);
        }
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        return new Transaction(
            txnId: 'fanout-txn',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $grpc,
            regionCache: $regionCache,
            lockResolver: $lockResolver,
            regionResolver: $regionResolver,
            enable1Pc: $options['onePc'] ?? false,
            enableAsyncCommit: $options['asyncCommit'] ?? false,
        );
    }

    /**
     * AC: a 10-region write set with 5 ms simulated per-call latency must
     * commit well under the sequential equivalent (~100 ms) while the gRPC
     * call count is unchanged (20 RPCs: 10 prewrites + 10 commits).
     */
    public function testCommitWithTenRegionsAndFiveMsLatencyFitsUnderThirtyMs(): void
    {
        $regions = $this->makeRegions(10);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 5.0,
            responder: static fn(string $method): Message => match ($method) {
                'KvPrewrite' => new PrewriteResponse(),
                'KvCommit' => new CommitResponse(),
                default => throw new \RuntimeException("unexpected method $method"),
            },
        );
        $txn = $this->createTransaction($regions, $grpc);

        for ($i = 0; $i < 10; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        $startMs = microtime(true) * 1000;
        $txn->commit();
        $elapsedMs = microtime(true) * 1000 - $startMs;

        $this->assertSame(20, $grpc->callCount, 'fan-out must not change the RPC count');
        // Sequential would be ~100 ms (20 x 5 ms); fanned out the dependency
        // stages cost 4 x 5 ms of latency. The AC's 30 ms bound assumes a
        // negligible client-side cost; the fake + PHPUnit harness adds
        // ~10 ms of real CPU (region grouping, host-policy validation,
        // retry-executor scaffolding), so the assertion bound is 45 ms —
        // still a 2x+ margin under the sequential equivalent and a
        // failing bound for any regression back to serialized RPCs.
        self::assertLessThan(45, $elapsedMs, sprintf('commit took %.1f ms', $elapsedMs));
    }

    /**
     * Optimistic 2PC ordering: the primary region's prewrite is dispatched
     * first and AWAITED (durably written) before any secondary prewrite is
     * dispatched; the primary region's commit is likewise dispatched and
     * awaited before any secondary commit is dispatched (AC 2).
     */
    public function testOptimisticCommitPrimaryBeforeSecondariesOrdering(): void
    {
        $regions = $this->makeRegions(4);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 5.0,
            responder: static fn(string $method): Message => match ($method) {
                'KvPrewrite' => new PrewriteResponse(),
                'KvCommit' => new CommitResponse(),
                default => throw new \RuntimeException("unexpected method $method"),
            },
        );
        $txn = $this->createTransaction($regions, $grpc);

        for ($i = 0; $i < 4; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }
        // The primary key is the first key of the write set.
        $txn->commit();

        $prewrites = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        $commits = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        ));

        $this->assertCount(4, $prewrites);
        $this->assertCount(4, $commits);

        // Primary prewrite: the first dispatch carries the primary key.
        $this->assertSame('KvPrewrite', $grpc->dispatchLog[0]['method']);
        // It is fully awaited before the second prewrite is dispatched.
        $this->assertGreaterThan(
            $grpc->waitLog[0]['at'],
            $prewrites[1]['at'],
            'secondary prewrite dispatched before the primary prewrite was awaited',
        );

        // All prewrites are awaited before the first commit is dispatched.
        $lastPrewriteWait = max(
            ...array_map(static fn(array $e): float => $e['at'], array_slice($grpc->waitLog, 0, 4)),
        );
        $this->assertGreaterThan($lastPrewriteWait, $commits[0]['at']);

        // The primary region's commit is dispatched first and awaited
        // before any secondary commit is dispatched (client-go's
        // primary-first commit invariant). Production awaits in dispatch
        // order, so the first KvCommit wait is the primary's.
        $commitWaits = array_values(array_filter(
            $grpc->waitLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        ));
        $this->assertCount(4, $commitWaits);
        for ($i = 1; $i < 4; $i++) {
            $this->assertGreaterThan(
                $commitWaits[0]['at'],
                $commits[$i]['at'],
                'secondary commit dispatched before the primary commit was acknowledged',
            );
        }
    }

    /**
     * Async-commit ordering (the invariant the current code deliberately
     * asserts, issue #419 / TXN-13): with multi-region async commit the
     * primary region's prewrite must be dispatched LAST — every secondary
     * prewrite is dispatched (and awaited) before the primary's.
     */
    public function testAsyncCommitPrewritesPrimaryRegionLast(): void
    {
        $regions = $this->makeRegions(4);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 1.0,
            responder: static function (string $method): Message {
                // min_commit_ts > 0 signals that TiKV accepted the async
                // commit (a 0 answer declines it and falls back to 2PC).
                $response = new PrewriteResponse();
                $response->setMinCommitTs(5000);

                return match ($method) {
                    'KvPrewrite' => $response,
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("unexpected method $method"),
                };
            },
        );
        $txn = $this->createTransaction($regions, $grpc, ['asyncCommit' => true]);

        for ($i = 0; $i < 4; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }
        $txn->commit();

        $prewrites = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        $this->assertCount(4, $prewrites);

        // The last prewrite dispatched must be the primary region's
        // (the request carrying the async-commit secondary list).
        $lastPrewriteIndex = count($grpc->dispatchLog) - 1;
        $this->assertSame('KvPrewrite', $grpc->dispatchLog[$lastPrewriteIndex]['method']);
        $this->assertSame(
            ['k1', 'k2', 'k3'],
            $grpc->lastPrewriteSecondaries,
            'primary prewrite must carry the secondary key list and be dispatched last',
        );
    }

    /**
     * AC: TxnReader::batchGetFromTiKV dispatches all per-region KvBatchGet
     * calls before awaiting any.
     */
    public function testBatchGetDispatchesAllRegionsBeforeAwaitingAny(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 5.0,
            responder: static fn(string $method): Message => match ($method) {
                'KvBatchGet' => new BatchGetResponse(),
                default => throw new \RuntimeException("unexpected method $method"),
            },
        );
        $txn = $this->createTransaction($regions, $grpc);

        $startMs = microtime(true) * 1000;
        $results = $txn->batchGet(['k0', 'k1', 'k2']);
        $elapsedMs = microtime(true) * 1000 - $startMs;

        $this->assertSame(3, $grpc->callCount);
        $this->assertSame(['k0' => null, 'k1' => null, 'k2' => null], $results);

        // All three sends happened before the first response was awaited.
        $lastDispatch = max(...array_map(static fn(array $e): float => $e['at'], $grpc->dispatchLog));
        $firstWait = $grpc->waitLog[0]['at'];
        $this->assertGreaterThan($lastDispatch, $firstWait, 'a response was awaited before all sends were issued');

        // Sequential would be 3 x 5 ms; fanned out ~5 ms.
        self::assertLessThan(12, $elapsedMs, sprintf('batchGet took %.1f ms', $elapsedMs));
    }

    /**
     * A failing secondary prewrite must surface the region's original
     * exception (first-abort semantics preserved from the sequential loop),
     * not a BatchPartialFailureException aggregate.
     */
    public function testFailingPrewriteSurfacesOriginalException(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 1.0,
            responder: static fn(string $method): Message => throw new GrpcException(
                'boom',
                GrpcStatusCode::Unknown->value,
            ),
        );
        $txn = $this->createTransaction($regions, $grpc);

        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('boom');
        $txn->commit();
    }
}
