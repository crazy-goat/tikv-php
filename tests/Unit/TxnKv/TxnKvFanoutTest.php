<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Fan-out tests for issue #291 (PERF-04) and issue #431 (GAP-17): the
 * transactional RPC paths (prewrite, secondary commit, batchGet) must
 * dispatch one RPC per region before awaiting any of them, so per-region
 * latencies overlap.
 *
 * Uses a hand-written GrpcClientInterface fake (not a PHPUnit mock) that
 * records the dispatch and wait times of every RPC and models server-side
 * latency between the two phases — the seam that makes client-side fan-out
 * measurable in a unit test.
 *
 * #431 adds the partial-failure half of the fan-out: a region that fails
 * must not abort its peers, and the retry must be scoped to the region that
 * failed.
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
        LoggerInterface $logger = new NullLogger(),
        TimeoutConfig $timeoutConfig = new TimeoutConfig(),
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
            logger: $logger,
            timeoutConfig: $timeoutConfig,
            enable1Pc: $options['onePc'] ?? false,
            enableAsyncCommit: $options['asyncCommit'] ?? false,
        );
    }

    /**
     * AC: a 10-region write set commits with full fan-out overlap — every
     * secondary prewrite (and every secondary commit) is dispatched before
     * any of its peers is awaited — while the gRPC call count is unchanged
     * (20 RPCs: 10 prewrites + 10 commits).
     *
     * Deliberately NO absolute wall-clock ceiling here (FAQ, #291 review
     * round 1): a bound tight enough to fail on a regression to serialized
     * RPCs (≥10 × latency) is close enough to the fake's own usleep
     * granularity + harness overhead (~10 ms of real CPU) to fail
     * deterministically on macOS in isolation. The fan-out is instead
     * proven from the SPREAD of the fake's own dispatch timestamps — a
     * property a serialized implementation cannot satisfy, measured on a
     * single clock, independent of harness overhead.
     */
    public function testCommitWithTenRegionsOverlapsSecondaryDispatchAndWait(): void
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

        $txn->commit();

        $this->assertSame(20, $grpc->callCount, 'fan-out must not change the RPC count');

        $prewriteWaits = array_values(array_filter(
            $grpc->waitLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        $commitWaits = array_values(array_filter(
            $grpc->waitLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        ));
        $prewriteDispatches = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        $commitDispatches = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        ));
        $this->assertCount(10, $prewriteDispatches);
        $this->assertCount(10, $prewriteWaits);
        $this->assertCount(10, $commitDispatches);
        $this->assertCount(10, $commitWaits);

        // Secondary prewrites: after the primary's prewrite is awaited, all
        // 9 remaining sends are issued back-to-back. The SPREAD of their
        // dispatch timestamps is the fan-out proof: a serialized
        // implementation dispatches each only after the previous one's wait
        // completes, spreading them by >= 9 x latencyMs (45 ms); fanned out,
        // the dispatch loop itself costs microseconds. The bound is 2x
        // latencyMs — measured between the fake's own dispatch stamps, so
        // PHPUnit harness overhead outside the dispatch loop cannot inflate
        // it, and a serialized regression overshoots it 4x+.
        $secondaryPrewriteDispatches = array_slice(
            array_map(static fn(array $e): float => $e['at'], $prewriteDispatches),
            1,
        );
        $this->assertLessThan(
            10.0,
            max(...$secondaryPrewriteDispatches) - min(...$secondaryPrewriteDispatches),
            'secondary prewrites were not dispatched in a single burst',
        );

        // Secondary commits: same burst property after the primary's commit.
        $secondaryCommitDispatches = array_slice(
            array_map(static fn(array $e): float => $e['at'], $commitDispatches),
            1,
        );
        $this->assertLessThan(
            10.0,
            max(...$secondaryCommitDispatches) - min(...$secondaryCommitDispatches),
            'secondary commits were not dispatched in a single burst',
        );
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

    /**
     * AC (issue #431, GAP-17): the parallel prewrite aggregates the
     * per-region failures instead of aborting at the first one.
     *
     * Two of the three regions fail. Every region's request must still be
     * dispatched and awaited — the fan-out waits for its in-flight calls so
     * the failure of one region is observed together with its peers' — and
     * the error that surfaces is the one the sequential loop would have
     * thrown first (issue #291's first-error contract).
     */
    public function testFailingRegionsAreAllObservedAndTheFirstErrorSurfaces(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 1.0,
            responder: static function (string $method, Message $request): Message {
                if ($method === 'KvPrewrite') {
                    $keys = self::requestKeys($request);
                    foreach (['k1', 'k2'] as $failing) {
                        if (in_array($failing, $keys, true)) {
                            // A fatal gRPC status: the region error is not
                            // retried, so the test does not wait for the
                            // retry budget.
                            throw new GrpcException(
                                "prewrite failed on {$failing}",
                                GrpcStatusCode::InvalidArgument->value,
                            );
                        }
                    }
                }

                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("unexpected method $method"),
                };
            },
        );
        $txn = $this->createTransaction($regions, $grpc);

        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        try {
            $txn->commit();
            self::fail('the failing prewrite must abort the commit');
        } catch (GrpcException $e) {
            self::assertSame('gRPC error: prewrite failed on k1', $e->getMessage());
        }

        // Every region's prewrite was dispatched and awaited — the fan-out
        // did not cancel the peer region when the first one failed.
        $prewriteDispatches = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        $prewriteWaits = array_values(array_filter(
            $grpc->waitLog,
            static fn(array $e): bool => $e['method'] === 'KvPrewrite',
        ));
        self::assertCount(3, $prewriteDispatches);
        self::assertCount(3, $prewriteWaits, 'the peer region must still be awaited');

        // The commit phase was never reached.
        self::assertSame(0, count(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        )));
    }

    /**
     * AC (issue #431, GAP-17): a failing region is retried on its own.
     *
     * The region that answers a retryable region error (epoch not match) is
     * re-dispatched by that region's own retry loop; the regions that
     * succeeded are NOT re-sent. The retry is therefore scoped to the
     * failing region instead of re-running the whole fan-out.
     */
    public function testFailingRegionIsRetriedOnItsOwnWhilePeersAreNotResent(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);

        /** @var array<string, int> $prewriteCalls */
        $prewriteCalls = [];
        $grpc = new FanoutFakeGrpc(
            latencyMs: 1.0,
            responder: static function (string $method, Message $request) use (&$prewriteCalls): Message {
                if ($method === 'KvPrewrite') {
                    $keys = self::requestKeys($request);
                    $key = $keys[0] ?? '';
                    $prewriteCalls[$key] = ($prewriteCalls[$key] ?? 0) + 1;
                    if ($key === 'k1' && $prewriteCalls[$key] === 1) {
                        $error = new Error();
                        $error->setMessage('epoch not match');
                        $error->setEpochNotMatch(new EpochNotMatch());

                        $response = new PrewriteResponse();
                        $response->setRegionError($error);

                        return $response;
                    }
                }

                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("unexpected method $method"),
                };
            },
        );
        $txn = $this->createTransaction($regions, $grpc);

        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        $txn->commit();

        // Only the failing region was re-sent; the peers were not.
        self::assertEquals(
            ['k0' => 1, 'k1' => 2, 'k2' => 1],
            $prewriteCalls,
            'the retry must be scoped to the failing region',
        );

        // The commit phase sent one request per region, as usual.
        $commitDispatches = array_values(array_filter(
            $grpc->dispatchLog,
            static fn(array $e): bool => $e['method'] === 'KvCommit',
        ));
        self::assertCount(3, $commitDispatches);
    }

    /**
     * AC (issue #431, GAP-17): the aggregated partial failure is reported
     * with the identity of EVERY failing region.
     *
     * The exception the commit path throws is the first failing region's
     * own error (issue #291's first-error contract), so the aggregate that
     * the fan-out built is the only place where the other regions' failures
     * can survive. It must name every failed region — the aggregate's own
     * keys are group indices, which identify no region.
     */
    public function testAggregatedPrewriteFailureNamesEveryFailingRegion(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 1.0,
            responder: static function (string $method, Message $request): Message {
                if ($method === 'KvPrewrite') {
                    $keys = self::requestKeys($request);
                    foreach (['k1', 'k2'] as $failing) {
                        if (in_array($failing, $keys, true)) {
                            throw new GrpcException(
                                "prewrite failed on {$failing}",
                                GrpcStatusCode::InvalidArgument->value,
                            );
                        }
                    }
                }

                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("unexpected method $method"),
                };
            },
        );

        $logger = new class extends AbstractLogger {
            /** @var list<array{message: string, context: array<string, mixed>}> */
            public array $records = [];

            /**
             * @param mixed                $level
             * @param array<string, mixed> $context
             */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = ['message' => (string) $message, 'context' => $context];
            }
        };

        $txn = $this->createTransaction($regions, $grpc, [], $logger);
        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        try {
            $txn->commit();
            self::fail('the failing prewrite must abort the commit');
        } catch (GrpcException) {
            // The thrown error is asserted by the sibling test; here only the
            // aggregate matters.
        }

        $reports = array_values(array_filter(
            $logger->records,
            static fn(array $record): bool => $record['message'] === 'Prewrite fan-out failed for some regions',
        ));
        self::assertCount(1, $reports, 'the aggregated failure must be reported once');

        $context = $reports[0]['context'];
        self::assertSame(2, $context['failedRegions']);
        self::assertSame(2, $context['totalRegions']);
        self::assertSame(
            [
                ['regionId' => 101, 'error' => 'gRPC error: prewrite failed on k1'],
                ['regionId' => 102, 'error' => 'gRPC error: prewrite failed on k2'],
            ],
            $context['errors'],
        );
    }

    /**
     * A TimeoutConfig whose 30 ms batch deadline passes the constructor check
     * (every per-call timeout is below it).
     */
    private function shortDeadlineConfig(): TimeoutConfig
    {
        return new TimeoutConfig(
            readTimeoutMs: 10,
            writeTimeoutMs: 10,
            batchReadTimeoutMs: 10,
            batchWriteTimeoutMs: 10,
            scanTimeoutMs: 10,
            deleteRangeTimeoutMs: 10,
            checksumTimeoutMs: 10,
            ingestTimeoutMs: 10,
            batchDeadlineMs: 30,
        );
    }

    /**
     * Issue #275: the batch deadline is finite by default, so it can run out
     * in the secondary-commit fan-out AFTER the primary commit. The
     * transaction is committed then, and commit() must not throw: a caller
     * that retries on an exception would apply the transaction twice.
     */
    public function testBatchDeadlineInTheSecondaryCommitFanOutDoesNotFailACommittedTransaction(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $commits = 0;
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: static function (string $method) use (&$commits): Message {
                if ($method === 'KvCommit') {
                    // The primary commit (first) is fast; each secondary
                    // wait outlasts the whole 30 ms batch deadline.
                    if ($commits++ > 0) {
                        usleep(40_000);
                    }

                    return new CommitResponse();
                }

                return new PrewriteResponse();
            },
        );
        $logger = $this->recordingLogger();
        $txn = $this->createTransaction($regions, $grpc, [], $logger, $this->shortDeadlineConfig());
        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        $txn->commit();

        $messages = array_column($logger->records, 'message');
        self::assertContains(
            'Secondary commit fan-out hit the batch deadline; locks will be resolved by readers',
            $messages,
        );
    }

    /**
     * Issue #275: a batch deadline in the rollback fan-out propagates like a
     * partial failure. A silent return would report a failed cleanup as a
     * success and leave the transaction closed, so nobody retries it.
     */
    public function testBatchDeadlineInTheRollbackFanOutPropagatesAndKeepsTheTransactionRollbackable(): void
    {
        $regions = $this->makeRegions(3);
        $this->stubPdClient($regions);
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: static function (string $method): Message {
                usleep(40_000);

                return new BatchRollbackResponse();
            },
        );
        $txn = $this->createTransaction($regions, $grpc, [], new NullLogger(), $this->shortDeadlineConfig());
        for ($i = 0; $i < 3; $i++) {
            $txn->set("k{$i}", "v{$i}");
        }

        try {
            $txn->rollback();
            self::fail('a batch deadline in the rollback fan-out must propagate');
        } catch (BatchDeadlineExceededException) {
            self::assertNotSame(TransactionStatus::RolledBack, $txn->getStatus());
        }
    }

    /**
     * @return AbstractLogger&object{records: list<array{message: string, context: array<string, mixed>}>}
     */
    private function recordingLogger(): AbstractLogger
    {
        return new class extends AbstractLogger {
            /** @var list<array{message: string, context: array<string, mixed>}> */
            public array $records = [];

            /**
             * @param mixed                $level
             * @param array<string, mixed> $context
             */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = ['message' => (string) $message, 'context' => $context];
            }
        };
    }

    /**
     * The keys a request carries, so a responder can tell the regions apart.
     *
     * @return list<string>
     */
    private static function requestKeys(Message $request): array
    {
        if ($request instanceof PrewriteRequest) {
            $keys = [];
            foreach ($request->getMutations() as $mutation) {
                $keys[] = $mutation->getKey();
            }

            return $keys;
        }

        if ($request instanceof CommitRequest) {
            $keys = [];
            foreach ($request->getKeys() as $key) {
                $keys[] = $key;
            }

            return $keys;
        }

        return [];
    }
}
