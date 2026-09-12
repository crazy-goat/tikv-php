<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Mutation;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #218 (TXN-13): the optimistic prewrite lock TTL scales with the
 * transaction's write-set size, and the prewrite loop heartbeats the primary
 * lock when it risks outliving that TTL.
 *
 * - AC-1: lock_ttl scales with the write set and is capped.
 * - AC-2: a prewrite loop that exceeds half the computed TTL heartbeats the
 *   primary lock (and a fast loop does not).
 * - AC-3: a large mutation count produces a lock_ttl above the 3000 ms
 *   baseline.
 *
 * The cache is the REAL RegionCache so grouping/heartbeat resolution behaves
 * like production; the fake clock is injectable through the committer's
 * optional `?\Closure $clock` constructor parameter, so the heartbeat tests
 * are deterministic and sleep-free.
 */
final class LockTtlScalingTest extends TestCase
{
    private InMemoryMetrics $metrics;

    private RegionCache $regionCache;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    /** @var list<PrewriteRequest> */
    private array $prewriteRequests = [];

    /** @var list<TxnHeartBeatRequest> */
    private array $heartbeatRequests = [];

    /** Fake monotonic clock value returned by the injected clock closure. */
    private int $nowMs = 0;

    protected function setUp(): void
    {
        $this->metrics = new InMemoryMetrics();
        $this->regionCache = new RegionCache(metrics: $this->metrics);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->prewriteRequests = [];
        $this->heartbeatRequests = [];
        $this->nowMs = 0;

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getTimestamp')->willReturn(2000);
    }

    private function makeRegion(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function resolver(): RegionResolver
    {
        return new RegionResolver($this->pdClient, $this->regionCache, $this->metrics);
    }

    /**
     * AC-3: drive a real Transaction::commit() and capture the PrewriteRequest.
     * With 500 mutations the TTL is 3000 + 500 * 10 = 8000 ms, comfortably
     * above the 3000 ms baseline that used to be hardcoded.
     */
    public function testOptimisticLockTtlScalesWithMutationCount(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->stubCommitRpc(prewriteClockJumpMs: 0);

        $resolver = $this->resolver();
        $txn = new Transaction(
            txnId: 'ttl-test',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $resolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $resolver,
            maxBackoffMs: 20000,
            retryDeadlineMs: 3000,
        );

        for ($i = 0; $i < 500; ++$i) {
            $txn->set(sprintf('key%04d', $i), 'v');
        }
        $txn->commit();

        self::assertCount(1, $this->prewriteRequests);
        self::assertSame(8000, (int) $this->prewriteRequests[0]->getLockTtl());
        self::assertGreaterThan(3000, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    /**
     * AC-1: the TTL is capped at MAX_LOCK_TTL_MS (120000 ms) regardless of the
     * write-set size. Exercised via reflection on the private scaling helper
     * (the same seam style as GrpcClient::channelArgs()) to avoid building a
     * huge transaction.
     */
    public function testOptimisticLockTtlIsCappedAtMaximum(): void
    {
        $committer = $this->createCommitter();
        $method = new \ReflectionMethod(TwoPhaseCommitter::class, 'optimisticLockTtlMs');

        $mutations = array_fill(0, 15000, new Mutation());

        self::assertSame(120000, $method->invoke($committer, $mutations));
        // A single mutation is still above the baseline, and an empty write
        // set is exactly the baseline.
        self::assertSame(3010, $method->invoke($committer, [new Mutation()]));
        self::assertSame(3000, $method->invoke($committer, []));
    }

    /**
     * AC-2: the primary's region is prewritten first; when the clock jumps
     * past half the computed TTL before the second region's turn, the loop
     * sends a KvTxnHeartBeat for the primary with advise_lock_ttl > 3000.
     * The second region's check (clock unchanged) must not fire again.
     */
    public function testPrewriteLoopHeartbeatsPrimaryWhenHalfTtlElapsed(): void
    {
        $region1 = $this->makeRegion(1, '', 'b1');
        $region2 = $this->makeRegion(2, 'b1', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturn($region1);
        $this->stubCommitRpc(prewriteClockJumpMs: 5000);

        $committer = $this->createCommitter(fn (): int => $this->nowMs);

        $state = new TransactionState();
        $state->setWrite('a1', 'v'); // primary — region 1, processed first
        $state->setWrite('b1', 'v'); // secondary — region 2

        $committer->commit($state, $this->createRetryExecutor(), static fn (): ?BackoffType => null);

        self::assertCount(1, $this->heartbeatRequests);
        self::assertGreaterThan(3000, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());
        self::assertSame(3020, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());
        self::assertSame('a1', $this->heartbeatRequests[0]->getPrimaryLock());
    }

    /**
     * AC-2 (negative): a fast prewrite loop that never reaches half the TTL
     * must not send a heartbeat.
     */
    public function testFastPrewriteDoesNotHeartbeat(): void
    {
        $region1 = $this->makeRegion(1, '', 'b1');
        $region2 = $this->makeRegion(2, 'b1', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturn($region1);
        $this->stubCommitRpc(prewriteClockJumpMs: 0);

        $committer = $this->createCommitter(fn (): int => $this->nowMs);

        $state = new TransactionState();
        $state->setWrite('a1', 'v');
        $state->setWrite('b1', 'v');

        $committer->commit($state, $this->createRetryExecutor(), static fn (): ?BackoffType => null);

        self::assertCount(0, $this->heartbeatRequests);
        self::assertCount(2, $this->prewriteRequests);
    }

    /**
     * AC-2 (regression, code-review finding): an accepted one-phase commit
     * writes the commit record inside the prewrite and leaves no primary lock,
     * so the prewrite loop must never heartbeat — a heartbeat would return
     * TxnNotFound and fail a transaction that is already durably committed.
     */
    public function testAcceptedOnePhaseCommitDoesNotHeartbeat(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);

        $prewrite = new PrewriteResponse();
        $prewrite->setOnePcCommitTs(12345);

        $this->grpc->method('call')->willReturnCallback(function (
            string $address,
            string $service,
            string $method,
            object $request,
        ) use ($prewrite): object {
            if ($request instanceof TxnHeartBeatRequest) {
                $this->heartbeatRequests[] = $request;
                throw new \RuntimeException('heartbeat must not be sent for an accepted 1PC');
            }
            // The single prewrite RPC alone outlives half the computed TTL.
            $this->nowMs = 5000;

            return $prewrite;
        });

        $committer = $this->createCommitter(fn (): int => $this->nowMs, enable1Pc: true);

        $state = new TransactionState();
        $state->setWrite('a1', 'v');

        $committer->commit($state, $this->createRetryExecutor(), static fn (): ?BackoffType => null);

        self::assertCount(0, $this->heartbeatRequests);
        self::assertSame(12345, $state->getCommitTs());
        self::assertSame(TransactionStatus::Committed, $state->getStatus());
    }

    private function createCommitter(?\Closure $clock = null, bool $enable1Pc = false): TwoPhaseCommitter
    {
        $resolver = $this->resolver();

        return new TwoPhaseCommitter(
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            regionResolver: $resolver,
            lockResolver: new LockResolver($this->grpc, $resolver, $this->regionCache, $this->pdClient, 1000),
            timeoutConfig: new TimeoutConfig(),
            maxBackoffMs: 20000,
            enable1Pc: $enable1Pc,
            clock: $clock,
        );
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 60000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: $this->resolver(),
            logger: new NullLogger(),
            deadlineMs: 3000,
        );
    }

    /**
     * Stub the prewrite/commit RPCs, capturing requests. When
     * $prewriteClockJumpMs > 0, the fake clock is advanced to that value after
     * each prewrite response (idempotent, so the second prewrite keeps the
     * clock at that value and the post-heartbeat window stays closed).
     */
    private function stubCommitRpc(int $prewriteClockJumpMs): void
    {
        $this->grpc->method('call')->willReturnCallback(function (
            string $address,
            string $service,
            string $method,
            object $request,
        ) use ($prewriteClockJumpMs): object {
            if ($request instanceof PrewriteRequest) {
                $this->prewriteRequests[] = $request;
            }
            if ($request instanceof TxnHeartBeatRequest) {
                $this->heartbeatRequests[] = $request;
                $response = new TxnHeartBeatResponse();
                $response->setLockTtl(3020);

                return $response;
            }

            $response = match ($method) {
                'KvPrewrite' => new PrewriteResponse(),
                'KvCommit' => new CommitResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            };
            if ($method === 'KvPrewrite' && $prewriteClockJumpMs > 0) {
                $this->nowMs = $prewriteClockJumpMs;
            }

            return $response;
        });
    }
}
