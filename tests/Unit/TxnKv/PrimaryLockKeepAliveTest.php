<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #429 (GAP-15): configurable lock TTLs and the opportunistic primary
 * lock heartbeat. The clock is injected, so no test sleeps.
 */
final class PrimaryLockKeepAliveTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private RegionCache $regionCache;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    /** @var list<PessimisticLockRequest> */
    private array $lockRequests = [];

    /** @var list<PrewriteRequest> */
    private array $prewriteRequests = [];

    /** @var list<TxnHeartBeatRequest> */
    private array $heartbeatRequests = [];

    private int $nowMs = 0;

    /** Fake server-side lock TTL (ms from the transaction start), raised by heartbeats. */
    private int $serverTtlMs = 0;

    private ?KeyError $heartbeatError = null;

    protected function setUp(): void
    {
        $this->regionCache = new RegionCache();
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->lockRequests = [];
        $this->prewriteRequests = [];
        $this->heartbeatRequests = [];
        $this->nowMs = 0;
        $this->serverTtlMs = 0;
        $this->heartbeatError = null;

        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);

        $this->grpc->method('call')->willReturnCallback(function (
            string $address,
            string $service,
            string $method,
            object $request,
        ): object {
            if ($request instanceof PessimisticLockRequest) {
                $this->lockRequests[] = $request;
                $this->serverTtlMs = max($this->serverTtlMs, (int) $request->getLockTtl());

                return new PessimisticLockResponse();
            }
            if ($request instanceof PrewriteRequest) {
                $this->prewriteRequests[] = $request;

                return new PrewriteResponse();
            }
            if ($request instanceof TxnHeartBeatRequest) {
                $this->heartbeatRequests[] = $request;
                $response = new TxnHeartBeatResponse();
                if ($this->heartbeatError instanceof KeyError) {
                    $response->setError($this->heartbeatError);

                    return $response;
                }
                // TiKV only ever raises a lock's TTL.
                $this->serverTtlMs = max($this->serverTtlMs, (int) $request->getAdviseLockTtl());
                $response->setLockTtl($this->serverTtlMs);

                return $response;
            }

            return match ($method) {
                'KvCommit' => new CommitResponse(),
                'KvBatchRollback' => new BatchRollbackResponse(),
                'KVPessimisticRollback' => new PessimisticRollbackResponse(),
                'KvScan' => new ScanResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            };
        });
    }

    private function transaction(
        bool $pessimistic,
        int $optimisticLockTtlMs = TwoPhaseCommitter::DEFAULT_OPTIMISTIC_LOCK_TTL_MS,
        int $pessimisticLockTtlMs = TwoPhaseCommitter::DEFAULT_PESSIMISTIC_LOCK_TTL_MS,
        bool $eagerPessimisticLocks = true,
    ): Transaction {
        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'keepalive-test',
            startTs: 1000,
            pessimistic: $pessimistic,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $resolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $resolver,
            maxBackoffMs: 20000,
            retryDeadlineMs: 3000,
            eagerPessimisticLocks: $eagerPessimisticLocks,
            optimisticLockTtlMs: $optimisticLockTtlMs,
            pessimisticLockTtlMs: $pessimisticLockTtlMs,
            clock: fn (): int => $this->nowMs,
        );
    }

    public function testConfiguredPessimisticTtlReachesLockAndPrewriteRequests(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 20000);
        $txn->set('a1', 'v');
        $txn->commit();

        self::assertCount(1, $this->lockRequests);
        self::assertSame(20000, (int) $this->lockRequests[0]->getLockTtl());
        self::assertCount(1, $this->prewriteRequests);
        self::assertSame(20000, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    public function testConfiguredOptimisticTtlIsTheBaselineOfTheScaling(): void
    {
        $txn = $this->transaction(false, optimisticLockTtlMs: 5000);
        $txn->set('a1', 'v');
        $txn->set('a2', 'v');
        $txn->commit();

        self::assertSame(5020, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    public function testDefaultTtlsAreUnchanged(): void
    {
        $optimistic = $this->transaction(false);
        $optimistic->set('a1', 'v');
        $optimistic->commit();
        self::assertSame(3010, (int) $this->prewriteRequests[0]->getLockTtl());

        $pessimistic = $this->transaction(true);
        $pessimistic->set('b1', 'v');
        self::assertSame(30000, (int) $this->lockRequests[0]->getLockTtl());
        $pessimistic->rollback();
    }

    /**
     * @return iterable<string, array{string, int}>
     */
    public static function invalidTtlProvider(): iterable
    {
        yield 'optimistic zero' => ['optimisticLockTtlMs', 0];
        yield 'optimistic negative' => ['optimisticLockTtlMs', -1];
        yield 'optimistic above cap' => ['optimisticLockTtlMs', TwoPhaseCommitter::MAX_LOCK_TTL_MS + 1];
        yield 'pessimistic zero' => ['pessimisticLockTtlMs', 0];
        yield 'pessimistic negative' => ['pessimisticLockTtlMs', -5];
        yield 'pessimistic above cap' => ['pessimisticLockTtlMs', TwoPhaseCommitter::MAX_LOCK_TTL_MS + 1];
    }

    #[DataProvider('invalidTtlProvider')]
    public function testTransactionRejectsAnInvalidLockTtl(string $name, int $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($name . ' must be between 1 and 120000');

        if ($name === 'optimisticLockTtlMs') {
            $this->transaction(true, optimisticLockTtlMs: $value);
        }
        $this->transaction(true, pessimisticLockTtlMs: $value);
    }

    public function testHeartbeatFiresAtHalfTtlAndNotBefore(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v'); // primary lock at t=0, expires at 10000

        $this->nowMs = 5000; // 5000 ms remain: exactly half, not less
        $txn->get('a1');
        self::assertCount(0, $this->heartbeatRequests);

        $this->nowMs = 5001;
        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);
        self::assertSame('a1', $this->heartbeatRequests[0]->getPrimaryLock());
        // TiKV counts the TTL from the start: uptime + TTL.
        self::assertSame(15001, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());

        // The lock now lives until 15001, so 10000 is still outside the window.
        $this->nowMs = 10000;
        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);

        $txn->rollback();
    }

    public function testEveryReadAndWriteOperationKeepsTheLockAlive(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->nowMs = 6000;
        $txn->set('a1', 'w');
        self::assertCount(1, $this->heartbeatRequests);

        $this->nowMs = 12000;
        $txn->delete('a1');
        self::assertCount(2, $this->heartbeatRequests);

        $this->nowMs = 18000;
        $txn->batchGet(['a1']);
        self::assertCount(3, $this->heartbeatRequests);

        $this->nowMs = 24000;
        $txn->scan('a0', 'a9');
        self::assertCount(4, $this->heartbeatRequests);

        $txn->rollback();
    }

    public function testLongTransactionWithPeriodicOperationsKeepsItsLock(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        // One operation every 4 s for 2 minutes. Without the keepalive the
        // fake server lock would expire at 10 s.
        for ($this->nowMs = 4000; $this->nowMs <= 120000; $this->nowMs += 4000) {
            $txn->get('a1');
            self::assertLessThan(
                $this->serverTtlMs,
                $this->nowMs,
                sprintf('the lock expired at %d ms', $this->serverTtlMs),
            );
        }

        self::assertNotSame([], $this->heartbeatRequests);
        $txn->commit();
    }

    public function testOptimisticTransactionDoesNotHeartbeatBeforePrewrite(): void
    {
        $txn = $this->transaction(false, optimisticLockTtlMs: 1000);
        $txn->set('a1', 'v');

        $this->nowMs = 600000;
        $txn->get('a1');
        $txn->set('a2', 'v');
        $txn->batchGet(['a1']);

        self::assertCount(0, $this->heartbeatRequests);
        $txn->rollback();
    }

    public function testPessimisticTransactionWithoutALockDoesNotHeartbeat(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);

        $this->nowMs = 600000;
        $txn->scan('a0', 'a9');

        self::assertCount(0, $this->heartbeatRequests);
    }

    public function testDeferredPessimisticLocksDoNotHeartbeatBeforeCommit(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000, eagerPessimisticLocks: false);
        $txn->set('a1', 'v');

        $this->nowMs = 600000;
        $txn->get('a1');

        self::assertCount(0, $this->heartbeatRequests);
        self::assertCount(0, $this->lockRequests);
        $txn->rollback();
    }

    public function testHeartbeatIsCappedAtTheMaximumTransactionTtl(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->nowMs = 3_599_000;
        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);
        self::assertSame(TwoPhaseCommitter::MAX_TXN_TTL_MS, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());

        // The lock already lives as long as the cap allows: no further RPCs.
        $this->nowMs = 3_700_000;
        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);

        $txn->rollback();
    }

    public function testHeartbeatFailureIsTypedAndRetriedByTheNextOperation(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->heartbeatError = (new KeyError())->setAbort('txn aborted in test');
        $this->nowMs = 6000;
        try {
            $txn->get('a1');
            self::fail('the heartbeat failure must surface');
        } catch (TransactionConflictException $e) {
            self::assertSame('Heartbeat failed: abort: txn aborted in test', $e->getMessage());
        }
        self::assertCount(1, $this->heartbeatRequests);

        // The expiry did not move, so the next operation heartbeats again.
        $this->heartbeatError = null;
        $txn->get('a1');
        self::assertCount(2, $this->heartbeatRequests);

        $txn->rollback();
    }

    public function testExplicitHeartbeatResetsTheOpportunisticWindow(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->nowMs = 6000;
        $granted = $txn->heartbeat(16000);
        self::assertSame(16000, $granted);
        self::assertCount(1, $this->heartbeatRequests);

        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);

        $txn->rollback();
    }

    public function testFirstLockAfterMoreThanOneTtlOfUptimeIsNotBornExpired(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);

        $this->nowMs = 15000;
        $txn->set('a1', 'v');

        // uptime (15000) + TTL (10000): the lock lives until start + 25000.
        self::assertSame(25000, (int) $this->lockRequests[0]->getLockTtl());

        // The tracked expiry is start + 25000: no heartbeat while 5000 or more remain...
        $this->nowMs = 20000;
        $txn->get('a1');
        self::assertCount(0, $this->heartbeatRequests);

        // ...and one as soon as less than half the TTL remains.
        $this->nowMs = 20001;
        $txn->get('a1');
        self::assertCount(1, $this->heartbeatRequests);
        self::assertSame(30001, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());

        $txn->rollback();
    }

    public function testOptimisticCommitAfterThinkTimeCarriesUptimeInThePrewriteTtl(): void
    {
        $txn = $this->transaction(false);
        $txn->set('a1', 'v');

        $this->nowMs = 10000;
        $txn->commit();

        // uptime (10000) + scaled TTL (3000 + 10 * 1 mutation).
        self::assertSame(13010, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    public function testPrewriteTtlIsCappedAtTheMaximumTransactionTtl(): void
    {
        $txn = $this->transaction(false);
        $txn->set('a1', 'v');

        $this->nowMs = 5_000_000;
        $txn->commit();

        self::assertSame(TwoPhaseCommitter::MAX_TXN_TTL_MS, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    public function testPessimisticCommitAfterAGapHeartbeatsBeforeThePrewrite(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->nowMs = 6000;
        $txn->commit();

        self::assertCount(1, $this->heartbeatRequests);
        self::assertSame(16000, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());
        // The prewrite then carries uptime + TTL as well.
        self::assertSame(16000, (int) $this->prewriteRequests[0]->getLockTtl());
    }

    public function testExplicitHeartbeatWithoutAnArgumentAsksForUptimePlusTtl(): void
    {
        $txn = $this->transaction(true, pessimisticLockTtlMs: 10000);
        $txn->set('a1', 'v');

        $this->nowMs = 4000;
        $granted = $txn->heartbeat();

        self::assertSame(14000, (int) $this->heartbeatRequests[0]->getAdviseLockTtl());
        self::assertSame(14000, $granted);

        $this->nowMs = 5_000_000;
        $txn->heartbeat();
        self::assertSame(TwoPhaseCommitter::MAX_TXN_TTL_MS, (int) $this->heartbeatRequests[1]->getAdviseLockTtl());

        $txn->rollback();
    }
}
