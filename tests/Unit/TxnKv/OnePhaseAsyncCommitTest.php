<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for one-phase commit and async commit support (issue #419).
 */
class OnePhaseAsyncCommitTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private const START_TS = 1000;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;

    /** @var list<PrewriteRequest> */
    private array $prewriteRequests = [];
    /** @var list<CommitRequest> */
    private array $commitRequests = [];

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
        $this->prewriteRequests = [];
        $this->commitRequests = [];
    }

    private function makeStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        return $store;
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

    /**
     * @param RegionInfo[] $regions regions sorted by startKey
     */
    private function stubRegionLookup(array $regions): void
    {
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($regions): ?RegionInfo {
                foreach ($regions as $region) {
                    if ($region->startKey <= $key && ($region->endKey === '' || $key < $region->endKey)) {
                        return $region;
                    }
                }

                return null;
            },
        );
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($regions[0]);
        $this->pdClient->method('scanRegions')->willReturn($regions);
    }

    /**
     * @param array{enable1Pc?: bool, enableAsyncCommit?: bool} $options
     */
    private function createTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: 'test-txn-1',
            startTs: self::START_TS,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver(
                $this->grpc,
                $this->regionResolver,
                $this->regionCache,
                $this->pdClient,
                self::START_TS,
            ),
            regionResolver: $this->regionResolver,
            enable1Pc: $options['enable1Pc'] ?? false,
            enableAsyncCommit: $options['enableAsyncCommit'] ?? false,
        );
    }

    private function mockGrpc(PrewriteResponse $prewriteResponse): void
    {
        $this->grpc->method('call')->willReturnCallback(function (
            string $addr,
            string $svc,
            string $method,
            ?object $request = null,
        ) use ($prewriteResponse): object {
            if ($request instanceof PrewriteRequest) {
                $this->prewriteRequests[] = $request;
            }
            if ($request instanceof CommitRequest) {
                $this->commitRequests[] = $request;
            }
            return match ($method) {
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => new CommitResponse(),
                'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            };
        });
    }

    public function testOnePhaseCommitSingleRegionSkipsCommitPhase(): void
    {
        $this->stubRegionLookup([$this->makeRegion(1, '', '')]);

        $response = new PrewriteResponse();
        $response->setOnePcCommitTs(5000);
        $response->setMinCommitTs(self::START_TS);
        $this->mockGrpc($response);

        // One-phase commit must not allocate a commit timestamp from PD.
        $this->pdClient->expects($this->never())->method('getTimestamp');

        $txn = $this->createTransaction(['enable1Pc' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(5000, $txn->getCommitTs());
        $this->assertCount(1, $this->prewriteRequests);
        $this->assertTrue($this->prewriteRequests[0]->getTryOnePc());
        $this->assertFalse($this->prewriteRequests[0]->getUseAsyncCommit());
        $this->assertSame(self::START_TS + 1, (int) $this->prewriteRequests[0]->getMinCommitTs());
    }

    public function testOnePhaseCommitDeclinedFallsBackToTwoPhase(): void
    {
        $this->stubRegionLookup([$this->makeRegion(1, '', '')]);

        $response = new PrewriteResponse();
        // TiKV declines 1PC by answering one_pc_commit_ts = 0.
        $response->setOnePcCommitTs(0);
        $this->mockGrpc($response);

        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $txn = $this->createTransaction(['enable1Pc' => true]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2000, $txn->getCommitTs());
        $this->assertCount(1, $this->commitRequests);
        $this->assertTrue($this->prewriteRequests[0]->getTryOnePc());
    }

    public function testOnePhaseCommitNotUsedForMultiRegionTransaction(): void
    {
        $region1 = $this->makeRegion(1, '', 'k2'); // holds the primary 'k1'
        $region2 = $this->makeRegion(2, 'k2', '');
        $this->stubRegionLookup([$region1, $region2]);

        $response = new PrewriteResponse();
        $this->mockGrpc($response);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $txn = $this->createTransaction(['enable1Pc' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        foreach ($this->prewriteRequests as $request) {
            $this->assertFalse($request->getTryOnePc());
        }
        $this->assertCount(2, $this->commitRequests);
    }

    public function testCommitOptionsDefaultToOff(): void
    {
        $this->stubRegionLookup([$this->makeRegion(1, '', '')]);

        $response = new PrewriteResponse();
        $this->mockGrpc($response);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $txn = $this->createTransaction();
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertFalse($this->prewriteRequests[0]->getTryOnePc());
        $this->assertFalse($this->prewriteRequests[0]->getUseAsyncCommit());
        $this->assertCount(1, $this->commitRequests);
    }

    public function testAsyncCommitDerivesCommitTsFromMinCommitTs(): void
    {
        $this->stubRegionLookup([$this->makeRegion(1, '', '')]);

        $response = new PrewriteResponse();
        $response->setMinCommitTs(4500);
        $this->mockGrpc($response);

        // Async commit must not allocate a commit timestamp from PD and
        // must not run the commit phase.
        $this->pdClient->expects($this->never())->method('getTimestamp');

        $txn = $this->createTransaction(['enableAsyncCommit' => true]);
        $txn->set('k1', 'v1'); // primary
        $txn->set('k2', 'v2'); // secondary
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        // Derived: max over the prewrite responses' min_commit_ts (no +1 —
        // TiKV writes the data at min_commit_ts, and a reader may be
        // granted exactly that timestamp by PD).
        $this->assertSame(4500, $txn->getCommitTs());
        $this->assertCount(0, $this->commitRequests);

        $request = $this->prewriteRequests[0];
        $this->assertTrue($request->getUseAsyncCommit());
        $this->assertFalse($request->getTryOnePc());
        $this->assertSame(['k2'], iterator_to_array($request->getSecondaries()));
        $this->assertSame(self::START_TS + 1, (int) $request->getMinCommitTs());
        $this->assertSame(self::START_TS + 15000, (int) $request->getMaxCommitTs());
    }

    public function testAsyncCommitDeclinedFallsBackToTwoPhase(): void
    {
        $this->stubRegionLookup([$this->makeRegion(1, '', '')]);

        $response = new PrewriteResponse();
        // TiKV declines async commit by answering min_commit_ts = 0.
        $this->mockGrpc($response);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $txn = $this->createTransaction(['enableAsyncCommit' => true]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2000, $txn->getCommitTs());
        $this->assertCount(1, $this->commitRequests);
        $this->assertTrue($this->prewriteRequests[0]->getUseAsyncCommit());
    }

    // ========================================================================
    // LockResolver — async-commit lock resolution via CheckSecondaryLocks
    // ========================================================================

    private function makeAsyncLock(): LockInfo
    {
        $lock = new LockInfo();
        $lock->setKey('s1');
        $lock->setLockVersion(self::START_TS);
        $lock->setPrimaryLock('p');
        $lock->setUseAsyncCommit(true);
        $lock->setMinCommitTs(4500);
        $lock->setSecondaries(['s2']);
        return $lock;
    }

    public function testResolveAsyncCommitLockUsesCheckSecondaryLocks(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getTimestamp')->willReturn(999999);

        // CheckTxnStatus: lock still active (commitVersion 0). Give a tiny
        // TTL so a buggy fallback would not hang the test.
        $status = new CheckTxnStatusResponse();
        $status->setCommitVersion(0);
        $status->setLockTtl(30);
        $secondary = new CheckSecondaryLocksResponse();
        // All secondaries resolved: transaction committed at 5555.
        $secondary->setCommitTs(5555);
        $resolved = new ResolveLockResponse();

        /** @var list<CheckSecondaryLocksRequest> $captured */
        $captured = [];
        $queue = [
            ['method' => 'KvCheckTxnStatus', 'response' => $status],
            ['method' => 'KvCheckSecondaryLocks', 'response' => $secondary],
            ['method' => 'KvResolveLock', 'response' => $resolved],
        ];
        $index = 0;
        $this->grpc->method('call')->willReturnCallback(function (
            string $addr,
            string $svc,
            string $method,
            ?object $request = null,
        ) use (
            &$captured,
            &$queue,
            &$index
): object {
            if ($request instanceof CheckSecondaryLocksRequest) {
                $captured[] = $request;
            }
            $call = $queue[$index++] ?? null;
            if ($call === null || $call['method'] !== $method) {
                throw new \RuntimeException("Unexpected method at $index: $method");
            }
            return $call['response'];
        });

        $resolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            self::START_TS,
        );
        $resolver->resolveLock('p', $this->makeAsyncLock());

        $this->assertCount(1, $captured);
        $this->assertSame(['s2'], iterator_to_array($captured[0]->getKeys()));
        $this->assertSame(self::START_TS, (int) $captured[0]->getStartVersion());
    }

    public function testResolveAsyncCommitLockStillActiveHelpCommits(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getTimestamp')->willReturn(999999);

        $status1 = new CheckTxnStatusResponse();
        $status1->setCommitVersion(0);
        $status1->setLockTtl(3000);
        // A secondary is still locked: the transaction is undecided, but
        // the reader may help-commit it at max(min_commit_ts) — exactly
        // client-go's resolveAsyncResolveData() after checkAllSecondaries().
        $secondaryLock = new LockInfo();
        $secondaryLock->setKey('s2');
        $secondaryLock->setLockVersion(self::START_TS);
        $secondaryLock->setUseAsyncCommit(true);
        $secondaryLock->setMinCommitTs(4600);
        $locked = new CheckSecondaryLocksResponse();
        $locked->setLocks([$secondaryLock]);
        $resolved = new ResolveLockResponse();

        /** @var list<CheckSecondaryLocksRequest|ResolveLockRequest> $captured */
        $captured = [];
        $queue = [
            ['method' => 'KvCheckTxnStatus', 'response' => $status1],
            ['method' => 'KvCheckSecondaryLocks', 'response' => $locked],
            ['method' => 'KvResolveLock', 'response' => $resolved],
        ];
        $index = 0;
        $this->grpc->method('call')->willReturnCallback(function (
            string $addr,
            string $svc,
            string $method,
            ?object $request = null,
        ) use (
            &$captured,
            &$queue,
            &$index
): object {
            if (
                $request instanceof CheckSecondaryLocksRequest
                || $request instanceof ResolveLockRequest
            ) {
                $captured[] = $request;
            }
            $call = $queue[$index++] ?? null;
            if ($call === null || $call['method'] !== $method) {
                throw new \RuntimeException("Unexpected method at $index: $method");
            }
            return $call['response'];
        });

        $resolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            self::START_TS,
        );
        $resolver->resolveLock('p', $this->makeAsyncLock());

        // CheckSecondaryLocks ran, then the help-commit ResolveLock finalized
        // the transaction at max(primary 4500, secondary 4600) = 4600 — no
        // TTL wait, no second CheckTxnStatus.
        $this->assertCount(2, $captured);
        $this->assertSame(['s2'], iterator_to_array($captured[0]->getKeys()));
        $this->assertSame(self::START_TS, (int) $captured[0]->getStartVersion());
        $this->assertInstanceOf(ResolveLockRequest::class, $captured[1]);
        $this->assertSame(self::START_TS, (int) $captured[1]->getStartVersion());
        $this->assertSame(4600, (int) $captured[1]->getCommitVersion());
    }
}
