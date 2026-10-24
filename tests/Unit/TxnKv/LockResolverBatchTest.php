<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\ResolvedTxnStatusCache;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Batch lock resolution and the resolved-status cache (issue #428).
 *
 * Region 1 covers ["", "5000"), region 2 covers ["5000", ""). The
 * boundary is a decimal string on purpose: a canonical decimal key becomes
 * an int array key in PHP, so the grouping code must survive it.
 */
class LockResolverBatchTest extends TestCase
{
    private const BOUNDARY = '5000';

    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;

    /** @var list<CheckTxnStatusRequest> */
    private array $checks = [];

    /** @var list<ResolveLockRequest> */
    private array $resolves = [];

    /** @var array<int, CheckTxnStatusResponse> status answer per lock start ts */
    private array $statusByTs = [];

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->checks = [];
        $this->resolves = [];
        $this->statusByTs = [];

        $left = new RegionInfo(1, 1, 1, 1, 1, '', self::BOUNDARY);
        $right = new RegionInfo(2, 2, 1, 1, 1, self::BOUNDARY, '');
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): RegionInfo => strcmp($key, self::BOUNDARY) < 0 ? $left : $right,
        );
        $store = new Store();
        $store->setAddress('addr:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn((1 << 42) | 999);

        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method, mixed $request): object {
                if ($request instanceof CheckTxnStatusRequest) {
                    $this->checks[] = $request;
                    return $this->statusByTs[(int) $request->getLockTs()]
                        ?? throw new \RuntimeException('No status staged for the lock ts');
                }
                if ($request instanceof ResolveLockRequest) {
                    $this->resolves[] = $request;
                    return new ResolveLockResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            },
        );
    }

    private function resolver(?ResolvedTxnStatusCache $cache = null): LockResolver
    {
        return new LockResolver(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            $this->regionCache,
            $this->pdClient,
            (1 << 42) | 17,
            statusCache: $cache ?? new ResolvedTxnStatusCache(),
        );
    }

    private function lock(string $key, int $ts, string $primary = '1000'): LockInfo
    {
        $lock = new LockInfo();
        $lock->setKey($key);
        $lock->setLockVersion($ts);
        $lock->setPrimaryLock($primary);

        return $lock;
    }

    private function txnStatus(int $commitTs = 0, int $ttl = 0, int $action = Action::NoAction): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion($commitTs);
        $response->setLockTtl($ttl);
        $response->setAction($action);

        return $response;
    }

    /**
     * @return list<string>
     */
    private function keysOf(ResolveLockRequest $request): array
    {
        return array_values(array_map(static fn (string $key): string => $key, iterator_to_array($request->getKeys())));
    }

    public function testCommittedStatusIsCachedAndSkipsSecondCheck(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);
        $resolver = $this->resolver();

        $resolver->resolveLock('1000', $this->lock('1001', 100));
        $resolver->resolveLock('1000', $this->lock('1002', 100));

        $this->assertCount(1, $this->checks, 'the second lock of a known transaction needs no CheckTxnStatus');
        $this->assertCount(2, $this->resolves);
        $this->assertSame(120, (int) $this->resolves[1]->getCommitVersion());
    }

    public function testRolledBackStatusIsCached(): void
    {
        $this->statusByTs[100] = $this->txnStatus(action: Action::TTLExpireRollback);
        $resolver = $this->resolver();

        $resolver->resolveLock('1000', $this->lock('1001', 100));
        $resolver->resolveLock('1000', $this->lock('1002', 100));

        $this->assertCount(1, $this->checks);
        $this->assertSame(0, (int) $this->resolves[1]->getCommitVersion(), 'cached rollback resolves with ts 0');
    }

    public function testLockNotExistRollbackIsCached(): void
    {
        $this->statusByTs[100] = $this->txnStatus(action: Action::LockNotExistRollback);
        $resolver = $this->resolver();

        $resolver->resolveLock('1000', $this->lock('1001', 100));
        $resolver->resolveLock('1000', $this->lock('1002', 100));

        $this->assertCount(1, $this->checks);
    }

    public function testLiveLockIsNotCached(): void
    {
        $this->statusByTs[100] = $this->txnStatus(ttl: 30000);
        $cache = new ResolvedTxnStatusCache();
        $resolver = $this->resolver($cache);

        for ($i = 0; $i < 2; ++$i) {
            try {
                $resolver->resolveLock('1000', $this->lock('1001', 100));
                $this->fail('A live lock must request a retry');
            } catch (TxnRetryableException $e) {
                $this->assertSame(BackoffType::TxnLock, $e->backoffType);
            }
        }

        $this->assertCount(2, $this->checks, 'a live transaction is asked again every time');
        $this->assertSame(0, $cache->size());
    }

    public function testNonFinalActionsAreNotCached(): void
    {
        $actions = [
            Action::MinCommitTSPushed,
            Action::TTLExpirePessimisticRollback,
            Action::LockNotExistDoNothing,
        ];
        foreach ($actions as $action) {
            $this->checks = [];
            $this->statusByTs[100] = $this->txnStatus(action: $action);
            $cache = new ResolvedTxnStatusCache();
            $resolver = $this->resolver($cache);

            try {
                $resolver->resolveLock('1000', $this->lock('1001', 100));
            } catch (TxnRetryableException) {
                // MinCommitTSPushed reports a live lock only with a TTL; none here.
            }

            $this->assertSame(0, $cache->size(), "action $action must not be cached");
        }
    }

    public function testCacheEvictsAtTheBoundAndAsksAgain(): void
    {
        $this->statusByTs[1] = $this->txnStatus(commitTs: 11);
        $this->statusByTs[2] = $this->txnStatus(commitTs: 12);
        $resolver = $this->resolver(new ResolvedTxnStatusCache(1));

        $resolver->resolveLock('1000', $this->lock('1001', 1));
        $resolver->resolveLock('1000', $this->lock('1001', 2));
        $resolver->resolveLock('1000', $this->lock('1001', 1));

        $this->assertCount(3, $this->checks, 'ts 1 was evicted by ts 2 and is checked again');
    }

    public function testSameTransactionSameRegionSendsOneResolveWithAllKeys(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);

        $this->resolver()->resolveLocks([
            $this->lock('1001', 100),
            $this->lock('1002', 100),
            $this->lock('1003', 100),
        ]);

        $this->assertCount(1, $this->checks, 'one status check per transaction');
        $this->assertCount(1, $this->resolves, 'one KvResolveLock per (region, transaction)');
        $this->assertSame(['1001', '1002', '1003'], $this->keysOf($this->resolves[0]));
        $this->assertSame(120, (int) $this->resolves[0]->getCommitVersion());
        $this->assertSame(100, (int) $this->resolves[0]->getStartVersion());
    }

    public function testRolledBackTransactionBatchUsesCommitVersionZero(): void
    {
        $this->statusByTs[100] = $this->txnStatus(action: Action::TTLExpireRollback);

        $this->resolver()->resolveLocks([$this->lock('1001', 100), $this->lock('1002', 100)]);

        $this->assertCount(1, $this->resolves);
        $this->assertSame(0, (int) $this->resolves[0]->getCommitVersion());
    }

    public function testLocksAcrossTwoRegionsSendTwoResolves(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);

        $this->resolver()->resolveLocks([
            $this->lock('1001', 100),
            $this->lock('6001', 100),
            $this->lock('1002', 100),
        ]);

        $this->assertCount(1, $this->checks);
        $this->assertCount(2, $this->resolves);
        $perRegion = [];
        foreach ($this->resolves as $request) {
            $perRegion[(int) $request->getContext()?->getRegionId()] = $this->keysOf($request);
        }
        $this->assertSame(['1001', '1002'], $perRegion[1]);
        $this->assertSame(['6001'], $perRegion[2]);
    }

    public function testMixedTransactionsResolveSeparately(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);
        $this->statusByTs[200] = $this->txnStatus(action: Action::TTLExpireRollback);

        $this->resolver()->resolveLocks([
            $this->lock('1001', 100),
            $this->lock('1002', 200),
            $this->lock('1003', 100),
        ]);

        $this->assertCount(2, $this->checks);
        $this->assertCount(2, $this->resolves);
        $byTs = [];
        foreach ($this->resolves as $request) {
            $byTs[(int) $request->getStartVersion()] = [(int) $request->getCommitVersion(), $this->keysOf($request)];
        }
        $this->assertSame([120, ['1001', '1003']], $byTs[100]);
        $this->assertSame([0, ['1002']], $byTs[200]);
    }

    public function testBatchUsesCacheAcrossCalls(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);
        $resolver = $this->resolver();

        $resolver->resolveLocks([$this->lock('1001', 100)]);
        $resolver->resolveLocks([$this->lock('1002', 100)]);

        $this->assertCount(1, $this->checks);
        $this->assertCount(2, $this->resolves);
    }

    public function testLiveTransactionDoesNotBlockResolvingTheFinishedOnes(): void
    {
        $this->statusByTs[100] = $this->txnStatus(ttl: 30000);
        $this->statusByTs[200] = $this->txnStatus(commitTs: 220);

        try {
            $this->resolver()->resolveLocks([$this->lock('1001', 100), $this->lock('1002', 200)]);
            $this->fail('A live transaction must request a retry');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertCount(1, $this->resolves, 'only the finished transaction is resolved');
        $this->assertSame(200, (int) $this->resolves[0]->getStartVersion());
    }

    public function testDuplicateKeysAreSentOnce(): void
    {
        $this->statusByTs[100] = $this->txnStatus(commitTs: 120);

        $this->resolver()->resolveLocks([$this->lock('1001', 100), $this->lock('1001', 100)]);

        $this->assertSame(['1001'], $this->keysOf($this->resolves[0]));
    }

    public function testCachedRollbackSkipsSecondaryCheckForAsyncCommitLock(): void
    {
        $cache = new ResolvedTxnStatusCache();
        $cache->put(100, 0);

        $lock = $this->lock('1001', 100);
        $lock->setUseAsyncCommit(true);
        $lock->setSecondaries(['1002', '6001']);

        $resolver = $this->resolver($cache);
        $resolver->resolveLock('1000', $lock);

        $this->assertSame([], $this->checks, 'a cached outcome needs no CheckTxnStatus');
        $this->assertCount(1, $this->resolves, 'no KvCheckSecondaryLocks, one KvResolveLock');
        $this->assertSame(0, (int) $this->resolves[0]->getCommitVersion());
    }

    public function testEmptyBatchMakesNoCall(): void
    {
        $this->resolver()->resolveLocks([]);

        $this->assertSame([], $this->checks);
        $this->assertSame([], $this->resolves);
    }
}
