<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\ResolvedLockTracker;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use InvalidArgumentException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #432 (GAP-18): a resolved lock is remembered per transaction and its
 * start timestamp is sent back on later requests as Context.resolved_locks
 * (and Context.committed_locks for a transaction resolved as committed).
 */
final class ResolvedLocksTest extends TestCase
{
    private const LOCK_TS = 500;
    private const COMMIT_TS = 600;
    private const PRIMARY_KEY = 'primary';
    private const READ_KEY = 'key';

    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private RegionInfo $region;

    /** @var array<int, CheckTxnStatusResponse> */
    private array $statusByTs = [];

    /** @var list<GetRequest> */
    private array $getRequests = [];

    /** @var list<GetResponse> */
    private array $getResponses = [];

    /** @var list<TxnHeartBeatRequest> */
    private array $heartbeatRequests = [];

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->statusByTs = [];
        $this->getRequests = [];
        $this->getResponses = [];
        $this->heartbeatRequests = [];

        $this->region = new RegionInfo(1, 1, 1, 1, 1, '', '');
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn((1 << 42) | 9);
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method, mixed $request): object {
                if ($request instanceof GetRequest) {
                    $this->getRequests[] = $request;
                    $response = array_shift($this->getResponses);
                    if (!$response instanceof GetResponse) {
                        throw new \RuntimeException('No read response staged');
                    }
                    return $response;
                }
                if ($request instanceof CheckTxnStatusRequest) {
                    return $this->statusByTs[(int) $request->getLockTs()]
                        ?? throw new \RuntimeException('No status staged for the lock ts');
                }
                if ($request instanceof ResolveLockRequest) {
                    return new ResolveLockResponse();
                }
                if ($request instanceof TxnHeartBeatRequest) {
                    $this->heartbeatRequests[] = $request;
                    return new TxnHeartBeatResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            },
        );
    }

    private function resolver(): LockResolver
    {
        return new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            1000,
        );
    }

    private function lock(string $key = self::READ_KEY, int $lockTs = self::LOCK_TS): LockInfo
    {
        $lock = new LockInfo();
        $lock->setKey($key);
        $lock->setPrimaryLock(self::PRIMARY_KEY);
        $lock->setLockVersion($lockTs);

        return $lock;
    }

    private function txnStatus(int $commitTs, int $action = Action::NoAction): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion($commitTs);
        $response->setAction($action);

        return $response;
    }

    private function reader(LockResolver $resolver): TxnReader
    {
        return new TxnReader(
            1000,
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            $resolver,
            $this->regionCache,
        );
    }

    private function executor(): RetryExecutor
    {
        return new RetryExecutor(
            1000,
            1000,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            new NullLogger(),
        );
    }

    /**
     * @param iterable<mixed> $values
     * @return list<int>
     */
    private function ints(iterable $values): array
    {
        $ints = [];
        foreach ($values as $value) {
            if (!is_int($value) && !is_string($value)) {
                self::fail('Expected a repeated scalar field');
            }
            $ints[] = (int) $value;
        }

        return $ints;
    }

    private function contextOf(GetRequest|TxnHeartBeatRequest $request): Context
    {
        $context = $request->getContext();
        if (!$context instanceof Context) {
            self::fail('Request carries no Context');
        }

        return $context;
    }

    public function testTrackerKeepsResolvedAndCommittedSeparately(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->record(100, 120);
        $tracker->record(200, 0);

        self::assertSame([100, 200], $tracker->getResolvedLocks());
        self::assertSame([100], $tracker->getCommittedLocks());
        self::assertSame(2, $tracker->resolvedCount());
        self::assertSame(1, $tracker->committedCount());
        self::assertFalse($tracker->isEmpty());
    }

    public function testTrackerEvictsTheOldestEntriesWhenFull(): void
    {
        $tracker = new ResolvedLockTracker(2);
        $tracker->record(1, 11);
        $tracker->record(2, 12);
        $tracker->record(3, 13);

        self::assertSame([2, 3], $tracker->getResolvedLocks());
        self::assertSame([2, 3], $tracker->getCommittedLocks());
    }

    public function testEvictionKeepsCommittedLocksASubsetOfResolvedLocks(): void
    {
        $tracker = new ResolvedLockTracker(2);
        $tracker->record(1, 11);
        $tracker->record(2, 0);
        $tracker->record(3, 0);

        self::assertSame([2, 3], $tracker->getResolvedLocks());
        self::assertSame([], $tracker->getCommittedLocks());
    }

    public function testTrackerDoesNotGrowOnDuplicateTimestamps(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->record(100, 0);
        $tracker->record(100, 0);
        $tracker->record(100, 120);

        self::assertSame([100], $tracker->getResolvedLocks());
        self::assertSame([100], $tracker->getCommittedLocks());
    }

    public function testTrackerAppliesBothSetsToAContext(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->record(100, 120);
        $tracker->record(200, 0);

        $context = new Context();
        $tracker->applyTo($context);

        self::assertSame([100, 200], $this->ints($context->getResolvedLocks()));
        self::assertSame([100], $this->ints($context->getCommittedLocks()));
    }

    public function testEmptyTrackerLeavesTheContextUntouched(): void
    {
        $context = new Context();
        (new ResolvedLockTracker())->applyTo($context);

        self::assertCount(0, $context->getResolvedLocks());
        self::assertCount(0, $context->getCommittedLocks());
    }

    public function testTrackerRejectsANonPositiveCapacity(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new ResolvedLockTracker(0);
    }

    public function testCommittedResolutionIsTrackedInBothSets(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::COMMIT_TS);
        $resolver = $this->resolver();

        $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());

        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testRolledBackResolutionIsTrackedAsResolvedOnly(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(0, Action::TTLExpireRollback);
        $resolver = $this->resolver();

        $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());

        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testBatchResolutionTracksEveryTransactionSeparately(): void
    {
        $this->statusByTs[100] = $this->txnStatus(120);
        $this->statusByTs[200] = $this->txnStatus(0, Action::LockNotExistRollback);
        $resolver = $this->resolver();

        $resolver->resolveLocks([
            $this->lock('a', 100),
            $this->lock('b', 100),
            $this->lock('c', 200),
        ]);

        self::assertSame([100, 200], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([100], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testSubsequentReadCarriesResolvedAndCommittedLocks(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::COMMIT_TS);

        $locked = new GetResponse();
        $keyError = new KeyError();
        $keyError->setLocked($this->lock());
        $locked->setError($keyError);

        $value = new GetResponse();
        $value->setValue('v');
        $this->getResponses = [$locked, $value];

        $reader = $this->reader($this->resolver());
        $state = new TransactionState();
        $classifier = static fn (): RetryVerdict => RetryVerdict::pass();

        try {
            $reader->get(self::READ_KEY, $state, $this->executor(), $classifier);
            self::fail('Expected the locked read to request a retry');
        } catch (TxnRetryableException) {
            // Expected.
        }

        self::assertSame('v', $reader->get(self::READ_KEY, $state, $this->executor(), $classifier));

        self::assertCount(2, $this->getRequests);
        $first = $this->contextOf($this->getRequests[0]);
        self::assertCount(0, $first->getResolvedLocks());
        self::assertCount(0, $first->getCommittedLocks());

        $second = $this->contextOf($this->getRequests[1]);
        self::assertSame([self::LOCK_TS], $this->ints($second->getResolvedLocks()));
        self::assertSame([self::LOCK_TS], $this->ints($second->getCommittedLocks()));
    }

    public function testSubsequentReadDoesNotMarkARolledBackLockAsCommitted(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(0, Action::TTLExpireRollback);

        $locked = new GetResponse();
        $keyError = new KeyError();
        $keyError->setLocked($this->lock());
        $locked->setError($keyError);

        $value = new GetResponse();
        $value->setValue('v');
        $this->getResponses = [$locked, $value];

        $reader = $this->reader($this->resolver());
        $state = new TransactionState();
        $classifier = static fn (): RetryVerdict => RetryVerdict::pass();

        try {
            $reader->get(self::READ_KEY, $state, $this->executor(), $classifier);
        } catch (TxnRetryableException) {
            // Expected.
        }

        $reader->get(self::READ_KEY, $state, $this->executor(), $classifier);

        $second = $this->contextOf($this->getRequests[1]);
        self::assertSame([self::LOCK_TS], $this->ints($second->getResolvedLocks()));
        self::assertCount(0, $second->getCommittedLocks());
    }

    public function testCommitterCarriesResolvedLocksOnItsRequests(): void
    {
        $resolver = $this->resolver();
        $resolver->getResolvedLockTracker()->record(self::LOCK_TS, self::COMMIT_TS);

        $committer = new TwoPhaseCommitter(
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            regionResolver: $this->regionResolver,
            lockResolver: $resolver,
            timeoutConfig: new TimeoutConfig(),
            maxBackoffMs: 20000,
        );

        $committer->heartbeat(
            self::PRIMARY_KEY,
            new TransactionState(),
            $this->executor(),
            static fn (): RetryVerdict => RetryVerdict::pass(),
        );

        self::assertCount(1, $this->heartbeatRequests);
        $context = $this->contextOf($this->heartbeatRequests[0]);
        self::assertSame([self::LOCK_TS], $this->ints($context->getResolvedLocks()));
        self::assertSame([self::LOCK_TS], $this->ints($context->getCommittedLocks()));
    }
}
