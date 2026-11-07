<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
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
 * (locks to ignore) or Context.committed_locks (locks to read through).
 */
final class ResolvedLocksTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private const START_TS = 1000;
    private const LOCK_TS = 500;
    private const COMMIT_TS = 600;
    private const FUTURE_COMMIT_TS = 1200;
    private const PRIMARY_KEY = 'primary';
    private const READ_KEY = 'key';

    private GrpcClientInterface&MockObject $grpc;
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private RegionInfo $region;
    private ResolveLockResponse $resolveResponse;

    /** @var array<int, CheckTxnStatusResponse> */
    private array $statusByTs = [];

    /** @var list<GetRequest> */
    private array $getRequests = [];

    /** @var list<GetResponse> */
    private array $getResponses = [];

    /** @var list<BatchGetRequest> */
    private array $batchGetRequests = [];

    /** @var list<BatchGetResponse> */
    private array $batchGetResponses = [];

    /** @var list<ScanRequest> */
    private array $scanRequests = [];

    /** @var list<ScanResponse> */
    private array $scanResponses = [];

    /** @var list<TxnHeartBeatRequest> */
    private array $heartbeatRequests = [];

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->statusByTs = [];
        $this->getRequests = [];
        $this->getResponses = [];
        $this->batchGetRequests = [];
        $this->batchGetResponses = [];
        $this->scanRequests = [];
        $this->scanResponses = [];
        $this->heartbeatRequests = [];
        $this->resolveResponse = new ResolveLockResponse();

        $this->region = new RegionInfo(1, 1, 1, 1, 1, '', '');
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->region);
        $this->pdClient->method('scanRegions')->willReturn([$this->region]);
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
                if ($request instanceof BatchGetRequest) {
                    $this->batchGetRequests[] = $request;
                    $response = array_shift($this->batchGetResponses);
                    if (!$response instanceof BatchGetResponse) {
                        throw new \RuntimeException('No batchGet response staged');
                    }
                    return $response;
                }
                if ($request instanceof ScanRequest) {
                    $this->scanRequests[] = $request;
                    $response = array_shift($this->scanResponses);
                    if (!$response instanceof ScanResponse) {
                        throw new \RuntimeException('No scan response staged');
                    }
                    return $response;
                }
                if ($request instanceof CheckTxnStatusRequest) {
                    return $this->statusByTs[(int) $request->getLockTs()]
                        ?? throw new \RuntimeException('No status staged for the lock ts');
                }
                if ($request instanceof ResolveLockRequest) {
                    return $this->resolveResponse;
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
            self::START_TS,
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
            self::START_TS,
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
            self::START_TS,
            self::START_TS,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            new NullLogger(),
        );
    }

    private function classifier(): \Closure
    {
        return RetryVerdict::pass(...);
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

    private function contextOf(
        GetRequest|BatchGetRequest|ScanRequest|TxnHeartBeatRequest $request,
    ): Context {
        $context = $request->getContext();
        if (!$context instanceof Context) {
            self::fail('Request carries no Context');
        }

        return $context;
    }

    private function lockedGetResponse(): GetResponse
    {
        $locked = new GetResponse();
        $keyError = new KeyError();
        $keyError->setLocked($this->lock());
        $locked->setError($keyError);

        return $locked;
    }

    private function regionErrorResponse(): ResolveLockResponse
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $error = new Error();
        $error->setNotLeader($notLeader);

        $response = new ResolveLockResponse();
        $response->setRegionError($error);

        return $response;
    }

    public function testTrackerKeepsTheTwoSetsDisjoint(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->recordResolved(100);
        $tracker->recordResolved(200);
        $tracker->recordCommitted(300);

        self::assertSame([100, 200], $tracker->getResolvedLocks());
        self::assertSame([300], $tracker->getCommittedLocks());
        self::assertSame(2, $tracker->resolvedCount());
        self::assertSame(1, $tracker->committedCount());
        self::assertFalse($tracker->isEmpty());
    }

    public function testTrackerEvictsTheOldestEntriesWhenFull(): void
    {
        $tracker = new ResolvedLockTracker(2);
        $tracker->recordResolved(1);
        $tracker->recordResolved(2);
        $tracker->recordResolved(3);
        $tracker->recordCommitted(4);
        $tracker->recordCommitted(5);
        $tracker->recordCommitted(6);

        self::assertSame([2, 3], $tracker->getResolvedLocks());
        self::assertSame([5, 6], $tracker->getCommittedLocks());
    }

    public function testTrackerDoesNotGrowOnDuplicateTimestamps(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->recordResolved(100);
        $tracker->recordResolved(100);
        $tracker->recordCommitted(200);
        $tracker->recordCommitted(200);

        self::assertSame([100], $tracker->getResolvedLocks());
        self::assertSame([200], $tracker->getCommittedLocks());
    }

    public function testTrackerAppliesBothSetsToAContext(): void
    {
        $tracker = new ResolvedLockTracker();
        $tracker->recordResolved(100);
        $tracker->recordCommitted(200);

        $context = new Context();
        $tracker->applyTo($context);

        self::assertSame([100], $this->ints($context->getResolvedLocks()));
        self::assertSame([200], $this->ints($context->getCommittedLocks()));
    }

    public function testEmptyTrackerLeavesTheWireBytesUnchanged(): void
    {
        $context = new Context();
        $before = $context->serializeToString();
        (new ResolvedLockTracker())->applyTo($context);

        self::assertSame('', $before);
        self::assertSame($before, $context->serializeToString());
    }

    public function testTrackerRejectsANonPositiveCapacity(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new ResolvedLockTracker(0);
    }

    public function testCommittedResolutionBelowStartTsIsCommittedOnly(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::COMMIT_TS);
        $resolver = $this->resolver();

        $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());

        self::assertSame([], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testCommittedResolutionAboveStartTsIsResolvedOnly(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::FUTURE_COMMIT_TS);
        $resolver = $this->resolver();

        $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());

        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testRolledBackResolutionIsResolvedOnly(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(0, Action::TTLExpireRollback);
        $resolver = $this->resolver();

        $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());

        self::assertSame([self::LOCK_TS], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testClassificationSplitsAtTheReaderStartTs(): void
    {
        $this->statusByTs[100] = $this->txnStatus(900);
        $this->statusByTs[200] = $this->txnStatus(self::START_TS);
        $this->statusByTs[300] = $this->txnStatus(1100);
        $this->statusByTs[400] = $this->txnStatus(0, Action::LockNotExistRollback);
        $resolver = $this->resolver();

        $resolver->resolveLocks([
            $this->lock('a', 100),
            $this->lock('b', 200),
            $this->lock('c', 300),
            $this->lock('d', 400),
        ]);

        self::assertSame([300, 400], $resolver->getResolvedLockTracker()->getResolvedLocks());
        self::assertSame([100, 200], $resolver->getResolvedLockTracker()->getCommittedLocks());
    }

    public function testSubsequentGetCarriesCommittedLocks(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::COMMIT_TS);

        $value = new GetResponse();
        $value->setValue('v');
        $this->getResponses = [$this->lockedGetResponse(), $value];

        $reader = $this->reader($this->resolver());
        $state = new TransactionState();

        try {
            $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier());
            self::fail('Expected the locked read to request a retry');
        } catch (TxnRetryableException) {
            // Expected.
        }

        self::assertSame('v', $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier()));

        self::assertCount(2, $this->getRequests);
        $first = $this->contextOf($this->getRequests[0]);
        self::assertSame([], $this->ints($first->getResolvedLocks()));
        self::assertSame([], $this->ints($first->getCommittedLocks()));

        $second = $this->contextOf($this->getRequests[1]);
        self::assertSame([], $this->ints($second->getResolvedLocks()));
        self::assertSame([self::LOCK_TS], $this->ints($second->getCommittedLocks()));
    }

    public function testSubsequentGetCarriesResolvedLocksForAFutureCommit(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::FUTURE_COMMIT_TS);

        $value = new GetResponse();
        $value->setValue('v');
        $this->getResponses = [$this->lockedGetResponse(), $value];

        $reader = $this->reader($this->resolver());
        $state = new TransactionState();

        try {
            $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier());
        } catch (TxnRetryableException) {
            // Expected.
        }

        $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier());

        $second = $this->contextOf($this->getRequests[1]);
        self::assertSame([self::LOCK_TS], $this->ints($second->getResolvedLocks()));
        self::assertSame([], $this->ints($second->getCommittedLocks()));
    }

    public function testSubsequentGetDoesNotMarkARolledBackLockAsCommitted(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(0, Action::TTLExpireRollback);

        $value = new GetResponse();
        $value->setValue('v');
        $this->getResponses = [$this->lockedGetResponse(), $value];

        $reader = $this->reader($this->resolver());
        $state = new TransactionState();

        try {
            $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier());
        } catch (TxnRetryableException) {
            // Expected.
        }

        $reader->get(self::READ_KEY, $state, $this->executor(), $this->classifier());

        $second = $this->contextOf($this->getRequests[1]);
        self::assertSame([self::LOCK_TS], $this->ints($second->getResolvedLocks()));
        self::assertSame([], $this->ints($second->getCommittedLocks()));
    }

    public function testBatchGetCarriesResolvedLocks(): void
    {
        $resolver = $this->resolver();
        $resolver->getResolvedLockTracker()->recordResolved(700);
        $resolver->getResolvedLockTracker()->recordCommitted(500);
        $this->batchGetResponses = [new BatchGetResponse()];

        $this->reader($resolver)->batchGet(
            ['k1', 'k2'],
            new TransactionState(),
            $this->executor(),
            $this->classifier(),
        );

        self::assertCount(1, $this->batchGetRequests);
        $context = $this->contextOf($this->batchGetRequests[0]);
        self::assertSame([700], $this->ints($context->getResolvedLocks()));
        self::assertSame([500], $this->ints($context->getCommittedLocks()));
    }

    public function testScanCarriesResolvedLocks(): void
    {
        $resolver = $this->resolver();
        $resolver->getResolvedLockTracker()->recordResolved(700);
        $resolver->getResolvedLockTracker()->recordCommitted(500);
        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v');
        $response = new ScanResponse();
        $response->setPairs([$pair]);
        $this->scanResponses = [$response];

        $this->reader($resolver)->scan(
            'k0',
            'k9',
            10,
            new TransactionState(),
            $this->executor(),
            $this->classifier(),
        );

        self::assertCount(1, $this->scanRequests);
        $context = $this->contextOf($this->scanRequests[0]);
        self::assertSame([700], $this->ints($context->getResolvedLocks()));
        self::assertSame([500], $this->ints($context->getCommittedLocks()));
    }

    public function testCommittedResolutionDoesNotRecordARegionError(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(self::COMMIT_TS);
        $this->resolveResponse = $this->regionErrorResponse();
        $resolver = $this->resolver();

        try {
            $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());
            self::fail('Expected the region error to propagate');
        } catch (RegionException) {
            // Expected: the resolution did not reach the region.
        }

        self::assertTrue($resolver->getResolvedLockTracker()->isEmpty());
    }

    public function testRolledBackResolutionDoesNotRecordARegionError(): void
    {
        $this->statusByTs[self::LOCK_TS] = $this->txnStatus(0, Action::TTLExpireRollback);
        $this->resolveResponse = $this->regionErrorResponse();
        $resolver = $this->resolver();

        try {
            $resolver->resolveLock(self::PRIMARY_KEY, $this->lock());
            self::fail('Expected the region error to propagate');
        } catch (RegionException) {
            // Expected: the resolution did not reach the region.
        }

        self::assertTrue($resolver->getResolvedLockTracker()->isEmpty());
    }

    public function testCommitterCarriesResolvedLocksOnItsRequests(): void
    {
        $resolver = $this->resolver();
        $resolver->getResolvedLockTracker()->recordResolved(700);
        $resolver->getResolvedLockTracker()->recordCommitted(500);

        $committer = new TwoPhaseCommitter(
            startTs: self::START_TS,
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
            $this->classifier(),
        );

        self::assertCount(1, $this->heartbeatRequests);
        $context = $this->contextOf($this->heartbeatRequests[0]);
        self::assertSame([700], $this->ints($context->getResolvedLocks()));
        self::assertSame([500], $this->ints($context->getCommittedLocks()));
    }
}
