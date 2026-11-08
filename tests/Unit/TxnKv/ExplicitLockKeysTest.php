<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\Mutation;
use CrazyGoat\Proto\Kvrpcpb\Op;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest\PessimisticAction;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\WriteConflict;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Explicit key locking for pessimistic transactions (issue #436, GAP-22).
 *
 * `lockKeys()` acquires a pessimistic lock without writing a value, and
 * `getForUpdate()` is the `SELECT ... FOR UPDATE` primitive: the lock and the
 * read happen in one `KvPessimisticLock` round trip, whose `return_values`
 * flag makes TiKV return the current value.
 */
class ExplicitLockKeysTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private LockResolver $lockResolver;
    private RegionInfo $testRegion;

    /** @var list<PessimisticLockRequest> */
    private array $lockRequests = [];

    /** @var list<PrewriteRequest> */
    private array $prewriteRequests = [];

    /** @var list<CommitRequest> */
    private array $commitRequests = [];

    /** @var list<BatchRollbackRequest> */
    private array $batchRollbackRequests = [];

    /** @var list<string> */
    private array $methods = [];

    protected function setUp(): void
    {
        $this->testRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            1000,
        );

        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);
    }

    /**
     * @param array{
     *     pessimistic?: bool,
     *     lockWaitTimeMs?: int,
     *     maxBackoffMs?: int,
     *     eagerPessimisticLocks?: bool,
     *     trackReadSet?: bool,
     *     maxReadSetEntries?: int,
     * } $options
     */
    private function createTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: 'test-txn-explicit-locks',
            startTs: 1000,
            pessimistic: $options['pessimistic'] ?? true,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: $options['maxBackoffMs'] ?? 20000,
            eagerPessimisticLocks: $options['eagerPessimisticLocks'] ?? true,
            lockWaitTimeMs: $options['lockWaitTimeMs'] ?? Transaction::DEFAULT_LOCK_WAIT_TIME_MS,
            maxReadSetEntries: $options['maxReadSetEntries'] ?? 0,
            trackReadSet: $options['trackReadSet'] ?? false,
        );
    }

    private function makeStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        return $store;
    }

    /**
     * Answer every pessimistic-lock RPC with the given response.
     */
    private function stubLockResponse(PessimisticLockResponse $lockResponse): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $addr,
                string $service,
                string $method,
                ?\Google\Protobuf\Internal\Message $request = null,
            ) use ($lockResponse): object {
                $this->methods[] = $method;
                if ($method === 'KvPessimisticLock' && $request instanceof PessimisticLockRequest) {
                    $this->lockRequests[] = $request;
                }
                if ($method === 'KvPrewrite' && $request instanceof PrewriteRequest) {
                    $this->prewriteRequests[] = $request;
                }
                if ($method === 'KvCommit' && $request instanceof CommitRequest) {
                    $this->commitRequests[] = $request;
                }
                if ($method === 'KvBatchRollback' && $request instanceof BatchRollbackRequest) {
                    $this->batchRollbackRequests[] = $request;
                }

                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    'KvCheckTxnStatus' => new CheckTxnStatusResponse(),
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KVPessimisticRollback' => new PessimisticRollbackResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );
    }

    /**
     * @return list<string>
     */
    private function lockedKeys(PessimisticLockRequest $request): array
    {
        return array_values(array_map(
            static fn (Mutation $mutation): string => $mutation->getKey(),
            iterator_to_array($request->getMutations()),
        ));
    }

    public function testLockKeysAcquiresLocksWithoutWritingValues(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->lockKeys('a', 'b');

        $this->assertSame([], $txn->getWriteSet());
        $this->assertSame([], $txn->getReadSet());
        $this->assertCount(1, $this->lockRequests);

        $request = $this->lockRequests[0];
        $this->assertSame(['a', 'b'], $this->lockedKeys($request));
        $this->assertSame('a', $request->getPrimaryLock());
        $this->assertTrue($request->getIsFirstLock());
        $this->assertFalse($request->getReturnValues());
        $this->assertSame(3000, $request->getForUpdateTs());
        $this->assertSame([], $txn->getWriteSet());
        $this->assertNotContains('KvPrewrite', $this->methods);
    }

    public function testLockKeysSendsTheConfiguredWaitTimeout(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction(['lockWaitTimeMs' => 250]);
        $txn->lockKeys('key');

        $this->assertCount(1, $this->lockRequests);
        $this->assertSame(250, $this->lockRequests[0]->getWaitTimeout());
    }

    public function testLockKeysIsIdempotentForAnAlreadyLockedKey(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->lockKeys('key');
        $txn->lockKeys('key');

        $this->assertCount(1, $this->lockRequests, 'a key already locked by this transaction is not re-locked');
    }

    public function testLockKeysConflictSurfacesAndReleasesPartialLocks(): void
    {
        $keyError = new KeyError();
        $keyError->setConflict(new WriteConflict());
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();

        try {
            $txn->lockKeys('key');
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertSame('Write conflict during pessimistic lock', $e->getMessage());
        }

        $this->assertSame([], $txn->getWriteSet());
        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertContains('KVPessimisticRollback', $this->methods);
        $this->assertNotContains('KvPrewrite', $this->methods);
        $this->assertNotContains('KvCommit', $this->methods);
    }

    public function testLockKeysWaitTimeoutExpirySurfacesWithoutPrewriting(): void
    {
        $lockInfo = new LockInfo();
        $lockInfo->setKey('key');
        $lockInfo->setPrimaryLock('key');
        $lockInfo->setLockVersion(2000);
        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction(['maxBackoffMs' => 100]);

        try {
            $txn->lockKeys('key');
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException $e) {
            $this->assertSame('key', $e->getKey());
            $this->assertSame(100, $e->getTimeoutMs());
        }

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertNotContains('KvPrewrite', $this->methods);
        $this->assertNotContains('KvCommit', $this->methods);
    }

    public function testLockKeysDeadlockIsDetected(): void
    {
        $deadlock = new Deadlock();
        $deadlock->setDeadlockKeyHash(12345);
        $deadlock->setDeadlockKey('blocking-key');
        $keyError = new KeyError();
        $keyError->setDeadlock($deadlock);
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();

        try {
            $txn->lockKeys('key');
            $this->fail('Expected DeadlockException was not thrown');
        } catch (DeadlockException $e) {
            $this->assertSame('Deadlock detected during pessimistic lock', $e->getMessage());
            $this->assertSame('blocking-key', $e->getDeadlockKey());
            $this->assertSame(12345, $e->getDeadlockKeyHash());
        }

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertNotContains('KvPrewrite', $this->methods);
    }

    public function testLockKeysRejectsAnOptimisticTransaction(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('lockKeys() is only available in a pessimistic transaction');

        $txn->lockKeys('key');
    }

    public function testGetForUpdateLocksAndReturnsTheValueInOneRoundTrip(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['current-value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();
        $value = $txn->getForUpdate('key');

        $this->assertSame('current-value', $value);
        $this->assertSame(['KvPessimisticLock'], $this->methods);
        $this->assertSame([], $txn->getWriteSet());

        $request = $this->lockRequests[0];
        $this->assertSame(['key'], $this->lockedKeys($request));
        $this->assertSame('key', $request->getPrimaryLock());
        $this->assertTrue($request->getReturnValues());
    }

    public function testGetForUpdatePopulatesTheTrackedReadSet(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['current-value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction(['trackReadSet' => true]);

        $this->assertSame('current-value', $txn->getForUpdate('key'));
        $this->assertSame(['key' => 'current-value'], $txn->getReadSet());
        $this->assertSame(1, $txn->getReadSetCount());
    }

    public function testGetForUpdateRecordsAMissingKeyInTheTrackedReadSet(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['']);
        $lockResponse->setNotFounds([true]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction(['trackReadSet' => true]);

        $this->assertNull($txn->getForUpdate('missing'));
        $this->assertSame(['missing' => null], $txn->getReadSet());
    }

    public function testGetForUpdateRespectsTheReadSetLimit(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction(['trackReadSet' => true, 'maxReadSetEntries' => 1]);
        $txn->getForUpdate('a');

        $this->expectException(TransactionTooLargeException::class);

        $txn->getForUpdate('b');
    }

    public function testEagerWriteDoesNotRequestLockReturnValues(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->set('key', 'value');

        $this->assertCount(1, $this->lockRequests);
        $this->assertFalse($this->lockRequests[0]->getReturnValues());
    }

    public function testDeferredPessimisticLocksDoNotRequestReturnValues(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction(['eagerPessimisticLocks' => false]);
        $txn->set('key', 'value');
        $txn->commit();

        $this->assertNotEmpty($this->lockRequests);
        $this->assertFalse($this->lockRequests[0]->getReturnValues());
    }

    public function testDeferredGetForUpdateOnABufferedWriteDoesNotRequestReturnValues(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction(['eagerPessimisticLocks' => false]);
        $txn->set('key', 'buffered');

        $this->assertSame('buffered', $txn->getForUpdate('key'));
        $this->assertCount(1, $this->lockRequests, 'deferred mode still acquires the lock');
        $this->assertFalse($this->lockRequests[0]->getReturnValues());
    }

    public function testGetForUpdateReturnsNullForAMissingKey(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['']);
        $lockResponse->setNotFounds([true]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();

        $this->assertNull($txn->getForUpdate('missing'));
        $this->assertSame(['KvPessimisticLock'], $this->methods);
    }

    public function testGetForUpdateReturnsTheBufferedWrite(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['old-value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();
        $txn->set('key', 'new-value');

        $this->assertSame('new-value', $txn->getForUpdate('key'));
    }

    public function testGetForUpdateConflictSurfacesAndReleasesPartialLocks(): void
    {
        $keyError = new KeyError();
        $keyError->setConflict(new WriteConflict());
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();

        try {
            $txn->getForUpdate('key');
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException) {
            // Expected.
        }

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertContains('KVPessimisticRollback', $this->methods);
    }

    public function testGetForUpdateRejectsAnOptimisticTransaction(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('getForUpdate() is only available in a pessimistic transaction');

        $txn->getForUpdate('key');
    }

    public function testCommitAfterLockKeysAndPartialWriteCommitsTheLockOnlyKey(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->lockKeys('a', 'b');
        $txn->set('b', 'value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $ops = $this->prewriteOps();
        $this->assertSame(Op::Lock, $ops['a'] ?? null);
        $this->assertSame(Op::Put, $ops['b'] ?? null);
        $actions = $this->prewriteActions();
        $this->assertSame(PessimisticAction::DO_PESSIMISTIC_CHECK, $actions['a'] ?? null);
        $this->assertSame(PessimisticAction::DO_CONSTRAINT_CHECK, $actions['b'] ?? null);
        $this->assertContains('a', $this->committedKeys());
        $this->assertContains('b', $this->committedKeys());
        $this->assertNotContains('KVPessimisticRollback', $this->methods);
    }

    public function testCommitAfterGetForUpdateAndPartialWriteCommitsTheLockOnlyKey(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['a-value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();
        $this->assertSame('a-value', $txn->getForUpdate('a'));
        $txn->set('b', 'value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $ops = $this->prewriteOps();
        $this->assertSame(Op::Lock, $ops['a'] ?? null);
        $this->assertSame(Op::Put, $ops['b'] ?? null);
        $this->assertContains('a', $this->committedKeys());
        $this->assertContains('b', $this->committedKeys());
    }

    public function testCommitAfterOnlyLockKeysCommitsAndReleasesTheLocks(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->lockKeys('a', 'b');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $ops = $this->prewriteOps();
        $this->assertSame(Op::Lock, $ops['a'] ?? null);
        $this->assertSame(Op::Lock, $ops['b'] ?? null);
        $this->assertContains('a', $this->committedKeys());
        $this->assertContains('b', $this->committedKeys());
        $this->assertNotContains('KVPessimisticRollback', $this->methods);
        $this->assertNotContains('KvBatchRollback', $this->methods);
    }

    public function testCommitAfterOnlyGetForUpdateCommitsAndReleasesTheLock(): void
    {
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setValues(['a-value']);
        $lockResponse->setNotFounds([false]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();
        $this->assertSame('a-value', $txn->getForUpdate('a'));
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(Op::Lock, $this->prewriteOps()['a'] ?? null);
        $this->assertContains('a', $this->committedKeys());
    }

    public function testRollbackAfterOnlyLockKeysRollsBackTheLockOnlyKey(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->lockKeys('a');
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertContains('a', $this->batchRollbackKeys());
    }

    public function testGetForUpdateSkipsTheRpcWhenTheBufferedValueWins(): void
    {
        $this->stubLockResponse(new PessimisticLockResponse());

        $txn = $this->createTransaction();
        $txn->set('key', 'buffered');

        $this->assertSame('buffered', $txn->getForUpdate('key'));
        $this->assertCount(1, $this->lockRequests, 'the eager set() already locked the key');
    }

    public function testGetForUpdateReadsTheResultsFieldOfANewerServer(): void
    {
        $result = new \CrazyGoat\Proto\Kvrpcpb\PessimisticLockKeyResult();
        $result->setValue('result-value');
        $result->setExistence(true);
        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setResults([$result]);
        $this->stubLockResponse($lockResponse);

        $txn = $this->createTransaction();

        $this->assertSame('result-value', $txn->getForUpdate('key'));
    }

    /**
     * @return array<string, int>
     */
    private function prewriteOps(): array
    {
        $ops = [];
        foreach ($this->prewriteRequests as $request) {
            foreach ($request->getMutations() as $mutation) {
                $ops[$mutation->getKey()] = $mutation->getOp();
            }
        }

        return $ops;
    }

    /**
     * @return array<string, int|null>
     */
    private function prewriteActions(): array
    {
        $actions = [];
        foreach ($this->prewriteRequests as $request) {
            foreach ($request->getMutations() as $index => $mutation) {
                $actions[$mutation->getKey()] = $request->getPessimisticActions()[$index] ?? null;
            }
        }

        return $actions;
    }

    /**
     * @return list<string>
     */
    private function committedKeys(): array
    {
        $keys = [];
        foreach ($this->commitRequests as $request) {
            foreach ($request->getKeys() as $key) {
                $keys[] = $key;
            }
        }

        return $keys;
    }

    /**
     * @return list<string>
     */
    private function batchRollbackKeys(): array
    {
        $keys = [];
        foreach ($this->batchRollbackRequests as $request) {
            foreach ($request->getKeys() as $key) {
                $keys[] = $key;
            }
        }

        return $keys;
    }
}
