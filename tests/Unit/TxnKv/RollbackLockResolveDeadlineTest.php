<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #569 (N2): a `KeyError.locked` response during rollback resolves the
 * lock through `LockResolver`, and both the nested resolution and the
 * rollback RPC itself must be capped by the rollback loop's remaining retry
 * deadline. Removing the budget from `handleRollbackError()` or from either
 * rollback RPC cap makes these tests fail.
 */
final class RollbackLockResolveDeadlineTest extends TestCase
{
    private const DEADLINE_MS = 500;

    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('store-1:20160');

        $this->pdClient->method('getRegion')->willReturn($this->region());
        $this->pdClient->method('scanRegions')->willReturn([$this->region()]);
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getTimestamp')->willReturn(9001);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn(9002);
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function createTransaction(
        FanoutFakeGrpc $grpc,
        bool $pessimistic,
        int $retryDeadlineMs = self::DEADLINE_MS,
    ): Transaction {
        $region = $this->region();
        $regionCache = new RegionCache();
        $regionCache->put($region);
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        return new Transaction(
            txnId: 'rollback-lock-deadline-txn',
            startTs: 1000,
            pessimistic: $pessimistic,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $grpc,
            regionCache: $regionCache,
            lockResolver: $lockResolver,
            regionResolver: $regionResolver,
            maxBackoffMs: 20000,
            retryDeadlineMs: $retryDeadlineMs,
            eagerPessimisticLocks: $pessimistic,
        );
    }

    private function lockedKeyError(): KeyError
    {
        $lock = new LockInfo();
        $lock->setKey('k1');
        $lock->setPrimaryLock('k1');
        $lock->setLockVersion(2000);

        $error = new KeyError();
        $error->setLocked($lock);

        return $error;
    }

    private function checkTxnStatusResponse(): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion(3000);

        return $response;
    }

    /**
     * @param list<array{method: string, timeoutMs: ?int}> $calls
     * @return list<array{method: string, timeoutMs: ?int}>
     */
    private function callsOf(array $calls, string $method): array
    {
        return array_values(array_filter(
            $calls,
            static fn (array $call): bool => $call['method'] === $method,
        ));
    }

    public function testBatchRollbackLockConflictResolvesAndCapsNestedAndOuterTimeouts(): void
    {
        $batchRollbackCalls = 0;
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: function (string $method) use (&$batchRollbackCalls): Message {
                return match ($method) {
                    'KvBatchRollback' => ++$batchRollbackCalls === 1
                        ? $this->lockedBatchRollbackResponse()
                        : new BatchRollbackResponse(),
                    'KvCheckTxnStatus' => $this->checkTxnStatusResponse(),
                    'KvResolveLock' => new ResolveLockResponse(),
                    default => throw new \RuntimeException("unexpected method {$method}"),
                };
            },
        );

        $txn = $this->createTransaction($grpc, pessimistic: false);
        $txn->set('k1', 'v1');
        $txn->rollback();

        self::assertSame(2, $batchRollbackCalls, 'the rollback must be retried after the lock is resolved');
        $nested = [
            ...$this->callsOf($grpc->callTimeouts, 'KvCheckTxnStatus'),
            ...$this->callsOf($grpc->callTimeouts, 'KvResolveLock'),
        ];
        self::assertCount(2, $nested);
        $this->assertCapped($nested);
        $this->assertCapped($this->callsOf($grpc->callTimeouts, 'KvBatchRollback'));
    }

    public function testPessimisticRollbackLockConflictResolvesAndCapsNestedAndOuterTimeouts(): void
    {
        $pessimisticRollbackCalls = 0;
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: function (string $method) use (&$pessimisticRollbackCalls): Message {
                return match ($method) {
                    'KvPessimisticLock' => new PessimisticLockResponse(),
                    'KVPessimisticRollback' => ++$pessimisticRollbackCalls === 1
                        ? $this->lockedPessimisticRollbackResponse()
                        : new PessimisticRollbackResponse(),
                    'KvCheckTxnStatus' => $this->checkTxnStatusResponse(),
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    default => throw new \RuntimeException("unexpected method {$method}"),
                };
            },
        );

        $txn = $this->createTransaction($grpc, pessimistic: true);
        $txn->set('k1', 'v1');
        $txn->rollback();

        self::assertSame(
            2,
            $pessimisticRollbackCalls,
            'the pessimistic rollback must be retried after the lock is resolved',
        );
        $nested = [
            ...$this->callsOf($grpc->callTimeouts, 'KvCheckTxnStatus'),
            ...$this->callsOf($grpc->callTimeouts, 'KvResolveLock'),
        ];
        self::assertCount(2, $nested);
        $this->assertCapped($nested);
        $this->assertCapped($this->callsOf($grpc->callTimeouts, 'KVPessimisticRollback'));
    }

    private function lockedBatchRollbackResponse(): BatchRollbackResponse
    {
        $response = new BatchRollbackResponse();
        $response->setError($this->lockedKeyError());

        return $response;
    }

    private function lockedPessimisticRollbackResponse(): PessimisticRollbackResponse
    {
        $response = new PessimisticRollbackResponse();
        $response->setErrors([$this->lockedKeyError()]);

        return $response;
    }

    /**
     * @param list<array{method: string, timeoutMs: ?int}> $calls
     */
    private function assertCapped(array $calls): void
    {
        self::assertNotEmpty($calls);
        foreach ($calls as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            self::assertLessThanOrEqual(
                self::DEADLINE_MS,
                $call['timeoutMs'],
                $call['method'] . ' must be capped by the rollback retry deadline',
            );
        }
    }
}
