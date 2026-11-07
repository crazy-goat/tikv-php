<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #569: a `KeyError.locked` response during prewrite resolves the lock
 * through `LockResolver`, which used to ignore the retry loop's remaining
 * wall-clock deadline. The resolver now caps its lock-resolution RPCs by that
 * budget; a live lock with a TTL longer than the budget stays retryable, but
 * the executor's own deadline check stops the loop with
 * `RetryBudgetExhaustedException` instead of letting the resolution (or a
 * backoff) overshoot the deadline.
 *
 * The tests drive the public `Transaction::commit()` path so the deadline is
 * the real one built by `RetryExecutor`, not a hand-passed value.
 */
final class PrewriteLockResolveDeadlineTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    /** @var list<int> Timeout passed to each PD TSO fetch */
    private array $tsoTimeouts = [];

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->tsoTimeouts = [];

        $store = new Store();
        $store->setId(1);
        $store->setAddress('store-1:20160');

        $this->pdClient->method('getRegion')->willReturn($this->region());
        $this->pdClient->method('scanRegions')->willReturn([$this->region()]);
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getTimestamp')->willReturn(9001);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturnCallback(
            function (int $timeoutMs): int {
                $this->tsoTimeouts[] = $timeoutMs;

                return 9002;
            },
        );
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
        int $retryDeadlineMs,
        int $maxBackoffMs = 20000,
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
            txnId: 'prewrite-lock-deadline-txn',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $grpc,
            regionCache: $regionCache,
            lockResolver: $lockResolver,
            regionResolver: $regionResolver,
            maxBackoffMs: $maxBackoffMs,
            retryDeadlineMs: $retryDeadlineMs,
        );
    }

    private function lockedPrewriteResponse(): PrewriteResponse
    {
        $lock = new LockInfo();
        $lock->setKey('k1');
        $lock->setPrimaryLock('k1');
        $lock->setLockVersion(2000);

        $keyError = new KeyError();
        $keyError->setLocked($lock);

        $response = new PrewriteResponse();
        $response->setErrors([$keyError]);

        return $response;
    }

    private function checkTxnStatusResponse(int $commitVersion, int $lockTtl): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion($commitVersion);
        $response->setLockTtl($lockTtl);

        return $response;
    }

    /**
     * The prewrite RPC itself is capped by the operation's remaining retry
     * deadline (issue #569): the configured write timeout is an upper bound,
     * not an independent budget.
     */
    public function testPrewriteRpcTimeoutIsCappedByTheRemainingRetryDeadline(): void
    {
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: fn(string $method): Message => match ($method) {
                'KvPrewrite' => new PrewriteResponse(),
                'KvCommit' => new CommitResponse(),
                default => throw new \RuntimeException("unexpected method {$method}"),
            },
        );

        $txn = $this->createTransaction($grpc, retryDeadlineMs: 1000);
        $txn->set('k1', 'v1');
        $txn->commit();

        $prewrites = array_values(array_filter(
            $grpc->callTimeouts,
            static fn (array $call): bool => $call['method'] === 'KvPrewrite',
        ));
        self::assertNotEmpty($prewrites);
        foreach ($prewrites as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            self::assertLessThanOrEqual(1000, $call['timeoutMs']);
        }
    }

    /**
     * A lock conflict is resolved (the owner committed) and the prewrite is
     * retried successfully — the resolve-then-retry contract is unchanged.
     */
    public function testPrewriteLockConflictResolvesAndRetriesThePrewrite(): void
    {
        $prewriteCalls = 0;
        /** @var list<string> $methods */
        $methods = [];
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: function (string $method) use (&$prewriteCalls, &$methods): Message {
                $methods[] = $method;

                return match ($method) {
                    'KvPrewrite' => ++$prewriteCalls === 1
                        ? $this->lockedPrewriteResponse()
                        : new PrewriteResponse(),
                    'KvCheckTxnStatus' => $this->checkTxnStatusResponse(3000, 0),
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KvCommit' => new CommitResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    default => throw new \RuntimeException("unexpected method {$method}"),
                };
            },
        );

        $txn = $this->createTransaction($grpc, retryDeadlineMs: 500);
        $txn->set('k1', 'v1');
        $txn->commit();

        self::assertSame(2, $prewriteCalls, 'the prewrite must be retried after the lock is resolved');
        self::assertContains('KvCheckTxnStatus', $methods);
        self::assertContains('KvResolveLock', $methods);
        self::assertSame(1, $this->countMethod($methods, 'KvCheckTxnStatus'));
        self::assertSame(1, $this->countMethod($methods, 'KvResolveLock'));

        // The nested resolution must use the prewrite loop's remaining
        // deadline, not the configured 5 s lockResolveTimeoutMs (issue #569,
        // N1). Without the budget in handlePrewriteErrors() these are 5000.
        $nested = array_values(array_filter(
            $grpc->callTimeouts,
            static fn (array $call): bool => in_array(
                $call['method'],
                ['KvCheckTxnStatus', 'KvResolveLock'],
                true,
            ),
        ));
        self::assertCount(2, $nested);
        foreach ($nested as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            self::assertLessThanOrEqual(500, $call['timeoutMs']);
        }

        // The PD TSO fetch performed by KvCheckTxnStatus shares the same
        // budget (issue #569, N1).
        self::assertNotEmpty($this->tsoTimeouts);
        foreach ($this->tsoTimeouts as $timeoutMs) {
            self::assertGreaterThan(0, $timeoutMs);
            self::assertLessThanOrEqual(500, $timeoutMs);
        }
    }

    /**
     * A lock TTL longer than the remaining retry deadline must not make the
     * commit sleep for the TTL: the resolver reports the live lock as
     * retryable, and the executor's own deadline check ends the loop with
     * `RetryBudgetExhaustedException` inside the budget.
     */
    public function testPrewriteDoesNotSleepPastTheRetryDeadlineForLongLockTtl(): void
    {
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: fn(string $method): Message => match ($method) {
                'KvPrewrite' => $this->lockedPrewriteResponse(),
                'KvCheckTxnStatus' => $this->checkTxnStatusResponse(0, 60000),
                'KvResolveLock' => new ResolveLockResponse(),
                'KvCommit' => new CommitResponse(),
                'KvBatchRollback' => new BatchRollbackResponse(),
                default => throw new \RuntimeException("unexpected method {$method}"),
            },
        );

        $txn = $this->createTransaction($grpc, retryDeadlineMs: 500);
        $txn->set('k1', 'v1');

        $startMs = microtime(true) * 1000;

        try {
            $txn->commit();
            self::fail('the commit must stop at the retry deadline');
        } catch (RetryBudgetExhaustedException $e) {
            $elapsedMs = (microtime(true) * 1000) - $startMs;
            self::assertLessThan(2000, $elapsedMs, 'the commit must not sleep for the lock TTL');
            self::assertStringContainsString('Retry deadline (500 ms) exhausted', $e->getMessage());
        }
    }

    /**
     * A standalone executor with `retryDeadlineMs = 0` keeps its historical
     * behaviour: the resolver reports the live lock as retryable and the
     * executor's cumulative-backoff budget (not a deadline) decides when to
     * give up. With `maxBackoffMs = 1` the first charge exhausts that budget,
     * so `RetryBudgetExhaustedException` is thrown with the original
     * `TxnRetryableException` as `getPrevious()`.
     */
    public function testNoDeadlineExecutorKeepsTheRetryableLockBehaviour(): void
    {
        $prewriteCalls = 0;
        $grpc = new FanoutFakeGrpc(
            latencyMs: 0.0,
            responder: function (string $method) use (&$prewriteCalls): Message {
                return match ($method) {
                    'KvPrewrite' => ++$prewriteCalls === 1
                        ? $this->lockedPrewriteResponse()
                        : new PrewriteResponse(),
                    'KvCheckTxnStatus' => $this->checkTxnStatusResponse(0, 60000),
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KvCommit' => new CommitResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    default => throw new \RuntimeException("unexpected method {$method}"),
                };
            },
        );

        $txn = $this->createTransaction($grpc, retryDeadlineMs: 0, maxBackoffMs: 1);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            self::fail('the live lock must be reported as retryable without a deadline');
        } catch (RetryBudgetExhaustedException $e) {
            self::assertInstanceOf(TxnRetryableException::class, $e->getPrevious());
            self::assertSame(1, $prewriteCalls);
        }

        // No deadline means no cap: the configured write timeout still
        // applies to the outer KvPrewrite (issue #569, N5).
        $prewrites = array_values(array_filter(
            $grpc->callTimeouts,
            static fn (array $call): bool => $call['method'] === 'KvPrewrite',
        ));
        self::assertNotEmpty($prewrites);
        self::assertSame(
            (new TimeoutConfig())->writeTimeoutMs,
            $prewrites[0]['timeoutMs'] ?? null,
        );
    }

    /**
     * @param list<string> $methods
     */
    private function countMethod(array $methods, string $method): int
    {
        return count(array_filter($methods, static fn (string $name): bool => $name === $method));
    }
}
