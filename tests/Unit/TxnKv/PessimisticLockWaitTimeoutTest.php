<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use InvalidArgumentException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * The pessimistic-lock wait timeout (issue #443, DIV-07).
 *
 * `PessimisticLockRequest.wait_timeout` tells TiKV how long it may block on
 * a contended key before it answers with a "locked" error. The field was
 * never set, so TiKV applied its own server default and the caller could
 * not bound the wait; and the value handed to TiKV now shrinks as the
 * client's lock-wait budget is consumed (client-go's
 * `calculateEffectiveWaitTime`).
 */
class PessimisticLockWaitTimeoutTest extends TestCase
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
     *     lockWaitTimeMs?: int,
     *     maxBackoffMs?: int,
     *     eagerPessimisticLocks?: bool,
     * } $options
     */
    private function createTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: 'test-txn-lock-wait',
            startTs: 1000,
            pessimistic: true,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: $options['maxBackoffMs'] ?? 20000,
            eagerPessimisticLocks: $options['eagerPessimisticLocks'] ?? true,
            lockWaitTimeMs: $options['lockWaitTimeMs'] ?? Transaction::DEFAULT_LOCK_WAIT_TIME_MS,
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
     * Answer every pessimistic-lock RPC with a "locked" error, so the client
     * keeps retrying until its lock-wait budget is spent.
     */
    private function alwaysConflicting(): void
    {
        $lockInfo = new LockInfo();
        $lockInfo->setKey('key');
        $lockInfo->setPrimaryLock('key');
        $lockInfo->setLockVersion(2000);

        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);

        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);

        $this->stubGrpc($lockResponse);
    }

    /**
     * Answer every pessimistic-lock RPC with success.
     */
    private function alwaysLockingSuccessfully(): void
    {
        $this->stubGrpc(new PessimisticLockResponse());
    }

    private function stubGrpc(PessimisticLockResponse $lockResponse): void
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

                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvCheckTxnStatus' => new CheckTxnStatusResponse(),
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KVPessimisticRollback' => new PessimisticRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );
    }

    /**
     * @return list<int>
     */
    private function capturedWaitTimeouts(): array
    {
        return array_map(
            static fn (PessimisticLockRequest $request): int => (int) $request->getWaitTimeout(),
            $this->lockRequests,
        );
    }

    public function testLockRequestCarriesTheConfiguredWaitTimeout(): void
    {
        $this->alwaysLockingSuccessfully();

        $txn = $this->createTransaction(['lockWaitTimeMs' => 250]);
        $txn->set('key', 'value');

        $this->assertSame([250], $this->capturedWaitTimeouts());
    }

    public function testDefaultWaitTimeoutMatchesTikvsOwnDefault(): void
    {
        $this->alwaysLockingSuccessfully();

        $txn = $this->createTransaction();
        $txn->set('key', 'value');

        $this->assertSame([1000], $this->capturedWaitTimeouts());
        $this->assertSame(1000, Transaction::DEFAULT_LOCK_WAIT_TIME_MS);
    }

    public function testWaitTimeoutShrinksAsTheLockWaitBudgetIsConsumed(): void
    {
        $this->alwaysConflicting();

        // The lock-wait budget is 250 ms and the configured wait time is
        // larger, so the value handed to TiKV is the remaining budget: 250 ms
        // for the first attempt, and 50 ms for the retry that follows the
        // 200 ms backoff (client-go's calculateEffectiveWaitTime).
        $txn = $this->createTransaction([
            'lockWaitTimeMs' => 1000,
            'maxBackoffMs' => 250,
        ]);

        try {
            $txn->set('key', 'value');
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException) {
            // The budget ran out; the assertions below describe the requests.
        }

        $this->assertSame([250, 50], $this->capturedWaitTimeouts());
    }

    public function testLockBudgetExhaustionRaisesLockWaitTimeoutInsteadOfPrewriting(): void
    {
        $this->alwaysConflicting();

        $txn = $this->createTransaction([
            'eagerPessimisticLocks' => false,
            'maxBackoffMs' => 100,
        ]);
        $txn->set('key', 'value');

        try {
            $txn->commit();
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException $e) {
            $this->assertSame('key', $e->getKey());
            $this->assertSame(100, $e->getTimeoutMs());
        }

        $this->assertNotContains('KvPrewrite', $this->methods);
        $this->assertNotContains('KvCommit', $this->methods);
    }

    public function testZeroWaitTimeoutLeavesTheDecisionToTiKV(): void
    {
        $this->alwaysLockingSuccessfully();

        // 0 is the protobuf's "use TiKV's own default timeout" — the only
        // value that is not shrunk as the budget is consumed.
        $txn = $this->createTransaction(['lockWaitTimeMs' => 0]);
        $txn->set('key', 'value');

        $this->assertSame([0], $this->capturedWaitTimeouts());
    }

    public function testAnExhaustedBudgetAsksTiKVNotToWaitAtAll(): void
    {
        $this->alwaysConflicting();

        // With no budget at all there is nothing to wait for: the request
        // carries a negative wait_timeout, which the protobuf defines as
        // "do not wait".
        $txn = $this->createTransaction(['maxBackoffMs' => 0]);

        try {
            $txn->set('key', 'value');
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException) {
            // The budget was zero; the assertions below describe the request.
        }

        $this->assertSame([-1], $this->capturedWaitTimeouts());
    }

    public function testANegativeWaitTimeoutIsRejected(): void
    {
        // A negative wait_timeout means "do not wait at all" in TiKV, so it
        // would silently change the meaning of the option. The public
        // constructor rejects it instead of forwarding it.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('lockWaitTimeMs must be >= 0');

        $this->createTransaction(['lockWaitTimeMs' => -1]);
    }
}
