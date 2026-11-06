<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #225 (TXN-20): `PessimisticLockRequest.is_first_lock` describes the
 * batch that holds the transaction's primary lock, not the first region batch.
 *
 * TiKV uses the flag to decide whether a transaction can be skipped by the
 * deadlock detector, so it may be set only on the request whose mutations
 * contain the primary key. The pessimistic-lock pass splits each region's
 * keys with the #430 size/count limits, which makes "the primary is in a
 * later batch of the first region" a real wire shape rather than a latent
 * ordering assumption.
 */
final class PessimisticLockFirstLockTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCache $regionCache;
    private RegionResolver $regionResolver;
    private LockResolver $lockResolver;
    private RegionInfo $leftRegion;
    private RegionInfo $rightRegion;

    /** @var list<PessimisticLockRequest> */
    private array $lockRequests = [];

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);

        $this->leftRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'm',
            peers: [],
        );
        $this->rightRegion = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: '',
            peers: [],
        );

        $this->regionCache = new RegionCache();
        $this->regionCache->put($this->leftRegion);
        $this->regionCache->put($this->rightRegion);

        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            1000,
        );

        $this->pdClient->method('getRegion')->willReturnCallback(
            fn (string $key): RegionInfo => KeyOrder::lt($key, 'm')
                ? $this->leftRegion
                : $this->rightRegion,
        );
        $this->pdClient->method('scanRegions')->willReturn([$this->leftRegion, $this->rightRegion]);
        $this->pdClient->method('getStore')->willReturnCallback(
            static function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress(sprintf('127.0.0.1:%d', 20160 + $storeId));

                return $store;
            },
        );
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $this->lockRequests = [];
    }

    /**
     * @param null|callable(PessimisticLockRequest, int): PessimisticLockResponse $lockResponder
     */
    private function stubGrpc(?callable $lockResponder = null): void
    {
        $this->grpc->method('call')->willReturnCallback(
            fn(string $address, string $service, string $method, Message $request): Message => match ($method) {
                'KvPessimisticLock' => $this->recordAndRespond($request, $lockResponder),
                'KvPrewrite' => new PrewriteResponse(),
                'KvCommit' => new CommitResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            },
        );
    }

    /**
     * @param null|callable(PessimisticLockRequest, int): PessimisticLockResponse $lockResponder
     */
    private function recordAndRespond(Message $request, ?callable $lockResponder): PessimisticLockResponse
    {
        if (!$request instanceof PessimisticLockRequest) {
            throw new \RuntimeException('Expected a PessimisticLockRequest');
        }

        $this->lockRequests[] = $request;
        if ($lockResponder === null) {
            return new PessimisticLockResponse();
        }

        return $lockResponder($request, count($this->lockRequests));
    }

    /**
     * @param array{maxTxnBatchCount?: int, maxTxnBatchSize?: int, maxBackoffMs?: int} $options
     */
    private function createTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: 'test-txn-first-lock',
            startTs: 1000,
            pessimistic: true,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: $options['maxBackoffMs'] ?? 20000,
            eagerPessimisticLocks: false,
            maxTxnBatchSize: $options['maxTxnBatchSize'] ?? 16384,
            maxTxnBatchCount: $options['maxTxnBatchCount'] ?? 512,
        );
    }

    /**
     * Pin the pessimistic primary independently of the first write key. The
     * production path sets it to the first eager key; the deferred path
     * derives it from the write set, and the regression scenario is exactly
     * the case where those two disagree.
     */
    private function setPrimary(Transaction $txn, string $primary): void
    {
        $state = (new \ReflectionProperty(Transaction::class, 'state'))->getValue($txn);
        self::assertInstanceOf(TransactionState::class, $state);
        $state->setPessimisticPrimaryKey($primary);
    }

    /**
     * @return list<string>
     */
    private function requestKeys(PessimisticLockRequest $request): array
    {
        $keys = [];
        foreach ($request->getMutations() as $mutation) {
            $keys[] = $mutation->getKey();
        }

        return $keys;
    }

    /**
     * @return list<PessimisticLockRequest>
     */
    private function flaggedRequests(): array
    {
        return array_values(array_filter(
            $this->lockRequests,
            static fn (PessimisticLockRequest $request): bool => $request->getIsFirstLock(),
        ));
    }

    public function testFirstLockFlagIsOnThePrimaryBatchWhenThePrimaryIsInTheFirstBatch(): void
    {
        $this->stubGrpc();
        $txn = $this->createTransaction(['maxTxnBatchCount' => 1]);
        $txn->set('a1', 'v1');
        $txn->set('a2', 'v2');
        $txn->set('a3', 'v3');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(3, $this->lockRequests);
        $flagged = $this->flaggedRequests();
        self::assertCount(1, $flagged);
        self::assertSame(['a1'], $this->requestKeys($flagged[0]));
    }

    public function testFirstLockFlagFollowsThePrimaryIntoALaterBatchOfTheFirstRegion(): void
    {
        $this->stubGrpc();
        $txn = $this->createTransaction(['maxTxnBatchCount' => 1]);
        $txn->set('a1', 'v1');
        $txn->set('a2', 'v2');
        $txn->set('a3', 'v3');
        $this->setPrimary($txn, 'a2');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(3, $this->lockRequests);
        self::assertSame(['a1'], $this->requestKeys($this->lockRequests[0]));
        self::assertFalse($this->lockRequests[0]->getIsFirstLock());

        $flagged = $this->flaggedRequests();
        self::assertCount(1, $flagged);
        self::assertSame(['a2'], $this->requestKeys($flagged[0]));
    }

    public function testFirstLockFlagFollowsThePrimaryIntoALaterRegion(): void
    {
        $this->stubGrpc();
        $txn = $this->createTransaction(['maxTxnBatchCount' => 2]);
        $txn->set('a1', 'v1');
        $txn->set('a2', 'v2');
        $txn->set('m1', 'v3');
        $txn->set('m2', 'v4');
        $this->setPrimary($txn, 'm1');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(2, $this->lockRequests);
        self::assertSame(['a1', 'a2'], $this->requestKeys($this->lockRequests[0]));
        self::assertFalse($this->lockRequests[0]->getIsFirstLock());
        self::assertSame(['m1', 'm2'], $this->requestKeys($this->lockRequests[1]));
        self::assertTrue($this->lockRequests[1]->getIsFirstLock());
    }

    public function testExactlyOneBatchCarriesTheFlagAcrossRegionsAndBatches(): void
    {
        $this->stubGrpc();
        $txn = $this->createTransaction(['maxTxnBatchCount' => 1]);
        foreach (['a1', 'a2', 'a3', 'm1', 'm2', 'm3'] as $index => $key) {
            $txn->set($key, 'v' . $index);
        }
        $this->setPrimary($txn, 'm2');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(6, $this->lockRequests);
        $flagged = $this->flaggedRequests();
        self::assertCount(1, $flagged);
        self::assertSame(['m2'], $this->requestKeys($flagged[0]));
    }

    public function testFirstLockFlagIsPreservedWhenThePrimaryBatchIsRetried(): void
    {
        $primaryAttempts = 0;
        $this->stubGrpc(
            function (PessimisticLockRequest $request) use (&$primaryAttempts): PessimisticLockResponse {
                if ($this->requestKeys($request) !== ['a2'] || $primaryAttempts++ > 0) {
                    return new PessimisticLockResponse();
                }

                $error = new Error();
                $error->setMessage('epoch not match');
                $error->setEpochNotMatch(new EpochNotMatch());
                $response = new PessimisticLockResponse();
                $response->setRegionError($error);

                return $response;
            },
        );

        // 300 ms is enough for the first 200 ms retry delay; the second
        // attempt succeeds, so the budget is never exhausted.
        $txn = $this->createTransaction(['maxTxnBatchCount' => 1, 'maxBackoffMs' => 300]);
        $txn->set('a1', 'v1');
        $txn->set('a2', 'v2');
        $txn->set('a3', 'v3');
        $this->setPrimary($txn, 'a2');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());

        $primaryRequests = array_values(array_filter(
            $this->lockRequests,
            fn (PessimisticLockRequest $request): bool => $this->requestKeys($request) === ['a2'],
        ));
        self::assertCount(2, $primaryRequests, 'the primary batch must be retried once');
        self::assertTrue($primaryRequests[0]->getIsFirstLock());
        self::assertTrue($primaryRequests[1]->getIsFirstLock(), 'the retry must keep is_first_lock');

        foreach ($this->lockRequests as $request) {
            if ($this->requestKeys($request) !== ['a2']) {
                self::assertFalse($request->getIsFirstLock());
            }
        }
    }
}
