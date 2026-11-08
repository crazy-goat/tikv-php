<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Client\Util\ShutdownState;
use PHPUnit\Framework\TestCase;

/**
 * Transactional range deletion (issue #435, GAP-21).
 *
 * `Transaction::deleteRange($start, $end)` reads the range at the
 * transaction's snapshot and buffers a delete for every key, so the deletion
 * is atomic with the transaction. The range obeys the transactional scan cap
 * and the transaction's write-set limits.
 */
final class TransactionDeleteRangeTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
    }

    protected function tearDown(): void
    {
        ShutdownState::resetForTests();
    }

    private function createTransaction(int $maxScanLimit = 10240, int $maxTxnEntryCount = 0): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-delete-range',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
            maxScanLimit: $maxScanLimit,
            maxTxnEntryCount: $maxTxnEntryCount,
        );
    }

    private function stubSingleRegion(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
    }

    private function createPessimisticTransaction(RegionCache $cache, int $maxTxnEntryCount = 0): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $cache);

        return new Transaction(
            txnId: 'test-txn-delete-range-pessimistic',
            startTs: 1000,
            pessimistic: true,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $cache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $cache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
            eagerPessimisticLocks: true,
            maxTxnEntryCount: $maxTxnEntryCount,
        );
    }

    /**
     * @param list<string> $stored
     * @param list<ScanRequest> $scanRequests
     * @param list<PessimisticLockRequest> $lockRequests
     */
    private function stubPessimisticGrpc(
        RegionCache $cache,
        array $stored,
        array &$scanRequests,
        array &$lockRequests,
    ): void {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
        $cache->put($region);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                $stored,
                &$scanRequests,
                &$lockRequests,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $this->assertInstanceOf(PessimisticLockRequest::class, $request);
                    $lockRequests[] = $request;

                    return new PessimisticLockResponse();
                }

                $this->assertSame('KvScan', $method);
                $this->assertInstanceOf(ScanRequest::class, $request);
                $this->assertFalse($request->getReverse());
                $scanRequests[] = $request;

                $matching = [];
                foreach ($stored as $key) {
                    if (KeyOrder::inRange($key, $request->getStartKey(), $request->getEndKey())) {
                        $matching[] = $key;
                    }
                }
                sort($matching, SORT_STRING);

                $response = new ScanResponse();
                $pairs = [];
                foreach (array_slice($matching, 0, $request->getLimit()) as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $pairs[] = $pair;
                }
                $response->setPairs($pairs);

                return $response;
            },
        );
    }

    /**
     * @param list<string>      $stored
     * @param list<ScanRequest> $capturedRequests
     */
    private function stubForwardScan(array $stored, array &$capturedRequests): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                $stored,
                &$capturedRequests,
            ): ScanResponse {
                $this->assertSame('KvScan', $method);
                $this->assertInstanceOf(ScanRequest::class, $request);
                $this->assertFalse($request->getReverse());
                $capturedRequests[] = $request;

                $matching = [];
                foreach ($stored as $key) {
                    if (KeyOrder::inRange($key, $request->getStartKey(), $request->getEndKey())) {
                        $matching[] = $key;
                    }
                }
                sort($matching, SORT_STRING);

                $response = new ScanResponse();
                $pairs = [];
                foreach (array_slice($matching, 0, $request->getLimit()) as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $pairs[] = $pair;
                }
                $response->setPairs($pairs);

                return $response;
            },
        );
    }

    public function testDeleteRangeBuffersADeleteForEveryScannedKey(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubForwardScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction();
        $deleted = $txn->deleteRange('', 'k4');

        $this->assertSame(3, $deleted);
        $this->assertSame(['k1' => null, 'k2' => null, 'k3' => null], $txn->getWriteSet());
        $this->assertCount(1, $capturedRequests);
        $this->assertSame('k4', $capturedRequests[0]->getEndKey());
    }

    public function testDeleteRangeReplacesABufferedSetWithADelete(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubForwardScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction();
        $txn->set('k2', 'buffered');

        $this->assertSame(3, $txn->deleteRange('', 'k4'));
        // The buffered set was already in the write set, so it keeps its
        // insertion position; every key is now a tombstone.
        $this->assertEqualsCanonicalizing(['k1', 'k2', 'k3'], array_keys($txn->getWriteSet()));
        $this->assertSame([null, null, null], array_values($txn->getWriteSet()));
    }

    public function testDeleteRangeOfAnEmptyRangeIsANoOp(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $txn = $this->createTransaction();

        $this->assertSame(0, $txn->deleteRange('k2', 'k2'));
        $this->assertSame([], $txn->getWriteSet());
    }

    public function testDeleteRangeRejectsAnInvertedRange(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('start key must not sort after end key');

        $this->createTransaction()->deleteRange('k9', 'k1');
    }

    public function testDeleteRangeFailsBeforeBufferingWhenTheRangeExceedsTheScanCap(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubForwardScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction(2);

        try {
            $txn->deleteRange('', 'k4');
            $this->fail('Expected ScanLimitExceededException was not thrown');
        } catch (ScanLimitExceededException) {
            $this->assertSame([], $txn->getWriteSet());
        }
    }

    public function testDeleteRangeStopsAtTheTransactionWriteSetLimit(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubForwardScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction(maxTxnEntryCount: 2);

        $this->expectException(TransactionTooLargeException::class);

        $txn->deleteRange('', 'k4');
    }

    public function testPessimisticEagerDeleteRangeLocksTheWholeRangeInOneBatch(): void
    {
        $cache = new RegionCache();

        /** @var list<ScanRequest> $scanRequests */
        $scanRequests = [];
        /** @var list<PessimisticLockRequest> $lockRequests */
        $lockRequests = [];
        $this->stubPessimisticGrpc($cache, ['k1', 'k2', 'k3'], $scanRequests, $lockRequests);

        $txn = $this->createPessimisticTransaction($cache);

        $this->assertSame(3, $txn->deleteRange('', 'k4'));

        // One batched KvPessimisticLock for the whole range, not one per key.
        $this->assertCount(1, $lockRequests);
        $lockedKeys = [];
        foreach ($lockRequests[0]->getMutations() as $mutation) {
            $lockedKeys[] = $mutation->getKey();
        }
        $this->assertSame(['k1', 'k2', 'k3'], $lockedKeys);
        $this->assertSame(['k1' => null, 'k2' => null, 'k3' => null], $txn->getWriteSet());
    }

    public function testPessimisticEagerDeleteRangeRejectsACumulativeLimitBeforeAnyLockRpc(): void
    {
        $cache = new RegionCache();

        /** @var list<ScanRequest> $scanRequests */
        $scanRequests = [];
        /** @var list<PessimisticLockRequest> $lockRequests */
        $lockRequests = [];
        $this->stubPessimisticGrpc($cache, ['k1', 'k2', 'k3'], $scanRequests, $lockRequests);

        $txn = $this->createPessimisticTransaction($cache, maxTxnEntryCount: 2);

        try {
            $txn->deleteRange('', 'k4');
            $this->fail('Expected TransactionTooLargeException was not thrown');
        } catch (TransactionTooLargeException) {
            // The cumulative count is 3 > 2, so the batch is rejected before
            // lockKeys() sends any KvPessimisticLock, and nothing is buffered.
            $this->assertSame([], $lockRequests);
            $this->assertSame([], $txn->getWriteSet());
        }
    }
}
