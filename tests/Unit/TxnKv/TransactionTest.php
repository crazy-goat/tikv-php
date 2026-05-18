<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TransactionTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private LockResolver $lockResolver;
    private RegionInfo $testRegion;

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
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            20000,
            new \Psr\Log\NullLogger(),
        );
    }

    /**
     * @param array{txnId?: string, startTs?: int, pessimistic?: bool, priority?: int} $options
     */
    private function createTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: $options['txnId'] ?? 'test-txn-1',
            startTs: $options['startTs'] ?? 1000,
            pessimistic: $options['pessimistic'] ?? true,
            priority: $options['priority'] ?? 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
        );
    }

    public function testConstruction(): void
    {
        $txn = $this->createTransaction();

        $this->assertSame('test-txn-1', $txn->getTxnId());
        $this->assertSame(1000, $txn->getStartTs());
        $this->assertTrue($txn->isPessimistic());
        $this->assertSame(0, $txn->getPriority());
        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertNull($txn->getCommitTs());
    }

    public function testSetAddsToWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $this->assertSame([], $txn->getWriteSet());

        $txn->set('key1', 'value1');
        $txn->set('key2', 'value2');

        $this->assertSame([
            'key1' => 'value1',
            'key2' => 'value2',
        ], $txn->getWriteSet());
    }

    public function testDeleteAddsNullToWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $txn->set('key1', 'value1');
        $txn->delete('key2');

        $this->assertSame([
            'key1' => 'value1',
            'key2' => null,
        ], $txn->getWriteSet());
    }

    public function testRollbackOnEmptyWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
    }

    public function testSetThrowsAfterRollback(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->rollback();

        $this->expectException(\RuntimeException::class);
        $txn->set('key1', 'value1');
    }

    public function testGetReadsFromWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $txn->set('key1', 'value1');

        $result = $txn->get('key1');
        $this->assertSame('value1', $result);
    }

    public function testGetReturnsNullForDeletedKey(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $txn->delete('key1');

        $result = $txn->get('key1');
        $this->assertNull($result);
    }

    public function testOptimisticMode(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $this->assertFalse($txn->isPessimistic());
    }

    public function testPessimisticMode(): void
    {
        $txn = $this->createTransaction(['pessimistic' => true]);
        $this->assertTrue($txn->isPessimistic());
    }

    public function testRollbackWithKeysCallsBatchRollback(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $rollbackResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();

        $this->grpc->method('call')->willReturn($rollbackResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('key1', 'value1');
        $txn->set('key2', 'value2');

        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertSame([], $txn->getWriteSet());
    }

    public function testCommitEmptyWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    public function testHeartbeatReturnsLockTtl(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $heartbeatResponse = new \CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse();
        $heartbeatResponse->setLockTtl(15000);

        $this->grpc->method('call')->willReturn($heartbeatResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('key1', 'value1');

        $lockTtl = $txn->heartbeat(10000);
        $this->assertSame(15000, $lockTtl);
    }

    public function testHeartbeatThrowsOnEmptyWriteSet(): void
    {
        $txn = $this->createTransaction(['pessimistic' => true]);

        $this->expectException(\LogicException::class);
        $txn->heartbeat();
    }

    public function testHeartbeatThrowsOnCommittedTransaction(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->commit();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $txn->heartbeat();
    }

    // ========================================================================
    // scan() — limit=0 handling
    // ========================================================================

    private function makeStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        return $store;
    }

    private function makeRegion(
        int $id,
        string $startKey,
        string $endKey,
    ): RegionInfo {
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
     * @param array<string, string> $keys
     */
    private function makeScanResponse(array $keys): ScanResponse
    {
        $response = new ScanResponse();
        $pairs = [];
        foreach ($keys as $k => $v) {
            $pair = new KvPair();
            $pair->setKey($k);
            $pair->setValue($v);
            $pairs[] = $pair;
        }
        $response->setPairs($pairs);
        return $response;
    }

    public function testScanWithLimitZeroUsesDefaultScanLimit(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())->method('call')->with(
            $this->anything(),
            $this->anything(),
            $this->anything(),
            // Protobuf checkUint32 sign-extends values > 0x7FFFFFFF,
            // so 4294967295 (uint32 max) is stored as -1 on 64-bit PHP
            $this->callback(fn(ScanRequest $request): bool => ($request->getLimit() & 0xFFFFFFFF) === 4294967295),
            $this->anything(),
        )->willReturn($this->makeScanResponse(['k1' => 'v1', 'k2' => 'v2']));

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '');

        $this->assertCount(2, $result);
    }

    public function testScanWithPositiveLimitPassesLimitThrough(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())->method('call')->with(
            $this->anything(),
            $this->anything(),
            $this->anything(),
            $this->callback(fn(ScanRequest $request): bool => $request->getLimit() === 5),
            $this->anything(),
        )->willReturn($this->makeScanResponse(['k1' => 'v1']));

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '', 5);

        $this->assertCount(1, $result);
    }

    public function testScanWithLimitZeroScansAllRegions(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response1 = $this->makeScanResponse(['k1' => 'v1', 'k2' => 'v2']);
        $response2 = $this->makeScanResponse(['k3' => 'v3', 'k4' => 'v4']);

        $this->grpc->expects($this->exactly(2))->method('call')->willReturn($response1, $response2);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '');

        $this->assertCount(4, $result);
    }

    public function testScanWithPositiveLimitStopsAfterLimit(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response1 = $this->makeScanResponse(['k1' => 'v1', 'k2' => 'v2']);

        $this->grpc->expects($this->once())->method('call')->willReturn($response1);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '', 2);

        $this->assertCount(2, $result);
    }

    public function testScanWithPositiveLimitAcrossMultipleRegions(): void
    {
        $region1 = $this->makeRegion(1, '', 'k2');
        $region2 = $this->makeRegion(2, 'k2', 'k4');
        $region3 = $this->makeRegion(3, 'k4', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2, $region3]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response1 = $this->makeScanResponse(['k1' => 'v1']);
        $response2 = $this->makeScanResponse(['k2' => 'v2', 'k3' => 'v3']);

        $this->grpc->expects($this->exactly(2))->method('call')->willReturn($response1, $response2);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '', 3);

        $this->assertCount(3, $result);
    }

    public function testScanMergesWriteSetIntoResults(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response = $this->makeScanResponse([
            'k1' => 'scanned-v1',
            'k2' => 'scanned-v2',
        ]);

        $this->grpc->expects($this->once())->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'local-v1');
        $result = $txn->scan('', '');

        $this->assertCount(2, $result);
        $this->assertSame('local-v1', $result[0]['value']);
        $this->assertSame('scanned-v2', $result[1]['value']);
    }

    public function testScanWithLimitZeroAndNoResultsReturnsEmpty(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())->method('call')->willReturn($this->makeScanResponse([]));

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '');

        $this->assertSame([], $result);
    }

    // ========================================================================
    // Retry / error classification
    // ========================================================================

    public function testGetWithKeyExistsIsFatal(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('KeyExists'));

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('KeyExists');

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->get('key');
    }

    public function testGetWithWriteConflictIsFatal(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('WriteConflict'));

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('WriteConflict');

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->get('key');
    }

    public function testGetWithLockRetriesWithTxnLock(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('Lock encountered')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    public function testGetWithStaleCommandRetriesWithStaleCmd(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('StaleCommand')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    public function testGetWithDiskFullRetriesWithDiskFull(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('DiskFull')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    public function testGetWithEpochNotMatchRetriesImmediately(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    public function testGetWithGrpcExceptionRetries(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('connection reset', 14)),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    public function testGetWithRegionExceptionRetriesAsRegionMiss(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new RegionException('test', 'region miss')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('recovered', $result);
    }

    /**
     * Verify Transaction delegates unknown TiKvException to ErrorClassifier.
     * Previously Transaction didn't handle StaleCommand, meaning it would
     * throw immediately. Now ErrorClassifier returns StaleCmd for it.
     */
    public function testTransactionNowHandlesPreviouslyMissingErrors(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('ok');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('ReadIndexNotReady')),
                $response,
            );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('ok', $result);
    }

    public function testSetWithPessimisticLockThrowsDeadlockException(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $deadlock = new Deadlock();
        $deadlock->setDeadlockKeyHash(12345);
        $deadlock->setDeadlockKey('blocking-key');
        $deadlock->setLockTs(999);

        $keyError = new KeyError();
        $keyError->setDeadlock($deadlock);

        $response = new PessimisticLockResponse();
        $response->setErrors([$keyError]);

        $this->grpc->method('call')
            ->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => true]);

        $this->expectException(DeadlockException::class);
        $this->expectExceptionMessage('Deadlock detected during pessimistic lock');

        $txn->set('key', 'value');
    }

    public function testPessimisticLockConflictStillThrowsTransactionConflict(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $keyError = new KeyError();
        $keyError->setConflict(new \CrazyGoat\Proto\Kvrpcpb\WriteConflict());

        $response = new PessimisticLockResponse();
        $response->setErrors([$keyError]);

        $this->grpc->method('call')
            ->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => true]);

        $this->expectException(TransactionConflictException::class);

        $txn->set('key', 'value');
    }

    public function testDeadlockExceptionCarriesDeadlockKeyAndHash(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $deadlock = new Deadlock();
        $deadlock->setDeadlockKeyHash(42);
        $deadlock->setDeadlockKey('my-key');
        $deadlock->setLockTs(777);

        $keyError = new KeyError();
        $keyError->setDeadlock($deadlock);

        $response = new PessimisticLockResponse();
        $response->setErrors([$keyError]);

        $this->grpc->method('call')
            ->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => true]);

        try {
            $txn->set('key', 'value');
            $this->fail('Expected DeadlockException was not thrown');
        } catch (DeadlockException $e) {
            $this->assertSame('my-key', $e->getDeadlockKey());
            $this->assertSame(42, $e->getDeadlockKeyHash());
            $this->assertSame(777, $e->getLockTs());
        }
    }
}
