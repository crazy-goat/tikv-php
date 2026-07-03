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
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
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
     * @param array{txnId?: string, startTs?: int, pessimistic?: bool, priority?: int, maxBackoffMs?: int} $options
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
            maxBackoffMs: $options['maxBackoffMs'] ?? 20000,
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

        $this->expectException(InvalidStateException::class);
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

        $this->expectException(InvalidStateException::class);
        $txn->heartbeat();
    }

    public function testHeartbeatThrowsOnCommittedTransaction(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->commit();

        $this->expectException(InvalidStateException::class);
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

    public function testScanWithLimitZeroUsesMaxScanLimit(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $this->grpc->expects($this->once())->method('call')->with(
            $this->anything(),
            $this->anything(),
            $this->anything(),
            // Limit=0 is normalized to MAX_SCAN_LIMIT (10240)
            $this->callback(fn(ScanRequest $request): bool => $request->getLimit() === 10240),
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
        $this->pdClient->method('getRegion')->willReturn($region);

        $scanResponse = $this->makeScanResponse([
            'k1' => 'scanned-v1',
            'k2' => 'scanned-v2',
        ]);

        $rollbackResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();

        $this->grpc->method('call')
            ->willReturnCallback(
                fn(string $addr, string $svc, string $method): object => match ($method) {
                    'KvScan' => $scanResponse,
                    'KvBatchRollback' => $rollbackResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                }
            );

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

    public function testScanLimitExceedingMaxThrowsInvalidArgument(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit (99999) exceeds maximum allowed scan limit of 10240');

        $txn->scan('', '', 99999);
    }

    public function testScanLimitAppliedAfterWriteSetMerge(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // TiKV returns 5 keys, but limit is 3
        $response = $this->makeScanResponse([
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
            'k4' => 'v4',
            'k5' => 'v5',
        ]);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('', '', 3);

        $this->assertCount(3, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame('k2', $result[1]['key']);
        $this->assertSame('k3', $result[2]['key']);
    }

    public function testScanIncludesInRangeWriteSetKeysNotReturnedByTiKv(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // TiKV returns k1, k3. Write set has k2 (in range) which should appear.
        $response = $this->makeScanResponse([
            'k1' => 'scanned-v1',
            'k3' => 'scanned-v3',
        ]);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'local-v1');
        $txn->set('k2', 'local-v2');
        $result = $txn->scan('', '', 10);

        $this->assertCount(3, $result);
        $this->assertSame('local-v1', $result[0]['value']);
        $this->assertSame('local-v2', $result[1]['value']);
        $this->assertSame('scanned-v3', $result[2]['value']);
    }

    public function testScanExcludesWriteSetKeysOutsideRange(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response = $this->makeScanResponse(['k1' => 'v1']);
        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('z-outside', 'should-not-appear');
        $result = $txn->scan('a', 'z');

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
    }

    public function testScanWithDeleteReducesCountBelowLimit(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // TiKV returns 3 keys, but one is deleted in write set
        $response = $this->makeScanResponse([
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
        ]);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->delete('k2');
        $result = $txn->scan('', '', 3);

        $this->assertCount(2, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame('k3', $result[1]['key']);
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
                $this->throwException(new TxnRetryableException(
                    'Lock encountered',
                    BackoffType::TxnLock,
                )),
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

    public function testCommitPessimisticLockThrowsDeadlockException(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

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
        $txn->set('key', 'value');

        $this->expectException(DeadlockException::class);
        $this->expectExceptionMessage('Deadlock detected during pessimistic lock');

        $txn->commit();
    }

    public function testCommitPessimisticLockConflictStillThrowsTransactionConflict(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        $keyError = new KeyError();
        $keyError->setConflict(new \CrazyGoat\Proto\Kvrpcpb\WriteConflict());

        $response = new PessimisticLockResponse();
        $response->setErrors([$keyError]);

        $this->grpc->method('call')
            ->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('key', 'value');

        $this->expectException(TransactionConflictException::class);

        $txn->commit();
    }

    public function testCommitPessimisticLockDeadlockExceptionCarriesKeyAndHash(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

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
        $txn->set('key', 'value');

        try {
            $txn->commit();
            $this->fail('Expected DeadlockException was not thrown');
        } catch (DeadlockException $e) {
            $this->assertSame('my-key', $e->getDeadlockKey());
            $this->assertSame(42, $e->getDeadlockKeyHash());
            $this->assertSame(777, $e->getLockTs());
        }
    }

    // ========================================================================
    // batchGet()
    // ========================================================================

    public function testBatchGetReturnsAllWriteSetValuesWithoutGrpc(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');

        $this->grpc->expects($this->never())->method('call');

        $result = $txn->batchGet(['k1', 'k2']);

        $this->assertSame(['k1' => 'v1', 'k2' => 'v2'], $result);
    }

    public function testBatchGetMergesWriteSetAndRemoteInOrder(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        $batchGetResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchGetResponse();
        $pair = new KvPair();
        $pair->setKey('k2');
        $pair->setValue('remote-v2');
        $batchGetResponse->setPairs([$pair]);

        $this->grpc->method('call')->willReturn($batchGetResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'local-v1');

        $result = $txn->batchGet(['k1', 'k2', 'k3']);

        $this->assertSame('local-v1', $result['k1']);
        $this->assertSame('remote-v2', $result['k2']);
        $this->assertNull($result['k3']);
    }

    public function testBatchGetEmptyInputReturnsEmpty(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);
        $this->assertSame([], $txn->batchGet([]));
    }

    // ========================================================================
    // get() — response error handling
    // ========================================================================

    public function testGetWithNotFoundReturnsNull(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(true);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('missing-key');

        $this->assertNull($result);
    }

    public function testGetWithLockKeyInResponseResolvesAndRetries(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $checkTxnStatusResponse = new \CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse();
        $checkTxnStatusResponse->setCommitVersion(0);
        $checkTxnStatusResponse->setLockTtl(0);

        $resolveLockResponse = new \CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse();

        $firstResponse = new GetResponse();
        $lockInfo = new \CrazyGoat\Proto\Kvrpcpb\LockInfo();
        $lockInfo->setKey('key');
        $lockInfo->setLockVersion(999);
        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);
        $firstResponse->setError($keyError);

        $secondResponse = new GetResponse();
        $secondResponse->setNotFound(false);
        $secondResponse->setValue('resolved-value');

        $callCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method
            ) use (
                &$callCount,
                $firstResponse,
                $secondResponse,
                $checkTxnStatusResponse,
                $resolveLockResponse,
            ): object {
                $callCount++;
                return match ($method) {
                    'KvGet' => $callCount === 1 ? $firstResponse : $secondResponse,
                    'KvCheckTxnStatus' => $checkTxnStatusResponse,
                    'KvResolveLock' => $resolveLockResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('key');

        $this->assertSame('resolved-value', $result);
    }

    public function testGetWithRetryableThrowsTransactionConflict(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $keyError = new KeyError();
        $keyError->setRetryable('optimistic lock not found');
        $response->setError($keyError);

        $this->grpc->method('call')->willReturn($response);

        $this->expectException(TransactionConflictException::class);
        $this->expectExceptionMessage('optimistic lock not found');

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->get('key');
    }

    // ========================================================================
    // commit()
    // ========================================================================

    public function testCommitOptimisticWithKeys(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $methodSequence = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method
            ) use (
                &$methodSequence,
                $prewriteResponse,
                $commitResponse,
            ): object {
                $methodSequence[] = $method;
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2000, $txn->getCommitTs());
        $this->assertSame(['KvPrewrite', 'KvCommit'], $methodSequence);
    }

    public function testCommitPessimisticWithKeys(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $this->grpc->method('call')
            ->willReturnCallback(fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => $commitResponse,
                'KvPessimisticLock' => new PessimisticLockResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(3000, $txn->getCommitTs());
    }

    // ========================================================================
    // rollback()
    // ========================================================================

    public function testRollbackWithPessimisticKeysCallsPessimisticRollbackAndBatchRollback(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        $methodSequence = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method
            ) use (
                &$methodSequence,
            ): object {
                $methodSequence[] = $method;
                return match ($method) {
                    'KvPessimisticLock' => new PessimisticLockResponse(),
                    'KVPessimisticRollback' => new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse(),
                    'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');

        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertSame([], $txn->getWriteSet());
        $this->assertContains('KVPessimisticRollback', $methodSequence);
        $this->assertContains('KvBatchRollback', $methodSequence);
    }

    // ========================================================================
    // scan() — write set interaction
    // ========================================================================

    public function testScanFiltersOutDeletedWriteSetKeys(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response = $this->makeScanResponse([
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
        ]);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->delete('k2');
        $result = $txn->scan('', '');

        $this->assertCount(2, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame('k3', $result[1]['key']);
    }

    public function testScanDeletedKeyNotInScannedResultsDoesNotAffectOutput(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $response = $this->makeScanResponse(['k1' => 'v1']);
        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->delete('missing-key');
        $result = $txn->scan('', '');

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
    }

    // ========================================================================
    // Retry — budget exhaustion
    // ========================================================================

    public function testExecuteWithRetryExhaustsBudgetAndThrowsOriginalException(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $originalException = new TiKvException('StaleCommand');

        $this->grpc->method('call')
            ->willThrowException($originalException);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('StaleCommand');

        $txn = $this->createTransaction(['pessimistic' => false, 'maxBackoffMs' => 1]);
        $txn->get('key');
    }

    // ========================================================================
    // get() — fills readSet
    // ========================================================================

    public function testGetPopulatesReadSet(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('some-value');

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->get('my-key');

        $this->assertArrayHasKey('my-key', $txn->getReadSet());
        $this->assertSame('some-value', $txn->getReadSet()['my-key']);
    }

    public function testGetPopulatesReadSetWithNullOnNotFound(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(true);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->get('missing');

        $this->assertNull($result);
        $this->assertArrayHasKey('missing', $txn->getReadSet());
        $this->assertNull($txn->getReadSet()['missing']);
    }

    // ========================================================================
    // Pessimistic lock batching
    // ========================================================================

    public function testPessimisticLockBatchesMultipleKeysInSameRegion(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $this->grpc->method('call')
            ->willReturnCallback(fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPessimisticLock' => $lockResponse,
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => $commitResponse,
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->set('k3', 'v3');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    public function testPessimisticLockSendsSingleRpcPerRegion(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $this->grpc->method('call')
            ->willReturnCallback(fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPessimisticLock' => $lockResponse,
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => $commitResponse,
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->set('k4', 'v4');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    public function testPessimisticLockSetsIsFirstLockTrueOnFirstBatchOnly(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getTimestamp')->willReturn(4000);

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $capturedRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                $lockResponse,
                $prewriteResponse,
                $commitResponse,
                &$capturedRequests,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $capturedRequests[] = $request;
                }
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->set('k4', 'v4');
        $txn->commit();

        $this->assertCount(2, $capturedRequests);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $capturedRequests[0]);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $capturedRequests[1]);
        $this->assertTrue($capturedRequests[0]->getIsFirstLock());
        $this->assertFalse($capturedRequests[1]->getIsFirstLock());
    }

    public function testPessimisticLockBatchSendsAllMutationsInSingleRequest(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(5000);

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                $lockResponse,
                $prewriteResponse,
                $commitResponse,
                &$capturedRequest,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->set('k3', 'v3');
        $txn->commit();

        $this->assertNotNull($capturedRequest);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $capturedRequest);
        $this->assertCount(3, $capturedRequest->getMutations());
    }

    public function testPessimisticLockDeduplicatesKeys(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(6000);

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request
            ) use (
                $lockResponse,
                $prewriteResponse,
                $commitResponse,
                &$capturedRequest,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->delete('k1');
        $txn->set('k1', 'v2');
        $txn->commit();

        $this->assertNotNull($capturedRequest);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $capturedRequest);
        $this->assertCount(1, $capturedRequest->getMutations());
    }

    // ========================================================================
    // 2PC commit ordering and retry (issue #76)
    // ========================================================================

    /**
     * Primary key's region must be committed before any secondary region.
     * TiKV rejects secondary commits that arrive before the primary's
     * commit, leaving the transaction half-committed.  See issue #76.
     */
    public function testCommitPrimaryRegionCommittedBeforeSecondaries(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getTimestamp')->willReturn(5000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        // Capture commit keys per region in invocation order.
        $commitsByRegion = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$commitsByRegion,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvCommit' && $request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                    $context = $request->getContext();
                    $regionId = $context !== null ? $context->getRegionId() : -1;
                    $commitsByRegion[] = ['regionId' => $regionId, 'keys' => iterator_to_array($request->getKeys())];
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1'); // primary, in region1
        $txn->set('k2', 'v2'); // primary region1
        $txn->set('k4', 'v4'); // secondary, in region2
        $txn->commit();

        // Primary region (1) must commit first; secondary region (2) after.
        $this->assertCount(2, $commitsByRegion);
        $this->assertSame(1, $commitsByRegion[0]['regionId']);
        $this->assertSame(2, $commitsByRegion[1]['regionId']);
        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    /**
     * Transient gRPC errors on a SECONDARY commit must be retried by the
     * retry executor (commits are idempotent).  Issue #76: previously a
     * transient secondary error threw after the primary commit, leaving
     * the txn half-committed with no retry.
     */
    public function testCommitSecondaryRegionTransientErrorIsRetriedNotFatal(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getTimestamp')->willReturn(6000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();
        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('transient not leader');

        // Fail the FIRST commit on region 2, then succeed.
        $region2CallCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$region2CallCount,
                $regionError,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvCommit' && $request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                    $context = $request->getContext();
                    $regionId = $context !== null ? $context->getRegionId() : -1;
                    if ($regionId === 2) {
                        $region2CallCount++;
                        if ($region2CallCount === 1) {
                            $resp = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();
                            $resp->setRegionError($regionError);
                            return $resp;
                        }
                    }
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->set('k4', 'v4');
        $txn->commit();

        // The secondary region was retried and eventually committed.
        $this->assertGreaterThanOrEqual(2, $region2CallCount);
        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    /**
     * Status must be set to Committed if commit returns successfully.
     * Even if secondary commits later fail, the transaction itself is
     * irrevocably committed in TiKV after primary succeeds (commits are
     * idempotent; the retry executor will retry on transient errors).
     *
     * Issue #76 AC: "status set Committed after primary success".
     */
    public function testCommitMarksStatusCommittedOnSuccessfulCommit(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');

        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo => $key < 'k3' ? $region1 : $region2,
        );
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getTimestamp')->willReturn(7000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $this->grpc->method('call')
            ->willReturnCallback(fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => $commitResponse,
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->set('k4', 'v4');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    /**
     * Bad region errors on the PRIMARY commit must NOT be retried (we
     * could lose the commitTs). The error must propagate up.
     */
    public function testCommitPrimaryRegionErrorIsNotRetriedAndPropagates(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(8000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('region not found');

        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();
        $commitResponse->setRegionError($regionError);

        $commitCallCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$commitCallCount,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvCommit') {
                    $commitCallCount++;
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected RegionException to propagate from primary commit');
        } catch (RegionException) {
            // Expected: error does NOT retry, error propagates.
        }

        // The primary commit must have been attempted exactly once — no retry
        // because re-trying would invalidate the commitTs captured at the
        // start of the commit phase.
        $this->assertSame(1, $commitCallCount);
        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
    }
}
