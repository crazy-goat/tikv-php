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
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
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
            $this->pdClient,
            1000, // matches createTransaction default startTs
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
     * Simulate the region cache being populated with the given regions
     * (TxnReader::scan() caches the scanRegions() result), so the retried
     * scan closure resolves regions through the cache like in production.
     *
     * @param RegionInfo[] $regions regions sorted by startKey
     */
    private function stubScanRegionLookup(array $regions): void
    {
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): ?RegionInfo => self::findRegionForKey($regions, $key),
        );
    }

    /**
     * @param RegionInfo[] $regions
     */
    private static function findRegionForKey(array $regions, string $key): ?RegionInfo
    {
        foreach ($regions as $region) {
            if ($region->startKey <= $key && ($region->endKey === '' || $key < $region->endKey)) {
                return $region;
            }
        }

        return null;
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
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region1, $region2]);
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
        $this->stubScanRegionLookup([$region1, $region2]);
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
        $this->stubScanRegionLookup([$region1, $region2, $region3]);
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

    public function testScanNormalizesNumericStringKeysFromTiKv(): void
    {
        // TiKV returns keys as strings, but PHP stores "12345"/"0" as int
        // array keys internally; the scan result must normalize them back to
        // strings before hasWriteSetKey() lookups and output records
        // (issue #322).
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        // TiKV returns keys as strings; build the pairs from a list so the
        // response carries the string keys "12345" and "0" (a literal array
        // key would be coerced to int and fail KvPair::setKey(string)).
        $pairs = [];
        foreach (['0', '12345'] as $key) {
            $pair = new KvPair();
            $pair->setKey($key);
            $pair->setValue('v-' . $key);
            $pairs[] = $pair;
        }
        $response = new ScanResponse();
        $response->setPairs($pairs);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction(['pessimistic' => false]);
        // '0' is also in the write set: the normalized string key must find it.
        $txn->set('0', 'local-v0');
        $result = $txn->scan('', '');

        $this->assertCount(2, $result);
        $this->assertSame('0', $result[0]['key']);
        $this->assertSame('local-v0', $result[0]['value']);
        $this->assertSame('12345', $result[1]['key']);
        $this->assertSame('v-12345', $result[1]['value']);
    }

    public function testScanWithLimitZeroAndNoResultsReturnsEmpty(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
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

    public function testScanNegativeLimitThrowsInvalidArgument(): void
    {
        $txn = $this->createTransaction(['pessimistic' => false]);

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $txn->scan('a', 'z', -1);
    }

    public function testScanLimitAppliedAfterWriteSetMerge(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region]);
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

    public function testCommitPessimisticLockBudgetExhaustedThrowsLockWaitTimeout(): void
    {
        $rawKey = 'sensitive-key-219'; // unique key so absence in the message is provable
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $lockInfo = new \CrazyGoat\Proto\Kvrpcpb\LockInfo();
        $lockInfo->setKey($rawKey);
        $lockInfo->setPrimaryLock($rawKey);
        $lockInfo->setLockVersion(1000);

        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);

        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);

        $methodSequence = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method
            ) use (
                &$methodSequence,
                $lockResponse,
            ): object {
                $methodSequence[] = $method;
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    'KvCheckTxnStatus' => new \CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse(),
                    'KvResolveLock' => new \CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true, 'maxBackoffMs' => 100]);
        $txn->set($rawKey, 'value');

        try {
            $txn->commit();
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException $e) {
            $this->assertSame($rawKey, $e->getKey());
            $this->assertSame(100, $e->getTimeoutMs());
            // Security: the message must not leak the raw key, only the redacted form.
            $this->assertStringContainsString(KeyRedactor::redact($rawKey), $e->getMessage());
            $this->assertStringNotContainsString($rawKey, $e->getMessage());
        }

        $this->assertNotContains('KvPrewrite', $methodSequence);
        $this->assertNotContains('KvCommit', $methodSequence);
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

    public function testBatchGetAcceptsNumericKeysFromArrayKeys(): void
    {
        // PHP coerces integer-like string keys ("12345", "0") to int when
        // an array is built and array_keys() then returns ints; the batch
        // read path must cast them back to strings before hasWriteSetKey()
        // and the proto setter (issue #322).
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        $batchGetResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchGetResponse();
        $pair = new KvPair();
        $pair->setKey('12345');
        $pair->setValue('remote-v1');
        $batchGetResponse->setPairs([$pair]);

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                $batchGetResponse,
                &$capturedRequest,
            ): object {
                if ($method === 'KvBatchGet') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvBatchGet' => $batchGetResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('0', 'local-v0');

        $result = $txn->batchGet(array_keys(['12345' => 'x', '0' => 'y']));

        $this->assertSame(['12345' => 'remote-v1', '0' => 'local-v0'], $result);
        // Only the non-write-set key reaches the wire, as the string "12345".
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\BatchGetRequest::class, $capturedRequest);
        $this->assertSame(['12345'], iterator_to_array($capturedRequest->getKeys()));
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

    public function testCommitWithNumericStringKeySucceeds(): void
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

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$capturedRequest,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvPrewrite') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('12345', 'value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertNotNull($capturedRequest);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PrewriteRequest::class, $capturedRequest);
        $mutations = $capturedRequest->getMutations();
        $this->assertCount(1, $mutations);
        $this->assertSame('12345', $mutations[0]->getKey());
    }

    public function testWriteSetPreservesNumericKeysAsStrings(): void
    {
        // PHP coerces integer-like string keys ("12345", "0") to int
        // array keys; the accessors must string-cast them back so callers
        // only ever see strings (issue #322).
        $state = new TransactionState();
        $state->setWrite('12345', 'v1');
        $state->setWrite('0', 'v2');

        $writeKeys = $state->getWriteKeys();
        $this->assertSame(['12345', '0'], $writeKeys);
        $this->assertSame('12345', $state->getPrimaryKey());
        $this->assertSame('v2', $state->getWriteSetValue('0'));
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

    public function testPessimisticLockSendsNumericStringKeyOnWire(): void
    {
        // Pessimistic-lock mutations must carry the numeric-string key as
        // the string "12345" on the wire (issue #322).
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

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
                $prewriteResponse,
                $commitResponse,
                &$capturedRequest,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPessimisticLock' => new PessimisticLockResponse(),
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('12345', 'v1');
        $txn->set('0', 'v2');
        $txn->commit();

        $this->assertNotNull($capturedRequest);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $capturedRequest);
        $mutations = $capturedRequest->getMutations();
        $this->assertCount(2, $mutations);
        $this->assertSame('12345', $mutations[0]->getKey());
        $this->assertSame('0', $mutations[1]->getKey());
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

    public function testRollbackWithNumericPrimaryKeySendsStringKeys(): void
    {
        // The first written key becomes the primary key; a numeric-string
        // primary must reach the KvBatchRollback wire request as the string
        // "12345", not the int 12345 (issue #322).
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$capturedRequest,
            ): object {
                if ($method === 'KvBatchRollback') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('12345', 'v1');
        $txn->set('0', 'v2');

        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest::class, $capturedRequest);
        $this->assertSame(['12345', '0'], iterator_to_array($capturedRequest->getKeys()));
    }

    // ========================================================================
    // scan() — write set interaction
    // ========================================================================

    public function testScanFiltersOutDeletedWriteSetKeys(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
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
        $this->stubScanRegionLookup([$region]);
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
    // scan() – region re-resolution inside the retried closure (issue #267)
    // ========================================================================

    public function testScanRetriesOnNotLeaderAndResolvesFreshRegion(): void
    {
        $oldRegion = $this->makeRegion(1, 'a', 'z');
        $newRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 30,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'z',
        );

        // The cache serves the old leader once; after the retry executor
        // switches the leader it serves the region with the new leader.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($oldRegion, $newRegion);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->once())->method('switchLeader')->willReturn(true);

        $this->pdClient->method('scanRegions')->willReturn([$oldRegion]);
        $this->pdClient->method('getStore')->willReturnCallback(
            function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress('127.0.0.1:2016' . $storeId);

                return $store;
            },
        );

        $leader = new \CrazyGoat\Proto\Metapb\Peer();
        $leader->setId(30);
        $leader->setStoreId(2);
        $notLeader = new \CrazyGoat\Proto\Errorpb\NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);
        $error = new \CrazyGoat\Proto\Errorpb\Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $cleanResponse = $this->makeScanResponse(['k1' => 'v1']);
        $addresses = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (string $address) use (
                &$callCount,
                &$addresses,
                $error,
                $cleanResponse,
            ): object {
                $callCount++;
                $addresses[] = $address;

                if ($callCount === 1) {
                    $response = new \CrazyGoat\Proto\Kvrpcpb\ScanResponse();
                    $response->setRegionError($error);

                    return $response;
                }

                return $cleanResponse;
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('a', 'z', 100);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame(2, $callCount);
        // The retry must target the NEW leader's store: proof that the
        // closure re-resolved the region after the leader switch.
        $this->assertSame(['127.0.0.1:20161', '127.0.0.1:20162'], $addresses);
    }

    public function testScanRetriesOnEpochNotMatchWithNarrowerRange(): void
    {
        $preSplit = $this->makeRegion(1, 'a', 'z');
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        // The cache serves the stale region for the first resolution and the
        // retry executor's invalidation lookup, then misses so the retry
        // falls back to PD for the post-split region.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $error = new \CrazyGoat\Proto\Errorpb\Error();
        $error->setMessage('epoch not match');

        $cleanResponse = $this->makeScanResponse(['k1' => 'v1']);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
                $cleanResponse,
            ): object {
                $callCount++;
                if ($request instanceof ScanRequest) {
                    $capturedRequests[] = $request;
                }

                if ($callCount === 1) {
                    $response = new \CrazyGoat\Proto\Kvrpcpb\ScanResponse();
                    $response->setRegionError($error);

                    return $response;
                }

                // The continuation over [k,z) returns no keys.
                if ($request instanceof ScanRequest && $request->getStartKey() === 'k') {
                    return new \CrazyGoat\Proto\Kvrpcpb\ScanResponse();
                }

                return $cleanResponse;
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('a', 'z', 100);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertCount(3, $capturedRequests);

        // The retry must use the post-split region and re-clip the range.
        $context = $capturedRequests[1]->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(2, $regionId);
        $this->assertSame('a', $capturedRequests[1]->getStartKey());
        $this->assertSame('k', $capturedRequests[1]->getEndKey());

        // A retry clipped to the fresh region must not silently drop the
        // remainder: the rest of the range is scanned afterwards.
        $context = $capturedRequests[2]->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('k', $capturedRequests[2]->getStartKey());
        $this->assertSame('z', $capturedRequests[2]->getEndKey());
    }

    public function testScanContinuesAfterSplitToCoverRemainder(): void
    {
        // The outer region enumeration is stale (it ran before the split):
        // the first attempt against the stale [a,z) fails with
        // EpochNotMatch, the retry resolves the post-split [a,k), and the
        // continuation must then scan [k,z) so keys of the original range
        // are not silently dropped.
        $preSplit = $this->makeRegion(1, 'a', 'z');
        $postSplitLower = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'a',
            endKey: 'k',
        );
        $postSplitUpper = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'k',
            endKey: 'z',
        );

        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($preSplit, $preSplit, null, null);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $this->pdClient->method('scanRegions')->willReturn([$preSplit]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $key === 'k' ? $postSplitUpper : $postSplitLower,
        );
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $error = new \CrazyGoat\Proto\Errorpb\Error();
        $error->setMessage('epoch not match');

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $callCount = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request
            ) use (
                &$callCount,
                &$capturedRequests,
                $error,
            ): object {
                $callCount++;
                if ($request instanceof ScanRequest) {
                    $capturedRequests[] = $request;
                }

                if ($callCount === 1) {
                    $response = new \CrazyGoat\Proto\Kvrpcpb\ScanResponse();
                    $response->setRegionError($error);

                    return $response;
                }

                $response = new \CrazyGoat\Proto\Kvrpcpb\ScanResponse();
                if ($request instanceof ScanRequest && $request->getStartKey() === 'k') {
                    // Continuation over the post-split [k,z).
                    $pair = new KvPair();
                    $pair->setKey('k3');
                    $pair->setValue('v3');
                    $response->setPairs([$pair]);
                } else {
                    // Retried scan of [a,k) after the split.
                    foreach (['k1' => 'v1', 'k2' => 'v2'] as $k => $v) {
                        $pair = new KvPair();
                        $pair->setKey($k);
                        $pair->setValue($v);
                        $response->setPairs([...$response->getPairs(), $pair]);
                    }
                }

                return $response;
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $result = $txn->scan('a', 'z', 100);

        // All three keys of the original range are returned, in order.
        $this->assertSame(['k1', 'k2', 'k3'], array_column($result, 'key'));
        $this->assertCount(3, $capturedRequests);

        $context = $capturedRequests[2]->getContext();
        $regionId = $context !== null ? $context->getRegionId() : -1;
        $this->assertSame(3, $regionId);
        $this->assertSame('k', $capturedRequests[2]->getStartKey());
        $this->assertSame('z', $capturedRequests[2]->getEndKey());
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

    /**
     * A second commit() after a failed commit phase must reuse the stored
     * commit timestamp instead of minting a new one, so a single
     * transaction is never committed at two different timestamps.
     *
     * Issue #217 / TXN-12 AC: two commit() calls result in exactly one
     * PdClient::getTimestamp() call for the commit phase.
     */
    public function testCommitRetryReusesCommitTimestampAfterSecondaryFailure(): void
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

        // Mint a fresh ts per call so a second mint would be detectable.
        $timestampCalls = 0;
        $this->pdClient->method('getTimestamp')->willReturnCallback(function () use (&$timestampCalls): int {
            $timestampCalls++;
            return 9000 + $timestampCalls;
        });

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        // First commit of the secondary region fails with a commit-phase
        // key error; the retry commit succeeds.
        $firstSecondaryCommit = true;
        $commitVersions = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$firstSecondaryCommit,
                &$commitVersions,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvCommit' && $request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                    $context = $request->getContext();
                    $regionId = $context !== null ? $context->getRegionId() : -1;
                    $commitVersions[] = $request->getCommitVersion();
                    if ($regionId === 2 && $firstSecondaryCommit) {
                        $firstSecondaryCommit = false;
                        $errorResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();
                        $keyError = new KeyError();
                        $keyError->setRetryable('simulated commit failure');
                        $errorResponse->setError($keyError);
                        return $errorResponse;
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

        try {
            $txn->commit();
            $this->fail('First commit should fail on the secondary region');
        } catch (TransactionConflictException) {
            // Expected: secondary commit failed after the primary committed.
        }

        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertSame(9001, $txn->getCommitTs());

        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(9001, $txn->getCommitTs());
        // Exactly one PD timestamp for the whole transaction: the retried
        // commit() must not mint a new one.
        $this->assertSame(1, $timestampCalls);
        // Every commit request across both attempts used the same ts.
        $this->assertNotEmpty($commitVersions);
        foreach ($commitVersions as $commitTs) {
            $this->assertSame(9001, $commitTs);
        }
    }

    public function testPessimisticRollbackUsesMaxForUpdateTsNotStartTs(): void
    {
        $capturedRollbackForUpdateTs = null;
        $lockCallCount = 0;

        // Set up grpc mock first so Transaction captures it.
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                ?\Google\Protobuf\Internal\Message $request = null,
            ) use (
                &$capturedRollbackForUpdateTs,
                &$lockCallCount,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $lockCallCount++;
                    return new PessimisticLockResponse();
                }
                if (
                    $method === 'KVPessimisticRollback'
                    && $request instanceof \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackRequest
                ) {
                    $capturedRollbackForUpdateTs = $request->getForUpdateTs();
                    return new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();
                }
                if ($method === 'KvPrewrite') {
                    // Simulate a prewrite failure: return a lock conflict that cannot be resolved
                    // so the commit throws and triggers rollback via __destruct.
                    $response = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
                    $keyError = new \CrazyGoat\Proto\Kvrpcpb\KeyError();
                    $keyError->setRetryable('simulated prewrite failure');
                    $response->setErrors([$keyError]);
                    return $response;
                }
                if ($method === 'KvBatchRollback') {
                    return new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
                }
                return match ($method) {
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        // Return a fresh timestamp (5000) that differs from startTs (1000).
        $this->pdClient->method('getTimestamp')->willReturn(5000);

        // Recreate resolver and lock resolver with the new grpc mock.
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            1000, // matches the transaction startTs below
        );

        $txn = $this->createTransaction([
            'pessimistic' => true,
            'startTs' => 1000,
        ]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');

        // Commit will: (1) acquire pessimistic locks (sets maxForUpdateTs=5000),
        // (2) fail during prewrite, (3) propagate TransactionConflictException.
        try {
            $txn->commit();
        } catch (\CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException) {
            // Expected: prewrite failure.
        }

        // Explicitly trigger rollback — this is what __destruct would do.
        // After a failed commit the state is still Active, so rollback proceeds.
        $txn->rollback();

        $this->assertGreaterThanOrEqual(1, $lockCallCount, 'Pessimistic locks should have been acquired');
        $this->assertNotNull($capturedRollbackForUpdateTs, 'PessimisticRollbackRequest was not captured');
        // The forUpdateTs must be the fresh PD timestamp (5000), not startTs (1000).
        $this->assertSame(5000, $capturedRollbackForUpdateTs);
    }
}
