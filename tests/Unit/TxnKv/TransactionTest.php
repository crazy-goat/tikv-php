<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\TestCase;

class TransactionTest extends TestCase
{
    private PdClientInterface $pdClient;
    private GrpcClientInterface $grpc;
    private RegionCacheInterface $regionCache;
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

        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->pdClient,
            $this->regionCache,
        );
    }

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
}
