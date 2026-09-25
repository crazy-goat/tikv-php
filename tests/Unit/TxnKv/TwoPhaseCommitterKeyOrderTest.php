<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\TestCase;

/**
 * Byte-order regression test for TwoPhaseCommitter::regionCoversAllKeys()
 * (issue #186).
 *
 * The guard answers "does this region still own the whole key group?", and
 * the group is discarded into a re-group (or a pessimistic-lock regroup)
 * when it says no. It used PHP's relational operators, so for numeric-string
 * keys it answered with the numeric order: "199" is bytewise INSIDE
 * ["", "20") but 199 >= 20 numerically, so every rollback of such a group
 * claimed the region had shrunk, re-grouped, and failed after the regroup
 * cap. Bytewise "1" < "10" < "15" < "199" < "2" < "20".
 */
final class TwoPhaseCommitterKeyOrderTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;
    private RegionInfo $region;

    protected function setUp(): void
    {
        // A region that bytewise contains "1", "10", "15", "199" and "2".
        $this->region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '20',
        );

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->region);
        $this->pdClient->method('scanRegions')->willReturn([$this->region]);
        $this->regionCache->method('put');
        // A real cache resolves a key inside the half-open region range
        // bytewise, never numerically (issue #186).
        $this->regionCache->method('getByKey')->willReturnCallback(
            fn(string $key): ?RegionInfo => KeyOrder::inRange($key, $this->region->startKey, $this->region->endKey)
                ? $this->region
                : null,
        );
    }

    private function createTransaction(): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-key-order',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
        );
    }

    public function testRollbackOfNumericKeysInsideTheRegionIsNotRegrouped(): void
    {
        /** @var list<BatchRollbackRequest> $requests */
        $requests = [];
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method, mixed $request) use (&$requests): object {
                $this->assertSame('KvBatchRollback', $method);
                $this->assertInstanceOf(BatchRollbackRequest::class, $request);
                $requests[] = $request;

                return new BatchRollbackResponse();
            },
        );

        $txn = $this->createTransaction();
        $txn->set('199', 'v');
        $txn->set('15', 'v');

        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertCount(1, $requests, 'the covered key group must be rolled back once');
        // The repeated field is not a PHP array and PHP coerces its
        // numeric-string values to int keys once flattened (issue #322), so
        // read them back as strings.
        // The wire order is the grouping order (write-set insertion order),
        // not a re-sort: BatchRollback is key-order agnostic.
        $this->assertSame(
            ['199', '15'],
            array_map(strval(...), iterator_to_array($requests[0]->getKeys())),
        );
    }
}
