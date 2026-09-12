<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\Op;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
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
use CrazyGoat\TiKV\Client\TxnKv\Exception\UndeterminedCommitException;
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

    public function testCommitOfDeleteSendsDeleteMutation(): void
    {
        // Issue #328: a delete in the write set must map to Op::Del with an
        // empty value in the actual PrewriteRequest — not an Op::Put with an
        // empty value, which would write '' instead of a tombstone.
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

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
                if ($method === 'KvPrewrite') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('keep', 'v');
        $txn->delete('gone');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertInstanceOf(PrewriteRequest::class, $capturedRequest);
        $byKey = [];
        foreach ($capturedRequest->getMutations() as $m) {
            $byKey[(string) $m->getKey()] = $m;
        }

        $this->assertSame(Op::Put, $byKey['keep']->getOp());
        $this->assertSame('v', $byKey['keep']->getValue());
        $this->assertSame(Op::Del, $byKey['gone']->getOp());
        $this->assertSame('', $byKey['gone']->getValue());
    }

    public function testCommitOfDeleteOnlyTransactionSendsOnlyDeleteMutations(): void
    {
        // Issue #328: a transaction containing only deletes must send only
        // Op::Del mutations on prewrite.
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

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
                if ($method === 'KvPrewrite') {
                    $capturedRequest = $request;
                }
                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->delete('gone1');
        $txn->delete('gone2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertInstanceOf(PrewriteRequest::class, $capturedRequest);
        $mutations = $capturedRequest->getMutations();
        $this->assertCount(2, $mutations);
        foreach ($mutations as $m) {
            $this->assertSame(Op::Del, $m->getOp());
            $this->assertSame('', $m->getValue());
        }
        $this->assertSame(['gone1', 'gone2'], array_map(
            static fn(\CrazyGoat\Proto\Kvrpcpb\Mutation $m): string => (string) $m->getKey(),
            iterator_to_array($mutations),
        ));
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
        // Without this the batch grouper previously dropped every key
        // silently and rollback "succeeded" without any RPC (issue #244).
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

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
     * @param array<string|int, string> $keys
     */
    private function makeScanResponse(array $keys): ScanResponse
    {
        $response = new ScanResponse();
        $pairs = [];
        foreach ($keys as $k => $v) {
            $pair = new KvPair();
            $pair->setKey((string) $k);
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

    public function testScanMergesWriteSetInByteOrderNotNumericOrder(): void
    {
        // TiKV returns pairs in byte order: '10' < '9' < 'b' (bytewise).
        // The write set adds '9'. PHP's default sort() compares numeric
        // strings numerically ('9' < '10'), which would return '9' before
        // '10' and break pagination on the last returned key (issue #331).
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $scanResponse = $this->makeScanResponse([
            '10' => 'a',
            'b' => 'c',
        ]);
        $this->grpc->method('call')->willReturn($scanResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('9', 'z');
        $result = $txn->scan('', '', 0);

        $this->assertSame(
            ['10', '9', 'b'],
            array_column($result, 'key'),
            'merged results must be in byte order, matching what TiKV returns',
        );
        // assertSame() against string literals also proves the keys are
        // string-typed: an int key (PHP numeric-key coercion) would fail
        // the identical comparison.
    }

    public function testScanRangeFilterUsesByteOrderForWriteSetKeys(): void
    {
        // Write set has '9'; scan range is ['10', 'b'). Bytewise '9' >= '10'
        // and '9' < 'b', so '9' is INSIDE the range and must appear after
        // '10' (byte order). PHP's loose comparison treats '9' < '10'
        // numerically and would drop it (issue #331).
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $scanResponse = $this->makeScanResponse(['10' => 'a']);
        $this->grpc->method('call')->willReturn($scanResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('9', 'z');
        $result = $txn->scan('10', 'b', 0);

        $this->assertSame(['10', '9'], array_column($result, 'key'));
    }

    public function testScanRangeFilterExcludesNumericKeyOutOfRangeBytewise(): void
    {
        // Write set has '19'; scan range is ['2', '3'). Numerically '19' is
        // inside [2, 3), but bytewise '19' < '2', so it is OUT of range and
        // must not appear (issue #331).
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $scanResponse = $this->makeScanResponse(['2' => 'a']);
        $this->grpc->method('call')->willReturn($scanResponse);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('19', 'z');
        $result = $txn->scan('2', '3', 0);

        $this->assertSame(['2'], array_column($result, 'key'));
    }

    public function testScanMergeWithBinaryKeys(): void
    {
        // Write-set keys "\x00a" (byte 0x00) and "\xffz" (byte 0xFF) sort
        // before and after the scanned 'b' bytewise; the range filter
        // ['\x00a', 'b') must exclude "\xffz" (issue #331).
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->stubScanRegionLookup([$region]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $scanResponse = $this->makeScanResponse(['b' => 'c']);
        $emptyResponse = $this->makeScanResponse([]);
        // The mock has no range filter; the second scan (endKey "\x00b")
        // gets an empty TiKV batch so only the in-range write-set key shows.
        $respond = static fn (ScanRequest $request): object => $request->getEndKey() === "\x00b"
            ? $emptyResponse
            : $scanResponse;
        $this->grpc->method('call')->willReturnCallback(
            static fn (string $addr, string $svc, string $method, ScanRequest $request): object => $respond($request),
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set("\x00a", 'low');
        $txn->set("\xffz", 'high');
        $result = $txn->scan('', '', 0);

        $this->assertSame(["\x00a", 'b', "\xffz"], array_column($result, 'key'));
        $this->assertSame(['low', 'c', 'high'], array_column($result, 'value'));

        $limited = $txn->scan('', "\x00b", 0);
        $this->assertSame(
            ["\x00a"],
            array_column($limited, 'key'),
            '"\xffz" is outside the range in byte order and must be filtered',
        );
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

    /**
     * The retry budget must reset per execute() (issue #243): Transaction
     * memoizes ONE RetryExecutor for its whole lifetime, so with a shared
     * accumulator the first operation that exhausts maxBackoffMs would leave
     * every later operation (including commit) unable to retry at all.
     * maxBackoffMs=4 with StaleCmd backoff 2,4 → 2+4=6 > 4 exhausts a budget
     * after exactly 2 attempts. Each get() must therefore be able to make its
     * own 2 attempts: 4 gRPC calls in total, and both gets recover.
     */
    public function testRetryBudgetResetsBetweenOperationsOnSameTransaction(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('recovered');

        $calls = 0;
        $this->grpc->method('call')->willReturnCallback(function () use (&$calls, $response): GetResponse {
            $calls++;
            if ($calls % 3 !== 0) {
                // 1st and 2nd call of each get() fail; the 3rd succeeds.
                throw new TiKvException('StaleCommand');
            }
            return $response;
        });

        $txn = $this->createTransaction(['pessimistic' => false, 'maxBackoffMs' => 6]);

        $this->assertSame('recovered', $txn->get('key1'));
        $this->assertSame(3, $calls);

        // Second operation on the SAME transaction: must get its own fresh
        // budget (2 StaleCommand retries), not the exhausted one from get() #1.
        $this->assertSame('recovered', $txn->get('key2'));
        $this->assertSame(6, $calls);
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

    /**
     * Issue #454 (TXN-25): a KvPessimisticLock response carrying a KeyError
     * variant that is neither deadlock, locked nor conflict (here
     * `commit_ts_expired`) used to fall through the error loop with no
     * exception, so the batch was treated as fully locked and the
     * transaction proceeded to prewrite keys it did not hold a lock on.
     * It must fail closed with a typed exception and never issue a
     * KvPrewrite or KvCommit.
     */
    public function testCommitPessimisticLockUnrecognisedVariantThrowsAndSkipsPrewrite(): void
    {
        $keyError = new KeyError();
        $keyError->setCommitTsExpired(new \CrazyGoat\Proto\Kvrpcpb\CommitTsExpired());

        $methodSequence = [];
        $this->stubPessimisticLockError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TiKvException was not thrown');
        } catch (TiKvException $e) {
            $this->assertSame('Pessimistic lock failed: CommitTsExpired', $e->getMessage());
        }

        $this->assertSame(['KvPessimisticLock'], $methodSequence);
    }

    /**
     * Issue #454 (TXN-25): `retryable` is a named variant that previously
     * fell through the pessimistic-lock error loop as success. It must map to
     * a typed TransactionConflictException and abort before prewrite, matching
     * `handlePrewriteErrors()` (a `retryable` there is also a conflict, not a
     * retry).
     */
    public function testCommitPessimisticLockRetryableThrowsAndSkipsPrewrite(): void
    {
        $keyError = new KeyError();
        $keyError->setRetryable('optimistic lock not found');

        $methodSequence = [];
        $this->stubPessimisticLockError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertSame('Pessimistic lock failed: retryable: optimistic lock not found', $e->getMessage());
        }

        $this->assertSame(['KvPessimisticLock'], $methodSequence);
    }

    /**
     * Issue #454 (TXN-25): `abort` is the other named string variant a
     * KvPessimisticLock KeyError can carry. It previously fell through as
     * success and must now abort with a typed exception before prewrite.
     */
    public function testCommitPessimisticLockAbortThrowsAndSkipsPrewrite(): void
    {
        $keyError = new KeyError();
        $keyError->setAbort('transaction aborted');

        $methodSequence = [];
        $this->stubPessimisticLockError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertSame('Pessimistic lock failed: abort: transaction aborted', $e->getMessage());
        }

        $this->assertSame(['KvPessimisticLock'], $methodSequence);
    }

    /**
     * Issue #454 (TXN-25): when a KvPessimisticLock response carries several
     * KeyErrors the first recognised variant wins — the loop throws and never
     * inspects the rest, so the transaction cannot proceed to prewrite.
     */
    public function testCommitPessimisticLockMultipleErrorsThrowsFirstVariant(): void
    {
        $first = new KeyError();
        $first->setAbort('first variant must win');
        $second = new KeyError();
        $second->setCommitTsExpired(new \CrazyGoat\Proto\Kvrpcpb\CommitTsExpired());

        $methodSequence = [];
        $this->stubPessimisticLockError($methodSequence, $first, $second);

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertSame('Pessimistic lock failed: abort: first variant must win', $e->getMessage());
        }

        $this->assertSame(['KvPessimisticLock'], $methodSequence);
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

    /**
     * Issue #420 (GAP-06): pessimisticLockBatch() must acquire exactly ONE
     * for_update_ts per locking pass, not one per region. Lock two keys
     * living in two different regions and assert both PessimisticLock RPCs
     * carry the same for_update_ts, and that only one extra getTimestamp()
     * call is spent on the lock pass (the second mints the commit ts).
     */
    public function testCommitPessimisticLockTwoRegionsSharesForUpdateTsPerPass(): void
    {
        $region1 = $this->makeRegion(1, '', 'k2'); // holds the primary 'k1'
        $region2 = $this->makeRegion(2, 'k2', ''); // holds 'k2'
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->stubScanRegionLookup([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $tsCalls = 0;
        $this->pdClient->method('getTimestamp')
            ->willReturnCallback(static function () use (&$tsCalls): int {
                ++$tsCalls;
                return 3000 * $tsCalls; // lock pass: 3000, commit ts: 6000
            });

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $forUpdateTsList = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                \CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest
                |\CrazyGoat\Proto\Kvrpcpb\PrewriteRequest
                |\CrazyGoat\Proto\Kvrpcpb\CommitRequest $request,
            ) use (
                &$forUpdateTsList,
                $lockResponse,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvPessimisticLock' && $request instanceof PessimisticLockRequest) {
                    $forUpdateTsList[] = $request->getForUpdateTs();
                    return $lockResponse;
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(6000, $txn->getCommitTs());
        // One TSO call for the whole locking pass + one for the commit ts.
        $this->assertSame(2, $tsCalls);
        $this->assertCount(2, $forUpdateTsList, 'both regions were locked');
        $this->assertSame([3000, 3000], $forUpdateTsList, 'one for_update_ts shared by the locking pass');
    }

    /**
     * Issue #292 (PERF-05): a pessimistic commit spanning five regions must
     * make exactly two PdClient::getTimestamp() calls — one for_update_ts
     * covering the whole locking pass and one commit ts. Before the TSO was
     * hoisted out of the per-region loop this was six or more.
     *
     * The spy counts at the PdClientInterface boundary, so it also pins the
     * caller contract independently of the TimestampOracle pool.
     */
    public function testCommitPessimisticLockFiveRegionsMakesExactlyTwoTsoCalls(): void
    {
        $regions = [
            $this->makeRegion(1, '', 'k2'),
            $this->makeRegion(2, 'k2', 'k3'),
            $this->makeRegion(3, 'k3', 'k4'),
            $this->makeRegion(4, 'k4', 'k5'),
            $this->makeRegion(5, 'k5', ''),
        ];
        $this->pdClient->method('scanRegions')->willReturn($regions);
        $this->stubScanRegionLookup($regions);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());

        $tsCalls = 0;
        $this->pdClient->method('getTimestamp')
            ->willReturnCallback(static function () use (&$tsCalls): int {
                ++$tsCalls;
                return 3000 * $tsCalls; // lock pass: 3000, commit ts: 6000
            });

        $lockResponse = new PessimisticLockResponse();
        $prewriteResponse = new PrewriteResponse();
        $commitResponse = new CommitResponse();

        $forUpdateTsList = [];
        $lockRpcCount = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                \CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest
                |\CrazyGoat\Proto\Kvrpcpb\PrewriteRequest
                |\CrazyGoat\Proto\Kvrpcpb\CommitRequest $request,
            ) use (
                &$forUpdateTsList,
                &$lockRpcCount,
                $lockResponse,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvPessimisticLock' && $request instanceof PessimisticLockRequest) {
                    $lockRpcCount++;
                    $forUpdateTsList[] = $request->getForUpdateTs();
                    return $lockResponse;
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->set('k3', 'v3');
        $txn->set('k4', 'v4');
        $txn->set('k5', 'v5');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2, $tsCalls, 'one for_update_ts + one commit ts for five regions');
        $this->assertSame(5, $lockRpcCount, 'each region is locked exactly once');
        $this->assertSame([3000, 3000, 3000, 3000, 3000], $forUpdateTsList);
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

    /**
     * Stub the region/PD dependencies and make KvPrewrite answer with a
     * response containing the given failing KeyErrors, in order. KvCommit is
     * stubbed so a test can prove it is never reached via the recorded method
     * sequence.
     *
     * @param list<string> $methodSequence Collected RPC method names, by ref.
     */
    private function stubPrewriteError(array &$methodSequence, KeyError ...$keyErrors): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $prewriteResponse = new PrewriteResponse();
        $prewriteResponse->setErrors($keyErrors);

        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$methodSequence,
                $prewriteResponse,
            ): object {
                $methodSequence[] = $method;
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });
    }

    /**
     * Stub the region/PD dependencies and make KvPessimisticLock answer with
     * a response containing the given failing KeyErrors, in order. Every
     * other RPC method (KvPrewrite, KvCommit) throws, so a test proves the
     * transaction never proceeds past the failed lock via the recorded
     * method sequence.
     *
     * @param list<string> $methodSequence Collected RPC method names, by ref.
     */
    private function stubPessimisticLockError(array &$methodSequence, KeyError ...$keyErrors): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors($keyErrors);

        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$methodSequence,
                $lockResponse,
            ): object {
                $methodSequence[] = $method;
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });
    }

    /**
     * Issue #214 (TXN-09): a prewrite KeyError carrying only `deadlock` used
     * to fall through handlePrewriteErrors() and the transaction proceeded
     * to KvCommit. It must raise DeadlockException carrying the deadlock
     * key/hash instead, and no KvCommit may be sent.
     */
    public function testCommitPrewriteDeadlockThrowsAndSkipsCommit(): void
    {
        $deadlock = new Deadlock();
        $deadlock->setDeadlockKeyHash(12345);
        $deadlock->setDeadlockKey('blocking-key');
        $deadlock->setLockTs(999);

        $keyError = new KeyError();
        $keyError->setDeadlock($deadlock);

        $methodSequence = [];
        $this->stubPrewriteError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected DeadlockException was not thrown');
        } catch (DeadlockException $e) {
            $this->assertSame('blocking-key', $e->getDeadlockKey());
            $this->assertSame(12345, $e->getDeadlockKeyHash());
            $this->assertSame(999, $e->getLockTs());
            $this->assertSame('Deadlock detected during prewrite', $e->getMessage());
        }

        $this->assertSame(['KvPrewrite'], $methodSequence);
    }

    /**
     * Issue #214 (TXN-09): when a prewrite response carries several KeyErrors
     * the first one wins — `handlePrewriteErrors()` throws on the first
     * variant it recognises and never inspects the rest.
     */
    public function testCommitPrewriteMultipleErrorsThrowsFirstVariantAndSkipsCommit(): void
    {
        $deadlock = new Deadlock();
        $deadlock->setDeadlockKey('first-key');
        $deadlock->setDeadlockKeyHash(7);
        $deadlock->setLockTs(11);

        $first = new KeyError();
        $first->setDeadlock($deadlock);

        // A later, equally fatal variant that must not be the one reported.
        $second = new KeyError();
        $second->setAbort('second variant must not win');

        $methodSequence = [];
        $this->stubPrewriteError($methodSequence, $first, $second);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected DeadlockException was not thrown');
        } catch (DeadlockException $e) {
            $this->assertSame('first-key', $e->getDeadlockKey());
            $this->assertSame(7, $e->getDeadlockKeyHash());
            $this->assertSame(11, $e->getLockTs());
        }

        $this->assertSame(['KvPrewrite'], $methodSequence);
    }

    /**
     * Issue #214 (TXN-09): an explicitly named prewrite variant that is not
     * one of the kill-switch fields (here `already_exist`) must map to a
     * typed TransactionConflictException, not fall through.
     */
    public function testCommitPrewriteAlreadyExistThrowsTransactionConflictAndSkipsCommit(): void
    {
        $keyError = new KeyError();
        $keyError->setAlreadyExist(new \CrazyGoat\Proto\Kvrpcpb\AlreadyExist());

        $methodSequence = [];
        $this->stubPrewriteError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertSame('Prewrite failed: key already exists', $e->getMessage());
        }

        $this->assertSame(['KvPrewrite'], $methodSequence);
    }

    /**
     * Issue #214 (TXN-09): any KeyError variant not explicitly mapped (here
     * `commit_ts_expired`) must fail closed with the base TiKvException and
     * must never be treated as a successful prewrite.
     */
    public function testCommitPrewriteUnrecognisedVariantThrowsAndSkipsCommit(): void
    {
        $keyError = new KeyError();
        $keyError->setCommitTsExpired(new \CrazyGoat\Proto\Kvrpcpb\CommitTsExpired());

        $methodSequence = [];
        $this->stubPrewriteError($methodSequence, $keyError);

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected TiKvException was not thrown');
        } catch (TiKvException $e) {
            $this->assertSame('Prewrite failed: CommitTsExpired', $e->getMessage());
        }

        $this->assertSame(['KvPrewrite'], $methodSequence);
    }

    /**
     * Issue #213 (TXN-08): prewrite must run inside the RetryExecutor, so a
     * NotLeader region error on the first attempt is retried against the
     * re-resolved region instead of aborting the whole transaction. The
     * retry must target the NEW leader's store, proving the closure
     * re-resolved the region after the executor's leader switch.
     */
    public function testCommitPrewriteRetriesOnNotLeaderAndResolvesFreshRegion(): void
    {
        $oldRegion = $this->makeRegion(1, '', '');
        $newRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 30,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );

        // The cache serves the old leader for the first resolution; after
        // RetryExecutor::handleNotLeader() switches the leader it serves the
        // region with the new leader.
        $this->regionCache->method('getByKey')->willReturnOnConsecutiveCalls($oldRegion, $newRegion);
        $this->regionCache->method('put');
        $this->regionCache->expects($this->once())->method('switchLeader')->willReturn(true);
        $this->pdClient->method('scanRegions')->willReturn([$oldRegion]);
        $this->pdClient->method('getRegion')->willReturn($newRegion);
        $this->pdClient->method('getStore')->willReturnCallback(
            function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress('127.0.0.1:2016' . $storeId);
                return $store;
            },
        );
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $leader = new \CrazyGoat\Proto\Metapb\Peer();
        $leader->setId(30);
        $leader->setStoreId(2);
        $notLeader = new \CrazyGoat\Proto\Errorpb\NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);
        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $prewriteAddresses = [];
        $prewriteCalls = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $svc,
                string $method,
            ) use (
                &$prewriteAddresses,
                &$prewriteCalls,
                $regionError,
            ): object {
                if ($method === 'KvPrewrite') {
                    $prewriteAddresses[] = $address;
                    $prewriteCalls++;
                    if ($prewriteCalls === 1) {
                        $response = new PrewriteResponse();
                        $response->setRegionError($regionError);
                        return $response;
                    }
                    return new PrewriteResponse();
                }
                return match ($method) {
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2, $prewriteCalls);
        $this->assertSame(['127.0.0.1:20161', '127.0.0.1:20162'], $prewriteAddresses);
    }

    /**
     * Issue #213 (TXN-08): a `KeyError.locked` on prewrite is resolved and
     * the prewrite is retried inside the RetryExecutor instead of the
     * `TxnRetryableException` escaping to the caller.
     */
    public function testCommitPrewriteRetriesAfterResolvedLockConflict(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $locked = new LockInfo();
        $locked->setKey('k1');
        $locked->setPrimaryLock('k1');
        $locked->setLockVersion(999);
        $keyError = new KeyError();
        $keyError->setLocked($locked);

        // The blocking transaction already committed at 5000, so resolveLock
        // finalizes the lock with KvResolveLock and the conflict clears.
        $status = new CheckTxnStatusResponse();
        $status->setCommitVersion(5000);

        $methods = [];
        $prewriteCalls = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $svc,
                string $method,
            ) use (
                &$methods,
                &$prewriteCalls,
                $keyError,
                $status,
            ): object {
                $methods[] = $method;
                if ($method === 'KvPrewrite') {
                    $prewriteCalls++;
                    if ($prewriteCalls === 1) {
                        $response = new PrewriteResponse();
                        $response->setErrors([$keyError]);
                        return $response;
                    }
                    return new PrewriteResponse();
                }
                return match ($method) {
                    'KvCheckTxnStatus' => $status,
                    'KvResolveLock' => new ResolveLockResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2, $prewriteCalls);
        $this->assertSame(
            ['KvPrewrite', 'KvCheckTxnStatus', 'KvResolveLock', 'KvPrewrite', 'KvCommit'],
            $methods,
        );
    }

    public function testSecondaryCommitFailureDoesNotFailCommittedTransaction(): void
    {
        // Issue #215 (TXN-10): once the primary region is committed the
        // transaction is durable in TiKV. A fatal error while committing a
        // secondary region must be logged and swallowed — commit() must not
        // throw, the status must be Committed, and no rollback may be issued.
        $region1 = $this->makeRegion(1, '', 'k2'); // holds the primary 'k1'
        $region2 = $this->makeRegion(2, 'k2', ''); // holds the secondary 'k2'
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->stubScanRegionLookup([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();
        $commitResponse = new \CrazyGoat\Proto\Kvrpcpb\CommitResponse();

        $methodSequence = [];
        $commitCalls = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$methodSequence,
                &$commitCalls,
                $prewriteResponse,
                $commitResponse,
            ): object {
                $methodSequence[] = $method;
                if ($method === 'KvCommit') {
                    $commitCalls++;
                    if ($commitCalls > 1) {
                        // Fatal, non-retryable failure on the secondary region.
                        throw new TiKvException('KeyExists');
                    }
                }
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1'); // primary — region 1
        $txn->set('k3', 'v3'); // secondary — region 2

        $txn->commit(); // must not throw

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertSame(2000, $txn->getCommitTs());
        $this->assertSame(
            ['KvPrewrite', 'KvPrewrite', 'KvCommit', 'KvCommit'],
            $methodSequence,
        );
        // No rollback of the committed transaction may be attempted — not
        // even via __destruct().
        $this->assertNotContains('KvBatchRollback', $methodSequence);
    }

    public function testRollbackAfterCommitThrows(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $this->grpc->method('call')->willReturnCallback(
            fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPrewrite' => new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse(),
                'KvCommit' => new \CrazyGoat\Proto\Kvrpcpb\CommitResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            },
        );

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $txn->rollback();
    }

    public function testCommitterRollbackRejectsCommittedState(): void
    {
        // The Transaction::rollback() path is blocked by ensureActive()
        // before the committer is reached; the committer-level guard is
        // defense-in-depth for direct TwoPhaseCommitter users. Exercise it
        // directly on a Committed state (issue #215).
        $state = new TransactionState();
        $state->setStatus(TransactionStatus::Committed);

        $committer = new \CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter(
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            regionResolver: $this->regionResolver,
            lockResolver: $this->lockResolver,
            timeoutConfig: new \CrazyGoat\TiKV\Client\Grpc\TimeoutConfig(),
            maxBackoffMs: 20000,
        );

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('commit phase');
        $retryExecutor = new \CrazyGoat\TiKV\Client\Retry\RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 60000,
            regionCache: $this->regionCache,
            grpc: $this->grpc,
            regionResolver: $this->regionResolver,
            logger: new \Psr\Log\NullLogger(),
        );
        $committer->rollback($state, $retryExecutor, static fn (): ?BackoffType => null);
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
        // Without this the batch grouper previously dropped every key
        // silently and the commit "succeeded" without any RPC (issue #244).
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
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

    public function testTransportFailureOnPrimaryCommitIsUndetermined(): void
    {
        // Issue #216: a transport-level failure on the primary KvCommit RPC
        // leaves the outcome unknown — the commit may already be applied. The
        // transaction must be closed (no destructor rollback) and the caller
        // must receive UndeterminedCommitException.
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('getTimestamp')->willReturn(3000);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);

        /** @var list<string> $rpcMethods */
        $rpcMethods = [];
        $prewriteResponse = new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse();

        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$rpcMethods,
                $prewriteResponse
): object {
                $rpcMethods[] = $method;
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => throw new GrpcException('UNAVAILABLE: connection reset', 14),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected UndeterminedCommitException');
        } catch (UndeterminedCommitException $e) {
            $this->assertInstanceOf(GrpcException::class, $e->getPrevious());
        }
        $this->assertSame(TransactionStatus::Undetermined, $txn->getStatus());
        $this->assertNotContains('KvBatchRollback', $rpcMethods);

        // The state is closed: no further rollback can be attempted.
        $this->expectException(InvalidStateException::class);
        $txn->rollback();
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
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
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
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
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

        // The secondary region commit fails with a commit-phase key error.
        // Issue #215 (TXN-10): this must NOT fail commit() — the primary is
        // already committed, so the transaction is durable. The single
        // timestamp minted for the primary commit must still be the commit
        // version on the wire for every region.
        $secondaryCommitFailed = false;
        $commitVersions = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                mixed $request,
            ) use (
                &$secondaryCommitFailed,
                &$commitVersions,
                $prewriteResponse,
                $commitResponse,
            ): object {
                if ($method === 'KvCommit' && $request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                    $context = $request->getContext();
                    $regionId = $context !== null ? $context->getRegionId() : -1;
                    $commitVersions[$regionId] = $request->getCommitVersion();
                    if ($regionId === 2 && !$secondaryCommitFailed) {
                        $secondaryCommitFailed = true;
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

        // The secondary commit failure is logged and swallowed (issue #215).
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        // Exactly one PD timestamp for the whole transaction: the failed
        // secondary region did not trigger a re-mint.
        $this->assertSame(1, $timestampCalls);
        $this->assertSame(9001, $txn->getCommitTs());
        // The primary used the same ts (the secondary request that failed is
        // recorded here too, proving no retry re-minted the timestamp).
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

    // ========================================================================
    // Pessimistic lock: the resolveLock TTL wait is charged to $elapsedMs (#470)
    // ========================================================================

    public function testPessimisticLockChargesLockWaitToBudget(): void
    {
        // maxBackoffMs = 100: the first locked response triggers resolveLock,
        // whose TTL wait must be charged to $elapsedMs. With the charge the
        // loop exits via the budget guard after ~one resolve; without it
        // (old behaviour) the wait was invisible to the budget.
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $rawKey = 'budget-key-470';
        $lockInfo = new LockInfo();
        $lockInfo->setKey($rawKey);
        $lockInfo->setPrimaryLock($rawKey);
        $lockInfo->setLockVersion(1000);

        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);

        $lockResponse = new PessimisticLockResponse();
        $lockResponse->setErrors([$keyError]);

        $stillActiveStatus = new \CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse();
        $stillActiveStatus->setCommitVersion(0);
        // A 20 s TTL equals the default maxBackoffMs cap: without the #470
        // cap-by-deadline this resolve slept the FULL ttl uncharged.
        $stillActiveStatus->setLockTtl(20000);
        $stillActiveStatus->setAction(\CrazyGoat\Proto\Kvrpcpb\Action::NoAction);

        $resolveLockResponse = new \CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse();

        $lockAttempts = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method
            ) use (
                &$lockAttempts,
                $lockResponse,
                $stillActiveStatus,
                $resolveLockResponse,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $lockAttempts++;
                }
                return match ($method) {
                    'KvPessimisticLock' => $lockResponse,
                    // Active lock with a 500 ms TTL: resolveLock waits (capped
                    // by the remaining 100 ms budget passed by the committer)
                    // before resolving as rolled back.
                    'KvCheckTxnStatus' => $stillActiveStatus,
                    'KvResolveLock' => $resolveLockResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true, 'maxBackoffMs' => 100]);
        $txn->set($rawKey, 'value');

        $startMs = microtime(true) * 1000;
        try {
            $txn->commit();
            $this->fail('Expected LockWaitTimeoutException was not thrown');
        } catch (LockWaitTimeoutException $e) {
            $this->assertSame(100, $e->getTimeoutMs());
            $this->assertStringNotContainsString($rawKey, $e->getMessage());
        }
        $elapsedMs = (microtime(true) * 1000) - $startMs;

        // The 20 s TTL wait must be capped by the remaining budget (<= 100 ms)
        // and charged to it; the whole commit fails in well under a second.
        // Without #470 the uncharged sleep alone took ~20 s per encounter.
        $this->assertLessThan(2500, $elapsedMs, 'Lock wait must be capped + charged, not silently stretch the budget');

        // The charged wait ends the loop quickly: at most one extra lock
        // attempt after the first resolve. Without the #470 cap+charge, this
        // single encounter slept ~20 s uncharged before the loop exited via
        // the while condition — caught above by the timing assertion.
        $this->assertLessThanOrEqual(2, $lockAttempts);
    }

    public function testPessimisticLockRetriesEpochNotMatchWithReResolvedRegion(): void
    {
        // Issue #500: a RegionException (EpochNotMatch) from the pessimistic
        // lock RPC used to escape pessimisticLockBatch() uncaught and abort
        // the whole transaction. It must instead invalidate the stale region
        // (RegionErrorHandler::check) and retry with a fresh resolution.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $freshRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: '',
            peers: [],
        );

        // First attempt uses the region from groupStringsByRegion(); the
        // retry re-resolves with a cache miss (null) and PD returns the
        // fresh region.
        $this->regionCache->method('getByKey')
            ->willReturnOnConsecutiveCalls(null, null, null, null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($freshRegion);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new PessimisticLockResponse();
        $staleResponse->setRegionError($regionError);
        $okResponse = new PessimisticLockResponse();

        $lockRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$lockRequests,
                $staleResponse,
                $okResponse,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $lockRequests[] = $request;
                    return count($lockRequests) === 1 ? $staleResponse : $okResponse;
                }
                return match ($method) {
                    'KvPrewrite' => new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse(),
                    'KvCommit' => new \CrazyGoat\Proto\Kvrpcpb\CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->commit();

        // The transaction must commit — the region error was retried, not fatal.
        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertCount(2, $lockRequests);

        // The retried request must carry the FRESH epoch (version 5), not the
        // stale one captured before the loop (the #267 stale-capture class).
        $retriedRequest = $lockRequests[1];
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $retriedRequest);
        $retriedContext = $retriedRequest->getContext();
        $this->assertNotNull($retriedContext);
        $this->assertSame(1, $retriedContext->getRegionId());
        $this->assertNotNull($retriedContext->getRegionEpoch());
        $this->assertSame(5, $retriedContext->getRegionEpoch()->getVersion());
    }

    public function testPessimisticLockRetriesNotLeaderWithReResolvedRegion(): void
    {
        // Issue #500: a NotLeader-carrying region error must invalidate the
        // stale leader and retry against the re-resolved region instead of
        // aborting the transaction.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $freshRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );

        // First attempt uses the region from groupStringsByRegion(); the
        // retry re-resolves with a cache miss (null) and PD returns the
        // fresh region.
        $this->regionCache->method('getByKey')
            ->willReturnOnConsecutiveCalls(null, null, null, null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($freshRegion);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $notLeader = new \CrazyGoat\Proto\Errorpb\NotLeader();
        $notLeader->setRegionId(1);
        $leader = new \CrazyGoat\Proto\Metapb\Peer();
        $leader->setStoreId(2);
        $notLeader->setLeader($leader);
        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $staleResponse = new PessimisticLockResponse();
        $staleResponse->setRegionError($regionError);
        $okResponse = new PessimisticLockResponse();

        $invalidatedRegionIds = [];
        $this->regionCache->method('invalidate')
            ->willReturnCallback(static function (int $regionId) use (&$invalidatedRegionIds): void {
                $invalidatedRegionIds[] = $regionId;
            });

        $lockRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$lockRequests,
                $staleResponse,
                $okResponse,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $lockRequests[] = $request;
                    return count($lockRequests) === 1 ? $staleResponse : $okResponse;
                }
                return match ($method) {
                    'KvPrewrite' => new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse(),
                    'KvCommit' => new \CrazyGoat\Proto\Kvrpcpb\CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertCount(2, $lockRequests);

        // check() (no retry executor owns this site) must have invalidated
        // the NotLeader-carrying region before the retry.
        $this->assertContains(1, $invalidatedRegionIds);

        // The retried request must target the hinted NEW leader (store 2).
        $retriedRequest = $lockRequests[1];
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $retriedRequest);
        $retriedContext = $retriedRequest->getContext();
        $this->assertNotNull($retriedContext);
        $this->assertNotNull($retriedContext->getPeer());
        $this->assertSame(2, $retriedContext->getPeer()->getStoreId());
    }

    public function testPessimisticLockThrowsRegionExceptionWhenRegionErrorsExhaustBudget(): void
    {
        // Issue #500: when region errors persist until the retry budget runs
        // out, the transaction fails with the RegionException itself — not
        // LockWaitTimeoutException (no lock conflict was ever reported).
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );

        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new PessimisticLockResponse();
        $staleResponse->setRegionError($regionError);

        $this->grpc->method('call')
            ->willReturnCallback(static fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvPessimisticLock' => $staleResponse,
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => true, 'maxBackoffMs' => 100]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected RegionException was not thrown');
        } catch (RegionException $e) {
            $this->assertStringContainsString('EpochNotMatch', $e->getMessage());
        }
    }

    public function testPessimisticLockRegroupsKeysAfterRegionSplit(): void
    {
        // Issue #503: if the region splits while the pessimistic lock is
        // being retried, the re-resolved region no longer covers the whole
        // key group. Before the fix the server kept returning region errors
        // until the retry budget was exhausted and the transaction failed
        // with the last RegionException. The group must instead be
        // re-grouped against the fresh region layout and locked per-region.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        // After the split: region 1 covers ['', 'k2'), region 2 ['k2', '']
        $splitRegion1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'k2',
            peers: [],
        );
        $splitRegion2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: 'k2',
            endKey: '',
            peers: [],
        );

        // Always miss the cache: every re-resolve goes to PD.
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            static fn(string $key): RegionInfo => $key >= 'k2' ? $splitRegion2 : $splitRegion1,
        );
        // First scanRegions (initial grouping): pre-split region covering
        // both keys. Second scanRegions (re-group after the split): the
        // two split regions.
        $this->pdClient->method('scanRegions')
            ->willReturnOnConsecutiveCalls(
                [$staleRegion],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
            );
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new PessimisticLockResponse();
        $staleResponse->setRegionError($regionError);
        $okResponse = new PessimisticLockResponse();

        $lockRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$lockRequests,
                $staleResponse,
                $okResponse,
            ): object {
                if ($method === 'KvPessimisticLock') {
                    $lockRequests[] = $request;
                    return count($lockRequests) === 1 ? $staleResponse : $okResponse;
                }
                return match ($method) {
                    'KvPrewrite' => new \CrazyGoat\Proto\Kvrpcpb\PrewriteResponse(),
                    'KvCommit' => new \CrazyGoat\Proto\Kvrpcpb\CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        // 3 lock RPCs: the failed pre-split attempt (both keys in region 1),
        // then one per split region after the re-group.
        $this->assertCount(3, $lockRequests);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $lockRequests[0]);
        $this->assertCount(2, $lockRequests[0]->getMutations());

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $lockRequests[1]);
        $this->assertSame(1, $lockRequests[1]->getContext()?->getRegionId());
        $this->assertCount(1, $lockRequests[1]->getMutations());
        $this->assertSame('k1', $lockRequests[1]->getMutations()[0]->getKey());

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest::class, $lockRequests[2]);
        $this->assertSame(2, $lockRequests[2]->getContext()?->getRegionId());
        $this->assertCount(1, $lockRequests[2]->getMutations());
        $this->assertSame('k2', $lockRequests[2]->getMutations()[0]->getKey());
    }

    public function testPessimisticLockGivesUpAfterTooManyRegroups(): void
    {
        // Issue #503 review: each re-group restores a fresh retry budget;
        // without a cap a pathological repeated split could retry forever.
        // A PD that always answers with a region that does not cover the
        // key group must exhaust the re-group cap and fail the transaction.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        // Inconsistent answer: claims to cover ''.. 'a', which does not
        // contain 'k1' — every re-resolve fails the coverage check.
        $neverCovering = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'a',
            peers: [],
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($neverCovering);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new PessimisticLockResponse();
        $staleResponse->setRegionError($regionError);

        $lockAttempts = 0;
        $this->grpc->method('call')
            ->willReturnCallback(static function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$lockAttempts,
                $staleResponse
): object {
                if ($method === 'KvPessimisticLock') {
                    $lockAttempts++;
                    return $staleResponse;
                }
                throw new \RuntimeException("Unexpected method: $method");
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('Expected RegionException was not thrown');
        } catch (RegionException $e) {
            $this->assertStringContainsString('EpochNotMatch', $e->getMessage());
        }

        // 1 initial attempt + 2 attempts per re-group cycle, capped at 10
        // re-groups — not an unbounded retry loop.
        $this->assertLessThanOrEqual(25, $lockAttempts);
    }

    public function testBatchRollbackRetriesEpochNotMatchWithReResolvedRegion(): void
    {
        // Issue #502: a RegionException (EpochNotMatch) from KvBatchRollback
        // must not burn the whole attempt cap against the stale region
        // captured by groupStringsByRegion() — the retry closure has to
        // re-resolve (the #267/#500 stale-capture class).
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $freshRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: '',
            peers: [],
        );

        // Consecutive getByKey calls: (1) closure attempt 1 region
        // resolution, (2) RetryExecutor invalidation lookup, (3) closure
        // attempt 2 resolution — null on the third models the cache
        // invalidation, so PD serves the fresh region.
        $this->regionCache->method('getByKey')
            ->willReturnOnConsecutiveCalls($staleRegion, $staleRegion, null, $freshRegion, $freshRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($freshRegion);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
        $staleResponse->setRegionError($regionError);
        $okResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();

        $rollbackRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$rollbackRequests,
                $staleResponse,
                $okResponse,
            ): object {
                if ($method === 'KvBatchRollback') {
                    $rollbackRequests[] = $request;
                    return count($rollbackRequests) === 1 ? $staleResponse : $okResponse;
                }
                throw new \RuntimeException("Unexpected method: $method");
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->rollback();

        // The rollback must succeed — the region error was retried, not fatal.
        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertCount(2, $rollbackRequests);

        // The retried request must carry the FRESH epoch (version 5), not the
        // stale one captured before the retry loop.
        $retriedRequest = $rollbackRequests[1];
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest::class, $retriedRequest);
        $retriedContext = $retriedRequest->getContext();
        $this->assertNotNull($retriedContext);
        $this->assertSame(1, $retriedContext->getRegionId());
        $this->assertNotNull($retriedContext->getRegionEpoch());
        $this->assertSame(5, $retriedContext->getRegionEpoch()->getVersion());
    }

    public function testPessimisticRollbackRetriesEpochNotMatchWithReResolvedRegion(): void
    {
        // Issue #502: a RegionException (EpochNotMatch) from
        // KVPessimisticRollback must not burn the whole attempt cap against
        // the stale region captured by groupStringsByRegion() — the retry
        // closure has to re-resolve (the #267/#500 stale-capture class).
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $freshRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: '',
            peers: [],
        );

        // Consecutive getByKey calls: pessimisticRollbackAll (1) attempt-1
        // resolution, (2) RetryExecutor invalidation lookup, (3) attempt-2
        // resolution (null → PD), then batchRollback resolutions.
        $this->regionCache->method('getByKey')
            ->willReturnOnConsecutiveCalls($staleRegion, $staleRegion, null, $freshRegion, $freshRegion);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($freshRegion);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $regionError = new \CrazyGoat\Proto\Errorpb\Error();
        $regionError->setMessage('EpochNotMatch');
        $regionError->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $staleResponse = new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();
        $staleResponse->setRegionError($regionError);
        $okResponse = new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();

        $rollbackRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$rollbackRequests,
                $staleResponse,
                $okResponse,
            ): object {
                if ($method === 'KVPessimisticRollback') {
                    $rollbackRequests[] = $request;
                    return count($rollbackRequests) === 1 ? $staleResponse : $okResponse;
                }
                return match ($method) {
                    'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->rollback();

        // The rollback must succeed — the region error was retried, not fatal.
        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertCount(2, $rollbackRequests);

        // The retried request must carry the FRESH epoch (version 5), not the
        // stale one captured before the retry loop.
        $retriedRequest = $rollbackRequests[1];
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackRequest::class, $retriedRequest);
        $retriedContext = $retriedRequest->getContext();
        $this->assertNotNull($retriedContext);
        $this->assertSame(1, $retriedContext->getRegionId());
        $this->assertNotNull($retriedContext->getRegionEpoch());
        $this->assertSame(5, $retriedContext->getRegionEpoch()->getVersion());
    }

    public function testBatchRollbackRegroupsKeysAfterRegionSplit(): void
    {
        // Issue #505: after a region split the re-resolved region no longer
        // covers the whole key group; before the fix the server kept
        // returning region errors until the retry budget was exhausted. The
        // group must instead be re-grouped against the fresh region layout
        // and rolled back per-region.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        // After the split: region 1 covers ['', 'k2'), region 2 ['k2', '']
        $splitRegion1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'k2',
            peers: [],
        );
        $splitRegion2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: 'k2',
            endKey: '',
            peers: [],
        );

        // Always miss the cache: every resolve goes to PD.
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            static fn(string $key): RegionInfo => $key >= 'k2' ? $splitRegion2 : $splitRegion1,
        );
        // First scanRegions (initial grouping): pre-split region covering
        // both keys. Second scanRegions (re-group after the split): the two
        // split regions.
        $this->pdClient->method('scanRegions')
            ->willReturnOnConsecutiveCalls(
                [$staleRegion],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
            );
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $rollbackRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$rollbackRequests,
            ): object {
                if ($method === 'KvBatchRollback') {
                    $rollbackRequests[] = $request;
                    return new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        // 2 rollback RPCs: one per split region after the re-group.
        $this->assertCount(2, $rollbackRequests);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest::class, $rollbackRequests[0]);
        $this->assertSame(1, $rollbackRequests[0]->getContext()?->getRegionId());
        $this->assertCount(1, $rollbackRequests[0]->getKeys());
        $this->assertSame('k1', $rollbackRequests[0]->getKeys()[0]);

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest::class, $rollbackRequests[1]);
        $this->assertSame(2, $rollbackRequests[1]->getContext()?->getRegionId());
        $this->assertCount(1, $rollbackRequests[1]->getKeys());
        $this->assertSame('k2', $rollbackRequests[1]->getKeys()[0]);
    }

    public function testBatchRollbackGivesUpAfterTooManyRegroups(): void
    {
        // Issue #505 review: each re-group restores a fresh retry budget;
        // without a cap a pathological repeated split could retry forever.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        // Inconsistent answer: claims to cover ''..'a', which does not
        // contain 'k1' — every resolve fails the coverage check.
        $neverCovering = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'a',
            peers: [],
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($neverCovering);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $this->grpc->method('call')
            ->willReturnCallback(static fn(): object => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse());

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('k1', 'v1');

        try {
            $txn->rollback();
            $this->fail('Expected RegionException was not thrown');
        } catch (\CrazyGoat\TiKV\Client\Exception\RegionException $e) {
            $this->assertStringContainsString(
                'Region split repeatedly invalidated the rollback group',
                $e->getMessage(),
            );
        }
    }

    public function testPessimisticRollbackRegroupsKeysAfterRegionSplit(): void
    {
        // Issue #505: the same split-regroup recovery as batchRollback(),
        // exercised through pessimisticRollbackAll().
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $splitRegion1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'k2',
            peers: [],
        );
        $splitRegion2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: 'k2',
            endKey: '',
            peers: [],
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturnCallback(
            static fn(string $key): RegionInfo => $key >= 'k2' ? $splitRegion2 : $splitRegion1,
        );
        $this->pdClient->method('scanRegions')
            ->willReturnOnConsecutiveCalls(
                [$staleRegion],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
                [$splitRegion1, $splitRegion2],
            );
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $rollbackRequests = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
                object $request,
            ) use (
                &$rollbackRequests,
            ): object {
                if ($method === 'KVPessimisticRollback') {
                    $rollbackRequests[] = $request;
                    return new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();
                }
                return match ($method) {
                    'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');
        $txn->set('k2', 'v2');
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        // 2 pessimistic rollback RPCs: one per split region after the
        // re-group.
        $this->assertCount(2, $rollbackRequests);
        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackRequest::class, $rollbackRequests[0]);
        $this->assertSame(1, $rollbackRequests[0]->getContext()?->getRegionId());
        $this->assertCount(1, $rollbackRequests[0]->getKeys());
        $this->assertSame('k1', $rollbackRequests[0]->getKeys()[0]);

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackRequest::class, $rollbackRequests[1]);
        $this->assertSame(2, $rollbackRequests[1]->getContext()?->getRegionId());
        $this->assertCount(1, $rollbackRequests[1]->getKeys());
        $this->assertSame('k2', $rollbackRequests[1]->getKeys()[0]);
    }

    public function testPessimisticRollbackGivesUpAfterTooManyRegroups(): void
    {
        // Issue #505: the regroup cap applies to pessimisticRollbackAll()
        // too.
        $staleRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
        $neverCovering = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 5,
            startKey: '',
            endKey: 'a',
            peers: [],
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($neverCovering);
        $this->pdClient->method('scanRegions')->willReturn([$staleRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);

        $this->grpc->method('call')
            ->willReturnCallback(static fn(string $addr, string $svc, string $method): object => match ($method) {
                'KVPessimisticRollback' => new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse(),
                'KvBatchRollback' => new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('k1', 'v1');

        try {
            $txn->rollback();
            $this->fail('Expected RegionException was not thrown');
        } catch (\CrazyGoat\TiKV\Client\Exception\RegionException $e) {
            $this->assertStringContainsString(
                'Region split repeatedly invalidated the rollback group',
                $e->getMessage(),
            );
        }
    }

    // ========================================================================
    // handleRollbackError() — key errors returned inside rollback responses
    // (issue #333, TEST-13)
    // ========================================================================

    private function setUpRollbackRegionMocking(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->makeStore());
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->pdClient->method('getTimestamp')->willReturn(3000);
    }

    public function testRollbackEncounteringLockResolvesItAndRetries(): void
    {
        $this->setUpRollbackRegionMocking();

        $lockInfo = new LockInfo();
        $lockInfo->setKey('key1');
        $lockInfo->setLockVersion(999);

        $lockedResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
        $keyError = new KeyError();
        $keyError->setLocked($lockInfo);
        $lockedResponse->setError($keyError);

        $checkTxnStatusResponse = new \CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse();
        // commitVersion=0 means the locking transaction was not committed:
        // the resolver must roll its lock back via KvResolveLock.
        $checkTxnStatusResponse->setCommitVersion(0);
        $checkTxnStatusResponse->setLockTtl(0);

        $resolveLockResponse = new \CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse();

        $methods = [];
        $batchRollbackCalls = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$methods,
                &$batchRollbackCalls,
                $lockedResponse,
                $checkTxnStatusResponse,
                $resolveLockResponse,
            ): object {
                $methods[] = $method;
                return match ($method) {
                    'KvBatchRollback' => $batchRollbackCalls++ === 0
                        ? $lockedResponse
                        : new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse(),
                    'KvCheckTxnStatus' => $checkTxnStatusResponse,
                    'KvResolveLock' => $resolveLockResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('key1', 'value1');

        // The Locked branch resolves the blocking lock and throws a
        // TxnRetryableException so the retry executor re-runs the rollback —
        // the second KvBatchRollback succeeds and rollback() returns cleanly.
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertSame(2, $batchRollbackCalls);
        // The lock was resolved between the two rollback attempts: rollback,
        // then CheckTxnStatus + ResolveLock, then the successful retry.
        $this->assertSame(
            ['KvBatchRollback', 'KvCheckTxnStatus', 'KvCheckTxnStatus', 'KvResolveLock', 'KvBatchRollback'],
            $methods,
        );
    }

    public function testRollbackRetryableErrorIsRetried(): void
    {
        $this->setUpRollbackRegionMocking();

        $errorResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
        $keyError = new KeyError();
        $keyError->setRetryable('simulated rollback failure');
        $errorResponse->setError($keyError);

        $batchRollbackCalls = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$batchRollbackCalls,
                $errorResponse
): object {
                if ($method === 'KvBatchRollback') {
                    return $batchRollbackCalls++ === 0
                        ? $errorResponse
                        : new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('key1', 'value1');

        // The Retryable branch throws TxnRetryableException, which the retry
        // executor backs off on and then re-runs the rollback.
        $txn->rollback();
        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertSame(2, $batchRollbackCalls);
    }

    public function testRollbackAbortErrorThrowsTransactionConflict(): void
    {
        $this->setUpRollbackRegionMocking();

        $errorResponse = new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
        $keyError = new KeyError();
        $keyError->setAbort('simulated abort');
        $errorResponse->setError($keyError);

        $this->grpc->method('call')
            ->willReturnCallback(static fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvBatchRollback' => $errorResponse,
                default => throw new \RuntimeException("Unexpected method: $method"),
            });

        $txn = $this->createTransaction(['pessimistic' => false]);
        $txn->set('key1', 'value1');

        try {
            $txn->rollback();
            $this->fail('Expected TransactionConflictException was not thrown');
        } catch (TransactionConflictException $e) {
            $this->assertStringContainsString('Abort during rollback', $e->getMessage());
        }
    }

    public function testPessimisticRollbackErrorsAreHandled(): void
    {
        $this->setUpRollbackRegionMocking();

        $errorResponse = new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();
        $keyError = new KeyError();
        $keyError->setRetryable('simulated pessimistic rollback failure');
        $errorResponse->setErrors([$keyError]);

        $pessimisticRollbackCalls = 0;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $addr,
                string $svc,
                string $method,
            ) use (
                &$pessimisticRollbackCalls,
                $errorResponse
): object {
                if ($method === 'KVPessimisticRollback') {
                    return $pessimisticRollbackCalls++ === 0
                        ? $errorResponse
                        : new \CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse();
                }
                if ($method === 'KvBatchRollback') {
                    return new \CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse();
                }
                throw new \RuntimeException("Unexpected method: $method");
            });

        $txn = $this->createTransaction(['pessimistic' => true]);
        $txn->set('key1', 'value1');

        // pessimisticRollbackAll() routes per-key errors through
        // handleRollbackError(); a retryable one is retried and then
        // succeeds, before the final batch rollback.
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
        $this->assertSame(2, $pessimisticRollbackCalls);
    }
}
