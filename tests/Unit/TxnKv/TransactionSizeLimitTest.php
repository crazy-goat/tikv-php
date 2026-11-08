<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #430 (GAP-16): transaction write-set limits, their queryability and
 * the size/count-bounded prewrite and commit requests.
 */
final class TransactionSizeLimitTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private LockResolver $lockResolver;

    /** @var list<PrewriteRequest> */
    private array $prewriteRequests = [];
    /** @var list<CommitRequest> */
    private array $commitRequests = [];

    protected function setUp(): void
    {
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
        $this->prewriteRequests = [];
        $this->commitRequests = [];
    }

    private function makeRegion(int $id = 1, string $startKey = '', string $endKey = ''): RegionInfo
    {
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

    private function stubSingleRegion(RegionInfo $region): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($store);
    }

    /**
     * Two adjacent regions split at "m": keys below "m" belong to the left
     * region, keys from "m" upwards to the right one. The cache answers every
     * key so the test exercises the per-region grouping, not PD resolution.
     */
    private function stubTwoRegions(RegionInfo $left, RegionInfo $right): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        $regionFor = static fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $left : $right;

        $this->regionCache->method('getByKey')->willReturnCallback($regionFor);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturnCallback($regionFor);
        $this->pdClient->method('scanRegions')->willReturn([$left, $right]);
        $this->pdClient->method('getStore')->willReturn($store);
    }

    /**
     * @param array{
     *     pessimistic?: bool,
     *     eagerPessimisticLocks?: bool,
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     *     enable1Pc?: bool,
     *     enableAsyncCommit?: bool,
     * } $options
     */
    private function makeTransaction(array $options = []): Transaction
    {
        return new Transaction(
            txnId: 'test-txn-size-limit',
            startTs: 1000,
            pessimistic: $options['pessimistic'] ?? false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
            enable1Pc: $options['enable1Pc'] ?? false,
            enableAsyncCommit: $options['enableAsyncCommit'] ?? false,
            eagerPessimisticLocks: $options['eagerPessimisticLocks'] ?? false,
            maxTxnEntryCount: $options['maxTxnEntryCount'] ?? 0,
            maxTxnEntrySize: $options['maxTxnEntrySize'] ?? 0,
            maxTxnTotalSize: $options['maxTxnTotalSize'] ?? 0,
            maxTxnBatchSize: $options['maxTxnBatchSize'] ?? TwoPhaseCommitter::DEFAULT_TXN_BATCH_SIZE,
            maxTxnBatchCount: $options['maxTxnBatchCount'] ?? TwoPhaseCommitter::DEFAULT_TXN_BATCH_COUNT,
            maxReadSetEntries: $options['maxReadSetEntries'] ?? 0,
            maxReadSetTotalSize: $options['maxReadSetTotalSize'] ?? 0,
            trackReadSet: $options['trackReadSet'] ?? false,
        );
    }

    // ========================================================================
    // TransactionState limits
    // ========================================================================

    public function testDefaultLimitsAreUnlimited(): void
    {
        $state = new TransactionState();

        for ($i = 0; $i < 1000; $i++) {
            $state->setWrite('key' . $i, str_repeat('v', 100));
        }

        self::assertSame(1000, $state->getWriteSetCount());
        self::assertGreaterThan(100000, $state->getWriteSetSize());
    }

    public function testDefaultReadSetLimitsAreUnlimited(): void
    {
        $state = new TransactionState(trackReadSet: true);

        for ($i = 0; $i < 1000; $i++) {
            $state->setReadValue('key' . $i, str_repeat('v', 100));
        }

        self::assertSame(1000, $state->getReadSetCount());
        self::assertGreaterThan(100000, $state->getReadSetSize());
    }

    public function testReadSetEntryLimitAllowsExactlyTheLimit(): void
    {
        $state = new TransactionState(0, 0, 0, 0, maxReadSetEntries: 2, trackReadSet: true);

        $state->setReadValue('a', '1');
        $state->setReadValue('b', '2');

        self::assertSame(2, $state->getReadSetCount());
    }

    public function testReadSetEntryLimitRejectsTheEntryOverTheLimit(): void
    {
        $state = new TransactionState(0, 0, 0, 0, maxReadSetEntries: 2, trackReadSet: true);
        $state->setReadValue('a', '1');
        $state->setReadValue('b', '2');

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxReadSetEntries of 2');
        $state->setReadValue('c', '3');
    }

    public function testReadSetTotalSizeLimitAllowsExactlyTheLimit(): void
    {
        $state = new TransactionState(maxReadSetTotalSize: 10, trackReadSet: true);

        $state->setReadValue('a', '1234');
        $state->setReadValue('b', '1234');

        self::assertSame(10, $state->getReadSetSize());
        self::assertSame(2, $state->getReadSetCount());
    }

    public function testReadSetTotalSizeLimitRejectsTheEntryOverTheLimit(): void
    {
        $state = new TransactionState(maxReadSetTotalSize: 10, trackReadSet: true);
        $state->setReadValue('a', '1234');
        $state->setReadValue('b', '1234');

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxReadSetTotalSize of 10 bytes');
        $state->setReadValue('c', '');
    }

    public function testRereadingAKeyDoesNotConsumeAdditionalReadSetBudget(): void
    {
        $state = new TransactionState(maxReadSetTotalSize: 6, trackReadSet: true);

        $state->setReadValue('a', '1234');
        $state->setReadValue('a', '1234');

        self::assertSame(1, $state->getReadSetCount());
        self::assertSame(5, $state->getReadSetSize());

        // A smaller replacement frees the difference: "a" + "1" = 2 bytes,
        // then "b" + "123" = 4 bytes, for 6 bytes total.
        $state->setReadValue('a', '1');
        $state->setReadValue('b', '123');
        self::assertSame(6, $state->getReadSetSize());

        $this->expectException(TransactionTooLargeException::class);
        $state->setReadValue('c', '');
    }

    public function testClearReadSetResetsTheAccounting(): void
    {
        $state = new TransactionState(maxReadSetEntries: 1, trackReadSet: true);
        $state->setReadValue('a', '1');

        $state->clearReadSet();

        self::assertSame(0, $state->getReadSetCount());
        self::assertSame(0, $state->getReadSetSize());

        // The freed budget is usable again.
        $state->setReadValue('b', '2');
        self::assertSame(1, $state->getReadSetCount());
    }

    public function testReadSetExceptionReportsTheLimitNameLimitAndActualValue(): void
    {
        $state = new TransactionState(maxReadSetEntries: 1, trackReadSet: true);
        $state->setReadValue('a', '1');

        try {
            $state->setReadValue('b', '2');
            self::fail('The read-set entry-count limit did not reject the second entry');
        } catch (TransactionTooLargeException $e) {
            self::assertSame('maxReadSetEntries', $e->getLimitName());
            self::assertSame(1, $e->getLimit());
            self::assertSame(2, $e->getActual());
        }
    }

    public function testEntryCountLimitAllowsExactlyTheLimit(): void
    {
        $state = new TransactionState(0, 2, 0, 0);

        $state->setWrite('a', '1');
        $state->setWrite('b', '2');

        self::assertSame(2, $state->getWriteSetCount());
    }

    public function testEntryCountLimitRejectsTheEntryOverTheLimit(): void
    {
        $state = new TransactionState(0, 2, 0, 0);
        $state->setWrite('a', '1');
        $state->setWrite('b', '2');

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxTxnEntryCount of 2');
        $state->setWrite('c', '3');
    }

    public function testEntrySizeLimitAllowsExactlyTheLimit(): void
    {
        $state = new TransactionState(0, 0, 5, 0);

        // key "ab" + value "cde" = 5 bytes.
        $state->setWrite('ab', 'cde');

        self::assertSame(5, $state->getWriteSetSize());
    }

    public function testEntrySizeLimitRejectsTheEntryOverTheLimit(): void
    {
        $state = new TransactionState(0, 0, 5, 0);

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxTxnEntrySize of 5 bytes');
        $state->setWrite('ab', 'cdef');
    }

    public function testTotalSizeLimitAllowsExactlyTheLimit(): void
    {
        $state = new TransactionState(0, 0, 0, 10);

        $state->setWrite('a', '1234');
        $state->setWrite('b', '1234');

        self::assertSame(10, $state->getWriteSetSize());
        self::assertSame(2, $state->getWriteSetCount());
    }

    public function testTotalSizeLimitRejectsTheEntryOverTheLimit(): void
    {
        $state = new TransactionState(0, 0, 0, 10);
        $state->setWrite('a', '1234');
        $state->setWrite('b', '1234');

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxTxnTotalSize of 10 bytes');
        $state->setWrite('c', '');
    }

    public function testOverwritingAnEntryDoesNotConsumeAdditionalBudget(): void
    {
        $state = new TransactionState(0, 0, 0, 6);

        $state->setWrite('a', '1234');
        $state->setWrite('a', '1234');

        self::assertSame(1, $state->getWriteSetCount());
        self::assertSame(5, $state->getWriteSetSize());

        // A smaller replacement frees the difference: "a" + "1" = 2 bytes,
        // then "b" + "123" = 4 bytes, for 6 bytes total.
        $state->setWrite('a', '1');
        $state->setWrite('b', '123');
        self::assertSame(6, $state->getWriteSetSize());

        $this->expectException(TransactionTooLargeException::class);
        $state->setWrite('c', '');
    }

    public function testClearWriteSetResetsTheAccounting(): void
    {
        $state = new TransactionState(0, 1, 0, 0);
        $state->setWrite('a', '1');

        $state->clearWriteSet();

        self::assertSame(0, $state->getWriteSetCount());
        self::assertSame(0, $state->getWriteSetSize());

        // The freed budget is usable again.
        $state->setWrite('b', '2');
        self::assertSame(1, $state->getWriteSetCount());
    }

    public function testSetWriteSetEnforcesTheLimits(): void
    {
        $state = new TransactionState(0, 1, 0, 0);

        $this->expectException(TransactionTooLargeException::class);
        $this->expectExceptionMessage('exceeding maxTxnEntryCount of 1');
        $state->setWriteSet(['a' => '1', 'b' => '2']);
    }

    public function testSetWriteSetEnforcesTheEntrySizeLimit(): void
    {
        $state = new TransactionState(0, 0, 3, 0);

        try {
            $state->setWriteSet(['ab' => 'cd']);
            self::fail('The entry-size limit did not reject the write set');
        } catch (TransactionTooLargeException $e) {
            // "ab" + "cd" = 4 bytes against a 3-byte limit. The accessors must
            // report the limit and the actual value separately: the message is
            // built from the factory parameters and cannot tell them apart
            // (issue #790).
            self::assertSame('maxTxnEntrySize', $e->getLimitName());
            self::assertSame(3, $e->getLimit());
            self::assertSame(4, $e->getActual());
            self::assertStringContainsString('exceeding maxTxnEntrySize of 3 bytes', $e->getMessage());
        }
    }

    public function testSetWriteSetEnforcesTheTotalSizeLimit(): void
    {
        $state = new TransactionState(0, 0, 0, 3);

        try {
            $state->setWriteSet(['ab' => 'cd']);
            self::fail('The total-size limit did not reject the write set');
        } catch (TransactionTooLargeException $e) {
            // "ab" + "cd" = 4 bytes against a 3-byte limit (issue #790).
            self::assertSame('maxTxnTotalSize', $e->getLimitName());
            self::assertSame(3, $e->getLimit());
            self::assertSame(4, $e->getActual());
            self::assertStringContainsString('exceeding maxTxnTotalSize of 3 bytes', $e->getMessage());
        }
    }

    public function testExceptionReportsTheLimitNameLimitAndActualValue(): void
    {
        $state = new TransactionState(0, 1, 0, 0);
        $state->setWrite('a', '1');

        try {
            $state->setWrite('b', '2');
            self::fail('The entry-count limit did not reject the second entry');
        } catch (TransactionTooLargeException $e) {
            self::assertSame('maxTxnEntryCount', $e->getLimitName());
            self::assertSame(1, $e->getLimit());
            self::assertSame(2, $e->getActual());
            self::assertStringContainsString('would hold 2 entries', $e->getMessage());
        }
    }

    public function testTransactionTooLargeExceptionIsClassifiedAsFatal(): void
    {
        self::assertTrue(
            ErrorClassifier::classify(TransactionTooLargeException::entryCount(3, 2))->isFatal(),
            'TransactionTooLargeException must never be retried',
        );
    }

    // ========================================================================
    // Transaction: queryability and "no RPC before the limit check"
    // ========================================================================

    public function testTransactionExposesWriteSetSizeAndCount(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $txn = $this->makeTransaction();

        $txn->set('k1', 'v1');
        $txn->delete('k2');

        self::assertSame(2, $txn->getWriteSetCount());
        self::assertSame(6, $txn->getWriteSetSize());

        // Replacing the value keeps both the count and the budget unchanged.
        $txn->set('k1', 'v2');
        self::assertSame(2, $txn->getWriteSetCount());
        self::assertSame(6, $txn->getWriteSetSize());

        $this->grpc->method('call')->willReturnCallback(
            static fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvBatchRollback' => new BatchRollbackResponse(),
                default => throw new \RuntimeException('Unexpected method: ' . $method),
            },
        );
        $txn->rollback();
    }

    public function testLimitIsEnforcedBeforeTheEagerPessimisticLockRpc(): void
    {
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('call');
        $this->pdClient->expects($this->never())->method('getTimestamp');

        $txn = $this->makeTransaction([
            'pessimistic' => true,
            'eagerPessimisticLocks' => true,
            'maxTxnEntrySize' => 1,
        ]);

        try {
            $txn->set('ab', 'x');
            self::fail('The entry-size limit did not reject the write');
        } catch (TransactionTooLargeException $e) {
            self::assertSame('maxTxnEntrySize', $e->getLimitName());
        }

        self::assertSame(0, $txn->getWriteSetCount());
        self::assertSame(0, $txn->getWriteSetSize());
    }

    public function testTransactionExposesReadSetSizeAndCountAndRejectsOverTheLimit(): void
    {
        $this->stubSingleRegion($this->makeRegion());

        $response = new GetResponse();
        $response->setNotFound(false);
        $response->setValue('v1');
        $this->grpc->method('call')->willReturnCallback(
            static fn(string $addr, string $svc, string $method): object => match ($method) {
                'KvGet' => $response,
                default => throw new \RuntimeException('Unexpected method: ' . $method),
            },
        );

        $txn = $this->makeTransaction(['maxReadSetEntries' => 1, 'trackReadSet' => true]);

        self::assertSame('v1', $txn->get('k1'));
        self::assertSame(1, $txn->getReadSetCount());
        self::assertSame(4, $txn->getReadSetSize()); // "k1" + "v1"

        try {
            $txn->get('k2');
            self::fail('The read-set entry-count limit did not reject the second read');
        } catch (TransactionTooLargeException $e) {
            self::assertSame('maxReadSetEntries', $e->getLimitName());
        }

        // The rejected value was not retained: the read set is unchanged.
        self::assertSame(1, $txn->getReadSetCount());
        self::assertSame(4, $txn->getReadSetSize());
        self::assertArrayNotHasKey('k2', $txn->getReadSet());
    }

    // ========================================================================
    // Batching: multi-request prewrite and commit
    // ========================================================================

    private function capturePrewriteAndCommit(?PrewriteResponse $prewriteResponse = null): void
    {
        $prewriteResponse ??= new PrewriteResponse();

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $addr,
                string $svc,
                string $method,
                Message $request,
            ) use ($prewriteResponse): object {
                if ($request instanceof PrewriteRequest) {
                    $this->prewriteRequests[] = $request;
                }
                if ($request instanceof CommitRequest) {
                    $this->commitRequests[] = $request;
                }

                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => new CommitResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    default => throw new \RuntimeException('Unexpected method: ' . $method),
                };
            },
        );
    }

    /**
     * @param list<PrewriteRequest> $requests
     * @return list<string>
     */
    private function prewriteKeys(array $requests): array
    {
        $keys = [];
        foreach ($requests as $request) {
            foreach ($request->getMutations() as $mutation) {
                $keys[] = $mutation->getKey();
            }
        }

        return $keys;
    }

    /**
     * @param list<CommitRequest> $requests
     * @return list<string>
     */
    private function commitKeys(array $requests): array
    {
        $keys = [];
        foreach ($requests as $request) {
            foreach ($request->getKeys() as $key) {
                $keys[] = $key;
            }
        }

        return $keys;
    }

    /**
     * @param list<string> $keys
     */
    private function assertKeysBelongToOneRegion(array $keys): void
    {
        $sides = array_unique(array_map(
            static fn(string $key): int => KeyOrder::lt($key, 'm') ? 1 : 2,
            $keys,
        ));

        self::assertCount(1, $sides, 'a request must not mix keys from two regions');
    }

    public function testPrewriteAndCommitAreSplitByCountPerBatch(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        $txn = $this->makeTransaction(['maxTxnBatchCount' => 2]);
        $keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6'];
        foreach ($keys as $index => $key) {
            $txn->set($key, 'v' . $index);
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertSame(2000, $txn->getCommitTs());

        self::assertCount(3, $this->prewriteRequests);
        self::assertCount(3, $this->commitRequests);
        self::assertEqualsCanonicalizing($keys, $this->prewriteKeys($this->prewriteRequests));
        self::assertEqualsCanonicalizing($keys, $this->commitKeys($this->commitRequests));

        foreach ($this->prewriteRequests as $request) {
            self::assertLessThanOrEqual(2, count($request->getMutations()));
        }
        foreach ($this->commitRequests as $request) {
            self::assertLessThanOrEqual(2, count($request->getKeys()));
        }

        // The primary key is written first in both phases.
        $firstMutation = iterator_to_array($this->prewriteRequests[0]->getMutations())[0];
        self::assertSame('k1', $firstMutation->getKey());
        self::assertSame(['k1', 'k2'], iterator_to_array($this->commitRequests[0]->getKeys()));
    }

    public function testPrewriteAndCommitAreSplitPerRegionWithoutMixingRegions(): void
    {
        $this->stubTwoRegions(
            $this->makeRegion(1, '', 'm'),
            $this->makeRegion(2, 'm', ''),
        );
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        // Alternating keys: a global split would produce [a1, m1] / [a2, m2]
        // and mix regions, while the per-region grouping must produce
        // [a1, a2] / [m1, m2].
        $txn = $this->makeTransaction(['maxTxnBatchCount' => 2]);
        $keys = ['a1', 'm1', 'a2', 'm2'];
        foreach ($keys as $index => $key) {
            $txn->set($key, 'v' . $index);
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(2, $this->prewriteRequests);
        self::assertCount(2, $this->commitRequests);

        foreach ($this->prewriteRequests as $request) {
            $requestKeys = self::prewriteKeys([$request]);
            self::assertCount(2, $requestKeys);
            $this->assertKeysBelongToOneRegion($requestKeys);
        }
        foreach ($this->commitRequests as $request) {
            $requestKeys = self::commitKeys([$request]);
            self::assertCount(2, $requestKeys);
            $this->assertKeysBelongToOneRegion($requestKeys);
        }

        // The primary key (a1) is in the first batch, and that batch is
        // dispatched first in both phases.
        self::assertSame(['a1', 'a2'], self::prewriteKeys([$this->prewriteRequests[0]]));
        self::assertSame(['a1', 'a2'], self::commitKeys([$this->commitRequests[0]]));
        self::assertSame(['m1', 'm2'], self::prewriteKeys([$this->prewriteRequests[1]]));
        self::assertSame(['m1', 'm2'], self::commitKeys([$this->commitRequests[1]]));
    }

    public function testPrewriteAndCommitAreSplitByBytesPerBatch(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        // Each key is 10 bytes and each value 5, so one prewrite entry is
        // 15 bytes: a 15-byte budget holds one entry per request. A commit
        // request carries only keys, so a 10-byte key fits one per request.
        $txn = $this->makeTransaction(['maxTxnBatchSize' => 15]);
        $keys = ['key0000001', 'key0000002', 'key0000003'];
        foreach ($keys as $key) {
            $txn->set($key, '12345');
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(3, $this->prewriteRequests);
        self::assertCount(3, $this->commitRequests);
        foreach ($this->prewriteRequests as $request) {
            self::assertCount(1, $request->getMutations());
        }
        foreach ($this->commitRequests as $request) {
            self::assertCount(1, $request->getKeys());
        }
    }

    public function testPrewriteAndCommitKeepItemsThatExactlyFillTheByteBudgetInOneBatch(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        // Two 5-byte keys with empty values: 10 payload bytes in prewrite and
        // 10 key bytes in commit. The splitter starts a new batch only when
        // the next item would exceed the budget, so both stay in one request
        // (a > to >= regression would split them).
        $txn = $this->makeTransaction(['maxTxnBatchSize' => 10]);
        $txn->set('abcde', '');
        $txn->set('fghij', '');
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(1, $this->prewriteRequests);
        self::assertCount(1, $this->commitRequests);
        self::assertSame(['abcde', 'fghij'], self::prewriteKeys($this->prewriteRequests));
        self::assertSame(['abcde', 'fghij'], self::commitKeys($this->commitRequests));
    }

    public function testOnePhaseCommitIsDisabledWhenTheRegionNeedsMultipleBatches(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        $txn = $this->makeTransaction(['enable1Pc' => true, 'maxTxnBatchCount' => 2]);
        foreach (['k1', 'k2', 'k3', 'k4'] as $index => $key) {
            $txn->set($key, 'v' . $index);
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(2, $this->prewriteRequests);
        self::assertCount(2, $this->commitRequests);
        foreach ($this->prewriteRequests as $request) {
            self::assertFalse($request->getTryOnePc(), 'a split transaction must not use 1PC');
        }
    }

    public function testAsyncCommitCarriesItsDecisionOnThePrimaryBatchLast(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $response = new PrewriteResponse();
        $response->setMinCommitTs(1500);
        $this->capturePrewriteAndCommit($response);

        $txn = $this->makeTransaction(['enableAsyncCommit' => true, 'maxTxnBatchCount' => 2]);
        foreach (['k1', 'k2', 'k3', 'k4'] as $index => $key) {
            $txn->set($key, 'v' . $index);
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertSame(1500, $txn->getCommitTs());
        self::assertCount(2, $this->prewriteRequests);
        self::assertCount(0, $this->commitRequests);

        // The primary batch is prewritten last and carries the async-commit
        // decision; the other batch of the same region is a plain prewrite.
        self::assertFalse($this->prewriteRequests[0]->getUseAsyncCommit());
        self::assertTrue($this->prewriteRequests[1]->getUseAsyncCommit());
        $firstMutation = iterator_to_array($this->prewriteRequests[1]->getMutations())[0];
        self::assertSame('k1', $firstMutation->getKey());
        self::assertEqualsCanonicalizing(
            ['k2', 'k3', 'k4'],
            iterator_to_array($this->prewriteRequests[1]->getSecondaries()),
        );
    }

    public function testCommitStartsWithThePrimaryBatchWhenItIsNotTheFirstBatch(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        $txn = $this->makeTransaction(['maxTxnBatchCount' => 1]);
        foreach (['a1', 'a2', 'a3', 'a4'] as $index => $key) {
            $txn->set($key, 'v' . $index);
        }

        $state = (new \ReflectionProperty(Transaction::class, 'state'))->getValue($txn);
        self::assertInstanceOf(TransactionState::class, $state);
        // A pessimistic primary that is not the first write key. The prewrite
        // phase already honours it; the commit phase must select the same
        // batch, not blindly commit batch 0 (issue #430 review, point 4).
        $state->setPessimisticPrimaryKey('a3');

        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(4, $this->prewriteRequests);
        self::assertCount(4, $this->commitRequests);
        self::assertSame(['a3'], self::prewriteKeys([$this->prewriteRequests[0]]));
        self::assertSame(['a3'], self::commitKeys([$this->commitRequests[0]]));
    }

    public function testFailedSecondaryBatchDoesNotCommitTheTransaction(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $prewriteCalls = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $addr,
                string $svc,
                string $method,
                Message $request,
            ) use (&$prewriteCalls): object {
                if ($request instanceof PrewriteRequest) {
                    $this->prewriteRequests[] = $request;
                    $prewriteCalls++;
                    $response = new PrewriteResponse();
                    if ($prewriteCalls === 2) {
                        $error = new KeyError();
                        $error->setAbort('simulated secondary batch failure');
                        $response->setErrors([$error]);
                    }

                    return $response;
                }
                if ($request instanceof CommitRequest) {
                    $this->commitRequests[] = $request;

                    return new CommitResponse();
                }
                if ($method === 'KvBatchRollback') {
                    return new BatchRollbackResponse();
                }

                throw new \RuntimeException('Unexpected method: ' . $method);
            },
        );

        $txn = $this->makeTransaction(['maxTxnBatchCount' => 2]);
        foreach (['k1', 'k2', 'k3', 'k4'] as $index => $key) {
            $txn->set($key, 'v' . $index);
        }

        try {
            $txn->commit();
            self::fail('A failing secondary prewrite batch must fail the transaction');
        } catch (TransactionConflictException $e) {
            self::assertSame('simulated secondary batch failure', $e->getMessage());
        }

        // The primary batch was prewritten, the second failed: the
        // transaction must stay Active with its buffered write set and the
        // primary lock recoverable, never rolled back or marked committed.
        self::assertSame(TransactionStatus::Active, $txn->getStatus());
        self::assertSame(4, $txn->getWriteSetCount());
        self::assertNull($txn->getCommitTs());
        self::assertCount(2, $this->prewriteRequests);
        self::assertSame(['k1', 'k2'], self::prewriteKeys([$this->prewriteRequests[0]]));
        self::assertSame(['k3', 'k4'], self::prewriteKeys([$this->prewriteRequests[1]]));
        self::assertCount(0, $this->commitRequests);
    }

    public function testDefaultBatchCountSplitsMoreThan512Mutations(): void
    {
        $this->stubSingleRegion($this->makeRegion());
        $this->pdClient->method('getTimestamp')->willReturn(2000);
        $this->capturePrewriteAndCommit();

        $txn = $this->makeTransaction();
        for ($i = 0; $i < 513; $i++) {
            $txn->set('key' . $i, 'v');
        }
        $txn->commit();

        self::assertSame(TransactionStatus::Committed, $txn->getStatus());
        self::assertCount(2, $this->prewriteRequests);
        self::assertCount(2, $this->commitRequests);
        self::assertSame(512, count($this->prewriteRequests[0]->getMutations()));
        self::assertSame(1, count($this->prewriteRequests[1]->getMutations()));
        self::assertSame(512, count($this->commitRequests[0]->getKeys()));
        self::assertSame(1, count($this->commitRequests[1]->getKeys()));
    }

    // ========================================================================
    // TxnKvClient::begin() options
    // ========================================================================

    private function makeClient(): TxnKvClient
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);

        return new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));
    }

    public function testBeginForwardsTheTransactionSizeAndBatchOptions(): void
    {
        $txn = $this->makeClient()->begin([
            'maxTxnEntryCount' => 100,
            'maxTxnEntrySize' => 1024,
            'maxTxnTotalSize' => 4096,
            'maxReadSetEntries' => 500,
            'maxReadSetTotalSize' => 2048,
            'trackReadSet' => true,
            'maxTxnBatchSize' => 8192,
            'maxTxnBatchCount' => 64,
        ]);

        self::assertSame(100, (new \ReflectionProperty(Transaction::class, 'maxTxnEntryCount'))->getValue($txn));
        self::assertSame(1024, (new \ReflectionProperty(Transaction::class, 'maxTxnEntrySize'))->getValue($txn));
        self::assertSame(4096, (new \ReflectionProperty(Transaction::class, 'maxTxnTotalSize'))->getValue($txn));
        self::assertSame(500, (new \ReflectionProperty(Transaction::class, 'maxReadSetEntries'))->getValue($txn));
        self::assertSame(2048, (new \ReflectionProperty(Transaction::class, 'maxReadSetTotalSize'))->getValue($txn));
        self::assertTrue((new \ReflectionProperty(Transaction::class, 'trackReadSet'))->getValue($txn));
        self::assertSame(8192, (new \ReflectionProperty(Transaction::class, 'maxTxnBatchSize'))->getValue($txn));
        self::assertSame(64, (new \ReflectionProperty(Transaction::class, 'maxTxnBatchCount'))->getValue($txn));
    }

    public function testBeginUsesUnlimitedLimitsAndDefaultBatchBounds(): void
    {
        $txn = $this->makeClient()->begin();

        self::assertSame(0, (new \ReflectionProperty(Transaction::class, 'maxTxnEntryCount'))->getValue($txn));
        self::assertSame(0, (new \ReflectionProperty(Transaction::class, 'maxTxnEntrySize'))->getValue($txn));
        self::assertSame(0, (new \ReflectionProperty(Transaction::class, 'maxTxnTotalSize'))->getValue($txn));
        self::assertSame(0, (new \ReflectionProperty(Transaction::class, 'maxReadSetEntries'))->getValue($txn));
        self::assertSame(0, (new \ReflectionProperty(Transaction::class, 'maxReadSetTotalSize'))->getValue($txn));
        self::assertFalse((new \ReflectionProperty(Transaction::class, 'trackReadSet'))->getValue($txn));
        self::assertSame(
            TwoPhaseCommitter::DEFAULT_TXN_BATCH_SIZE,
            (new \ReflectionProperty(Transaction::class, 'maxTxnBatchSize'))->getValue($txn),
        );
        self::assertSame(
            TwoPhaseCommitter::DEFAULT_TXN_BATCH_COUNT,
            (new \ReflectionProperty(Transaction::class, 'maxTxnBatchCount'))->getValue($txn),
        );
    }

    /**
     * @return iterable<string, array{string, mixed, string}>
     */
    public static function invalidTxnSizeOptionProvider(): iterable
    {
        foreach (
            [
                'maxTxnEntryCount',
                'maxTxnEntrySize',
                'maxTxnTotalSize',
                'maxReadSetEntries',
                'maxReadSetTotalSize',
            ] as $key
        ) {
            yield $key . ' string' => [$key, '10', "options['$key'] must be an int"];
            yield $key . ' negative' => [$key, -1, "options['$key'] must be >= 0"];
        }
        foreach (['maxTxnBatchSize', 'maxTxnBatchCount'] as $key) {
            yield $key . ' string' => [$key, '10', "options['$key'] must be an int"];
            yield $key . ' zero' => [$key, 0, "options['$key'] must be >= 1"];
            yield $key . ' negative' => [$key, -1, "options['$key'] must be >= 1"];
        }
        yield 'trackReadSet string' => ['trackReadSet', '1', "options['trackReadSet'] must be a bool"];
        yield 'trackReadSet int' => ['trackReadSet', 1, "options['trackReadSet'] must be a bool"];
    }

    #[DataProvider('invalidTxnSizeOptionProvider')]
    public function testBeginRejectsAnInvalidTransactionSizeOption(string $key, mixed $value, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        $this->makeClient()->begin([$key => $value]); // @phpstan-ignore argument.type
    }

    /**
     * @return iterable<string, array{
     *     array{
     *         maxTxnEntryCount?: int,
     *         maxTxnEntrySize?: int,
     *         maxTxnTotalSize?: int,
     *         maxTxnBatchSize?: int,
     *         maxTxnBatchCount?: int,
     *     },
     *     string,
     * }>
     */
    public static function invalidTransactionLimitProvider(): iterable
    {
        yield 'negative entry count' => [['maxTxnEntryCount' => -1], 'maxTxnEntryCount must be >= 0'];
        yield 'negative entry size' => [['maxTxnEntrySize' => -1], 'maxTxnEntrySize must be >= 0'];
        yield 'negative total size' => [['maxTxnTotalSize' => -1], 'maxTxnTotalSize must be >= 0'];
        yield 'negative read set entries' => [['maxReadSetEntries' => -1], 'maxReadSetEntries must be >= 0'];
        yield 'negative read set total size' => [['maxReadSetTotalSize' => -1], 'maxReadSetTotalSize must be >= 0'];
        yield 'zero batch size' => [['maxTxnBatchSize' => 0], 'maxTxnBatchSize must be >= 1'];
        yield 'zero batch count' => [['maxTxnBatchCount' => 0], 'maxTxnBatchCount must be >= 1'];
    }

    /**
     * @param array{
     *     maxTxnEntryCount?: int,
     *     maxTxnEntrySize?: int,
     *     maxTxnTotalSize?: int,
     *     maxReadSetEntries?: int,
     *     maxReadSetTotalSize?: int,
     *     trackReadSet?: bool,
     *     maxTxnBatchSize?: int,
     *     maxTxnBatchCount?: int,
     * } $options
     */
    #[DataProvider('invalidTransactionLimitProvider')]
    public function testTransactionConstructorRejectsInvalidLimits(array $options, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        $this->makeTransaction($options);
    }
}
