<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests for TxnKV client requiring running TiKV cluster.
 *
 * Run with: docker-compose --profile test up --build php-test
 */
class TxnKvE2ETest extends TestCase
{
    private static ?TxnKvClient $client = null;

    private TxnKvClient $testClient;

    /** @var string[] Keys created during the current test, cleaned up in tearDown */
    private array $keysToCleanup = [];

    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        self::$client = TxnKvClient::create($pdEndpoints);
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$client instanceof TxnKvClient) {
            self::$client->close();
            self::$client = null;
        }
    }

    protected function setUp(): void
    {
        if (!self::$client instanceof TxnKvClient) {
            $this->markTestSkipped('TiKV cluster not available');
        }
        $this->testClient = self::$client;
        $this->keysToCleanup = [];
    }

    protected function tearDown(): void
    {
        foreach ($this->keysToCleanup as $key) {
            try {
                $txn = $this->testClient->begin(['pessimistic' => false]);
                $txn->delete($key);
                $txn->commit();
            } catch (\Exception) {
                // Ignore errors during cleanup
            }
        }
    }

    private function uniqueKey(string $prefix): string
    {
        return $prefix . '-' . uniqid();
    }

    // ========================================================================
    // Basic transactional operations
    // ========================================================================

    public function testBeginReturnsActiveTransaction(): void
    {
        $txn = $this->testClient->begin();

        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertNotEmpty($txn->getTxnId());
        $this->assertGreaterThan(0, $txn->getStartTs());

        $txn->rollback();
    }

    public function testBeginWithPessimisticMode(): void
    {
        $txn = $this->testClient->begin(['pessimistic' => true]);

        $this->assertTrue($txn->isPessimistic());

        $txn->rollback();
    }

    public function testBeginWithOptimisticMode(): void
    {
        $txn = $this->testClient->begin(['pessimistic' => false]);

        $this->assertFalse($txn->isPessimistic());

        $txn->rollback();
    }

    public function testSetAndGetInTransaction(): void
    {
        $key = $this->uniqueKey('txn-set-get');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key, 'hello');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertNotNull($txn->getCommitTs());
    }

    public function testReadAfterWriteReturnsLocalValue(): void
    {
        $key = $this->uniqueKey('txn-local-read');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key, 'local-value');

        $this->assertSame('local-value', $txn->get($key));

        $txn->rollback();
    }

    public function testDeleteInTransaction(): void
    {
        $key = $this->uniqueKey('txn-delete');
        $this->keysToCleanup[] = $key;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key, 'initial');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->delete($key);

        $this->assertNull($txn->get($key));

        $txn->commit();
    }

    public function testRollbackDoesNotPersistWrites(): void
    {
        $key = $this->uniqueKey('txn-rollback');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key, 'should-not-persist');
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertNull($readTxn->get($key));
        $readTxn->rollback();
    }

    public function testCommitEmptyWriteSet(): void
    {
        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
    }

    public function testRollbackEmptyWriteSet(): void
    {
        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());
    }

    // ========================================================================
    // Snapshot isolation (MVCC reads)
    // ========================================================================

    public function testSnapshotIsolation(): void
    {
        $key = $this->uniqueKey('txn-snapshot');
        $this->keysToCleanup[] = $key;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key, 'v1');
        $setupTxn->commit();

        $readerTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('v1', $readerTxn->get($key));

        $writerTxn = $this->testClient->begin(['pessimistic' => false]);
        $writerTxn->set($key, 'v2');
        $writerTxn->commit();

        $this->assertSame('v1', $readerTxn->get($key));

        $readerTxn->rollback();
    }

    public function testReadCommittedAfterOtherTransaction(): void
    {
        $key = $this->uniqueKey('txn-read-committed');
        $this->keysToCleanup[] = $key;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key, 'initial');
        $setupTxn->commit();

        $newTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('initial', $newTxn->get($key));
        $newTxn->rollback();
    }

    // ========================================================================
    // Multiple keys
    // ========================================================================

    public function testMultipleKeysInTransaction(): void
    {
        $key1 = $this->uniqueKey('txn-multi-1');
        $key2 = $this->uniqueKey('txn-multi-2');
        $key3 = $this->uniqueKey('txn-multi-3');
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;
        $this->keysToCleanup[] = $key3;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key1, 'value1');
        $txn->set($key2, 'value2');
        $txn->set($key3, 'value3');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('value1', $readTxn->get($key1));
        $this->assertSame('value2', $readTxn->get($key2));
        $this->assertSame('value3', $readTxn->get($key3));
        $readTxn->rollback();
    }

    public function testBatchGetInTransaction(): void
    {
        $key1 = $this->uniqueKey('txn-bget-1');
        $key2 = $this->uniqueKey('txn-bget-2');
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key1, 'v1');
        $setupTxn->set($key2, 'v2');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $results = $txn->batchGet([$key1, $key2]);

        $this->assertSame('v1', $results[$key1]);
        $this->assertSame('v2', $results[$key2]);

        $txn->rollback();
    }

    // ========================================================================
    // Scan
    // ========================================================================

    public function testScanInTransaction(): void
    {
        $prefix = 'txn-scan-' . uniqid();
        $key1 = $prefix . '-a';
        $key2 = $prefix . '-b';
        $key3 = $prefix . '-c';
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;
        $this->keysToCleanup[] = $key3;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key1, 'va');
        $setupTxn->set($key2, 'vb');
        $setupTxn->set($key3, 'vc');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $results = $txn->scan($prefix, $prefix . '~');

        $this->assertGreaterThanOrEqual(3, count($results));

        $map = [];
        foreach ($results as $entry) {
            $map[$entry['key']] = $entry['value'];
        }
        $this->assertSame('va', $map[$key1]);
        $this->assertSame('vb', $map[$key2]);
        $this->assertSame('vc', $map[$key3]);

        $txn->rollback();
    }

    public function testScanWithLimitZeroReturnsAll(): void
    {
        $prefix = 'txn-scan-zero-' . uniqid();
        $key1 = $prefix . '-a';
        $key2 = $prefix . '-b';
        $key3 = $prefix . '-c';
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;
        $this->keysToCleanup[] = $key3;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key1, 'v1');
        $setupTxn->set($key2, 'v2');
        $setupTxn->set($key3, 'v3');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $results = $txn->scan($prefix, $prefix . '~', 0);

        $this->assertCount(3, $results);
        $this->assertSame('v1', $results[0]['value']);
        $this->assertSame('v2', $results[1]['value']);
        $this->assertSame('v3', $results[2]['value']);

        $txn->rollback();
    }

    public function testScanWithPositiveLimitReturnsExactlyThatMany(): void
    {
        $prefix = 'txn-scan-limit-' . uniqid();
        $key1 = $prefix . '-a';
        $key2 = $prefix . '-b';
        $key3 = $prefix . '-c';
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;
        $this->keysToCleanup[] = $key3;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key1, 'v1');
        $setupTxn->set($key2, 'v2');
        $setupTxn->set($key3, 'v3');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $results = $txn->scan($prefix, $prefix . '~', 2);

        $this->assertCount(2, $results);

        $txn->rollback();
    }

    public function testScanWithLimitOneReturnsSingleKey(): void
    {
        $prefix = 'txn-scan-one-' . uniqid();
        $key1 = $prefix . '-a';
        $key2 = $prefix . '-b';
        $this->keysToCleanup[] = $key1;
        $this->keysToCleanup[] = $key2;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key1, 'v1');
        $setupTxn->set($key2, 'v2');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $results = $txn->scan($prefix, $prefix . '~', 1);

        $this->assertCount(1, $results);
        $this->assertSame($key1, $results[0]['key']);
        $this->assertSame('v1', $results[0]['value']);

        $txn->rollback();
    }

    // ========================================================================
    // Pessimistic transactions
    // ========================================================================

    public function testPessimisticSetAndCommit(): void
    {
        $key = $this->uniqueKey('txn-pess-set');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => true]);
        $txn->set($key, 'pessimistic-value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('pessimistic-value', $readTxn->get($key));
        $readTxn->rollback();
    }

    public function testPessimisticRollback(): void
    {
        $key = $this->uniqueKey('txn-pess-rb');
        $this->keysToCleanup[] = $key;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key, 'initial');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => true]);
        $txn->set($key, 'should-not-persist');
        $txn->rollback();

        $this->assertSame(TransactionStatus::RolledBack, $txn->getStatus());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('initial', $readTxn->get($key));
        $readTxn->rollback();
    }

    // ========================================================================
    // Transaction state enforcement
    // ========================================================================

    public function testSetThrowsAfterCommit(): void
    {
        $key = $this->uniqueKey('txn-state');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key, 'value');
        $txn->commit();

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $txn->set($key, 'another');
    }

    public function testGetThrowsAfterRollback(): void
    {
        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->rollback();

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $txn->get('any-key');
    }

    // ========================================================================
    // Overwrite within transaction
    // ========================================================================

    public function testOverwriteWithinTransaction(): void
    {
        $key = $this->uniqueKey('txn-overwrite');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->set($key, 'first');
        $this->assertSame('first', $txn->get($key));

        $txn->set($key, 'second');
        $this->assertSame('second', $txn->get($key));

        $txn->commit();

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('second', $readTxn->get($key));
        $readTxn->rollback();
    }

    public function testDeleteThenSetWithinTransaction(): void
    {
        $key = $this->uniqueKey('txn-del-set');
        $this->keysToCleanup[] = $key;

        $setupTxn = $this->testClient->begin(['pessimistic' => false]);
        $setupTxn->set($key, 'initial');
        $setupTxn->commit();

        $txn = $this->testClient->begin(['pessimistic' => false]);
        $txn->delete($key);
        $this->assertNull($txn->get($key));

        $txn->set($key, 'restored');
        $this->assertSame('restored', $txn->get($key));

        $txn->commit();

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('restored', $readTxn->get($key));
        $readTxn->rollback();
    }

    // ========================================================================
    // Client lifecycle (close behavior)
    // ========================================================================

    private function createFreshTxnClient(): TxnKvClient
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        return TxnKvClient::create($pdEndpoints);
    }

    public function testCloseThenBeginThrowsClientClosedException(): void
    {
        $client = $this->createFreshTxnClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->begin();
    }

    public function testCloseIsIdempotent(): void
    {
        $client = $this->createFreshTxnClient();
        $client->close();

        // Second close must not throw
        $client->close();

        // Must remain closed (still throw)
        $this->expectException(ClientClosedException::class);
        $client->begin();
    }

    public function testTransactionBeforeCloseRemainsUsable(): void
    {
        $key = $this->uniqueKey('txn-lifecycle');
        $otherKey = $this->uniqueKey('txn-lifecycle-other');
        $this->keysToCleanup[] = $key;
        $this->keysToCleanup[] = $otherKey;

        $client = $this->createFreshTxnClient();
        $txn = $client->begin(['pessimistic' => false]);
        $txn->set($key, 'value-before-close');

        // Close the client — this releases the shared gRPC connection pool.
        $client->close();

        // Reading a key from the local write set does NOT require a gRPC call.
        $this->assertSame('value-before-close', $txn->get($key));

        // But reading a key NOT in the write set requires a remote gRPC call,
        // which is blocked by the GrpcClient closed guard.
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidStateException::class);
        $this->expectExceptionMessage('gRPC client is closed');
        $txn->get($otherKey);
    }

    public function testMultiplePostCloseBeginAllThrow(): void
    {
        $client = $this->createFreshTxnClient();
        $client->close();

        for ($i = 0; $i < 3; $i++) {
            try {
                $client->begin();
                $this->fail('Expected ClientClosedException on iteration ' . $i);
            } catch (ClientClosedException) {
                // Expected
            }
        }
    }
}
