<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\Proto\Kvrpcpb\Mutation;
use CrazyGoat\Proto\Kvrpcpb\Op;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\SplitRegionRequest;
use CrazyGoat\Proto\Kvrpcpb\SplitRegionResponse;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests for TxnKV client requiring running TiKV cluster.
 *
 * Run with: make test-e2e (or select the E2E-TxnKV suite in the V1 Compose
 * override; the default php-test command runs E2E-RawKV).
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

    public function testPessimisticReadModifyWriteDetectsInterveningCommit(): void
    {
        $key = $this->uniqueKey('txn-pess-rmw-conflict');
        $this->keysToCleanup[] = $key;

        $setup = $this->testClient->begin(['pessimistic' => false]);
        $setup->set($key, '0');
        $setup->commit();

        $staleWriter = $this->testClient->begin(['pessimistic' => true]);
        $staleValue = (int) $staleWriter->get($key);

        $concurrentWriter = $this->testClient->begin(['pessimistic' => true]);
        $concurrentValue = (int) $concurrentWriter->get($key);
        $concurrentWriter->set($key, (string) ($concurrentValue + 1));
        $concurrentWriter->commit();

        try {
            $staleWriter->set($key, (string) ($staleValue + 1));
            $this->fail('A stale pessimistic read-modify-write must conflict at set()');
        } catch (TransactionConflictException) {
            $this->assertSame(TransactionStatus::RolledBack, $staleWriter->getStatus());
        }

        $verify = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('1', $verify->get($key));
        $verify->rollback();
    }

    public function testPessimisticWritesSerializeBeforeCommit(): void
    {
        if (!function_exists('proc_open')) {
            $this->markTestSkipped('proc_open is required for the concurrency test');
        }

        $key = $this->uniqueKey('txn-pess-serialize');
        $this->keysToCleanup[] = $key;

        $setup = $this->testClient->begin(['pessimistic' => false]);
        $setup->set($key, 'initial');
        $setup->commit();

        $holder = $this->testClient->begin(['pessimistic' => true]);
        $holder->set($key, 'holder');

        $pdEndpoints = getenv('PD_ENDPOINTS') ?: 'pd:2379';
        $environment = getenv();
        $environment['PD_ENDPOINTS'] = $pdEndpoints;
        $projectRoot = dirname(__DIR__, 2);
        $environment['TXKV_TEST_AUTOLOAD'] = $projectRoot . '/vendor/autoload.php';
        $environment['TXKV_TEST_KEY'] = $key;
        $childScript = <<<'PHP'
require (string) getenv('TXKV_TEST_AUTOLOAD');
try {
    $logger = new class extends \Psr\Log\AbstractLogger {
        public function log($level, string|\Stringable $message, array $context = []): void
        {
            if ((string) $message === 'PessimisticLock') {
                fwrite(STDOUT, "locking\n");
                fflush(STDOUT);
            }
        }
    };
    $client = \CrazyGoat\TiKV\Client\TxnKv\TxnKvClient::create(
        explode(',', (string) getenv('PD_ENDPOINTS')),
        $logger,
    );
    try {
        $txn = $client->begin(['pessimistic' => true]);
        fwrite(STDOUT, "set-start\n");
        fflush(STDOUT);
        try {
            $txn->set((string) getenv('TXKV_TEST_KEY'), 'child');
            fwrite(STDOUT, "set-returned\n");
            fflush(STDOUT);
            $txn->commit();
            fwrite(STDOUT, "success\n");
        } catch (\CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException) {
            // A waiter may observe a write conflict when the holder rolls
            // back; that is still a valid serialized outcome. The important
            // assertion is that set() did not complete before release.
            fwrite(STDOUT, "conflict\n");
        }
    } finally {
        $client->close();
    }
} catch (\Throwable $e) {
    fwrite(STDERR, $e->getMessage());
    exit(1);
}
PHP;

        $process = null;
        $pipes = [];
        try {
            $process = proc_open(
                [PHP_BINARY, '-r', $childScript],
                [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
                $pipes,
                $projectRoot,
                $environment,
            );
            $this->assertIsResource($process);
            $this->assertIsResource($pipes[1]);
            $this->assertIsResource($pipes[2]);
            stream_set_blocking($pipes[1], false);

            $observedOutput = '';
            $deadline = microtime(true) + 10.0;
            while (microtime(true) < $deadline) {
                $read = [$pipes[1]];
                $write = null;
                $except = null;
                if (stream_select($read, $write, $except, 0, 100000) > 0) {
                    $chunk = stream_get_contents($pipes[1]);
                    if (is_string($chunk) && $chunk !== '') {
                        $observedOutput .= $chunk;
                    }
                }
                if (
                    str_contains($observedOutput, "set-start\n")
                    && str_contains($observedOutput, "locking\n")
                ) {
                    break;
                }
                $status = proc_get_status($process);
                if (!$status['running']) {
                    break;
                }
            }
            $this->assertStringContainsString("set-start\n", $observedOutput);
            $this->assertStringContainsString("locking\n", $observedOutput);

            $waitOutput = $observedOutput;
            $waitDeadline = microtime(true) + 0.3;
            do {
                $chunk = stream_get_contents($pipes[1]);
                if (is_string($chunk) && $chunk !== '') {
                    $waitOutput .= $chunk;
                }
                usleep(10000);
                $status = proc_get_status($process);
            } while ($status['running'] && microtime(true) < $waitDeadline);
            $this->assertStringNotContainsString(
                "set-returned\n",
                $waitOutput,
                'set() returned before the holder released its lock',
            );
            $this->assertTrue(
                $status['running'],
                'The competing set() completed before the holder released its lock',
            );

            $holder->rollback();
            $childOutput = $waitOutput;
            $deadline = microtime(true) + 15.0;
            do {
                $chunk = stream_get_contents($pipes[1]);
                if (is_string($chunk) && $chunk !== '') {
                    $childOutput .= $chunk;
                }
                usleep(50000);
                $status = proc_get_status($process);
            } while ($status['running'] && microtime(true) < $deadline);

            $this->assertFalse($status['running'], 'The competing transaction did not finish after rollback');
            $childOutput .= (string) stream_get_contents($pipes[1]);
            $this->assertSame(0, $status['exitcode'], stream_get_contents($pipes[2]));
            $this->assertTrue(
                str_contains($childOutput, 'success') || str_contains($childOutput, 'conflict'),
                'The competing transaction ended with an unexpected result: ' . $childOutput,
            );
        } finally {
            try {
                if ($holder->isPessimistic() && $holder->getStatus() === TransactionStatus::Active) {
                    $holder->rollback();
                }
            } finally {
                foreach ($pipes as $pipe) {
                    if (is_resource($pipe)) {
                        fclose($pipe);
                    }
                }
                if (is_resource($process)) {
                    $status = proc_get_status($process);
                    if ($status['running']) {
                        proc_terminate($process);
                    }
                    proc_close($process);
                }
            }
        }

        $verify = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame(
            str_contains($childOutput, 'success') ? 'child' : 'initial',
            $verify->get($key),
        );
        $verify->rollback();
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

    public function testAbandonedPrewriteLockIsResolvedAfterTtlExpiry(): void
    {
        $key = $this->uniqueKey('txn-abandoned-lock');
        $this->keysToCleanup[] = $key;

        // Transaction A writes but never commits: only the prewrite phase runs.
        // Keep it referenced so its destructor does not roll back before the
        // reader below resolves the lock.
        $txnA = $this->testClient->begin(['pessimistic' => false]);
        $txnA->set($key, 'abandoned-value');
        $startTs = $txnA->getStartTs();

        $this->prewriteOnly($key, $startTs, 'abandoned-value');

        // Wait until the lock TTL (1 s) has certainly elapsed.
        sleep(3);

        // A later reader must resolve the abandoned lock instead of blocking
        // on it: the rolled-back put is invisible, so the read returns null.
        $txnB = $this->testClient->begin(['pessimistic' => false]);
        $this->assertNull($txnB->get($key));
        $txnB->rollback();

        unset($txnA);
    }

    // ========================================================================
    //  Multi-region TxnKV workload (GAP-01 fix validation)
    // ========================================================================

    /**
     * Runs a transactional workload against a transactional keyspace
     * pre-split into at least three regions.
     *
     * TiKV stores transactional region boundaries in a memory-comparable
     * encoded form, so a region lookup that compares the raw user key against
     * those boundaries misroutes once the transactional keyspace covers more
     * than one region — TiKV then rejects the request with a "Key ... is out
     * of [region ...]" (KeyNotInRegion-class) error. The GAP-01 fix encodes
     * lookup keys with the memory-comparable codec (and losslessly decodes the
     * returned boundaries), so this workload must succeed across regions. The
     * keys deliberately include binary bytes and the exact split-boundary keys
     * to exercise the cases where raw byte ordering differs from the encoded
     * ordering.
     */
    public function testTxnWorkloadAcrossPreSplitRegions(): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];

        $this->splitTxnKeyspaceIntoRegions($pdEndpoints, 3);

        // Keys spread across the three regions. "\x01\x00" and "mrr" are the
        // exact split boundaries (their MCE encodings are the regions' start
        // keys), so a raw pre-fix lookup misroutes them; the binary keys
        // exercise the byte-ordering divergence directly. All keys stay below
        // the 0x72 ('r') keyspace-namespace boundary so the workload range maps
        // onto exactly the three split regions and nothing else.
        $keys = [
            "\x00",
            "\x01\x00",
            "\x01\x00mid-" . uniqid(),
            'mrr',
            'mrrX-' . uniqid(),
            'qzzz-' . uniqid(),
        ];
        foreach ($keys as $key) {
            $this->keysToCleanup[] = $key;
        }

        $txn = $this->testClient->begin(['pessimistic' => false]);
        foreach ($keys as $i => $key) {
            $txn->set($key, 'value-' . $i);
        }
        $txn->commit();
        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());

        // Reads across the regions.
        foreach ($keys as $i => $key) {
            $read = $this->testClient->begin(['pessimistic' => false]);
            $this->assertSame('value-' . $i, $read->get($key), sprintf('read-back of key %s', bin2hex($key)));
            $read->rollback();
        }

        // Batch-read across the regions (exercises the scanRegions()-based
        // batch resolve path through RegionResolver::batchResolveRegions()).
        $batch = $this->testClient->begin(['pessimistic' => false]);
        $values = $batch->batchGet($keys);
        $batch->rollback();

        foreach ($keys as $i => $key) {
            $this->assertSame('value-' . $i, $values[$key] ?? null, sprintf('batchGet of key %s', bin2hex($key)));
        }

        // Forward scan across all three regions. With the lossless MCE decoder
        // RegionCache stores the true user-key boundaries, so each per-region
        // sub-scan starts exactly at the split boundary ("\x01\x00" / "mrr").
        // The old escape decoder stopped at the first 0x00 0x00 and produced
        // "\x01", one byte below the region's lower bound, which TiKV rejects
        // as an invalid range. TxnKV has no reverse-scan API (only RawKV does),
        // so a forward scan is the strongest cross-region scan coverage
        // available for the transactional path.
        $scan = $this->testClient->begin(['pessimistic' => false]);
        $scanned = $scan->scan("\x00", "\x72");
        $scan->rollback();

        $scannedMap = [];
        foreach ($scanned as $entry) {
            $scannedMap[$entry['key']] = $entry['value'];
        }
        foreach ($keys as $i => $key) {
            $this->assertSame('value-' . $i, $scannedMap[$key] ?? null, sprintf('scan of key %s', bin2hex($key)));
        }
    }

    /**
     * Ensure the transactional keyspace hosting the test's workload range
     * covers at least $targetRegionCount regions, splitting it with TiKV's
     * SplitRegion RPC when needed.
     *
     * The splits run through a Mode::Txn connection bundle so region
     * discovery matches the production TxnKV path; the split keys themselves
     * are raw user keys (TiKV encodes the boundary internally).
     *
     * @param string[] $pdEndpoints
     */
    private function splitTxnKeyspaceIntoRegions(array $pdEndpoints, int $targetRegionCount): void
    {
        // The transactional keyspace is discovered and looked up through the
        // same Mode::Txn codec the production client uses: PD reports region
        // boundaries MCE-encoded, so scanRegions()/getRegion() take raw user
        // keys and return decoded boundaries. The split keys themselves stay
        // raw — SplitRegion accepts a raw user key.
        $bundle = ConnectionFactory::create($pdEndpoints, null, [], new CodecV1(Mode::Txn));

        try {
            $regionsForWorkloadRange = (fn(): array => $bundle->pdClient->scanRegions("\x00", "\x72"));

            $splitAt = function (string $withinKey, string $splitKey) use ($bundle): void {
                $region = $bundle->pdClient->getRegion($withinKey);
                $store = $bundle->pdClient->getStore($region->leaderStoreId);
                $this->assertNotNull($store, 'Leader store must be resolvable for SplitRegion');

                $request = new SplitRegionRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($region));
                $request->setSplitKey($splitKey);

                /** @var SplitRegionResponse $response */
                $response = $bundle->grpc->call(
                    (string) $store->getAddress(),
                    'tikvpb.Tikv',
                    'SplitRegion',
                    $request,
                    SplitRegionResponse::class,
                    5000,
                );

                if ($response->hasRegionError()) {
                    throw new \RuntimeException(sprintf(
                        'SplitRegion failed: %s',
                        $response->getRegionError() !== null
                            ? $response->getRegionError()->getMessage()
                            : 'unknown region error',
                    ));
                }
            };

            $attempt = 0;
            while (count($regionsForWorkloadRange()) < $targetRegionCount && $attempt < 5) {
                $attempt++;
                // Split the region covering the workload range into three
                // pieces: [empty, "\x01\x00"), ["\x01\x00", "mrr"), ["mrr", ...).
                $splitAt("\x00", "\x01\x00");
                usleep(200_000);
                $splitAt('mrrX', 'mrr');
                // Wait for PD to learn the new boundaries and re-balance.
                for ($i = 0; $i < 30; $i++) {
                    if (count($regionsForWorkloadRange()) >= $targetRegionCount) {
                        break;
                    }
                    usleep(500_000);
                }
            }

            $this->assertGreaterThanOrEqual(
                $targetRegionCount,
                count($regionsForWorkloadRange()),
                sprintf(
                    'Pre-splitting the transactional keyspace did not produce %d regions',
                    $targetRegionCount,
                ),
            );
        } finally {
            $bundle->grpc->close();
            $bundle->pdClient->close();
        }
    }

    /**
     * Send a bare KvPrewrite for the given key, simulating a transaction
     * abandoned right after its prewrite phase.
     */
    private function prewriteOnly(string $key, int $startTs, string $value): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        // This is a transactional prewrite: region lookups must run in the
        // MCE-encoded key space like TxnKvClient::create() (issue #415).
        $bundle = ConnectionFactory::create($pdEndpoints, null, [], new CodecV1(Mode::Txn));

        try {
            $region = $bundle->pdClient->getRegion($key);
            $store = $bundle->pdClient->getStore($region->leaderStoreId);
            $this->assertNotNull($store, 'Leader store must be resolvable for prewrite');

            $mutation = new Mutation();
            $mutation->setOp(Op::Put);
            $mutation->setKey($key);
            $mutation->setValue($value);

            $request = new PrewriteRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setMutations([$mutation]);
            $request->setPrimaryLock($key);
            $request->setStartVersion($startTs);
            $request->setLockTtl(1000);

            /** @var PrewriteResponse $response */
            $response = $bundle->grpc->call(
                (string) $store->getAddress(),
                'tikvpb.Tikv',
                'KvPrewrite',
                $request,
                PrewriteResponse::class,
                5000,
            );

            RegionErrorHandler::check($response);

            $errors = $response->getErrors();
            $this->assertCount(0, $errors, 'Prewrite for abandoned lock must succeed');
        } finally {
            $bundle->grpc->close();
            $bundle->pdClient->close();
        }
    }
}
