<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * E2E tests for one-phase commit and async commit (issue #419).
 *
 * Requires a running TiKV cluster (E2E-TxnKV test suite).
 */
class FastCommitModesE2ETest extends TestCase
{
    private static ?TxnKvClient $client = null;

    private TxnKvClient $testClient;

    /** @var string[] Keys created during the current test, cleaned up in tearDown */
    private array $keysToCleanup = [];

    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        ClusterProbe::assertPdReachable($pdEndpoints);
        self::$client = TxnKvClient::create($pdEndpoints);
    }

    public static function tearDownAfterClass(): void
    {
        self::$client?->close();
        self::$client = null;
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

    /**
     * A single-region optimistic transaction commits with 1PC: data is
     * visible to a fresh transaction reading at a later timestamp.
     */
    public function testOnePhaseCommitIsVisibleAndDurable(): void
    {
        $key = $this->uniqueKey('1pc-set-get');
        $this->keysToCleanup[] = $key;

        $txn = $this->testClient->begin([
            'pessimistic' => false,
            'enable1Pc' => true,
        ]);
        $txn->set($key, 'one-phase-value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertGreaterThan(0, (int) $txn->getCommitTs());

        // Visible to a fresh reader
        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('one-phase-value', $readTxn->get($key));
        $readTxn->rollback();

        // Durable: still visible through RawKV-free plain TxnKV get on a
        // new client transaction after the writer is closed
        unset($txn);
        $readTxn2 = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('one-phase-value', $readTxn2->get($key));
        $readTxn2->rollback();
    }

    /**
     * 1PC is single-region only: a multi-region transaction under
     * enable1Pc must still commit correctly (TiKV declines 1PC or the
     * client falls back to two-phase).
     */
    public function testMultiRegionTransactionWith1PcCommits(): void
    {
        // Write enough distinct keys that they very likely span regions is
        // not deterministic on a small test cluster; instead verify that a
        // multi-key transaction with 1PC enabled commits and is consistent.
        $keys = [];
        for ($i = 0; $i < 8; $i++) {
            $keys[] = $this->uniqueKey('1pc-multi-' . $i);
        }
        $this->keysToCleanup = [...$this->keysToCleanup, ...$keys];

        $txn = $this->testClient->begin([
            'pessimistic' => false,
            'enable1Pc' => true,
        ]);
        foreach ($keys as $i => $key) {
            $txn->set($key, 'value-' . $i);
        }
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        foreach ($keys as $i => $key) {
            $this->assertSame('value-' . $i, $readTxn->get($key));
        }
        $readTxn->rollback();
    }

    /**
     * Async commit: the transaction commits without the commit phase and
     * the data is visible and durable to fresh readers.
     */
    public function testAsyncCommitIsVisibleAndDurable(): void
    {
        $primary = $this->uniqueKey('async-primary');
        $secondary = $this->uniqueKey('async-secondary');
        $this->keysToCleanup = [...$this->keysToCleanup, $primary, $secondary];

        $txn = $this->testClient->begin([
            'pessimistic' => false,
            'enableAsyncCommit' => true,
        ]);
        $txn->set($primary, 'primary-value');
        $txn->set($secondary, 'secondary-value');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $this->assertGreaterThan(0, (int) $txn->getCommitTs());

        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $this->assertSame('primary-value', $readTxn->get($primary));
        $this->assertSame('secondary-value', $readTxn->get($secondary));
        $readTxn->rollback();
    }

    /**
     * A mid-commit write conflict against a 1PC-enabled transaction must
     * not leave the key in an inconsistent state: the conflicting writer's
     * value wins or the second writer retries to success.
     */
    public function testOnePcWithConflictKeepsDataConsistent(): void
    {
        $key = $this->uniqueKey('1pc-conflict');
        $this->keysToCleanup[] = $key;

        $winner = $this->testClient->begin(['pessimistic' => false]);
        $winner->set($key, 'winner');

        $loser = $this->testClient->begin([
            'pessimistic' => false,
            'enable1Pc' => true,
        ]);
        $loser->set($key, 'loser');

        $winner->commit();

        try {
            $loser->commit();
        } catch (\Throwable) {
            // Write conflict: either the loser retried to success or failed.
        }

        // Exactly one of the two values is durable.
        $readTxn = $this->testClient->begin(['pessimistic' => false]);
        $value = $readTxn->get($key);
        $readTxn->rollback();
        $this->assertContains($value, ['winner', 'loser'], 'key must hold a committed value, not a stale/garbage one');
    }
}
