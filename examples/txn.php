<?php
require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;

/**
 * Move $amount from one account to another atomically, retrying on conflict.
 *
 * The retry has to build a NEW transaction: the failed one holds a
 * startTs that has already lost the race, so re-running the same object can
 * never succeed.
 */
function transfer(TxnKvClient $client, string $from, string $to, int $amount): void
{
    for ($attempt = 1; ; $attempt++) {
        $txn = $client->begin(); // pessimistic by default
        try {
            $fromBalance = (int) $txn->get($from);
            $toBalance = (int) $txn->get($to);

            if ($fromBalance < $amount) {
                throw new RuntimeException("Insufficient funds in {$from}");
            }

            $txn->set($from, (string) ($fromBalance - $amount));
            $txn->set($to, (string) ($toBalance + $amount));
            $txn->commit();

            return;
        } catch (TransactionConflictException | DeadlockException | LockWaitTimeoutException | TxnRetryableException $e) {
            if ($txn->getStatus() === TransactionStatus::Active) {
                $txn->rollback();
            }
            if ($attempt >= 5) {
                throw $e;
            }
            echo "  Conflict on attempt {$attempt} ({$e->getMessage()}), retrying...\n";
            usleep(50_000 * $attempt);
        } catch (Throwable $e) {
            if ($txn->getStatus() === TransactionStatus::Active) {
                $txn->rollback();
            }
            throw $e;
        }
    }
}

$pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', getenv('PD_ENDPOINTS')) : ['127.0.0.1:2379'];
$client = TxnKvClient::create($pdEndpoints);

try {
    echo "TiKV PHP Client - Transaction Example\n";
    echo "======================================\n\n";

    echo "Seeding accounts...\n";
    $seed = $client->begin();
    $seed->set('txn:account:alice', '1000');
    $seed->set('txn:account:bob', '0');
    $seed->commit();
    echo "✓ alice=1000 bob=0\n\n";

    echo "Transferring 100 from alice to bob...\n";
    transfer($client, 'txn:account:alice', 'txn:account:bob', 100);
    echo "✓ committed\n\n";

    echo "Reading both accounts back in one snapshot...\n";
    $read = $client->begin();
    echo "  alice = " . $read->get('txn:account:alice') . "\n";
    echo "  bob   = " . $read->get('txn:account:bob') . "\n";
    echo "  startTs = " . $read->getStartTs() . "\n";
    echo "  status   = " . $read->getStatus()->name . "\n";
    $read->rollback();
    echo "\n";

    echo "Rolling back an uncommitted transfer...\n";
    $aborted = $client->begin();
    $aborted->set('txn:account:alice', '999999');
    $aborted->rollback();
    echo "  status after rollback = " . $aborted->getStatus()->name . "\n";
    $check = $client->begin();
    echo "  alice is still " . $check->get('txn:account:alice') . "\n";
    $check->rollback();
    echo "\n";

    $cleanup = $client->begin();
    $cleanup->delete('txn:account:alice');
    $cleanup->delete('txn:account:bob');
    $cleanup->commit();

    echo "All transaction operations completed successfully!\n";
} catch (Throwable $e) {
    echo 'Error: ' . $e->getMessage() . "\n";
    echo 'Stack trace:' . "\n" . $e->getTraceAsString() . "\n";
    exit(1);
} finally {
    $client->close();
}
