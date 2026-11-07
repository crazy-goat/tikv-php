<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use CrazyGoat\TiKV\Client\Util\ShutdownState;
use CrazyGoat\TiKV\Tests\Unit\Support\RecordingLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for issue #226 (TXN-21): TxnKvClient::close() rolls the
 * transactions it began back before it releases the transport and the PD
 * client they depend on.
 */
class TxnKvClientCloseTransactionTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RecordingLogger $logger;
    private RegionInfo $region;

    /** @var list<string> */
    private array $rpcMethods = [];

    protected function setUp(): void
    {
        $this->region = new RegionInfo(
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
        $this->pdClient->method('getTimestamp')->willReturn(5000);
        $this->pdClient->method('getClusterId')->willReturn(null);
        $this->pdClient->method('getStore')->willReturn($this->store());
        $this->pdClient->method('getRegion')->willReturn($this->region);
        $this->pdClient->method('scanRegions')->willReturn([$this->region]);

        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->regionCache->method('put');

        $this->logger = new RecordingLogger();

        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
    }

    public function testCloseRollsBackLivePessimisticTransactionBeforeClosingTransport(): void
    {
        $this->stubRpcResponses();
        $this->grpc->method('close')->willReturnCallback(function (): void {
            $this->rpcMethods[] = 'close';
        });
        $this->pdClient->method('close')->willReturnCallback(function (): void {
            $this->rpcMethods[] = 'pd-close';
        });

        $client = $this->createClient();
        $transaction = $client->beginPessimistic();
        $transaction->set('key1', 'value1');

        $client->close();

        $this->assertSame(TransactionStatus::RolledBack, $transaction->getStatus());
        $this->assertContains('KVPessimisticRollback', $this->rpcMethods);
        $this->assertContains('KvBatchRollback', $this->rpcMethods);
        $this->assertSame(
            ['close', 'pd-close'],
            array_slice($this->rpcMethods, -2),
            'the transport and the PD client must be closed after the rollback RPC',
        );
    }

    /**
     * Issue #287 (GRPC-28): once PHP is shutting down, close() must not
     * issue a rollback RPC — ext-grpc may already be past module shutdown.
     * The transaction keeps its locks until the lock TTL expires.
     */
    public function testCloseSkipsLiveTransactionRollbackDuringShutdown(): void
    {
        $this->stubRpcResponses();
        $client = $this->createClient();
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');

        ShutdownState::markShutdown();
        try {
            $client->close();

            $this->assertSame(TransactionStatus::Active, $transaction->getStatus());
            $this->assertSame([], $this->rpcMethods, 'close() must not issue a rollback RPC during shutdown');

            // Destroy the transaction while the flag is still set, so its
            // destructor also skips the rollback RPC.
            unset($transaction);
        } finally {
            ShutdownState::resetForTests();
        }
    }

    public function testCloseDoesNotRollBackCommittedTransaction(): void
    {
        $this->stubRpcResponses();
        $closeCalls = 0;
        $this->grpc->method('close')->willReturnCallback(function () use (&$closeCalls): void {
            $closeCalls++;
        });

        $client = $this->createClient();
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');
        $transaction->commit();

        $this->assertSame(TransactionStatus::Committed, $transaction->getStatus());

        $client->close();

        $this->assertSame(1, $closeCalls);
        $this->assertNotContains('KvBatchRollback', $this->rpcMethods);
        $this->assertSame(TransactionStatus::Committed, $transaction->getStatus());
    }

    public function testCloseDoesNotRollBackAnExplicitlyRolledBackTransaction(): void
    {
        $this->stubRpcResponses();

        $client = $this->createClient();
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');
        $transaction->rollback();

        $this->assertSame(1, count(array_keys($this->rpcMethods, 'KvBatchRollback', true)));

        $client->close();

        $this->assertSame(
            1,
            count(array_keys($this->rpcMethods, 'KvBatchRollback', true)),
            'close() must not roll a transaction back a second time',
        );
    }

    public function testCloseLogsRollbackFailureAndDoesNotThrow(): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method): object {
                $this->rpcMethods[] = $method;

                if ($method === 'KvBatchRollback') {
                    throw new GrpcException('rollback boom', 14);
                }

                return match ($method) {
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException('Unexpected method: ' . $method),
                };
            },
        );
        $this->grpc->method('close');

        $client = $this->createClient(maxBackoffMs: 0);
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');

        $client->close();

        $this->assertContains('Transaction rollback failed during client close', $this->logger->messages);
        $this->assertSame(
            TransactionStatus::Active,
            $transaction->getStatus(),
            'a failed close() rollback leaves the transaction to its destructor',
        );
    }

    public function testCloseRollsBackTheRemainingTransactionsWhenOneFails(): void
    {
        $rollbackCalls = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$rollbackCalls): object {
                $this->rpcMethods[] = $method;

                if ($method === 'KvBatchRollback') {
                    $rollbackCalls++;
                    if ($rollbackCalls === 1) {
                        throw new GrpcException('rollback boom', 14);
                    }
                }

                return match ($method) {
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException('Unexpected method: ' . $method),
                };
            },
        );
        $this->grpc->method('close');

        $client = $this->createClient(maxBackoffMs: 0);
        $failing = $client->beginOptimistic();
        $failing->set('key1', 'value1');
        $succeeding = $client->beginOptimistic();
        $succeeding->set('key2', 'value2');

        $client->close();

        $this->assertSame(TransactionStatus::Active, $failing->getStatus());
        $this->assertSame(TransactionStatus::RolledBack, $succeeding->getStatus());
        $this->assertContains('Transaction rollback failed during client close', $this->logger->messages);
    }

    public function testCloseRollsBackLiveTransactionOnlyOnce(): void
    {
        $this->stubRpcResponses();
        $closeCalls = 0;
        $this->grpc->method('close')->willReturnCallback(function () use (&$closeCalls): void {
            $closeCalls++;
        });

        $client = $this->createClient();
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');

        $client->close();
        $client->close();

        $this->assertSame(1, $closeCalls);
        $this->assertSame(1, count(array_keys($this->rpcMethods, 'KvBatchRollback', true)));
        $this->assertSame(TransactionStatus::RolledBack, $transaction->getStatus());
    }

    public function testCloseRollsBackLiveTransactionWithASharedTransport(): void
    {
        $this->stubRpcResponses();
        $this->grpc->expects($this->never())->method('close');

        $client = $this->createClient(ownsGrpcClient: false);
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');

        $client->close();

        $this->assertSame(TransactionStatus::RolledBack, $transaction->getStatus());
        $this->assertContains('KvBatchRollback', $this->rpcMethods);
    }

    /**
     * @return iterable<string, array{\Closure(Transaction): void}>
     */
    public static function postCloseOperationProvider(): iterable
    {
        yield 'commit' => [static function (Transaction $txn): void {
            $txn->commit();
        }];
        yield 'rollback' => [static function (Transaction $txn): void {
            $txn->rollback();
        }];
        yield 'set' => [static function (Transaction $txn): void {
            $txn->set('key2', 'value2');
        }];
        yield 'delete' => [static function (Transaction $txn): void {
            $txn->delete('key1');
        }];
        yield 'get' => [static function (Transaction $txn): void {
            $txn->get('key1');
        }];
        yield 'batchGet' => [static function (Transaction $txn): void {
            $txn->batchGet(['key1']);
        }];
        yield 'scan' => [static function (Transaction $txn): void {
            $txn->scan('a', 'z');
        }];
        yield 'heartbeat' => [static function (Transaction $txn): void {
            $txn->heartbeat();
        }];
    }

    /**
     * A transaction the client rolled back during close() is terminal: using
     * it afterwards is a lifecycle error, not a second RPC (issue #342).
     *
     * @param \Closure(Transaction): void $operation
     */
    #[DataProvider('postCloseOperationProvider')]
    public function testTransactionCannotBeUsedAfterClientClose(\Closure $operation): void
    {
        $this->stubRpcResponses();
        $this->grpc->method('close');

        $client = $this->createClient();
        $transaction = $client->beginOptimistic();
        $transaction->set('key1', 'value1');

        $client->close();

        $this->assertSame(TransactionStatus::RolledBack, $transaction->getStatus());

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $operation($transaction);
    }

    private function createClient(bool $ownsGrpcClient = true, int $maxBackoffMs = 20000): TxnKvClient
    {
        return new TxnKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxBackoffMs: $maxBackoffMs,
            logger: $this->logger,
            ownsGrpcClient: $ownsGrpcClient,
        );
    }

    private function stubRpcResponses(): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method): object {
                $this->rpcMethods[] = $method;

                return match ($method) {
                    'KvPessimisticLock' => new PessimisticLockResponse(),
                    'KVPessimisticRollback' => new PessimisticRollbackResponse(),
                    'KvBatchRollback' => new BatchRollbackResponse(),
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException('Unexpected method: ' . $method),
                };
            },
        );
    }

    private function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        return $store;
    }
}
