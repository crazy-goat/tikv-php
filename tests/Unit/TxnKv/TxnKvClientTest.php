<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CommandPri;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

class TxnKvClientTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    public function testCreateThrowsOnEmptyPdEndpoints(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('PD endpoints array must not be empty');
        TxnKvClient::create([]);
    }

    public function testConstruction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        $this->assertInstanceOf(TxnKvClient::class, $client);
    }

    public function testGetMetricsReturnsInjectedInstance(): void
    {
        $metrics = new InMemoryMetrics();
        $client = new TxnKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
            metrics: $metrics,
        );

        $this->assertSame($metrics, $client->getMetrics());
    }

    public function testCreatePassesMetricsOptionToClient(): void
    {
        $metrics = new InMemoryMetrics();
        $client = TxnKvClient::create(
            ['pd:2379'],
            options: ['metrics' => $metrics],
        );

        $this->assertSame($metrics, $client->getMetrics());
        $client->close();
    }

    public function testBeginRejectsNonBoolEagerPessimisticLocks(): void
    {
        $client = new TxnKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['eagerPessimisticLocks'] must be a bool");
        $client->begin(['eagerPessimisticLocks' => 'yes']); // @phpstan-ignore argument.type
    }

    public function testBeginDefaultsToPessimistic(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));

        // Issue #182: the issue called the pessimistic default a defect in
        // itself ("Pessimistic is the begin() default"), so the default is a
        // contract of its own. Until this test existed, flipping
        // `$options['pessimistic'] ?? true` to `?? false` passed the whole
        // Unit suite: every other test names the mode explicitly, and the two
        // mode tests below pass it in.
        $txn = $client->begin();

        $this->assertTrue($txn->isPessimistic());
        $txn->rollback();
    }

    public function testBeginDefaultsToEagerPessimisticLocks(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));

        $txn = $client->begin();
        $property = new \ReflectionProperty(Transaction::class, 'eagerPessimisticLocks');

        $this->assertTrue($property->getValue($txn));
        $txn->rollback();
    }

    public function testBeginCanDisableEagerPessimisticLocks(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));

        $txn = $client->begin(['eagerPessimisticLocks' => false]);
        $property = new \ReflectionProperty(Transaction::class, 'eagerPessimisticLocks');

        $this->assertFalse($property->getValue($txn));
        $txn->rollback();
    }

    public function testCloseThrowsOnBeginAfterClose(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->begin();
    }

    public function testBeginReturnsActiveTransaction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin();

        $this->assertInstanceOf(Transaction::class, $txn);
        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertSame(1000, $txn->getStartTs());
    }

    public function testBeginWithPessimisticMode(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(2000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin(['pessimistic' => true]);

        $this->assertTrue($txn->isPessimistic());
    }

    public function testBeginWithOptimisticMode(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(3000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin(['pessimistic' => false]);

        $this->assertFalse($txn->isPessimistic());
    }

    // ========================================================================
    // beginOptimistic() / beginPessimistic() — issue #439
    // ========================================================================

    public function testBeginOptimisticCreatesAnOptimisticTransaction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(6000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginOptimistic();

        $this->assertFalse($txn->isPessimistic());
        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertSame(6000, $txn->getStartTs());
    }

    public function testBeginPessimisticCreatesAPessimisticTransaction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(7000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginPessimistic();

        $this->assertTrue($txn->isPessimistic());
        $this->assertSame(TransactionStatus::Active, $txn->getStatus());
        $this->assertSame(7000, $txn->getStartTs());
    }

    public function testBeginOptimisticKeepsTheOtherBeginOptions(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(8000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginOptimistic(['priority' => 5]);

        $this->assertFalse($txn->isPessimistic());
        $this->assertSame(5, $txn->getPriority());
    }

    public function testBeginPessimisticKeepsTheOtherBeginOptions(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(9000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginPessimistic(['priority' => 2]);

        $this->assertTrue($txn->isPessimistic());
        $this->assertSame(2, $txn->getPriority());
    }

    public function testBeginOptimisticAcceptsARedundantPessimisticOption(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(10000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginOptimistic(['pessimistic' => false]);

        $this->assertFalse($txn->isPessimistic());
    }

    public function testBeginOptimisticRejectsAContradictingPessimisticOption(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(11000);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("beginOptimistic() fixes the transaction mode");
        $client->beginOptimistic(['pessimistic' => true]);
    }

    public function testBeginPessimisticRejectsAContradictingPessimisticOption(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(12000);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("beginPessimistic() fixes the transaction mode");
        $client->beginPessimistic(['pessimistic' => false]);
    }

    public function testBeginOptimisticThrowsAfterClose(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->beginOptimistic();
    }

    public function testBeginOptimisticReportsTheClosedClientBeforeAContradictingOption(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();

        // The closed client comes first, exactly as in begin(): the mode
        // check must not hide the reason why no transaction can be started.
        $this->expectException(ClientClosedException::class);
        $client->beginOptimistic(['pessimistic' => true]);
    }

    public function testBeginOptimisticSharesTheValidationOfBegin(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(13000);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        // The option handling stays in begin(), so the new entry points
        // reject a malformed option with the same error as begin() does.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['enable1Pc'] must be a bool");
        $client->beginOptimistic(['enable1Pc' => 'yes']); // @phpstan-ignore argument.type
    }

    public function testBeginPessimisticForwardsTheLockWaitTime(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(14000);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginPessimistic(['lockWaitTimeMs' => 250]);
        $property = new \ReflectionProperty(Transaction::class, 'lockWaitTimeMs');

        $this->assertSame(250, $property->getValue($txn));
    }

    public function testBeginOptimisticForwardsTheLockWaitTime(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(15000);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->beginOptimistic(['lockWaitTimeMs' => 250]);
        $property = new \ReflectionProperty(Transaction::class, 'lockWaitTimeMs');

        $this->assertSame(250, $property->getValue($txn));
    }

    // ========================================================================
    // Cluster ID
    // ========================================================================

    public function testGetClusterIdReturnsNullWhenNotDiscovered(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        $this->assertNull($client->getClusterId());
    }

    public function testGetClusterIdReturnsDiscoveredId(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(67890);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);

        $this->assertSame(67890, $client->getClusterId());
    }

    // ========================================================================
    // close()
    // ========================================================================

    public function testCloseCallsGrpcAndPdClientClose(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->expects($this->once())->method('close');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())->method('close');

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();
    }

    public function testCloseIdempotent(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->expects($this->once())->method('close');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())->method('close');

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();
        $client->close(); // second close must not throw or call close again
    }

    public function testCloseContinuesWhenGrpcCloseThrows(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->expects($this->once())->method('close');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('grpc boom'));

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();
    }

    public function testCloseContinuesWhenPdCloseThrows(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('pd boom'));

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())->method('close');

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();
    }

    public function testCloseSwallowsBothSubCloseExceptions(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('pd boom'));

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('grpc boom'));

        $client = new TxnKvClient($pdClient, $grpc);
        $client->close();
    }

    // ========================================================================
    // begin() — priority propagation
    // ========================================================================

    public function testBeginWithPriorityReachesTheRpcContext(): void
    {
        // The priority option must reach the wire, not only the transaction
        // object: the previous version of this test (then named
        // testBeginWithPriorityPropagatesToTransaction) asserted
        // $txn->getPriority() alone — a constructor round-trip that stayed
        // green while the value never left the client (issue #336).
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(4000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->method('getRegion')->willReturn($this->testRegion());
        $pdClient->method('getStore')->willReturn($this->testStore());

        $capturedRequests = [];
        $grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($grpc);
        $grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$capturedRequests): object {
                if (in_array($method, ['KvGet', 'KvPrewrite', 'KvCommit'], true)) {
                    $capturedRequests[$method] = $request;
                }

                return match ($method) {
                    'KvGet' => new GetResponse(),
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin(['pessimistic' => false, 'priority' => CommandPri::High]);
        $txn->get('k1');
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(CommandPri::High, $txn->getPriority());

        $capturedMethods = array_keys($capturedRequests);
        sort($capturedMethods);
        $this->assertSame(['KvCommit', 'KvGet', 'KvPrewrite'], $capturedMethods);

        $getRequest = $capturedRequests['KvGet'];
        $this->assertInstanceOf(GetRequest::class, $getRequest);
        $getContext = $getRequest->getContext();
        $this->assertNotNull($getContext);
        $this->assertSame(CommandPri::High, $getContext->getPriority(), 'priority missing on KvGet');

        $prewriteRequest = $capturedRequests['KvPrewrite'];
        $this->assertInstanceOf(PrewriteRequest::class, $prewriteRequest);
        $prewriteContext = $prewriteRequest->getContext();
        $this->assertNotNull($prewriteContext);
        $this->assertSame(CommandPri::High, $prewriteContext->getPriority(), 'priority missing on KvPrewrite');

        $commitRequest = $capturedRequests['KvCommit'];
        $this->assertInstanceOf(CommitRequest::class, $commitRequest);
        $commitContext = $commitRequest->getContext();
        $this->assertNotNull($commitContext);
        $this->assertSame(CommandPri::High, $commitContext->getPriority(), 'priority missing on KvCommit');
    }

    private function testRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
    }

    private function testStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        return $store;
    }

    public function testBeginWithoutPriorityDefaultsToZero(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(5000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin();

        $this->assertSame(0, $txn->getPriority());
    }

    // ========================================================================
    // begin() — pessimistic-lock wait timeout (issue #443, DIV-07)
    // ========================================================================

    public function testBeginWithLockWaitTimePropagatesToTransaction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(6000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin(['lockWaitTimeMs' => 250]);
        $property = new \ReflectionProperty(Transaction::class, 'lockWaitTimeMs');

        $this->assertSame(250, $property->getValue($txn));
    }

    public function testBeginWithoutLockWaitTimeUsesTheDefault(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(6000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin();
        $property = new \ReflectionProperty(Transaction::class, 'lockWaitTimeMs');

        $this->assertSame(Transaction::DEFAULT_LOCK_WAIT_TIME_MS, $property->getValue($txn));
    }

    public function testBeginRejectsNonIntLockWaitTime(): void
    {
        $client = new TxnKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['lockWaitTimeMs'] must be an int");
        $client->begin(['lockWaitTimeMs' => '250']); // @phpstan-ignore argument.type
    }

    public function testBeginRejectsNegativeLockWaitTime(): void
    {
        $client = new TxnKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['lockWaitTimeMs'] must be >= 0");
        $client->begin(['lockWaitTimeMs' => -1]);
    }
}
