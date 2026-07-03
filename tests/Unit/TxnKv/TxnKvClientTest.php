<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

class TxnKvClientTest extends TestCase
{
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

    public function testBeginWithPriorityPropagatesToTransaction(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(4000);
        $pdClient->method('getClusterId')->willReturn(null);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $client = new TxnKvClient($pdClient, $grpc);
        $txn = $client->begin(['priority' => 5]);

        $this->assertSame(5, $txn->getPriority());
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
}
