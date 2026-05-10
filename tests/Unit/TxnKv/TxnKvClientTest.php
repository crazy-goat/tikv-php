<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

class TxnKvClientTest extends TestCase
{
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
}
