<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * The gRPC transport belongs to whoever created it (issue #407).
 *
 * The transport is long-lived and shared: one instance keeps a channel cache
 * that only pays off when it serves many requests, and it is the natural
 * single transport for a RawKV and a transactional client that talk to the
 * same cluster. Sharing it therefore has to be safe, and safety starts with
 * the ownership rule:
 *
 * - a transport the factory builds belongs to the client, which closes it;
 * - a transport the application supplies (options['grpcClient']) belongs to
 *   the application, which closes it — the client never does, or closing one
 *   client would break every other client sharing the transport;
 * - a client assembled by hand owns the transport it is given and closes it
 *   too, unless it is told otherwise with `ownsGrpcClient: false`.
 *
 * The tests below pin that rule on the observable behaviour (does the
 * transport get closed?) rather than on the flag that carries it, so a
 * refactor that moves the flag cannot silently break the contract.
 *
 * Every test also asserts that the client really uses the transport the test
 * handed it. Without that assertion the ownership tests would pass on a
 * build that ignores options['grpcClient'] altogether — the supplied
 * transport would sit unused, untouched and unclosed, and the test would
 * prove nothing.
 */
class SharedGrpcClientOwnershipTest extends TestCase
{
    public function testClientDoesNotCloseATransportSuppliedByTheApplication(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('close');

        $client = RawKvClient::create(['127.0.0.1:2379'], options: ['grpcClient' => $grpc]);

        $this->assertSame($grpc, $this->transportOf($client), 'the client did not use the supplied transport');

        $client->close();
    }

    public function testTransactionalClientDoesNotCloseATransportSuppliedByTheApplication(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('close');

        $client = TxnKvClient::create(['127.0.0.1:2379'], options: ['grpcClient' => $grpc]);

        $this->assertSame($grpc, $this->transportOf($client), 'the client did not use the supplied transport');

        $client->close();
    }

    public function testClientClosesTheTransportItBuilt(): void
    {
        $client = RawKvClient::create(['127.0.0.1:2379']);
        $transport = $this->transportOf($client);

        $this->assertFalse($this->isClosed($transport));

        $client->close();

        $this->assertTrue($this->isClosed($transport));
    }

    public function testTransactionalClientClosesTheTransportItBuilt(): void
    {
        $client = TxnKvClient::create(['127.0.0.1:2379']);
        $transport = $this->transportOf($client);

        $this->assertFalse($this->isClosed($transport));

        $client->close();

        $this->assertTrue($this->isClosed($transport));
    }

    /**
     * One transport, two clients: both use it, and closing one of them must
     * leave the transport — and therefore the other client — usable.
     */
    public function testTwoClientsCanShareOneTransportAndClosingOneKeepsItOpen(): void
    {
        $grpc = new GrpcClient();
        $options = ['grpcClient' => $grpc];

        $raw = RawKvClient::create(['127.0.0.1:2379'], options: $options);
        $txn = TxnKvClient::create(['127.0.0.1:2379'], options: $options);

        $this->assertSame($grpc, $this->transportOf($raw), 'the RawKV client did not use the shared transport');
        $this->assertSame($grpc, $this->transportOf($txn), 'the transactional client did not use the shared transport');

        $raw->close();
        $this->assertFalse($this->isClosed($grpc), 'closing the RawKV client closed the shared transport');

        $txn->close();
        $this->assertFalse($this->isClosed($grpc), 'closing the transactional client closed the shared transport');

        // The application owns the transport, so it is the one that closes it.
        $grpc->close();
        $this->assertTrue($this->isClosed($grpc));
    }

    /**
     * The documented escape hatch of the hand-assembled path: a client that
     * does not own the transport never closes it, so the application stays
     * its owner (issue #407, issue #204 will flip the default).
     */
    public function testHandAssembledClientDoesNotCloseATransportItDoesNotOwn(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('close');

        $client = new RawKvClient(
            $this->createMock(PdClientInterface::class),
            $grpc,
            ownsGrpcClient: false,
        );

        $client->close();
    }

    /**
     * The transport a client uses, unwrapped from the API V2 decorator the
     * client puts around it.
     */
    private function transportOf(object $client): GrpcClientInterface
    {
        $decorated = (new \ReflectionProperty($client, 'grpc'))->getValue($client);
        self::assertInstanceOf(ApiV2GrpcClient::class, $decorated);

        $transport = (new \ReflectionProperty(ApiV2GrpcClient::class, 'inner'))->getValue($decorated);
        self::assertInstanceOf(GrpcClientInterface::class, $transport);

        return $transport;
    }

    private function isClosed(GrpcClientInterface $transport): bool
    {
        self::assertInstanceOf(GrpcClient::class, $transport);

        $closed = (new \ReflectionProperty(GrpcClient::class, 'closed'))->getValue($transport);
        self::assertIsBool($closed);

        return $closed;
    }
}
