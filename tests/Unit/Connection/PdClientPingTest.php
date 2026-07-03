<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\GetMembersResponse;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use PHPUnit\Framework\TestCase;

class PdClientPingTest extends TestCase
{
    public function testImplementsInterface(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $client = new PdClient($grpc, 'pd:2379');
        $this->assertInstanceOf(PdClientInterface::class, $client);
    }

    public function testPingReturnsClusterIdFromResponseHeader(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(7777);

        $response = new GetMembersResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with('pd:2379', 'pdpb.PD', 'GetMembers')
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $clusterId = $client->ping();

        $this->assertSame(7777, $clusterId);
        $this->assertSame(7777, $client->getClusterId());
    }

    public function testPingReturnsNullWhenResponseHasNoHeader(): void
    {
        $response = new GetMembersResponse();

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $this->assertNull($client->ping());
    }

    public function testPingPropagatesGrpcError(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new GrpcException('connection refused', 14));

        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('connection refused');
        $client->ping();
    }
}
