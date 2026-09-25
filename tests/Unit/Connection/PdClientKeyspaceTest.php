<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Keyspacepb\KeyspaceMeta;
use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceRequest;
use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceResponse;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;

final class PdClientKeyspaceTest extends TestCase
{
    public function testLoadsKeyspaceThroughKeyspaceService(): void
    {
        $meta = new KeyspaceMeta();
        $meta->setId(42);
        $meta->setName('tenant-a');
        $response = new LoadKeyspaceResponse();
        $response->setKeyspace($meta);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use ($response): Message {
                $this->assertSame('pd:2379', $address);
                $this->assertSame('keyspacepb.Keyspace', $service);
                $this->assertSame('LoadKeyspace', $method);
                $this->assertSame(LoadKeyspaceResponse::class, $responseClass);
                $this->assertInstanceOf(LoadKeyspaceRequest::class, $request);
                $this->assertSame('tenant-a', $request->getName());
                $this->assertNotNull($request->getHeader());
                $this->assertSame(0, $request->getHeader()->getClusterId());

                return $response;
            });

        $this->assertSame(42, (new PdClient($grpc, 'pd:2379'))->getKeyspaceId('tenant-a'));
    }
}
