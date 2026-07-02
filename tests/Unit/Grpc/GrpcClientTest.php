<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use PHPUnit\Framework\TestCase;

class GrpcClientTest extends TestCase
{
    use GrpcExtensionGate;

    private GrpcClient $client;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
        $this->client = new GrpcClient();
    }

    protected function tearDown(): void
    {
        if (isset($this->client)) {
            $this->client->close();
        }
    }

    public function testImplementsInterface(): void
    {
        $this->assertInstanceOf(GrpcClientInterface::class, $this->client);
    }

    public function testCloseIsIdempotent(): void
    {
        $this->client->close();
        $this->client->close();
        $this->expectNotToPerformAssertions();
    }

    public function testCallWithInvalidAddressThrowsGrpcException(): void
    {
        $this->expectException(GrpcException::class);

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test');

        $this->client->call(
            'invalid-address:99999',
            'tikvpb.Tikv',
            'RawGet',
            $request,
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
            timeoutMs: 1000,
        );
    }

    public function testCloseContinuesAfterChannelCloseThrows(): void
    {
        $throwing = $this->createMock(\Grpc\Channel::class);
        $throwing->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('boom'));

        $ok = $this->createMock(\Grpc\Channel::class);
        $ok->expects($this->once())->method('close');

        $this->injectChannels([
            'addr-throws:1' => $throwing,
            'addr-ok:1' => $ok,
        ]);

        $this->client->close();

        $this->assertSame([], $this->readChannels());
    }

    public function testCloseResetsChannelsEvenWhenAllThrow(): void
    {
        $throwing = $this->createMock(\Grpc\Channel::class);
        $throwing->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('boom'));

        $this->injectChannels(['addr:1' => $throwing]);

        $this->client->close();

        $this->assertSame([], $this->readChannels());
    }

    public function testCloseIsIdempotentAfterPartialFailure(): void
    {
        $throwing = $this->createMock(\Grpc\Channel::class);
        $throwing->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('boom'));

        $this->injectChannels(['addr:1' => $throwing]);

        $this->client->close();
        $this->client->close();
    }

    /**
     * @param array<string, \Grpc\Channel> $channels
     */
    private function injectChannels(array $channels): void
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        $prop->setValue($this->client, $channels);
    }

    /**
     * @return array<string, \Grpc\Channel>
     */
    private function readChannels(): array
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        /** @var array<string, \Grpc\Channel> $value */
        $value = $prop->getValue($this->client);
        return $value;
    }
}
