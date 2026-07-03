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
    private GrpcClient $insecureClient;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
        $this->client = new GrpcClient();
        $this->insecureClient = new GrpcClient(allowInsecure: true);
    }

    protected function tearDown(): void
    {
        if (isset($this->client)) {
            $this->client->close();
        }
        if (isset($this->insecureClient)) {
            $this->insecureClient->close();
        }
    }

    public function testImplementsInterface(): void
    {
        $this->assertInstanceOf(GrpcClientInterface::class, $this->client);
    }

    public function testCloseIsIdempotent(): void
    {
        $this->insecureClient->close();
        $this->insecureClient->close();
        $this->expectNotToPerformAssertions();
    }

    public function testCallWithInvalidAddressThrowsGrpcException(): void
    {
        $this->expectException(GrpcException::class);

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test');

        $this->insecureClient->call(
            'invalid-address:99999',
            'tikvpb.Tikv',
            'RawGet',
            $request,
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
            timeoutMs: 1000,
        );
    }

    public function testRejectsInsecureConnectionByDefault(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidStateException::class);
        $this->expectExceptionMessage('TLS is not configured');

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test');

        $this->client->call(
            'some-address:20160',
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

        $this->injectChannels($this->insecureClient, [
            'addr-throws:1' => $throwing,
            'addr-ok:1' => $ok,
        ]);

        $this->insecureClient->close();

        $this->assertSame([], $this->readChannels($this->insecureClient));
    }

    public function testCloseResetsChannelsEvenWhenAllThrow(): void
    {
        $throwing = $this->createMock(\Grpc\Channel::class);
        $throwing->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('boom'));

        $this->injectChannels($this->insecureClient, ['addr:1' => $throwing]);

        $this->insecureClient->close();

        $this->assertSame([], $this->readChannels($this->insecureClient));
    }

    public function testCloseIsIdempotentAfterPartialFailure(): void
    {
        $throwing = $this->createMock(\Grpc\Channel::class);
        $throwing->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('boom'));

        $this->injectChannels($this->insecureClient, ['addr:1' => $throwing]);

        $this->insecureClient->close();
        $this->insecureClient->close();
    }

    /**
     * @param array<string, \Grpc\Channel> $channels
     */
    private function injectChannels(GrpcClient $client, array $channels): void
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        $prop->setValue($client, $channels);
    }

    /**
     * @return array<string, \Grpc\Channel>
     */
    private function readChannels(GrpcClient $client): array
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        /** @var array<string, \Grpc\Channel> $value */
        $value = $prop->getValue($client);
        return $value;
    }
}
