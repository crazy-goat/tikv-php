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

    public function testDefaultMaxChannels(): void
    {
        $ref = new \ReflectionProperty(GrpcClient::class, 'maxChannels');
        $val = $ref->getValue($this->client);
        $this->assertSame(64, $val);
    }

    public function testDefaultIdleTtlMs(): void
    {
        $ref = new \ReflectionProperty(GrpcClient::class, 'idleTtlMs');
        $val = $ref->getValue($this->client);
        $this->assertSame(600000, $val);
    }

    public function testCustomMaxChannels(): void
    {
        $client = new GrpcClient(maxChannels: 10);
        $ref = new \ReflectionProperty(GrpcClient::class, 'maxChannels');
        $val = $ref->getValue($client);
        $this->assertSame(10, $val);
        $client->close();
    }

    public function testMaxChannelsMustBeAtLeastOne(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        new GrpcClient(maxChannels: 0);
    }

    public function testCustomIdleTtlMs(): void
    {
        $client = new GrpcClient(idleTtlMs: 5000);
        $ref = new \ReflectionProperty(GrpcClient::class, 'idleTtlMs');
        $val = $ref->getValue($client);
        $this->assertSame(5000, $val);
        $client->close();
    }

    public function testChannelCountStartsAtZero(): void
    {
        $this->assertSame(0, $this->client->getChannelCount());
    }

    public function testGetChannelIncreasesCount(): void
    {
        $channel = $this->client->getChannel('127.0.0.1:20160');
        $this->assertInstanceOf(\Grpc\Channel::class, $channel);
        $this->assertSame(1, $this->client->getChannelCount());
    }

    public function testGetChannelReturnsSameChannelForSameAddress(): void
    {
        $channel1 = $this->client->getChannel('127.0.0.1:20160');
        $channel2 = $this->client->getChannel('127.0.0.1:20160');
        $this->assertSame($channel1, $channel2);
        $this->assertSame(1, $this->client->getChannelCount());
    }

    public function testGetChannelCreatesDifferentChannelForDifferentAddress(): void
    {
        $channel1 = $this->client->getChannel('127.0.0.1:20160');
        $channel2 = $this->client->getChannel('127.0.0.1:20161');
        $this->assertNotSame($channel1, $channel2);
        $this->assertSame(2, $this->client->getChannelCount());
    }

    public function testCloseChannelRemovesFromCache(): void
    {
        $this->client->getChannel('127.0.0.1:20160');
        $this->assertSame(1, $this->client->getChannelCount());

        $this->client->closeChannel('127.0.0.1:20160');
        $this->assertSame(0, $this->client->getChannelCount());
    }

    public function testGetChannelRecreatesAfterClose(): void
    {
        $channel1 = $this->client->getChannel('127.0.0.1:20160');
        $this->client->closeChannel('127.0.0.1:20160');
        $channel2 = $this->client->getChannel('127.0.0.1:20160');
        $this->assertNotSame($channel1, $channel2);
    }

    public function testDestructClosesAllChannels(): void
    {
        $client = new GrpcClient();
        $client->getChannel('127.0.0.1:20160');
        $client->getChannel('127.0.0.1:20161');
        $this->assertSame(2, $client->getChannelCount());

        $client->__destruct();
        $this->assertSame(0, $client->getChannelCount());
    }

    public function testMaxChannelsEvictsLru(): void
    {
        // Create a client with maxChannels=2 for testing
        $client = new GrpcClient(maxChannels: 2);

        // Fill the cache
        $channel1 = $client->getChannel('addr:1');
        $client->getChannel('addr:2');
        $this->assertSame(2, $client->getChannelCount());

        // Access channel1 to make it most recently used
        $client->getChannel('addr:1');

        // Add a third channel, should evict addr:2 (LRU)
        $channel3 = $client->getChannel('addr:3');

        // addr:1 should still be cached, addr:2 evicted, addr:3 added
        $this->assertSame(2, $client->getChannelCount());
        $this->assertSame($channel1, $client->getChannel('addr:1'));
        $this->assertSame($channel3, $client->getChannel('addr:3'));

        $client->close();
    }

    public function testMaxChannelsEvictsCorrectLru(): void
    {
        // Test with maxChannels=3
        $client = new GrpcClient(maxChannels: 3);

        $c1 = $client->getChannel('a:1');
        $c2 = $client->getChannel('b:2');
        $c3 = $client->getChannel('c:3');
        $this->assertSame(3, $client->getChannelCount());

        // Order: a:1 (LRU), b:2, c:3 (MRU)
        // Access c:3 again to make it MRU: a:1 (LRU), b:2, c:3 (MRU)
        $client->getChannel('c:3');

        // Access a:1 to make it MRU: b:2 (LRU), c:3, a:1 (MRU)
        $client->getChannel('a:1');

        // Add d:4, should evict b:2 (LRU)
        $client->getChannel('d:4');

        $this->assertSame(3, $client->getChannelCount());
        // a:1, c:3, d:4 should be cached. b:2 should be evicted.
        $this->assertSame($c1, $client->getChannel('a:1'));
        $this->assertSame($c3, $client->getChannel('c:3'));
        $this->assertNotSame($c2, $client->getChannel('d:4'));

        $client->close();
    }

    /**
     * @param array<string, \Grpc\Channel> $channels
     */
    private function injectChannels(array $channels): void
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        $now = microtime(true);
        $entries = [];
        foreach ($channels as $addr => $channel) {
            $entries[$addr] = [
                'channel' => $channel,
                'lastUsed' => $now,
                'createdAt' => $now,
            ];
        }
        $prop->setValue($this->client, $entries);
    }

    /**
     * @return array<string, \Grpc\Channel>
     */
    private function readChannels(): array
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        /** @var array<string, array{channel: \Grpc\Channel, lastUsed: float, createdAt: float}> $entries */
        $entries = $prop->getValue($this->client);
        $channels = [];
        foreach ($entries as $addr => $entry) {
            $channels[$addr] = $entry['channel'];
        }
        return $channels;
    }
}
