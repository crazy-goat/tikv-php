<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Tests\Unit\Region\TestLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

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

    public function testCallAfterCloseThrowsInvalidStateException(): void
    {
        $this->client->close();

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('gRPC client is closed');

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test');

        $this->client->call(
            '127.0.0.1:20160',
            'tikvpb.Tikv',
            'RawGet',
            $request,
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
        );
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

    /**
     * A non-positive timeout is a configuration error (issue #276, GRPC-17):
     * it used to produce an infinite deadline, which the gRPC C core never
     * expires and PHP cannot interrupt.
     */
    #[DataProvider('provideNonPositiveTimeouts')]
    public function testCallWithANonPositiveTimeoutThrows(int $timeoutMs): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf('timeoutMs must be greater than 0, got %d', $timeoutMs));

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test');

        $this->client->call(
            '127.0.0.1:20160',
            'tikvpb.Tikv',
            'RawGet',
            $request,
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
            timeoutMs: $timeoutMs,
        );
    }

    /**
     * @return array<string, array{int}>
     */
    public static function provideNonPositiveTimeouts(): array
    {
        return [
            'zero' => [0],
            'negative' => [-1],
        ];
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

    // ========================================================================
    // allowInsecure
    // ========================================================================

    public function testAllowInsecureDefaultsToTrue(): void
    {
        $ref = new \ReflectionProperty(GrpcClient::class, 'allowInsecure');
        $val = $ref->getValue($this->client);
        $this->assertTrue($val);
    }

    public function testAllowInsecureCanBeSetToFalse(): void
    {
        $client = new GrpcClient(allowInsecure: false);
        $ref = new \ReflectionProperty(GrpcClient::class, 'allowInsecure');
        $val = $ref->getValue($client);
        $this->assertFalse($val);
        $client->close();
    }

    public function testGetChannelAfterCloseThrowsInvalidStateException(): void
    {
        $this->client->close();

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('gRPC client is closed');

        $this->client->getChannel('127.0.0.1:20160');
    }

    public function testGetChannelAfterCloseDoesNotReopen(): void
    {
        $this->client->getChannel('127.0.0.1:20160');
        $this->assertSame(1, $this->client->getChannelCount());

        $this->client->close();
        $this->assertSame(0, $this->client->getChannelCount());

        try {
            $this->client->getChannel('127.0.0.1:20160');
        } catch (InvalidStateException) {
            // expected
        }

        $this->client->close();

        $this->assertSame(0, $this->client->getChannelCount());
    }

    public function testGetChannelWithAllowInsecureFalseThrowsWithoutTls(): void
    {
        $client = new GrpcClient(allowInsecure: false);
        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('TLS is not configured');
        $client->getChannel('127.0.0.1:20160');
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

    // ========================================================================
    // In-flight call tracking (issue #278)
    // ========================================================================

    public function testBusyChannelsAreNotEvictedWhenTheSoftCapIsExceeded(): void
    {
        $logger = new TestLogger();
        $client = new GrpcClient($logger, maxChannels: 2);

        // Un-awaited calls on two addresses: both channels are busy from
        // now on, and nothing closes them before the futures settle.
        $futureA = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $futureB = $client->callAsync(
            '127.0.0.1:2',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );

        $channels = $this->readChannels($client);
        $channelA = $channels['127.0.0.1:1'];
        $channelB = $channels['127.0.0.1:2'];

        $this->assertSame(2, $client->getChannelCount());
        $this->assertSame(1, $this->inFlightCount($client, '127.0.0.1:1'));
        $this->assertSame(1, $this->inFlightCount($client, '127.0.0.1:2'));

        // The third address does not fit under the cap any more, but both
        // cached channels carry a call: they must not be closed.
        $client->getChannel('127.0.0.1:3');

        $this->assertSame(3, $client->getChannelCount());
        $channels = $this->readChannels($client);
        $this->assertArrayHasKey('127.0.0.1:1', $channels);
        $this->assertArrayHasKey('127.0.0.1:2', $channels);
        $this->assertFalse($this->isChannelClosed($channelA));
        $this->assertFalse($this->isChannelClosed($channelB));

        $warnings = array_map(
            static fn (array $record): string => $record['message'],
            $logger->records(LogLevel::WARNING),
        );
        $this->assertContains(
            'Channel cache at capacity but all channels are busy; exceeding soft cap',
            $warnings,
        );

        unset($futureA, $futureB);
        $client->close();
    }

    public function testSettledChannelIsEvictedNormally(): void
    {
        $client = new GrpcClient(maxChannels: 2);

        $futureA = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $futureB = $client->callAsync(
            '127.0.0.1:2',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );

        $channels = $this->readChannels($client);
        $channelA = $channels['127.0.0.1:1'];
        $channelB = $channels['127.0.0.1:2'];

        $client->getChannel('127.0.0.1:3');
        $this->assertSame(3, $client->getChannelCount());

        // Settle the call on the first address. The endpoint is dead, so
        // the receive fails — but the call is over either way.
        try {
            $futureA->wait();
        } catch (GrpcException) {
        }

        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        // The first address is idle again and the least recently used one,
        // so the next address evicts it.
        $client->getChannel('127.0.0.1:4');

        $channels = $this->readChannels($client);
        $this->assertArrayNotHasKey('127.0.0.1:1', $channels);
        $this->assertTrue($this->isChannelClosed($channelA));

        // The second address still carries a call: it survives the eviction.
        $this->assertArrayHasKey('127.0.0.1:2', $channels);
        $this->assertFalse($this->isChannelClosed($channelB));

        unset($futureA, $futureB);
        $client->close();
    }

    public function testInFlightCounterIsReleasedWhenTheCallSettles(): void
    {
        $client = new GrpcClient();

        $future = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $this->assertSame(1, $this->inFlightCount($client, '127.0.0.1:1'));

        try {
            $future->wait();
        } catch (GrpcException) {
        }

        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        $client->close();
    }

    public function testInFlightCounterIsReleasedOnCancellationAndNeverGoesNegative(): void
    {
        $client = new GrpcClient();

        $future = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $this->assertSame(1, $this->inFlightCount($client, '127.0.0.1:1'));

        $future->cancel();
        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        // A double settle must not push the counter below zero.
        $future->cancel();
        try {
            $future->wait();
        } catch (GrpcException) {
        }
        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        $client->close();
    }

    /**
     * A failed wait releases the in-flight slot exactly once.
     *
     * The failure a real channel can produce is a gRPC status other than
     * OK (the endpoint is dead here); the paths that fail while the
     * response is read — a status that cannot be read, a response that
     * cannot be deserialized — need a server that answers with such a
     * response and are pinned in `GrpcFutureTest`, which drives the
     * settlement hook itself.
     */
    public function testInFlightCounterIsReleasedOnceWhenTheCallFailsAndIsThenCancelled(): void
    {
        $client = new GrpcClient();

        $future = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $this->assertSame(1, $this->inFlightCount($client, '127.0.0.1:1'));

        try {
            $future->wait();
        } catch (GrpcException) {
        }

        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        // The failed call is over: cancelling it must not release a
        // second slot — another call may occupy it.
        $future->cancel();
        $this->assertSame(0, $this->inFlightCount($client, '127.0.0.1:1'));

        $client->close();
    }

    public function testCloseChannelDefersTheCloseUntilTheCallSettles(): void
    {
        $client = new GrpcClient();

        $future = $client->callAsync(
            '127.0.0.1:1',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );

        $channels = $this->readChannels($client);
        $channel = $channels['127.0.0.1:1'];

        // The address leaves the cache right away — the next call gets a
        // fresh channel — but the busy channel is not destroyed.
        $client->closeChannel('127.0.0.1:1');
        $this->assertSame(0, $client->getChannelCount());
        $this->assertFalse($this->isChannelClosed($channel));

        // The last call settles: now the channel can go.
        try {
            $future->wait();
        } catch (GrpcException) {
        }

        $this->assertTrue($this->isChannelClosed($channel));

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
                'inFlight' => 0,
            ];
        }
        $prop->setValue($this->client, $entries);
    }

    /**
     * @return array<string, \Grpc\Channel>
     */
    private function readChannels(?GrpcClient $client = null): array
    {
        $channels = [];
        foreach ($this->readChannelEntries($client ?? $this->client) as $addr => $entry) {
            $channels[$addr] = $entry['channel'];
        }
        return $channels;
    }

    /**
     * The raw cache entries, in-flight counters included (issue #278).
     *
     * @return array<string, array{channel: \Grpc\Channel, lastUsed: float, createdAt: float, inFlight: int}>
     */
    private function readChannelEntries(GrpcClient $client): array
    {
        $prop = new \ReflectionProperty(GrpcClient::class, 'channels');
        /** @var array<string, array{channel: \Grpc\Channel, lastUsed: float, createdAt: float, inFlight: int}> $entries */
        $entries = $prop->getValue($client);

        return $entries;
    }

    /**
     * The number of calls that were dispatched on a cached channel and have
     * not settled yet.
     */
    private function inFlightCount(GrpcClient $client, string $address): int
    {
        $entries = $this->readChannelEntries($client);

        return $entries[$address]['inFlight'] ?? 0;
    }

    /**
     * Whether a gRPC channel was destroyed.
     *
     * `Grpc\Channel::close()` destroys the underlying channel, and every
     * later call on it fails — ext-grpc reports that as a RuntimeException.
     */
    private function isChannelClosed(\Grpc\Channel $channel): bool
    {
        try {
            $channel->getConnectivityState();
        } catch (\Throwable) {
            return true;
        }

        return false;
    }

    private function rawGetRequest(): RawGetRequest
    {
        $request = new RawGetRequest();
        $request->setKey('test');

        return $request;
    }
}
