<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Tests\Unit\Region\TestLogger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

/**
 * Issue #277 follow-up: the `maxChannels` soft cap evicts down to the cap and
 * logs an overshoot once per episode. Unique unused ports per test: see
 * {@see GrpcClientIdleSweepTest}.
 */
final class GrpcClientChannelCapTest extends TestCase
{
    use GrpcExtensionGate;

    private const WARNING = 'Channel cache at capacity but all channels are busy; exceeding soft cap';

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testOverCapCacheShrinksToTheCapInOneCall(): void
    {
        $client = new GrpcClient(maxChannels: 2);
        $channels = [];
        $now = microtime(true);
        foreach ([1 => 'a', 2 => 'b', 3 => 'c', 4 => 'd'] as $age => $name) {
            $channel = $this->createMock(\Grpc\Channel::class);
            $channel->expects($this->once())->method('close');
            $channels['127.0.0.1:4110' . $age] = [
                'channel' => $channel,
                'lastUsed' => $now - (10 - $age),
                'createdAt' => $now,
                'inFlight' => 0,
            ];
        }
        $channels['127.0.0.1:41105'] = [
            'channel' => $this->createMock(\Grpc\Channel::class),
            'lastUsed' => $now,
            'createdAt' => $now,
            'inFlight' => 0,
        ];
        // The first four are evicted (oldest first) ; the fifth is the
        // most recently used one and stays next to the new channel.
        $channels['127.0.0.1:41105']['channel']->expects($this->never())->method('close');
        $property = new \ReflectionProperty(GrpcClient::class, 'channels');
        $property->setValue($client, $channels);

        $client->getChannel('127.0.0.1:41106');

        $this->assertSame(2, $client->getChannelCount());
        $cached = $property->getValue($client);
        $this->assertIsArray($cached);
        $this->assertSame(['127.0.0.1:41105', '127.0.0.1:41106'], array_keys($cached));
        $property->setValue($client, []);
        $client->close();
    }

    public function testSoftCapWarningIsLoggedOncePerEpisode(): void
    {
        $logger = new TestLogger();
        $client = new GrpcClient($logger, maxChannels: 1);

        $futures = [];
        foreach (['41120', '41121', '41122', '41123'] as $port) {
            $futures[] = $client->callAsync(
                '127.0.0.1:' . $port,
                'tikvpb.Tikv',
                'RawGet',
                $this->rawGetRequest(),
                RawGetResponse::class,
            );
        }

        $this->assertSame(4, $client->getChannelCount());
        $this->assertSame(1, $this->softCapWarnings($logger));

        unset($futures);
        $client->close();
    }

    public function testSoftCapWarningIsLoggedAgainAfterTheCacheIsBackUnderTheCap(): void
    {
        $logger = new TestLogger();
        $client = new GrpcClient($logger, maxChannels: 1);

        $first = $this->dispatch($client, '41130');
        $second = $this->dispatch($client, '41131');
        $this->assertSame(1, $this->softCapWarnings($logger));

        // Both calls settle: the next address evicts down to the cap, which
        // ends the episode.
        foreach ([$first, $second] as $future) {
            try {
                $future->wait();
            } catch (\Throwable) {
            }
        }
        $client->getChannel('127.0.0.1:41132');
        $this->assertSame(1, $client->getChannelCount());

        // A new overshoot is a new episode.
        $third = $this->dispatch($client, '41133');
        $fourth = $this->dispatch($client, '41134');
        $this->assertSame(2, $this->softCapWarnings($logger));

        unset($third, $fourth);
        $client->close();
    }

    public function testSoftCapWarningIsLoggedAgainWhenTheCacheEmptiesBeforeTheNextOvershoot(): void
    {
        $logger = new TestLogger();
        $client = new GrpcClient($logger, maxChannels: 1);

        $first = $this->dispatch($client, '41140');
        $second = $this->dispatch($client, '41141');
        $this->assertSame(1, $this->softCapWarnings($logger));

        foreach ([$first, $second] as $future) {
            try {
                $future->wait();
            } catch (\Throwable) {
            }
        }
        $client->closeChannel('127.0.0.1:41140');
        $client->closeChannel('127.0.0.1:41141');
        $this->assertSame(0, $client->getChannelCount());

        // The cache is under the cap, so the next call takes the early
        // return that ends the episode.
        $third = $this->dispatch($client, '41142');
        $fourth = $this->dispatch($client, '41143');
        $this->assertSame(2, $this->softCapWarnings($logger));

        unset($third, $fourth);
        $client->close();
    }

    private function dispatch(GrpcClient $client, string $port): \CrazyGoat\TiKV\Client\Batch\GrpcFuture
    {
        return $client->callAsync(
            '127.0.0.1:' . $port,
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
    }

    private function softCapWarnings(TestLogger $logger): int
    {
        return count(array_filter(
            $logger->records(LogLevel::WARNING),
            static fn (array $record): bool => $record['message'] === self::WARNING,
        ));
    }

    private function rawGetRequest(): RawGetRequest
    {
        $request = new RawGetRequest();
        $request->setKey('test');

        return $request;
    }
}
