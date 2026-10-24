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
