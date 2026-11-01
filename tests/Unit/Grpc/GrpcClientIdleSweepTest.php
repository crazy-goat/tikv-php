<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use PHPUnit\Framework\TestCase;

/**
 * Issue #277: the idle sweep also runs on the cache-hit path.
 *
 * A client that only ever asks for channels it already holds never creates a
 * channel, so the sweep that ran only before a creation never ran: the
 * channels of stores that left the cluster stayed open for ever. The tests
 * drive a fake clock; the addresses are ports nothing listens on, and a
 * channel is created lazily, so no connection is opened.
 */
final class GrpcClientIdleSweepTest extends TestCase
{
    use GrpcExtensionGate;

    /*
     * Every test uses its own pair of addresses. ext-grpc shares a channel
     * per target across the whole process, and a channel that other tests
     * used for a failed RPC reports TRANSIENT_FAILURE: `getChannel()` then
     * reconnects and takes the cache-miss path, which would hide the path
     * under test.
     */

    /** Idle TTL of 100 s: the cache-hit sweep interval is 10 s. */
    private const IDLE_TTL_MS = 100_000;

    private float $now = 1000.0;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testCacheHitReapsAnIdleChannel(): void
    {
        $client = $this->client();
        $client->getChannel('127.0.0.1:41010');
        $client->getChannel('127.0.0.1:41011');
        $this->assertSame(2, $client->getChannelCount());

        $this->now += 200.0;
        $client->getChannel('127.0.0.1:41011');

        $this->assertSame(1, $client->getChannelCount());
        $client->close();
    }

    public function testSweepIsRateLimitedOnTheCacheHitPath(): void
    {
        $client = $this->client();
        $client->getChannel('127.0.0.1:41020');
        $client->getChannel('127.0.0.1:41021');

        // A sweep runs here (the first hit), A is not idle yet.
        $this->now = 1099.0;
        $client->getChannel('127.0.0.1:41021');
        $this->assertSame(2, $client->getChannelCount());

        // A is idle now, but the last sweep was 2 s ago: no sweep.
        $this->now = 1101.0;
        $client->getChannel('127.0.0.1:41021');
        $this->assertSame(2, $client->getChannelCount());

        // The 10 s interval has passed: the sweep reaps A.
        $this->now = 1110.0;
        $client->getChannel('127.0.0.1:41021');
        $this->assertSame(1, $client->getChannelCount());
        $client->close();
    }

    public function testReturnedChannelIsNeverReaped(): void
    {
        $client = $this->client();
        $channel = $client->getChannel('127.0.0.1:41030');

        $this->now += 10_000.0;

        $this->assertSame($channel, $client->getChannel('127.0.0.1:41030'));
        $this->assertSame(1, $client->getChannelCount());
        $client->close();
    }

    public function testChannelWithInFlightCallIsNotReaped(): void
    {
        $client = $this->client();
        $future = $client->callAsync(
            '127.0.0.1:41040',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );
        $client->getChannel('127.0.0.1:41041');

        $this->now += 200.0;
        $client->getChannel('127.0.0.1:41041');

        // A is idle but busy: it stays.
        $this->assertSame(2, $client->getChannelCount());

        try {
            $future->wait();
        } catch (GrpcException) {
        }

        // The call settled: the next sweep reaps A.
        $this->now += 20.0;
        $client->getChannel('127.0.0.1:41041');
        $this->assertSame(1, $client->getChannelCount());
        $client->close();
    }

    public function testReapIdleChannelsDropsIdleChannelsWithoutARequest(): void
    {
        $client = $this->client();
        $client->getChannel('127.0.0.1:41050');
        $client->getChannel('127.0.0.1:41051');

        $this->now += 50.0;
        $client->reapIdleChannels();
        $this->assertSame(2, $client->getChannelCount());

        $this->now += 100.0;
        $client->reapIdleChannels();
        $this->assertSame(0, $client->getChannelCount());
        $client->close();
    }

    public function testReapIdleChannelsSkipsAChannelWithAnInFlightCall(): void
    {
        $client = $this->client();
        $future = $client->callAsync(
            '127.0.0.1:41060',
            'tikvpb.Tikv',
            'RawGet',
            $this->rawGetRequest(),
            RawGetResponse::class,
        );

        $this->now += 200.0;
        $client->reapIdleChannels();

        $this->assertSame(1, $client->getChannelCount());
        unset($future);
        $client->close();
    }

    public function testReapIdleChannelsIsANoOpAfterClose(): void
    {
        $reads = 0;
        $client = new GrpcClient(clock: static function () use (&$reads): float {
            ++$reads;

            return 1000.0;
        });
        $client->close();

        $client->reapIdleChannels();

        // The closed guard returns before the clock is read.
        $this->assertSame(0, $reads);
    }

    public function testApiV2DecoratorForwardsReapIdleChannels(): void
    {
        $inner = $this->createMock(\CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface::class);
        $inner->expects($this->once())->method('reapIdleChannels');

        (new \CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient(
            $inner,
            $this->createMock(\CrazyGoat\TiKV\Client\Codec\CodecInterface::class),
            \CrazyGoat\TiKV\Client\Codec\Mode::Raw,
        ))->reapIdleChannels();
    }

    private function client(): GrpcClient
    {
        return new GrpcClient(
            idleTtlMs: self::IDLE_TTL_MS,
            clock: fn(): float => $this->now,
        );
    }

    private function rawGetRequest(): RawGetRequest
    {
        $request = new RawGetRequest();
        $request->setKey('test');

        return $request;
    }
}
