<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use Grpc\Channel;
use PHPUnit\Framework\TestCase;

/**
 * Issue #717: a channel closed after a transport error must be replaced by a
 * **new connection**, not by the persistent entry the failed channel came
 * from.
 *
 * ext-grpc resolves `new Grpc\Channel($target, $opts)` from its process-global
 * persistent table unless `$opts['force_new']` is true, so a fresh PHP handle
 * alone still talks over the connection that just failed. The
 * `channelFactory` seam receives the options the channel would be built from,
 * which is the layer the transport controls — no server is needed to pin it.
 *
 * The idle and LRU evictions are the counter-case: they drop a healthy handle
 * and must keep resolving to the warm persistent channel, so their
 * replacements must NOT carry `force_new`.
 */
final class GrpcClientForceReconnectTest extends TestCase
{
    use GrpcExtensionGate;

    private const ADDRESS = '127.0.0.1:41700';

    /** Idle TTL of 100 s: advancing 200 s makes the channel evictable. */
    private const IDLE_TTL_MS = 100_000;

    private float $now = 1000.0;

    /** @var list<array<string, mixed>> */
    private array $seenOptions = [];

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testTheChannelAfterCloseChannelForcesANewConnection(): void
    {
        $client = $this->client();

        try {
            $first = $client->getChannel(self::ADDRESS);
            self::assertArrayNotHasKey('force_new', $this->seenOptions[0]);

            $client->closeChannel(self::ADDRESS);
            $second = $client->getChannel(self::ADDRESS);

            self::assertNotSame($first, $second);
            self::assertCount(2, $this->seenOptions);
            self::assertSame(
                true,
                $this->seenOptions[1]['force_new'] ?? null,
                'the retry after a transport error must get a new connection',
            );
        } finally {
            $client->close();
        }
    }

    public function testTheForcedReconnectIsConsumedByTheNextChannelOnly(): void
    {
        $client = $this->client();

        try {
            $client->getChannel(self::ADDRESS);
            $client->closeChannel(self::ADDRESS);

            $reconnected = $client->getChannel(self::ADDRESS);

            // A cache hit creates no channel at all, so the armed flag cannot
            // leak into a later creation.
            self::assertSame($reconnected, $client->getChannel(self::ADDRESS));
            self::assertCount(2, $this->seenOptions);

            // Evicting the forced channel later follows the ordinary idle
            // path: the replacement reuses the persistent entry.
            $this->now += 200.0;
            $client->reapIdleChannels();
            $client->getChannel(self::ADDRESS);

            self::assertCount(3, $this->seenOptions);
            self::assertArrayNotHasKey(
                'force_new',
                $this->seenOptions[2],
                'an idle eviction drops a healthy handle and must reuse the persistent channel',
            );
        } finally {
            $client->close();
        }
    }

    public function testADeferredCloseStillArmsTheForcedReconnect(): void
    {
        $client = $this->client();

        try {
            // A busy channel leaves the cache without being closed (issue
            // #278), and the retry that closed it runs before it settles.
            $busy = $this->createMock(Channel::class);
            $closed = false;
            $busy->method('close')->willReturnCallback(static function () use (&$closed): void {
                $closed = true;
            });

            $channels = new \ReflectionProperty(GrpcClient::class, 'channels');
            $channels->setValue($client, [
                self::ADDRESS => [
                    'channel' => $busy,
                    'lastUsed' => $this->now,
                    'createdAt' => $this->now,
                    'inFlight' => 1,
                ],
            ]);

            $client->closeChannel(self::ADDRESS);

            self::assertFalse($closed, 'a channel with in-flight calls must not be closed (issue #278)');

            $client->getChannel(self::ADDRESS);

            self::assertCount(1, $this->seenOptions);
            self::assertSame(true, $this->seenOptions[0]['force_new'] ?? null);
        } finally {
            $client->close();
        }
    }

    private function client(): GrpcClient
    {
        $this->seenOptions = [];

        return new GrpcClient(
            idleTtlMs: self::IDLE_TTL_MS,
            clock: fn (): float => $this->now,
            channelFactory: function (string $address, array $options): Channel {
                $this->seenOptions[] = $options;

                return $this->createMock(Channel::class);
            },
        );
    }
}
