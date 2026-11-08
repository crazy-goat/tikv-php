<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use Grpc\Channel;
use PHPUnit\Framework\TestCase;

/**
 * Issue #717: a channel closed after a transport error must be replaced by a
 * **new connection**, not by the connection the failed channel came from.
 *
 * Two process-global pools decide that, and both have to be bypassed:
 *
 * - ext-grpc resolves `new Grpc\Channel($target, $opts)` from its persistent
 *   table unless `$opts['force_new']` is true;
 * - gRPC core resolves the channel's subchannel — the thing that owns the TCP
 *   connection — from its global subchannel pool unless
 *   `$opts['grpc.use_local_subchannel_pool']` is set. `force_new` alone still
 *   lands on the connection that just failed.
 *
 * The `channelFactory` seam asserts both options at the layer the transport
 * controls, and `testTheRetryAfterCloseChannelOpensASecondTcpConnection()`
 * measures the property that matters: a plain TCP listener accepts a second
 * connection after `closeChannel()`.
 *
 * The idle and LRU evictions are the counter-case: they drop a healthy handle
 * and must keep resolving to the warm persistent channel, so their
 * replacements must carry neither option.
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

    /** @var list<resource> Accepted sockets, kept open for the connection count. */
    private array $acceptedSockets = [];

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    protected function tearDown(): void
    {
        foreach ($this->acceptedSockets as $socket) {
            fclose($socket);
        }
        $this->acceptedSockets = [];
    }

    public function testTheChannelAfterCloseChannelForcesANewConnection(): void
    {
        $client = $this->client();

        try {
            $first = $client->getChannel(self::ADDRESS);
            self::assertArrayNotHasKey('force_new', $this->seenOptions[0]);
            self::assertArrayNotHasKey('grpc.use_local_subchannel_pool', $this->seenOptions[0]);

            $client->closeChannel(self::ADDRESS);
            $second = $client->getChannel(self::ADDRESS);

            self::assertNotSame($first, $second);
            self::assertCount(2, $this->seenOptions);
            self::assertSame(
                true,
                $this->seenOptions[1]['force_new'] ?? null,
                'the retry after a transport error must not reuse the persistent channel',
            );
            self::assertSame(
                1,
                $this->seenOptions[1]['grpc.use_local_subchannel_pool'] ?? null,
                'the retry must get its own subchannel, not the pooled one',
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
            // path: the replacement reuses the warm persistent channel.
            $this->now += 200.0;
            $client->reapIdleChannels();
            $client->getChannel(self::ADDRESS);

            self::assertCount(3, $this->seenOptions);
            self::assertArrayNotHasKey(
                'force_new',
                $this->seenOptions[2],
                'an idle eviction drops a healthy handle and must reuse the persistent channel',
            );
            self::assertArrayNotHasKey(
                'grpc.use_local_subchannel_pool',
                $this->seenOptions[2],
                'an idle eviction must keep the pooled subchannel and its TCP connection',
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
            self::assertSame(1, $this->seenOptions[0]['grpc.use_local_subchannel_pool'] ?? null);
        } finally {
            $client->close();
        }
    }

    public function testAFailedChannelConstructionKeepsTheReconnectArmed(): void
    {
        $attempts = 0;
        $this->seenOptions = [];

        $client = new GrpcClient(
            channelFactory: function (string $address, array $options) use (&$attempts): Channel {
                $attempts++;
                $this->seenOptions[] = $options;

                if ($attempts === 2) {
                    throw new \RuntimeException('cannot create the channel');
                }

                return $this->createMock(Channel::class);
            },
        );

        try {
            $client->getChannel(self::ADDRESS);
            $client->closeChannel(self::ADDRESS);

            try {
                $client->getChannel(self::ADDRESS);
                self::fail('The failing factory should have thrown');
            } catch (\RuntimeException) {
            }

            // The flag is consumed only after a successful construction, so
            // the next attempt still reconnects instead of silently falling
            // back to the persistent entry.
            $client->getChannel(self::ADDRESS);

            self::assertCount(3, $this->seenOptions);
            self::assertSame(true, $this->seenOptions[2]['force_new'] ?? null);
            self::assertSame(1, $this->seenOptions[2]['grpc.use_local_subchannel_pool'] ?? null);
        } finally {
            $client->close();
        }
    }

    /**
     * The property the issue is about: the retry runs on a different TCP
     * connection.
     *
     * A plain listener that never speaks HTTP/2 is enough — the client opens
     * the TCP connection, so each accepted socket is one connection attempt.
     * Measured on grpc 1.83.0: with `force_new` alone the listener accepts
     * one socket (the global subchannel pool hands the old subchannel back);
     * with `force_new` and the local subchannel pool it accepts two.
     */
    public function testTheRetryAfterCloseChannelOpensASecondTcpConnection(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        self::assertIsResource($server, "failed to listen on a random port: $errstr ($errno)");
        stream_set_blocking($server, false);

        try {
            $address = stream_socket_get_name($server, false);
            self::assertIsString($address);

            $client = new GrpcClient();

            try {
                $client->getChannel($address)->getConnectivityState(true);

                self::assertSame(
                    1,
                    $this->acceptUpTo($server, 1),
                    'the first channel must open exactly one TCP connection',
                );

                $client->closeChannel($address);
                $client->getChannel($address)->getConnectivityState(true);

                self::assertSame(
                    2,
                    $this->acceptUpTo($server, 2),
                    'the retry after closeChannel() must open a second TCP connection',
                );
            } finally {
                $client->close();
            }
        } finally {
            fclose($server);
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

    /**
     * Accept connections until `$expected` were counted or the deadline
     * passes, and return the count. Accepted sockets stay open in
     * `$acceptedSockets` so the client keeps seeing a live TCP connection.
     *
     * @param resource $server
     */
    private function acceptUpTo($server, int $expected, float $timeoutSeconds = 5.0): int
    {
        $deadline = microtime(true) + $timeoutSeconds;

        do {
            while (($socket = @stream_socket_accept($server, 0)) !== false) {
                $this->acceptedSockets[] = $socket;
            }

            if (count($this->acceptedSockets) >= $expected) {
                break;
            }

            usleep(10_000);
        } while (microtime(true) < $deadline);

        return count($this->acceptedSockets);
    }
}
