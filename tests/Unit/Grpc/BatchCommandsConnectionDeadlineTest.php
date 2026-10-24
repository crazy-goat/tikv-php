<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\BatchCommandsConnection;
use Grpc\Channel;
use Grpc\ChannelCredentials;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A BatchCommands stream carries a fixed lifetime deadline, and a
 * non-positive one is a configuration error (issue #276, GRPC-17).
 *
 * The stream is opened with a deadline because a silently dead store must
 * not block a RECV batch forever: once the deadline fires the transport
 * discards the stream and falls back to unary calls. `0` used to mean "no
 * deadline at all" — the one deadline the gRPC C core never expires — so a
 * misconfigured stream lifetime turned a dead store into a hang.
 *
 * The check happens before the `Grpc\Call` is constructed, so it does not
 * need a live connection; the channel is only a required argument.
 */
class BatchCommandsConnectionDeadlineTest extends TestCase
{
    use GrpcExtensionGate;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    /**
     * @return array<string, array{int}>
     */
    public static function provideNonPositiveDeadlines(): array
    {
        return [
            'zero' => [0],
            'negative' => [-1],
        ];
    }

    #[DataProvider('provideNonPositiveDeadlines')]
    public function testANonPositiveStreamDeadlineIsRejected(int $deadlineMs): void
    {
        $channel = new Channel('127.0.0.1:1', [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        try {
            $this->expectException(InvalidArgumentException::class);
            $this->expectExceptionMessage(sprintf('deadlineMs must be greater than 0, got %d', $deadlineMs));

            BatchCommandsConnection::open($channel, $deadlineMs);
        } finally {
            // The channel is never used, but it must not outlive the test.
            $channel->close();
        }
    }
}
