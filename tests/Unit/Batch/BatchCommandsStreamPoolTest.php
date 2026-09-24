<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamInterface;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamPool;
use PHPUnit\Framework\TestCase;

/**
 * Stream-reuse semantics of the per-store BatchCommands stream pool
 * (issue #418) against fake streams — no ext-grpc involved.
 */
final class BatchCommandsStreamPoolTest extends TestCase
{
    public function testReusesOneStreamPerAddressAcrossGets(): void
    {
        $created = [];
        $factory = static function (string $address) use (&$created): BatchCommandsStreamInterface {
            $stream = new FakeBatchCommandsStream();
            $created[] = $stream;

            return $stream;
        };

        $pool = new BatchCommandsStreamPool($factory);

        $first = $pool->get('tikv1:20160');
        $second = $pool->get('tikv1:20160');
        self::assertSame($first, $second, 'the stream must be reused for the same address');
        self::assertCount(1, $created, 'the factory must run once per address');

        $other = $pool->get('tikv2:20160');
        self::assertNotSame($first, $other, 'a distinct address gets its own stream');
        self::assertCount(2, $created);
    }

    public function testDiscardClosesAndReplacesADeadStream(): void
    {
        $created = [];
        $factory = static function (string $address) use (&$created): BatchCommandsStreamInterface {
            $stream = new FakeBatchCommandsStream();
            $created[] = $stream;

            return $stream;
        };

        $pool = new BatchCommandsStreamPool($factory);
        $dead = $pool->get('tikv1:20160');
        assert($dead instanceof FakeBatchCommandsStream);

        $pool->discard('tikv1:20160');
        self::assertTrue($dead->closed, 'a discarded stream must be closed');

        $replacement = $pool->get('tikv1:20160');
        self::assertNotSame($dead, $replacement, 'the next exchange must open a fresh stream');
        self::assertCount(2, $created);
    }

    public function testDiscardUnknownAddressIsSafe(): void
    {
        $calls = 0;
        $factory = static function (string $address) use (&$calls): BatchCommandsStreamInterface {
            ++$calls;

            return new FakeBatchCommandsStream();
        };
        $pool = new BatchCommandsStreamPool($factory);

        $pool->discard('never-opened:20160');

        self::assertSame(0, $calls, 'discarding an unknown address must not touch the factory');
    }

    public function testCloseClosesEveryStream(): void
    {
        $created = [];
        $factory = static function (string $address) use (&$created): FakeBatchCommandsStream {
            $stream = new FakeBatchCommandsStream();
            $created[] = $stream;

            return $stream;
        };

        $pool = new BatchCommandsStreamPool($factory);
        $pool->get('tikv1:20160');
        $pool->get('tikv2:20160');

        $pool->close();

        foreach ($created as $stream) {
            self::assertTrue($stream->closed);
        }
    }
}
