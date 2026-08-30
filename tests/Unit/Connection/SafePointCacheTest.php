<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\SafePointCache;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class SafePointCacheTest extends TestCase
{
    public function testFetchesOnceWithinRefreshInterval(): void
    {
        $fetches = 0;
        $cache = new SafePointCache(
            static function () use (&$fetches): int {
                $fetches++;

                return 1000;
            },
            30000,
        );

        $this->assertSame(1000, $cache->get());
        $this->assertSame(1000, $cache->get());
        $this->assertSame(1000, $cache->get());
        $this->assertSame(1, $fetches);
    }

    public function testRefetchesAfterRefreshIntervalElapses(): void
    {
        $fetches = 0;
        $cache = new SafePointCache(
            static function () use (&$fetches): int {
                $fetches++;

                return $fetches === 1 ? 1000 : 2000;
            },
            1, // smallest possible window — expires immediately after the read
        );

        $this->assertSame(1000, $cache->get());

        // Advance the clock past the 1ms refresh window.
        usleep(2000);

        $this->assertSame(2000, $cache->get());
        $this->assertSame(2, $fetches);
    }

    public function testInvalidateForcesRefetch(): void
    {
        $fetches = 0;
        $cache = new SafePointCache(
            static function () use (&$fetches): int {
                $fetches++;

                return $fetches * 100;
            },
            30000,
        );

        $this->assertSame(100, $cache->get());
        $cache->invalidate();
        $this->assertSame(200, $cache->get());
        $this->assertSame(2, $fetches);
    }

    public function testFetchFailureThrowsAndDoesNotCache(): void
    {
        $fetches = 0;
        $cache = new SafePointCache(
            static function () use (&$fetches): int {
                $fetches++;
                if ($fetches === 1) {
                    throw new TiKvException('PD unreachable');
                }

                return 500;
            },
            30000,
        );

        try {
            $cache->get();
            self::fail('Expected TiKvException');
        } catch (TiKvException $e) {
            $this->assertSame('PD unreachable', $e->getMessage());
        }

        // A failed fetch must not poison the cache — next call re-fetches.
        $this->assertSame(500, $cache->get());
        $this->assertSame(2, $fetches);
    }

    public function testRejectsNegativeSafePoint(): void
    {
        $cache = new SafePointCache(static fn (): int => -1, 30000);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('invalid GC safe point');
        $cache->get();
    }

    public function testRejectsRefreshIntervalBelowOne(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('refreshIntervalMs must be >= 1');
        new SafePointCache(static fn (): int => 0, 0);
    }

    public function testDefaultRefreshIntervalIsThirtySeconds(): void
    {
        $this->assertSame(30000, SafePointCache::DEFAULT_REFRESH_INTERVAL_MS);
    }
}
