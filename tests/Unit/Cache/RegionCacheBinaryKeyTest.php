<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

/**
 * Region cache key routing must follow TiKV byte-order semantics, not PHP's
 * numeric-string comparison. A key like '9' must route by byte order
 * ('9' > '100'), and binary boundaries (inclusive start, exclusive end,
 * empty endKey = +infinity) must be respected.
 */
#[CoversClass(RegionCache::class)]
final class RegionCacheBinaryKeyTest extends TestCase
{
    private function region(int $id, string $start, string $end): RegionInfo
    {
        return new RegionInfo($id, $id, $id, 1, 1, $start, $end);
    }

    public function testGetByKeyUsesByteOrderNotNumericOrder(): void
    {
        $cache = new RegionCache();
        $cache->put($this->region(1, '', '100'));
        $cache->put($this->region(2, '100', ''));

        // '9' > '100' in byte order, so it belongs to region 2
        self::assertSame(2, $cache->getByKey('9')?->regionId);
        self::assertSame(2, $cache->getByKey('20')?->regionId);
        self::assertSame(1, $cache->getByKey('0999')?->regionId);
    }

    public function testGetByKeyHandlesBinaryBoundaries(): void
    {
        $cache = new RegionCache();
        $cache->put($this->region(1, '', "\x00\xff"));
        $cache->put($this->region(2, "\x00\xff", "\xff\xff"));
        $cache->put($this->region(3, "\xff\xff", ''));

        self::assertSame(1, $cache->getByKey("\x00")?->regionId);
        self::assertSame(1, $cache->getByKey("\x00\xfe")?->regionId);
        self::assertSame(2, $cache->getByKey("\x00\xff")?->regionId);   // inclusive start
        self::assertSame(2, $cache->getByKey("\xff\xfe")?->regionId);
        self::assertSame(3, $cache->getByKey("\xff\xff")?->regionId);
        self::assertSame(3, $cache->getByKey("\xff\xff\x00")?->regionId);
    }

    public function testGetByKeyAtExclusiveEndBelongsToNextRegion(): void
    {
        // endKey is exclusive: a key equal to a region's endKey must NOT be
        // found in that region, and must route to the next region (or miss if
        // it is the final region's end).
        $cache = new RegionCache();
        $cache->put($this->region(1, '', '100'));
        $cache->put($this->region(2, '100', '200'));
        $cache->put($this->region(3, '200', ''));

        self::assertSame(2, $cache->getByKey('100')?->regionId);
        self::assertSame(3, $cache->getByKey('200')?->regionId);
    }

    public function testGetByKeyEmptyEndKeyIsPlusInfinity(): void
    {
        // The last region has an empty endKey (unbounded): every key greater
        // than its startKey must route to it, even a high byte-order key.
        $cache = new RegionCache();
        $cache->put($this->region(1, '', '100'));
        $cache->put($this->region(2, '100', ''));

        self::assertSame(2, $cache->getByKey("\xff\xff")?->regionId);
        self::assertSame(2, $cache->getByKey('9')?->regionId);
    }
}
