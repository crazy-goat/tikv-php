<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\TxnKv\ResolvedTxnStatusCache;
use PHPUnit\Framework\TestCase;

class ResolvedTxnStatusCacheTest extends TestCase
{
    public function testDefaultCapacityMatchesClientGo(): void
    {
        $this->assertSame(2048, ResolvedTxnStatusCache::DEFAULT_CAPACITY);
    }

    public function testUnknownStartTsIsNull(): void
    {
        $this->assertNull((new ResolvedTxnStatusCache())->get(7));
    }

    public function testStoresCommitAndRollbackOutcomes(): void
    {
        $cache = new ResolvedTxnStatusCache();
        $cache->put(10, 15);
        $cache->put(11, 0);

        $this->assertSame(15, $cache->get(10));
        $this->assertSame(0, $cache->get(11), 'a rollback (0) is a stored outcome, not a miss');
    }

    public function testEvictsOldestEntryAtTheBound(): void
    {
        $cache = new ResolvedTxnStatusCache(2);
        $cache->put(1, 101);
        $cache->put(2, 102);
        $cache->put(3, 103);

        $this->assertSame(2, $cache->size());
        $this->assertNull($cache->get(1), 'FIFO: the oldest entry leaves first');
        $this->assertSame(102, $cache->get(2));
        $this->assertSame(103, $cache->get(3));
    }

    public function testOverwriteDoesNotEvict(): void
    {
        $cache = new ResolvedTxnStatusCache(2);
        $cache->put(1, 101);
        $cache->put(2, 102);
        $cache->put(2, 202);

        $this->assertSame(101, $cache->get(1));
        $this->assertSame(202, $cache->get(2));
    }

    public function testRejectsNonPositiveCapacity(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new ResolvedTxnStatusCache(0);
    }
}
