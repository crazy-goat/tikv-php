<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use PHPUnit\Framework\TestCase;

class StoreCacheTest extends TestCase
{
    public function testGetCacheMiss(): void
    {
        $cache = new StoreCache();
        $this->assertNull($cache->get(1));
    }

    public function testPutAndGet(): void
    {
        $cache = new StoreCache();

        $store = new Store();
        $store->setId(1);
        $store->setAddress("127.0.0.1:20160");

        $cache->put($store);

        $cached = $cache->get(1);
        $this->assertNotNull($cached);
        $this->assertSame("127.0.0.1:20160", $cached->getAddress());
    }

    public function testInvalidate(): void
    {
        $cache = new StoreCache();

        $store = new Store();
        $store->setId(1);
        $store->setAddress("127.0.0.1:20160");

        $cache->put($store);
        $this->assertNotNull($cache->get(1));

        $cache->invalidate(1);
        $this->assertNull($cache->get(1));
    }

    public function testClear(): void
    {
        $cache = new StoreCache();

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("127.0.0.1:20160");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("127.0.0.1:20161");

        $cache->put($store1);
        $cache->put($store2);

        $cache->clear();

        $this->assertNull($cache->get(1));
        $this->assertNull($cache->get(2));
    }

    public function testTtlExpiration(): void
    {
        $cache = new class extends StoreCache {
            protected function now(): int {
                return 1000;
            }
        };

        $store = new Store();
        $store->setId(1);
        $store->setAddress("127.0.0.1:20160");

        $cache->put($store);
        $this->assertNotNull($cache->get(1));

        // Simulate time passing beyond TTL (600s default + up to 60s jitter = 1660)
        $cache = new class(600, 0) extends StoreCache {
            protected function now(): int {
                return 2000;
            }
        };

        $this->assertNull($cache->get(1));
    }

    public function testOverwriteExisting(): void
    {
        $cache = new StoreCache();

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("127.0.0.1:20160");

        $store2 = new Store();
        $store2->setId(1);
        $store2->setAddress("127.0.0.1:20161");

        $cache->put($store1);
        $cache->put($store2);

        $cached = $cache->get(1);
        $this->assertNotNull($cached);
        $this->assertSame("127.0.0.1:20161", $cached->getAddress());
    }

    // ========================================================================
    //  Max entries cap and LRU eviction
    // ========================================================================

    public function testDefaultMaxEntries(): void
    {
        $cache = new StoreCache();
        $ref = new \ReflectionProperty(StoreCache::class, 'maxEntries');
        $val = $ref->getValue($cache);
        $this->assertSame(128, $val);
    }

    public function testCustomMaxEntries(): void
    {
        $cache = new StoreCache(maxEntries: 3);
        $ref = new \ReflectionProperty(StoreCache::class, 'maxEntries');
        $val = $ref->getValue($cache);
        $this->assertSame(3, $val);
    }

    public function testEvictsLruWhenAtCapacity(): void
    {
        $cache = new StoreCache(ttlSeconds: 3600, jitterSeconds: 0, maxEntries: 2);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("addr:1");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("addr:2");

        $store3 = new Store();
        $store3->setId(3);
        $store3->setAddress("addr:3");

        $cache->put($store1);
        $cache->put($store2);

        // Access store1 to make it most recently used
        $this->assertNotNull($cache->get(1));

        // Add store3, should evict store2 (LRU)
        $cache->put($store3);

        $this->assertNotNull($cache->get(1), 'store1 should still be cached');
        $this->assertNotNull($cache->get(3), 'store3 should be cached');
        $this->assertNull($cache->get(2), 'store2 should have been evicted (LRU)');
    }

    public function testEvictsCorrectLruWithMultipleEntries(): void
    {
        $cache = new StoreCache(ttlSeconds: 3600, jitterSeconds: 0, maxEntries: 3);

        $s1 = new Store(); $s1->setId(1); $s1->setAddress("a:1");
        $s2 = new Store(); $s2->setId(2); $s2->setAddress("b:2");
        $s3 = new Store(); $s3->setId(3); $s3->setAddress("c:3");
        $s4 = new Store(); $s4->setId(4); $s4->setAddress("d:4");

        // Put 3 entries: order of last-access = 1 (LRU), 2, 3 (MRU)
        $cache->put($s1);
        $cache->put($s2);
        $cache->put($s3);

        // Access s3 again: order = 1 (LRU), 2, 3 (MRU)
        $cache->get(3);
        // Access s1: order = 2 (LRU), 3, 1 (MRU)
        $cache->get(1);

        // Add s4, should evict s2 (LRU)
        $cache->put($s4);

        $this->assertNotNull($cache->get(1), 'store1 should be cached');
        $this->assertNotNull($cache->get(3), 'store3 should be cached');
        $this->assertNotNull($cache->get(4), 'store4 should be cached');
        $this->assertNull($cache->get(2), 'store2 should have been evicted (LRU)');
    }

    public function testPutExistingDoesNotCountTowardsCapacity(): void
    {
        // Overwriting an existing entry should not trigger eviction
        $cache = new StoreCache(ttlSeconds: 3600, jitterSeconds: 0, maxEntries: 2);

        $s1 = new Store(); $s1->setId(1); $s1->setAddress("a:1");
        $s2 = new Store(); $s2->setId(2); $s2->setAddress("b:2");

        $cache->put($s1);
        $cache->put($s2);

        // Overwrite s1 — should not count as a new entry
        $s1new = new Store(); $s1new->setId(1); $s1new->setAddress("a:1-new");
        $cache->put($s1new);

        // Both s1 and s2 should still be present
        $this->assertNotNull($cache->get(1));
        $this->assertNotNull($cache->get(2));
    }

    public function testClearResetsCache(): void
    {
        $cache = new StoreCache(ttlSeconds: 3600, jitterSeconds: 0, maxEntries: 3);

        $s1 = new Store(); $s1->setId(1); $s1->setAddress("a:1");
        $s2 = new Store(); $s2->setId(2); $s2->setAddress("b:2");
        $s3 = new Store(); $s3->setId(3); $s3->setAddress("c:3");

        $cache->put($s1);
        $cache->put($s2);
        $cache->put($s3);

        $cache->clear();

        $this->assertNull($cache->get(1));
        $this->assertNull($cache->get(2));
        $this->assertNull($cache->get(3));

        // After clear, re-adding should work
        $cache->put($s1);
        $this->assertNotNull($cache->get(1));
    }
}
