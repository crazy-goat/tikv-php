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

    public function testMaxCapacityEvictsOldest(): void
    {
        $cache = new StoreCache(ttlSeconds: 600, jitterSeconds: 0, maxEntries: 3);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("addr1");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("addr2");

        $store3 = new Store();
        $store3->setId(3);
        $store3->setAddress("addr3");

        $store4 = new Store();
        $store4->setId(4);
        $store4->setAddress("addr4");

        $cache->put($store1);
        $cache->put($store2);
        $cache->put($store3);

        // All three fit within the cap
        $this->assertNotNull($cache->get(1));
        $this->assertNotNull($cache->get(2));
        $this->assertNotNull($cache->get(3));

        // Adding a fourth evicts store 1 (oldest)
        $cache->put($store4);

        $this->assertNull($cache->get(1), 'Store 1 should have been evicted');
        $this->assertNotNull($cache->get(2));
        $this->assertNotNull($cache->get(3));
        $this->assertNotNull($cache->get(4));
    }

    public function testMaxCapacityEvictsCorrectlyOnRefresh(): void
    {
        $cache = new StoreCache(ttlSeconds: 600, jitterSeconds: 0, maxEntries: 3);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("addr1");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("addr2");

        $store3 = new Store();
        $store3->setId(3);
        $store3->setAddress("addr3");

        $store4 = new Store();
        $store4->setId(4);
        $store4->setAddress("addr4");

        $cache->put($store1);
        $cache->put($store2);
        $cache->put($store3);

        // Refresh store 1 (moves it to end of order)
        $cache->put($store1);

        // Now adding store 4 should evict store 2 (the oldest), not store 1
        $cache->put($store4);

        $this->assertNotNull($cache->get(1), 'Store 1 was refreshed and should remain');
        $this->assertNull($cache->get(2), 'Store 2 was oldest and should have been evicted');
        $this->assertNotNull($cache->get(3));
        $this->assertNotNull($cache->get(4));
    }

    public function testClearResetsOrder(): void
    {
        $cache = new StoreCache(ttlSeconds: 600, jitterSeconds: 0, maxEntries: 3);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("addr1");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("addr2");

        $cache->put($store1);
        $cache->put($store2);
        $cache->clear();

        $this->assertNull($cache->get(1));
        $this->assertNull($cache->get(2));
    }

    public function testPutAfterClearRefillsCorrectly(): void
    {
        $cache = new StoreCache(ttlSeconds: 600, jitterSeconds: 0, maxEntries: 2);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress("addr1");

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress("addr2");

        $cache->put($store1);
        $cache->put($store2);
        $cache->clear();

        $store3 = new Store();
        $store3->setId(3);
        $store3->setAddress("addr3");

        $cache->put($store3);
        $this->assertNotNull($cache->get(3));
        $this->assertNull($cache->get(1));
    }
}
