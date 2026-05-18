<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;

final class RegionResolver
{
    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly RegionCacheInterface $regionCache,
    ) {
    }

    public function getRegionInfo(string $key): RegionInfo
    {
        $region = $this->regionCache->getByKey($key);
        if ($region instanceof RegionInfo) {
            return $region;
        }

        $region = $this->pdClient->getRegion($key);
        $this->regionCache->put($region);

        return $region;
    }

    public function resolveStoreAddress(int $storeId): string
    {
        $store = $this->pdClient->getStore($storeId);
        if (!$store instanceof \CrazyGoat\Proto\Metapb\Store) {
            throw new StoreNotFoundException($storeId);
        }

        $address = $store->getAddress();
        if ($address === '') {
            throw new StoreNotFoundException($storeId);
        }

        return $address;
    }
}
