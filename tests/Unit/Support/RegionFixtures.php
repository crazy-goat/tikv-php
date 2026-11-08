<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

/**
 * The default "one region, one store" cluster layout the TxnKV unit tests stub
 * PD with: region 1 led by peer 1 on store 1, with epoch 1 and the whole
 * keyspace `['', '')`, plus store 1 at {@see self::STORE_ADDRESS}.
 *
 * The fixture used to be copied into every test that needed a resolvable
 * region — twice as private helpers in `TxnKvClientTest`, again in
 * `TransactionTest::setUp()`, `makeStore()` and `stubRegionAndStore()`, and
 * inline in the multi-transaction cache test. The copies drifted independently
 * of the production code, which is exactly what a shared fixture prevents:
 * `RegionInfo` is readonly and no test mutates the store it gets back, so the
 * default layout can be built fresh per call and still describe one cluster.
 *
 * Deliberately NOT named `*Test.php`, so PHPUnit's `Unit` suite does not collect
 * it — the same reason `BinaryKeyVectors` is not a test. Tests that need a
 * *different* layout (several regions, a second store) keep building their own
 * values; this class only fixes the default.
 */
final class RegionFixtures
{
    /** The address store 1 advertises in the default layout. */
    public const STORE_ADDRESS = '127.0.0.1:20160';

    public static function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
    }

    public static function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress(self::STORE_ADDRESS);

        return $store;
    }
}
