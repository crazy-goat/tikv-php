<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use PHPUnit\Framework\TestCase;

class RegionContextTest extends TestCase
{
    public function testCreatesContextFromRegionInfo(): void
    {
        $region = new RegionInfo(
            regionId: 42,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
        );

        $ctx = RegionContextFactory::fromRegionInfo($region);

        $this->assertInstanceOf(Context::class, $ctx);
        $this->assertSame(42, $ctx->getRegionId());
        $peer = $ctx->getPeer();
        $this->assertNotNull($peer);
        $this->assertSame(7, $peer->getId());
        $this->assertSame(3, $peer->getStoreId());
        $epoch = $ctx->getRegionEpoch();
        $this->assertNotNull($epoch);
        $this->assertSame(1, $epoch->getConfVer());
        $this->assertSame(10, $epoch->getVersion());
    }
}
