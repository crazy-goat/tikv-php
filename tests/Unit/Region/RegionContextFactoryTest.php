<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use PHPUnit\Framework\TestCase;

class RegionContextFactoryTest extends TestCase
{
    public function testFromRegionInfoBuildsContext(): void
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

    public function testFromRegionInfoSetsApiV2Context(): void
    {
        $region = new RegionInfo(42, 7, 3, 1, 10);
        $codec = new CodecV2(Mode::Txn, 42, 'tenant-a');

        $context = RegionContextFactory::fromRegionInfo($region, $codec);

        $this->assertSame(2, $context->getApiVersion());
        $this->assertSame('tenant-a', $context->getKeyspaceName());
        $this->assertSame(42, $context->getKeyspaceId());
    }

    public function testFromRegionInfoWithZeroValues(): void
    {
        $region = new RegionInfo(0, 0, 0, 0, 0);

        $ctx = RegionContextFactory::fromRegionInfo($region);

        $this->assertInstanceOf(Context::class, $ctx);
        $this->assertSame(0, $ctx->getRegionId());
        $peer = $ctx->getPeer();
        $this->assertNotNull($peer);
        $this->assertSame(0, $peer->getId());
        $this->assertSame(0, $peer->getStoreId());
        $epoch = $ctx->getRegionEpoch();
        $this->assertNotNull($epoch);
        $this->assertSame(0, $epoch->getConfVer());
        $this->assertSame(0, $epoch->getVersion());
    }
}
