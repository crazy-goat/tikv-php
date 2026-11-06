<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionCacheWarmer;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #425: the client-level wiring of the opt-in region-cache refresh and
 * preload. The warmer's own behaviour is covered by
 * {@see RegionCacheWarmerTest}; these tests pin the public contract — preload
 * serves no user RPC, and both methods are a no-op after `close()`.
 */
class RegionCacheWarmerClientTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private RegionCache $cache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->cache = new RegionCache();
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    public function testRawKvPreloadCachesRegionsWithoutAnyStoreRpc(): void
    {
        $region = $this->region(1, '100', '200');
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('100', '200', 128)
            ->willReturn([$region]);

        $client = new RawKvClient($this->pdClient, $this->grpc, $this->cache);

        $this->assertSame(1, $client->preloadRegions([['100', '200']]));
        $this->assertSame($region, $this->cache->getByKey('150'));
    }

    public function testRawKvWarmupMethodsAreNoOpsAfterClose(): void
    {
        $this->pdClient->method('getRegion')
            ->willThrowException(new \LogicException('PD must not be touched after close'));
        $this->pdClient->method('scanRegions')
            ->willThrowException(new \LogicException('PD must not be touched after close'));

        $client = new RawKvClient($this->pdClient, $this->grpc, $this->cache);
        $client->close();

        $this->assertSame(0, $client->refreshRegionCache(true));
        $this->assertSame(0, $client->preloadRegions([['100', '200']]));
    }

    public function testTxnKvWarmupMethodsAreNoOpsAfterClose(): void
    {
        $this->pdClient->method('getRegion')
            ->willThrowException(new \LogicException('PD must not be touched after close'));
        $this->pdClient->method('scanRegions')
            ->willThrowException(new \LogicException('PD must not be touched after close'));

        $client = new TxnKvClient($this->pdClient, $this->grpc, $this->cache);
        $client->close();

        $this->assertSame(0, $client->refreshRegionCache(true));
        $this->assertSame(0, $client->preloadRegions([['100', '200']]));
    }

    public function testTxnKvPreloadCachesRegionsWithoutAnyStoreRpc(): void
    {
        $region = $this->region(1, '100', '200');
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->willReturn([$region]);

        $client = new TxnKvClient($this->pdClient, $this->grpc, $this->cache);

        $this->assertSame(1, $client->preloadRegions([['100', '200']]));
        $this->assertSame($region, $this->cache->getByKey('150'));
    }

    public function testRawKvUsesTheInjectedWarmer(): void
    {
        $region = $this->region(1, '100', '200');
        $this->cache->put($region);
        $fresh = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: '100',
            endKey: '200',
        );
        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->with('100')
            ->willReturn($fresh);

        $warmer = new RegionCacheWarmer(
            $this->pdClient,
            $this->cache,
            new NullLogger(),
            clockMs: fn (): int => 1_000_000,
        );
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->cache,
            regionCacheWarmer: $warmer,
        );

        $this->assertSame(1, $client->refreshRegionCache(true));
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
    }

    public function testTxnKvUsesTheInjectedWarmer(): void
    {
        $region = $this->region(1, '100', '200');
        $this->cache->put($region);
        $fresh = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: '100',
            endKey: '200',
        );
        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->willReturn($fresh);

        $warmer = new RegionCacheWarmer(
            $this->pdClient,
            $this->cache,
            new NullLogger(),
        );
        $client = new TxnKvClient(
            $this->pdClient,
            $this->grpc,
            $this->cache,
            regionCacheWarmer: $warmer,
        );

        $this->assertSame(1, $client->refreshRegionCache(true));
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
    }

    public function testRawKvRejectsANegativeRefreshInterval(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->cache,
            regionCacheRefreshMs: -1,
        );
    }

    public function testTxnKvRejectsANegativeRefreshInterval(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new TxnKvClient(
            $this->pdClient,
            $this->grpc,
            $this->cache,
            regionCacheRefreshMs: -1,
        );
    }
}
