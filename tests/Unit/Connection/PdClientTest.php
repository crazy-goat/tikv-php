<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\GetStoreResponse;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class PdClientTest extends TestCase
{
    public function testImplementsInterface(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $client = new PdClient($grpc, 'pd:2379');

        $this->assertInstanceOf(PdClientInterface::class, $client);
    }

    public function testGetRegionReturnsRegionInfo(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(10);

        $region = new Region();
        $region->setId(42);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(7);
        $leader->setStoreId(3);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('my-key');

        $this->assertInstanceOf(RegionInfo::class, $result);
        $this->assertSame(42, $result->regionId);
        $this->assertSame(7, $result->leaderPeerId);
        $this->assertSame(3, $result->leaderStoreId);
        $this->assertSame(1, $result->epochConfVer);
        $this->assertSame(10, $result->epochVersion);
    }

    public function testClusterIdMismatchRetries(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(999);

        $region = new Region();
        $region->setId(1);
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnCallback(function () use ($response): Message {
                static $callCount = 0;
                $callCount++;
                if ($callCount === 1) {
                    throw new GrpcException('mismatch cluster id, need 999 but got 0', 2);
                }
                return $response;
            });

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('key');

        $this->assertSame(1, $result->regionId);
    }

    public function testCloseDoesNotCloseGrpc(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('close');

        $client = new PdClient($grpc, 'pd:2379');
        $client->close();
    }

    public function testCloseClearsStoreCache(): void
    {
        $cache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\StoreCacheInterface::class);
        $cache->expects($this->once())->method('clear');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $client = new PdClient($grpc, 'pd:2379', new NullLogger(), $cache);
        $client->close();
    }

    public function testCloseResetsClusterId(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $client = new PdClient($grpc, 'pd:2379');
        $client->setClusterId(42);
        $this->assertSame(42, $client->getClusterId());

        $client->close();
        $this->assertNull($client->getClusterId());
    }

    public function testCloseResetsTso(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $client = new PdClient($grpc, 'pd:2379');

        // Trigger TSO creation by calling getTimestamp (it will fail since we
        // didn't set up a response, but the TSO object will be created).
        try {
            $client->getTimestamp();
        } catch (\Throwable) {
            // Expected to fail — we just need TSO to be initialized
        }

        $client->close();

        // After close, TSO should be reset and a new getTimestamp call should
        // create a fresh TSO rather than using the old one
        $this->assertNull(
            (new \ReflectionProperty(PdClient::class, 'tso'))->getValue($client)
        );
    }

    public function testGetRegionLogsGrpcCall(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('debug')
            ->with('PD gRPC call', ['method' => 'GetRegion', 'address' => 'pd1:2379']);

        $response = $this->makeGetRegionResponse();

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd1:2379', $logger);
        $client->getRegion('key');
    }

    public function testGetRegionLogsClusterIdLearned(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('info')
            ->with('Learned cluster ID', ['clusterId' => 12345]);

        $response = $this->makeGetRegionResponse();

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379', $logger);
        $client->getRegion('key');
    }

    public function testClusterIdMismatchLogsWarning(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('warning')
            ->with('Cluster ID mismatch, retrying', ['method' => 'GetRegion', 'clusterId' => 99]);

        $response = $this->makeGetRegionResponse();

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnCallback(function () use ($response): Message {
                static $callCount = 0;
                $callCount++;
                if ($callCount === 1) {
                    throw new GrpcException('mismatch cluster id, need 99 but got 0', 2);
                }
                return $response;
            });

        $client = new PdClient($grpc, 'pd:2379', $logger);
        $client->getRegion('key');
    }

    public function testGetRegionPopulatesPeers(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $peer1 = new Peer();
        $peer1->setId(10);
        $peer1->setStoreId(1);

        $peer2 = new Peer();
        $peer2->setId(20);
        $peer2->setStoreId(2);

        $peer3 = new Peer();
        $peer3->setId(30);
        $peer3->setStoreId(3);

        $region = new Region();
        $region->setId(42);
        $region->setRegionEpoch($epoch);
        $region->setPeers([$peer1, $peer2, $peer3]);

        $leader = new Peer();
        $leader->setId(10);
        $leader->setStoreId(1);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('key');

        $this->assertCount(3, $result->peers);
        $this->assertInstanceOf(PeerInfo::class, $result->peers[0]);
        $this->assertSame(10, $result->peers[0]->peerId);
        $this->assertSame(1, $result->peers[0]->storeId);
        $this->assertSame(20, $result->peers[1]->peerId);
        $this->assertSame(2, $result->peers[1]->storeId);
        $this->assertSame(30, $result->peers[2]->peerId);
        $this->assertSame(3, $result->peers[2]->storeId);
    }

    public function testGetRegionReturnsEmptyPeersWhenNoPeersInResponse(): void
    {
        $response = $this->makeGetRegionResponse();

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('key');

        $this->assertSame([], $result->peers);
    }

    // ========================================================================
    //  fail-closed behavior for missing leader/region (#104)
    // ========================================================================

    public function testGetRegionFailsClosedWhenPdReturnsNoRegion(): void
    {
        // PD returned a response with no Region — previously fabricated as
        // regionId=0/leaderStoreId=1 and cached, silently misrouting future
        // requests. Must throw a TiKvException instead.
        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('PD GetRegion returned no region for key');

        $client->getRegion('key');
    }

    public function testGetRegionReportsUnknownStoreIdZeroWhenLeaderMissing(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(42);
        $region->setRegionEpoch($epoch);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        // No leader set.

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('key');

        // Fail closed: leaderStoreId 0 ("unknown") matches no real store, so
        // resolveStoreAddress() raises StoreNotFoundException rather than
        // silently routing to a guessed store id 1.
        $this->assertSame(42, $result->regionId);
        $this->assertSame(0, $result->leaderPeerId);
        $this->assertSame(0, $result->leaderStoreId);
    }

    private function makeGetRegionResponse(): GetRegionResponse
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setStartKey('');
        $region->setEndKey('');
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $header = new ResponseHeader();
        $header->setClusterId(12345);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        return $response;
    }

    // ========================================================================
    //  scanRegions tests
    // ========================================================================

    public function testScanRegionsReturnsEmptyArrayWhenNoRegions(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('', '');

        $this->assertSame([], $result);
    }

    public function testScanRegionsReturnsSingleRegion(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(2);
        $epoch->setVersion(20);

        $region = new Region();
        $region->setId(99);
        $region->setStartKey('a');
        $region->setEndKey('b');
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(5);
        $leader->setStoreId(2);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([$region]);
        $response->setLeaders([$leader]);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('a', 'b');

        $this->assertCount(1, $result);
        $this->assertSame(99, $result[0]->regionId);
        $this->assertSame(5, $result[0]->leaderPeerId);
        $this->assertSame(2, $result[0]->leaderStoreId);
        $this->assertSame(2, $result[0]->epochConfVer);
        $this->assertSame(20, $result[0]->epochVersion);
        $this->assertSame('a', $result[0]->startKey);
        $this->assertSame('b', $result[0]->endKey);
    }

    public function testScanRegionsReturnsMultipleRegions(): void
    {
        $region1 = new Region();
        $region1->setId(1);
        $region1->setStartKey('');
        $region1->setEndKey('m');
        $epoch1 = new RegionEpoch();
        $epoch1->setConfVer(1);
        $epoch1->setVersion(1);
        $region1->setRegionEpoch($epoch1);

        $region2 = new Region();
        $region2->setId(2);
        $region2->setStartKey('m');
        $region2->setEndKey('');
        $epoch2 = new RegionEpoch();
        $epoch2->setConfVer(1);
        $epoch2->setVersion(2);
        $region2->setRegionEpoch($epoch2);

        $leader1 = new Peer();
        $leader1->setId(10);
        $leader1->setStoreId(1);

        $leader2 = new Peer();
        $leader2->setId(20);
        $leader2->setStoreId(2);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([$region1, $region2]);
        $response->setLeaders([$leader1, $leader2]);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('', '');

        $this->assertCount(2, $result);
        $this->assertSame(1, $result[0]->regionId);
        $this->assertSame(10, $result[0]->leaderPeerId);
        $this->assertSame(2, $result[1]->regionId);
        $this->assertSame(20, $result[1]->leaderPeerId);
    }

    public function testScanRegionsPopulatesPeers(): void
    {
        $peer1 = new Peer();
        $peer1->setId(100);
        $peer1->setStoreId(10);

        $peer2 = new Peer();
        $peer2->setId(200);
        $peer2->setStoreId(20);

        $region = new Region();
        $region->setId(5);
        $region->setStartKey('');
        $region->setEndKey('');
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);
        $region->setRegionEpoch($epoch);
        $region->setPeers([$peer1, $peer2]);

        $leader = new Peer();
        $leader->setId(100);
        $leader->setStoreId(10);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([$region]);
        $response->setLeaders([$leader]);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('', '');

        $this->assertCount(1, $result);
        $this->assertCount(2, $result[0]->peers);
        $this->assertSame(100, $result[0]->peers[0]->peerId);
        $this->assertSame(10, $result[0]->peers[0]->storeId);
        $this->assertSame(200, $result[0]->peers[1]->peerId);
        $this->assertSame(20, $result[0]->peers[1]->storeId);
    }

    public function testScanRegionsWithNoLeaderReportsUnknownStoreIdZero(): void
    {
        $region = new Region();
        $region->setId(7);
        $region->setStartKey('');
        $region->setEndKey('');
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);
        $region->setRegionEpoch($epoch);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([$region]);
        $response->setLeaders([]);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('', '');

        // Fail closed: a missing leader reports store id 0 ("unknown"), which
        // matches no real store, so resolveStoreAddress() raises an explicit
        // StoreNotFoundException instead of silently routing to store id 1.
        $this->assertCount(1, $result);
        $this->assertSame(7, $result[0]->regionId);
        $this->assertSame(0, $result[0]->leaderPeerId);
        $this->assertSame(0, $result[0]->leaderStoreId);
    }

    public function testScanRegionsWithMixedLeaderedAndLeaderlessRegions(): void
    {
        // Two regions: the first has a leader, the second does not. Verifies
        // the per-index $leaders[$index] ?? null alignment maps the right
        // leader to the right region, and the leaderless one reports store
        // id 0 rather than inheriting the neighboring region's leader.
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region1 = new Region();
        $region1->setId(1);
        $region1->setStartKey('');
        $region1->setEndKey('m');
        $region1->setRegionEpoch($epoch);

        $region2 = new Region();
        $region2->setId(2);
        $region2->setStartKey('m');
        $region2->setEndKey('');
        $region2->setRegionEpoch($epoch);

        $leader1 = new Peer();
        $leader1->setId(10);
        $leader1->setStoreId(5);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([$region1, $region2]);
        // Only the first region has a leader; the second is leaderless.
        $response->setLeaders([$leader1]);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->scanRegions('', '');

        $this->assertCount(2, $result);
        // First region: leader present.
        $this->assertSame(1, $result[0]->regionId);
        $this->assertSame(10, $result[0]->leaderPeerId);
        $this->assertSame(5, $result[0]->leaderStoreId);
        // Second region: leaderless -> unknown (store id 0), not store id 5.
        $this->assertSame(2, $result[1]->regionId);
        $this->assertSame(0, $result[1]->leaderPeerId);
        $this->assertSame(0, $result[1]->leaderStoreId);
    }

    public function testScanRegionsPassesStartKeyEndKeyAndLimit(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new \CrazyGoat\Proto\Pdpb\ScanRegionsResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                'ScanRegions',
                $this->callback(function (\CrazyGoat\Proto\Pdpb\ScanRegionsRequest $req): bool {
                    $this->assertSame('start-key', $req->getStartKey());
                    $this->assertSame('end-key', $req->getEndKey());
                    $this->assertSame(10, $req->getLimit());
                    return true;
                }),
                $this->anything(),
            )
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $client->scanRegions('start-key', 'end-key', 10);
    }

    // ========================================================================
    //  getStore tests
    // ========================================================================

    public function testGetStoreReturnsStoreFromCache(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $cache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\StoreCacheInterface::class);
        $cache->expects($this->once())->method('get')->with(1)->willReturn($store);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');

        $client = new PdClient($grpc, 'pd:2379', new NullLogger(), $cache);
        $result = $client->getStore(1);

        $this->assertSame($store, $result);
    }

    public function testGetStoreFetchesFromGrpcOnCacheMiss(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $cache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\StoreCacheInterface::class);
        $cache->expects($this->once())->method('get')->with(1)->willReturn(null);
        $cache->expects($this->once())->method('put')->with($store);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetStoreResponse();
        $response->setHeader($header);
        $response->setStore($store);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379', new NullLogger(), $cache);
        $result = $client->getStore(1);

        $this->assertSame($store, $result);
    }

    public function testGetStoreWithoutCacheStillFetches(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetStoreResponse();
        $response->setHeader($header);
        $response->setStore($store);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getStore(1);

        $this->assertSame($store, $result);
    }

    public function testGetStoreReturnsNullWhenStoreNotInResponse(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetStoreResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getStore(99);

        $this->assertNull($result);
    }

    public function testGetStoreDoesNotCacheWhenStoreIsNull(): void
    {
        $cache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\StoreCacheInterface::class);
        $cache->expects($this->once())->method('get')->with(1)->willReturn(null);
        $cache->expects($this->never())->method('put');

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetStoreResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379', new NullLogger(), $cache);
        $result = $client->getStore(1);

        $this->assertNull($result);
    }

    // ========================================================================
    //  extractClusterIdFromError tests (indirect via callWithClusterIdRetry)
    // ========================================================================

    public function testNonMismatchErrorDoesNotRetry(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new GrpcException('Some other PD error', 2));

        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('Some other PD error');

        $client->getRegion('key');
    }

    public function testClusterIdMismatchWithDifferentValuesParsed(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(555);

        $region = new Region();
        $region->setId(10);
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnCallback(function () use ($response): Message {
                static $callCount = 0;
                $callCount++;
                if ($callCount === 1) {
                    throw new GrpcException('mismatch cluster id, need 555 but got 0', 2);
                }
                return $response;
            });

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getRegion('key');

        $this->assertSame(10, $result->regionId);
        $this->assertSame(555, $client->getClusterId());
    }

    // ========================================================================
    //  learnClusterId tests (indirect via getRegion)
    // ========================================================================

    public function testLearnClusterIdDoesNotRelearnAfterFirstCall(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $header1 = new ResponseHeader();
        $header1->setClusterId(100);

        $response1 = new GetRegionResponse();
        $response1->setHeader($header1);
        $response1->setRegion($region);
        $response1->setLeader($leader);

        $header2 = new ResponseHeader();
        $header2->setClusterId(999);

        $response2 = new GetRegionResponse();
        $response2->setHeader($header2);
        $response2->setRegion($region);
        $response2->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($response1, $response2);

        $client = new PdClient($grpc, 'pd:2379');

        $client->getRegion('key1');
        $this->assertSame(100, $client->getClusterId());

        $client->getRegion('key2');
        $this->assertSame(100, $client->getClusterId());
    }

    public function testLearnClusterIdWithResponseWithoutHeader(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $response = new GetRegionResponse();
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $client->getRegion('key');

        $this->assertNull($client->getClusterId());
    }

    // ========================================================================
    //  createHeader tests (indirect via getRegion — cluster ID in header)
    // ========================================================================

    public function testCreateHeaderSendsClusterIdZeroBeforeLearning(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $header = new ResponseHeader();
        $header->setClusterId(42);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                'GetRegion',
                $this->callback(function (\CrazyGoat\Proto\Pdpb\GetRegionRequest $req): bool {
                    $hdr = $req->getHeader();
                    $this->assertNotNull($hdr);
                    /** @var \CrazyGoat\Proto\Pdpb\RequestHeader $hdr */
                    $this->assertSame(0, $hdr->getClusterId());
                    return true;
                }),
                $this->anything(),
            )
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $client->getRegion('key');
    }

    public function testCreateHeaderSendsLearnedClusterIdAfterFirstCall(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $header1 = new ResponseHeader();
        $header1->setClusterId(77);

        $response1 = new GetRegionResponse();
        $response1->setHeader($header1);
        $response1->setRegion($region);
        $response1->setLeader($leader);

        $response2 = new GetRegionResponse();
        $response2->setHeader($header1);
        $response2->setRegion($region);
        $response2->setLeader($leader);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (\CrazyGoat\Proto\Pdpb\GetRegionRequest $req): bool {
                    static $callNum = 0;
                    $callNum++;
                    $hdr = $req->getHeader();
                    $this->assertNotNull($hdr);
                    /** @var \CrazyGoat\Proto\Pdpb\RequestHeader $hdr */
                    if ($callNum === 1) {
                        $this->assertSame(0, $hdr->getClusterId());
                    } else {
                        $this->assertSame(77, $hdr->getClusterId());
                    }
                    return true;
                }),
            )
            ->willReturnOnConsecutiveCalls($response1, $response2);

        $client = new PdClient($grpc, 'pd:2379');
        $client->getRegion('key1');
        $client->getRegion('key2');
    }

    public function testGetTimestampReturnsMonotonicTimestamp(): void
    {
        $ts = new \CrazyGoat\Proto\Pdpb\Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(10);

        $tsoResponse = new \CrazyGoat\Proto\Pdpb\TsoResponse();
        $tsoResponse->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($tsoResponse);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->getTimestamp();

        $expected = (1715000000000 << 18) | 10;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampAfterClusterIdLearned(): void
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $getRegionHeader = new ResponseHeader();
        $getRegionHeader->setClusterId(200);

        $getRegionResponse = new GetRegionResponse();
        $getRegionResponse->setHeader($getRegionHeader);
        $getRegionResponse->setRegion($region);
        $getRegionResponse->setLeader($leader);

        $ts = new \CrazyGoat\Proto\Pdpb\Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(1);

        $tsoResponse = new \CrazyGoat\Proto\Pdpb\TsoResponse();
        $tsoResponse->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($getRegionResponse, $tsoResponse);

        $client = new PdClient($grpc, 'pd:2379');
        $client->getRegion('key');
        $result = $client->getTimestamp();

        $expected = (1715000000000 << 18) | 1;
        $this->assertSame($expected, $result);
    }
}
