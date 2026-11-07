<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\TestCase;

/**
 * A client closes only the connections it owns (issue #204, RAW-19).
 *
 * The public constructor accepts a PD client and a transport the caller
 * already built, and sharing both between clients is a supported pattern.
 * A destructor that closed those dependencies would break every other client
 * using them, non-deterministically, at whatever point PHP happens to collect
 * the first client. These tests pin the ownership rule on observable
 * behaviour: which `close()` calls reach the dependencies.
 */
class RawKvClientOwnershipTest extends TestCase
{
    /**
     * The failure scenario from the issue: two clients share one PD client and
     * one transport, one is destroyed, and the survivor keeps working.
     */
    public function testDestroyingOneClientLeavesASharedDependencyUsable(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);
        $regionCache = $this->createMock(RegionCacheInterface::class);

        $pdClient->expects($this->never())->method('close');
        $grpc->expects($this->never())->method('close');

        $regionCache->method('getByKey')->willReturn(null);
        $regionCache->method('put');
        $pdClient->method('getRegion')->willReturn($this->region());
        $pdClient->method('getStore')->willReturn($this->store());

        $response = new RawGetResponse();
        $response->setValue('still-open');
        $grpc->method('call')->willReturn($response);

        $first = new RawKvClient($pdClient, $grpc, $regionCache);
        $second = new RawKvClient($pdClient, $grpc, $regionCache);
        $first->setColumnFamily('cf1');
        $second->setColumnFamily('cf2');

        unset($first);

        $this->assertSame(
            'still-open',
            $second->get('key'),
            'destroying the first client closed a connection the second one still uses',
        );
    }

    /**
     * An explicit close() follows the same ownership rule: the caller may
     * close a hand-assembled client and keep using the shared dependencies.
     */
    public function testHandAssembledClientDoesNotCloseItsDependencies(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $pdClient->expects($this->never())->method('close');
        $grpc->expects($this->never())->method('close');

        $client = new RawKvClient($pdClient, $grpc);

        $client->close();
        $client->close();
    }

    /**
     * Ownership can be transferred explicitly, which is what create() does.
     */
    public function testExplicitOwnershipClosesBothConnections(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);

        $pdClient->expects($this->once())->method('close');
        $grpc->expects($this->once())->method('close');

        $client = new RawKvClient(
            $pdClient,
            $grpc,
            ownsGrpcClient: true,
            ownsPdClient: true,
        );

        $client->close();
        $client->close();
    }

    /**
     * create() builds the bundle itself, so the client owns both connections.
     * The transport half is pinned behaviourally by
     * SharedGrpcClientOwnershipTest; PD has no public seam, so the flag is
     * asserted directly.
     */
    public function testCreateOwnsTheConnectionsItBuilds(): void
    {
        $client = RawKvClient::create(['127.0.0.1:2379']);

        self::assertTrue($this->property($client, 'ownsGrpcClient'));
        self::assertTrue($this->property($client, 'ownsPdClient'));

        $client->close();
    }

    private function property(RawKvClient $client, string $name): bool
    {
        $value = (new \ReflectionProperty(RawKvClient::class, $name))->getValue($client);
        self::assertIsBool($value);

        return $value;
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        return $store;
    }
}
