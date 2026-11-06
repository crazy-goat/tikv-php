<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * The RawKV client option must reach the wire Context of every request
 * builder, and an unconfigured client must keep the request bytes unchanged
 * (issue #427).
 */
final class ResourceControlTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->region());
    }

    public function testGroupAndTagReachTheReadAndWriteContexts(): void
    {
        /** @var array<string, RawGetRequest|RawPutRequest|RawGetKeyTTLRequest> $captured */
        $captured = [];
        $this->grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$captured): object {
                if (
                    !$request instanceof RawGetRequest
                    && !$request instanceof RawPutRequest
                    && !$request instanceof RawGetKeyTTLRequest
                ) {
                    throw new \RuntimeException("Unexpected request for {$method}");
                }
                $captured[$method] = $request;

                return match ($method) {
                    'RawGet' => new RawGetResponse(),
                    'RawPut' => new RawPutResponse(),
                    'RawGetKeyTTL' => new RawGetKeyTTLResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $client = $this->client(new ResourceControl('tenant-a', 'nightly-job'));
        $client->get('key');
        $client->put('key', 'value');
        $client->getKeyTTL('key');

        $this->assertSame(['RawGet', 'RawPut', 'RawGetKeyTTL'], array_keys($captured));

        foreach ($captured as $method => $request) {
            $context = $request->getContext();
            $this->assertNotNull($context, "no Context on {$method}");
            $controlContext = $context->getResourceControlContext();
            $this->assertNotNull($controlContext, "no resource control context on {$method}");
            $this->assertSame('tenant-a', $controlContext->getResourceGroupName(), "wrong group on {$method}");
            $this->assertSame('nightly-job', $context->getResourceGroupTag(), "wrong tag on {$method}");
        }
    }

    public function testUnsetResourceControlLeavesTheWireBytesUnchanged(): void
    {
        /** @var RawGetRequest|null $captured */
        $captured = null;
        $this->grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$captured): object {
                if (!$request instanceof RawGetRequest) {
                    throw new \RuntimeException("Unexpected request for {$method}");
                }
                $captured = $request;

                return new RawGetResponse();
            },
        );

        $this->client(new ResourceControl())->get('key');

        $this->assertNotNull($captured);
        $context = $captured->getContext();
        $this->assertNotNull($context);
        $this->assertFalse($context->hasResourceControlContext());
        $this->assertSame('', $context->getResourceGroupTag());

        // The same Context built by hand without resource control must
        // serialize to exactly the same bytes (the fields are not sent).
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);
        $peer = new Peer();
        $peer->setId(1);
        $peer->setStoreId(1);
        $reference = new Context();
        $reference->setRegionId(1);
        $reference->setRegionEpoch($epoch);
        $reference->setPeer($peer);

        $this->assertSame($reference->serializeToString(), $context->serializeToString());
    }

    private function client(ResourceControl $resourceControl): RawKvClient
    {
        return new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            resourceControl: $resourceControl,
        );
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
}
