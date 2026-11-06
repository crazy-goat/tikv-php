<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use PHPUnit\Framework\TestCase;

/**
 * Issue #426: the RawKV client applies the optional Context control fields
 * to its reads and writes.
 */
class RequestContextWireTest extends TestCase
{
    public function testRawKvClientAppliesRequestContextToReadsAndWrites(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getStore')->willReturn($this->store());

        $region = new RegionInfo(1, 1, 1, 1, 1);
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn($region);

        $capturedRequests = [];
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$capturedRequests): object {
                $capturedRequests[$method] = $request;

                return match ($method) {
                    'RawGet' => new RawGetResponse(),
                    'RawPut' => new RawPutResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $client = new RawKvClient(
            $pdClient,
            $grpc,
            $regionCache,
            requestContext: new RequestContextOptions(
                notFillCache: true,
                requestSource: 'tikv-php-test',
                resourceGroupTag: 'scan',
            ),
        );

        $client->get('k1');
        $client->put('k1', 'v1');

        foreach (['RawGet' => RawGetRequest::class, 'RawPut' => RawPutRequest::class] as $method => $class) {
            $request = $capturedRequests[$method];
            $this->assertInstanceOf($class, $request);
            $context = $request->getContext();
            $this->assertNotNull($context, "missing Context on $method");
            $this->assertTrue($context->getNotFillCache(), "not_fill_cache missing on $method");
            $this->assertSame('tikv-php-test', $context->getRequestSource(), "request_source missing on $method");
            $this->assertSame('scan', $context->getResourceGroupTag(), "resource_group_tag missing on $method");
        }
    }

    public function testWithoutRequestContextTheDefaultContextIsUnchanged(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getStore')->willReturn($this->store());

        $region = new RegionInfo(1, 1, 1, 1, 1);
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn($region);

        $captured = null;
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$captured): RawGetResponse {
                $captured = $request;

                return new RawGetResponse();
            },
        );

        $client = new RawKvClient($pdClient, $grpc, $regionCache);
        $client->get('k1');

        $this->assertInstanceOf(RawGetRequest::class, $captured);
        $context = $captured->getContext();
        $this->assertNotNull($context);
        $this->assertFalse($context->getNotFillCache());
        $this->assertSame('', $context->getRequestSource());
        $this->assertSame('', $context->getResourceGroupTag());
        $this->assertSame(0, $context->getMaxExecutionDurationMs());
    }

    private function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        return $store;
    }
}
