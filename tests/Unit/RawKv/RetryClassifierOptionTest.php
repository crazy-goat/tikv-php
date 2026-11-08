<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #719: RawKvClient exposes its retry classifier through the
 * constructor and through `options['retryClassifier']`.
 */
class RetryClassifierOptionTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($store);
    }

    public function testConstructorClassifierStopsAnOtherwiseRetryableError(): void
    {
        // DataIsNotReady is retried by the built-in classifier; the custom
        // classifier makes it fatal, so the client must issue one RPC only.
        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('DataIsNotReady'));

        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
        );

        $this->expectException(TiKvException::class);

        $client->get('key');
    }

    public function testScanUsesTheClientClassifier(): void
    {
        // The scanner owns its own retry executor; the client must hand the
        // classifier down to it (issue #719 covers scans, not only point
        // operations).
        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('DataIsNotReady'));

        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
        );

        $this->expectException(TiKvException::class);

        $client->scan('a', 'b', 10);
    }

    public function testCreateRejectsANonCallableRetryClassifier(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['retryClassifier'] must be callable, string given");

        RawKvClient::create(['127.0.0.1:2379'], options: ['retryClassifier' => 'not-a-callable']);
    }

    public function testCreateResolvesTheClassifierOption(): void
    {
        $client = RawKvClient::create(
            ['127.0.0.1:2379'],
            options: [
                'retryClassifier' => static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            ],
        );

        $property = new \ReflectionProperty(RawKvClient::class, 'retryClassifier');
        $classifier = $property->getValue($client);

        self::assertInstanceOf(\Closure::class, $classifier);
        self::assertTrue($classifier(new TiKvException('anything'))->isFatal());

        $client->close();
    }

    public function testCreateDefaultsToNoClassifier(): void
    {
        $client = RawKvClient::create(['127.0.0.1:2379']);

        $property = new \ReflectionProperty(RawKvClient::class, 'retryClassifier');

        self::assertNull($property->getValue($client));

        $client->close();
    }
}
