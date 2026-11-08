<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\ImportSstpb\SwitchModeResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use Google\Protobuf\Internal\Message;
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
        $this->regionCache->method('put');

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

    public function testCompareAndSwapFallsThroughToTheClientClassifier(): void
    {
        // CAS installs a per-call classifier that is fatal for transport
        // errors. For everything else it must pass, so the client-level
        // classifier is consulted (issue #719 review): DataIsNotReady is
        // retried by the built-in classifier, and only the custom fatal
        // verdict stops it on the first attempt.
        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('DataIsNotReady'));

        $seen = [];
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static function (TiKvException $e) use (&$seen): RetryVerdict {
                $seen[] = $e->getMessage();

                return RetryVerdict::fatal();
            },
        );
        $client->setAtomicForCAS(true);

        try {
            $client->compareAndSwap('key', null, 'value');
            self::fail('the fatal verdict should surface');
        } catch (TiKvException) {
        }

        self::assertSame(['DataIsNotReady'], $seen);
    }

    public function testDeleteRangeUsesTheClientClassifier(): void
    {
        // RawKvRangeOps owns its executor. The wait phase of the delete-range
        // fan-out goes through it, so the client classifier must reach it.
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn (): GrpcFuture => GrpcFuture::fromWaiter(
                static fn (): Message => throw new TiKvException('DataIsNotReady'),
            ),
        );

        $seen = [];
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static function (TiKvException $e) use (&$seen): RetryVerdict {
                $seen[] = $e->getMessage();

                return RetryVerdict::fatal();
            },
        );

        try {
            $client->deleteRange('a', 'b');
            self::fail('the fatal verdict should surface as a partial failure');
        } catch (BatchPartialFailureException) {
        }

        self::assertSame(['DataIsNotReady'], $seen);
    }

    public function testIngestUsesTheClientClassifier(): void
    {
        // SstIngestor owns its executor too; the client must forward the
        // classifier to it.
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $this->pdClient->method('getAllStores')->willReturn([$store]);
        $this->grpc->method('callAsync')->willReturnCallback(
            static fn (): GrpcFuture => GrpcFuture::fromWaiter(
                static fn (): Message => new SwitchModeResponse(),
            ),
        );
        $this->grpc->method('callStreaming')
            ->willThrowException(new TiKvException('DataIsNotReady'));

        $seen = [];
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static function (TiKvException $e) use (&$seen): RetryVerdict {
                $seen[] = $e->getMessage();

                return RetryVerdict::fatal();
            },
        );

        try {
            $client->ingest(['k' => 'v']);
            self::fail('the fatal verdict should surface');
        } catch (TiKvException) {
        }

        self::assertSame(['DataIsNotReady'], $seen);
    }

    public function testClientLevelFatalVerdictRunsTheFatalSideEffects(): void
    {
        // EpochNotMatch is retried by the built-in classifier. A client-level
        // fatal() verdict must still run the executor's fatal path, which
        // drops the cached region the error is about.
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->grpc->method('call')->willThrowException(new RegionException(
            'RawGet',
            'epoch not match',
            errorKind: ErrorKind::EpochNotMatch,
        ));
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1, 'fatal_region_error');

        $seen = [];
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            retryClassifier: static function (TiKvException $e) use (&$seen): RetryVerdict {
                $seen[] = $e->getMessage();

                return RetryVerdict::fatal();
            },
        );

        try {
            $client->get('key');
            self::fail('the fatal verdict should rethrow the region error');
        } catch (RegionException) {
        }

        self::assertCount(1, $seen);
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
