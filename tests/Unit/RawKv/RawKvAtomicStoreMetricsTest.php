<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\DataIsNotReady;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\RawKv\RawKvAtomic;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Per-store metrics for CAS retries (issue #434, N1/N3).
 *
 * The `targetStoreId` provider must report the store of the *current*
 * attempt. When a retry fails inside `getRegionInfo()` (PD unreachable) the
 * provider must not fall back to the previous attempt's store: that would
 * attribute a PD failure to an unrelated TiKV store.
 */
final class RawKvAtomicStoreMetricsTest extends TestCase
{
    public function testRetryAfterPdFailureIsNotAttributedToThePreviousStore(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn(null);
        $regionCache->method('put');

        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 5,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $store = new Store();
        $store->setId(5);
        $store->setAddress('tikv5:20160');
        $pdClient->method('getStore')->willReturn($store);

        $regionLookups = 0;
        $pdClient->method('getRegion')->willReturnCallback(
            function () use (&$regionLookups, $region): RegionInfo {
                $regionLookups++;
                if ($regionLookups === 1) {
                    return $region;
                }

                // Second attempt: PD is down before the store is known.
                throw new GrpcException('pd down', GrpcStatusCode::Unavailable->value);
            },
        );

        $grpc->method('call')->willReturnCallback(
            static function (): RawCASResponse {
                $response = new RawCASResponse();
                $error = new Error();
                $error->setDataIsNotReady(new DataIsNotReady());
                $response->setRegionError($error);

                return $response;
            },
        );

        $metrics = new InMemoryMetrics();
        $regionResolver = new RegionResolver($pdClient, $regionCache);
        $retryExecutor = new RetryExecutor(
            maxBackoffMs: 1000,
            serverBusyBudgetMs: 1000,
            regionCache: $regionCache,
            grpc: $grpc,
            regionResolver: $regionResolver,
            logger: new NullLogger(),
            metrics: $metrics,
        );
        $atomic = new RawKvAtomic($grpc, $regionResolver, new TimeoutConfig());

        try {
            $atomic->compareAndSwap('key', 'old', 'new', 0, $retryExecutor);
            self::fail('Expected the PD failure to propagate');
        } catch (GrpcException) {
            // expected: a PD transport error is fatal for the CAS path
        }

        self::assertSame(1, $metrics->getStoreFailures(5));
        self::assertSame(0, $metrics->getStoreSuccesses(5));
        self::assertSame(['data_is_not_ready' => 1], $metrics->getStoreErrorKinds(5));
    }
}
