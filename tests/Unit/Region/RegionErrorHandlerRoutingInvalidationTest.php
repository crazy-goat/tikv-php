<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Errorpb\DiskFull;
use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\IsWitness;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Errorpb\RaftEntryTooLarge;
use CrazyGoat\Proto\Errorpb\RecoveryInProgress;
use CrazyGoat\Proto\Errorpb\RegionNotFound;
use CrazyGoat\Proto\Errorpb\RegionNotInitialized;
use CrazyGoat\Proto\Errorpb\ServerIsBusy;
use CrazyGoat\Proto\Errorpb\StoreNotMatch;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #245 (REG-14): the TxnKV-style call site passes its cache and region
 * ID to RegionErrorHandler::check(), so check() is the drop path there. It
 * must use the same routing table as RetryExecutor: a non-routing region
 * error (ServerIsBusy, DiskFull, IsWitness, RecoveryInProgress) reached the
 * right region and must keep the cached entry, while a routing error still
 * invalidates before the exception is thrown.
 */
class RegionErrorHandlerRoutingInvalidationTest extends TestCase
{
    private const REGION_ID = 42;

    private RegionCacheInterface&MockObject $cache;

    protected function setUp(): void
    {
        $this->cache = $this->createMock(RegionCacheInterface::class);
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    /**
     * @param callable(Error): void $setVariant
     */
    private function responseWithRegionError(callable $setVariant): RawGetResponse
    {
        $error = new Error();
        $error->setMessage('region error from TiKV');
        $setVariant($error);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        return $response;
    }

    /**
     * @param callable(Error): void $setVariant
     */
    private function checkAndCatch(
        callable $setVariant,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): RegionException {
        try {
            RegionErrorHandler::check(
                $this->responseWithRegionError($setVariant),
                $this->cache,
                self::REGION_ID,
                notLeaderOwnedByRetryExecutor: $notLeaderOwnedByRetryExecutor,
            );
        } catch (RegionException $e) {
            return $e;
        }

        $this->fail('Expected RegionException');
    }

    /**
     * @return array<string, array{callable(Error): void, ErrorKind}>
     */
    public static function provideNonRoutingRegionErrors(): array
    {
        return [
            'ServerIsBusy' => [static function (Error $error): void {
                $error->setServerIsBusy(new ServerIsBusy());
            }, ErrorKind::ServerIsBusy],
            'DiskFull' => [static function (Error $error): void {
                $error->setDiskFull(new DiskFull());
            }, ErrorKind::DiskFull],
            'IsWitness' => [static function (Error $error): void {
                $error->setIsWitness(new IsWitness());
            }, ErrorKind::IsWitness],
            'RecoveryInProgress' => [static function (Error $error): void {
                $error->setRecoveryInProgress(new RecoveryInProgress());
            }, ErrorKind::RecoveryInProgress],
        ];
    }

    #[DataProvider('provideNonRoutingRegionErrors')]
    public function testNonRoutingRegionErrorDoesNotInvalidateTheRegion(
        callable $setVariant,
        ErrorKind $expectedKind,
    ): void {
        $this->cache->expects($this->never())->method('invalidate');

        $exception = $this->checkAndCatch($setVariant);

        $this->assertSame($expectedKind, $exception->errorKind);
    }

    /**
     * @return array<string, array{callable(Error): void, ErrorKind}>
     */
    public static function provideRoutingRegionErrors(): array
    {
        return [
            'EpochNotMatch' => [static function (Error $error): void {
                $error->setEpochNotMatch(new EpochNotMatch());
            }, ErrorKind::EpochNotMatch],
            'RegionNotFound' => [static function (Error $error): void {
                $error->setRegionNotFound(new RegionNotFound());
            }, ErrorKind::RegionNotFound],
            'KeyNotInRegion' => [static function (Error $error): void {
                $error->setKeyNotInRegion(new KeyNotInRegion());
            }, ErrorKind::KeyNotInRegion],
            'StoreNotMatch' => [static function (Error $error): void {
                $error->setStoreNotMatch(new StoreNotMatch());
            }, ErrorKind::StoreNotMatch],
            'RegionNotInitialized' => [static function (Error $error): void {
                $error->setRegionNotInitialized(new RegionNotInitialized());
            }, ErrorKind::RegionNotInitialized],
        ];
    }

    #[DataProvider('provideRoutingRegionErrors')]
    public function testRoutingRegionErrorInvalidatesWithKindTaggedReason(
        callable $setVariant,
        ErrorKind $expectedKind,
    ): void {
        $this->cache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'region_error:' . $expectedKind->value);

        $exception = $this->checkAndCatch($setVariant);

        $this->assertSame($expectedKind, $exception->errorKind);
    }

    public function testNotLeaderWithHintOwnedByRetryExecutorLeavesRegionCached(): void
    {
        $this->cache->expects($this->never())->method('invalidate');

        $exception = $this->checkAndCatch(static function (Error $error): void {
            $leader = new Peer();
            $leader->setId(2);
            $leader->setStoreId(3);

            $notLeader = new NotLeader();
            $notLeader->setRegionId(self::REGION_ID);
            $notLeader->setLeader($leader);
            $error->setNotLeader($notLeader);
        });

        $this->assertNotNull($exception->notLeader);
        $this->assertSame(3, (int) $exception->notLeader->getLeader()?->getStoreId());
    }

    public function testNotLeaderWithHintNotOwnedByRetryExecutorInvalidatesWithNotLeaderReason(): void
    {
        $this->cache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'not_leader');

        $exception = $this->checkAndCatch(
            static function (Error $error): void {
                $leader = new Peer();
                $leader->setId(2);
                $leader->setStoreId(3);

                $notLeader = new NotLeader();
                $notLeader->setRegionId(self::REGION_ID);
                $notLeader->setLeader($leader);
                $error->setNotLeader($notLeader);
            },
            notLeaderOwnedByRetryExecutor: false,
        );

        $this->assertNotNull($exception->notLeader);
    }

    public function testFatalNonRoutingRegionErrorKeepsTheRegion(): void
    {
        $this->cache->expects($this->never())->method('invalidate');

        $exception = $this->checkAndCatch(static function (Error $error): void {
            $error->setRaftEntryTooLarge(new RaftEntryTooLarge());
        });

        $this->assertSame(ErrorKind::RaftEntryTooLarge, $exception->errorKind);
    }

    public function testUnclassifiedRegionErrorInvalidatesWithTheHistoricalReason(): void
    {
        $this->cache->expects($this->once())
            ->method('invalidate')
            ->with(self::REGION_ID, 'region_error');

        $exception = $this->checkAndCatch(static function (Error $error): void {
        });

        $this->assertNull($exception->errorKind);
    }

    public function testMetricLabelNamesTheRegionErrorKind(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->region());

        $response = $this->responseWithRegionError(static function (Error $error): void {
            $error->setEpochNotMatch(new EpochNotMatch());
        });

        try {
            RegionErrorHandler::check($response, $cache, self::REGION_ID);
        } catch (RegionException) {
            // expected — check() invalidates before throwing
        }

        $this->assertSame(1, $metrics->getInvalidations('region_error:epoch_not_match'));
        $this->assertSame(0, $metrics->getInvalidations('region_error'));
    }

    public function testNonRoutingRegionErrorEmitsNoMetricAndKeepsTheEntry(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->region());

        $response = $this->responseWithRegionError(static function (Error $error): void {
            $error->setServerIsBusy(new ServerIsBusy());
        });

        try {
            RegionErrorHandler::check($response, $cache, self::REGION_ID);
        } catch (RegionException) {
            // expected
        }

        $this->assertSame(0, $metrics->getInvalidations('region_error'));
        $this->assertSame(0, $metrics->getInvalidations('region_error:server_is_busy'));
        $this->assertNotNull($cache->getByKey('some_key'), 'a non-routing error must keep the cached region');
    }
}
