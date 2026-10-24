<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #253 (REG-22): a region PD returns for a key must contain that key
 * before it is cached.
 */
final class PdRegionContainmentTest extends TestCase
{
    private function makeRegion(int $id, string $startKey, string $endKey): RegionInfo
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

    /**
     * Decimal boundaries: the byte order of the keys disagrees with their
     * numeric order (issue #232).
     *
     * @return iterable<string, array{string, string, string}>
     */
    public static function nonContainingRegions(): iterable
    {
        yield 'key equal to endKey' => ['1000', '', '1000'];
        yield 'bytewise above endKey' => ['200', '', '1000'];
        yield 'bytewise below startKey' => ['100', '20', ''];
        yield 'bytewise "99" is above "100"' => ['99', '', '100'];
    }

    #[DataProvider('nonContainingRegions')]
    public function testNonContainingRegionIsNotCachedAndIsRetryable(string $key, string $start, string $end): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->method('getRegion')->willReturn($this->makeRegion(7, $start, $end));
        $cache = new RegionCache();
        $resolver = new RegionResolver($pd, $cache);

        try {
            $resolver->getRegionInfo($key);
            self::fail('Expected RegionException');
        } catch (RegionException $e) {
            self::assertSame(BackoffType::RegionMiss, ErrorClassifier::classify($e));
        }

        self::assertNotInstanceOf(RegionInfo::class, $cache->getByKey($key));
        self::assertNotInstanceOf(RegionInfo::class, $cache->getByKey($start));
    }

    public function testContainingRegionIsCached(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->method('getRegion')->willReturn($this->makeRegion(7, '100', ''));
        $cache = new RegionCache();
        $resolver = new RegionResolver($pd, $cache);

        self::assertSame(7, $resolver->getRegionInfo('99')->regionId);
        self::assertInstanceOf(RegionInfo::class, $cache->getByKey('99'));
    }

    public function testUnboundedLastRegionContainsKey(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->method('getRegion')->willReturn($this->makeRegion(9, '1000', ''));
        $resolver = new RegionResolver($pd, new RegionCache());

        self::assertSame(9, $resolver->getRegionInfo('2000')->regionId);
    }

    public function testRetryRecoversFromStaleAnswerAndCachesOnlyTheGoodRegion(): void
    {
        $bad = $this->makeRegion(7, '', '1000');
        $good = $this->makeRegion(8, '1000', '');
        $pd = $this->createMock(PdClientInterface::class);
        $pd->expects(self::exactly(2))
            ->method('getRegion')
            ->willReturnOnConsecutiveCalls($bad, $good);
        $cache = new RegionCache();
        $resolver = new RegionResolver($pd, $cache);
        $executor = new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $cache,
            grpc: $this->createMock(GrpcClientInterface::class),
            regionResolver: $resolver,
            logger: new NullLogger(),
        );

        $result = $executor->execute('2000', fn(): RegionInfo => $resolver->getRegionInfo('2000'));

        self::assertSame(8, $result->regionId);
        self::assertSame(8, $cache->getByKey('2000')?->regionId);
        self::assertNotInstanceOf(RegionInfo::class, $cache->getByKey('1'));
    }
}
