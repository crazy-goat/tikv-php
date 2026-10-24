<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

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
     * @return iterable<string, array{string, string, string}>
     */
    public static function nonContainingRegions(): iterable
    {
        yield 'key equal to endKey' => ['m', 'a', 'm'];
        yield 'key after endKey' => ['n', 'a', 'm'];
        yield 'key before startKey' => ['a', 'b', 'm'];
        // Bytewise "99" > "100", so ["", "100") does not contain "99".
        yield 'numeric strings compared bytewise' => ['99', '', '100'];
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
        $pd->method('getRegion')->willReturn($this->makeRegion(7, 'a', 'z'));
        $cache = new RegionCache();
        $resolver = new RegionResolver($pd, $cache);

        self::assertSame(7, $resolver->getRegionInfo('m')->regionId);
        self::assertInstanceOf(RegionInfo::class, $cache->getByKey('m'));
    }

    public function testUnboundedLastRegionContainsKey(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->method('getRegion')->willReturn($this->makeRegion(9, 'm', ''));
        $resolver = new RegionResolver($pd, new RegionCache());

        self::assertSame(9, $resolver->getRegionInfo('zzz')->regionId);
    }
}
