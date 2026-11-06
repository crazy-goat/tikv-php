<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\RegionCacheWarmer;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Issue #425: `options['regionCacheRefreshMs']` reaches the warmer through
 * `create()` and is validated there. Mirrors the option-validation tests of
 * the other client options (e.g. `RetryDeadlineTest`).
 */
final class RegionCacheWarmerOptionTest extends TestCase
{
    /**
     * @return array<string, array{0: class-string<RawKvClient|TxnKvClient>}>
     */
    public static function clients(): array
    {
        return [
            'RawKV' => [RawKvClient::class],
            'TxnKV' => [TxnKvClient::class],
        ];
    }

    /**
     * @param class-string<RawKvClient|TxnKvClient> $clientClass
     */
    #[DataProvider('clients')]
    public function testTheOptionDefaultsToDisabled(string $clientClass): void
    {
        $client = $clientClass::create(['127.0.0.1:2379'], options: ['grpcClient' => $this->grpc()]);

        try {
            $this->assertSame(0, $this->intervalOf($client));
        } finally {
            $client->close();
        }
    }

    /**
     * @param class-string<RawKvClient|TxnKvClient> $clientClass
     */
    #[DataProvider('clients')]
    public function testTheOptionIsThreadedIntoTheWarmer(string $clientClass): void
    {
        $client = $clientClass::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => $this->grpc(), 'regionCacheRefreshMs' => 5000],
        );

        try {
            $this->assertSame(5000, $this->intervalOf($client));
        } finally {
            $client->close();
        }
    }

    /**
     * @param class-string<RawKvClient|TxnKvClient> $clientClass
     */
    #[DataProvider('clients')]
    public function testANonIntOptionIsRejected(string $clientClass): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['regionCacheRefreshMs'] must be an int (milliseconds)");

        $clientClass::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => $this->grpc(), 'regionCacheRefreshMs' => '5000'],
        );
    }

    /**
     * @param class-string<RawKvClient|TxnKvClient> $clientClass
     */
    #[DataProvider('clients')]
    public function testANegativeOptionIsRejected(string $clientClass): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['regionCacheRefreshMs'] must be >= 0");

        $clientClass::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => $this->grpc(), 'regionCacheRefreshMs' => -1],
        );
    }

    public function testTheWarmerRejectsANonPositiveRegionBound(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('maxRegionsPerRefresh must be >= 1');

        new RegionCacheWarmer(
            $this->createMock(\CrazyGoat\TiKV\Client\Connection\PdClientInterface::class),
            new \CrazyGoat\TiKV\Client\Cache\RegionCache(),
            maxRegionsPerRefresh: 0,
        );
    }

    private function grpc(): GrpcClientInterface
    {
        return $this->createMock(GrpcClientInterface::class);
    }

    private function intervalOf(object $client): int
    {
        $warmer = (new \ReflectionProperty($client, 'regionCacheWarmer'))->getValue($client);
        self::assertInstanceOf(RegionCacheWarmer::class, $warmer);

        $interval = (new \ReflectionProperty(RegionCacheWarmer::class, 'refreshIntervalMs'))->getValue($warmer);
        if (!is_int($interval)) {
            throw new \LogicException('refreshIntervalMs must be an int');
        }

        return $interval;
    }
}
