<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\RawKvCrud;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

/**
 * Guard for the #247 wiring: both factory paths must pass the ONE
 * ConnectionBundle store cache to the client, the PdClient that reads it and
 * the RegionResolver that invalidates it. Dropping any `storeCache:`
 * argument would leave every behavioural test green (they build the resolver
 * directly) while disabling store-address invalidation in production.
 */
class StoreCacheWiringTest extends TestCase
{
    private function property(object $object, string $name): mixed
    {
        return (new ReflectionProperty($object, $name))->getValue($object);
    }

    private function assertResolverShares(StoreCache $expected, mixed $resolver): void
    {
        self::assertInstanceOf(RegionResolver::class, $resolver);
        self::assertSame($expected, $this->property($resolver, 'storeCache'));
    }

    public function testRawKvFactorySharesTheStoreCacheWithPdClientAndResolver(): void
    {
        $client = RawKvClient::create(
            ['pd:2379'],
            options: ['grpcClient' => $this->createMock(GrpcClientInterface::class)],
        );

        $storeCache = $this->property($client, 'storeCache');
        self::assertInstanceOf(StoreCache::class, $storeCache);

        $pdClient = $this->property($client, 'pdClient');
        self::assertInstanceOf(PdClientInterface::class, $pdClient);
        self::assertSame($storeCache, $this->property($pdClient, 'storeCache'));

        // RawKvClient does not retain the resolver itself: its CRUD component
        // is built with the resolver the constructor created.
        $crud = $this->property($client, 'crud');
        self::assertInstanceOf(RawKvCrud::class, $crud);
        $this->assertResolverShares($storeCache, $this->property($crud, 'regionResolver'));
    }

    public function testTxnKvFactorySharesTheStoreCacheWithPdClientAndResolver(): void
    {
        $client = TxnKvClient::create(
            ['pd:2379'],
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'gcSafePointValidation' => false,
            ],
        );

        $storeCache = $this->property($client, 'storeCache');
        self::assertInstanceOf(StoreCache::class, $storeCache);

        $pdClient = $this->property($client, 'pdClient');
        self::assertInstanceOf(PdClientInterface::class, $pdClient);
        self::assertSame($storeCache, $this->property($pdClient, 'storeCache'));

        $this->assertResolverShares($storeCache, $this->property($client, 'regionResolver'));
    }
}
