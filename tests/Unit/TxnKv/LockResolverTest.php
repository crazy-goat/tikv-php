<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use PHPUnit\Framework\TestCase;

class LockResolverTest extends TestCase
{
    public function testConstruction(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $regionCache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\RegionCacheInterface::class);

        $resolver = new LockResolver($grpc, $pdClient, $regionCache);

        $this->assertInstanceOf(LockResolver::class, $resolver);
    }

    public function testGetGrpcReturnsGrpcClient(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $regionCache = $this->createMock(\CrazyGoat\TiKV\Client\Cache\RegionCacheInterface::class);

        $resolver = new LockResolver($grpc, $pdClient, $regionCache);

        $this->assertSame($grpc, $resolver->getGrpc());
    }
}
