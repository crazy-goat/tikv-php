<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #301 (PERF-14): the client must build one RegionResolver and share it
 * between every collaborator and every per-operation RetryExecutor. Building
 * a fresh resolver per operation would discard the per-store health state a
 * previous retry loop learned (issue #424) and silently defeat any future
 * per-resolver caching or memoisation.
 */
final class RawKvClientRegionResolverReuseTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RawKvClient $client;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache);
    }

    public function testEveryRetryExecutorReusesTheClientsSingleResolver(): void
    {
        $resolver = $this->property(RawKvClient::class, 'regionResolver', $this->client);
        self::assertInstanceOf(RegionResolver::class, $resolver);

        // Every collaborator the constructor builds must receive that same
        // instance, not a copy of its own.
        foreach (['crud', 'atomic', 'batch', 'scanner', 'rangeOps', 'ingestor'] as $name) {
            $collaborator = $this->property(RawKvClient::class, $name, $this->client);
            self::assertIsObject($collaborator, sprintf('%s must be an object', $name));
            self::assertSame(
                $resolver,
                $this->property($collaborator::class, 'regionResolver', $collaborator),
                sprintf('%s must use the client resolver', $name),
            );
        }

        // The executor is per-operation and created by a private method, so
        // invoke it directly and assert the resolver it receives.
        $method = new \ReflectionMethod(RawKvClient::class, 'createRetryExecutor');
        $first = $method->invoke($this->client);
        $second = $method->invoke($this->client);

        self::assertInstanceOf(RetryExecutor::class, $first);
        self::assertInstanceOf(RetryExecutor::class, $second);
        self::assertSame($resolver, $this->property(RetryExecutor::class, 'regionResolver', $first));
        self::assertSame($resolver, $this->property(RetryExecutor::class, 'regionResolver', $second));
        self::assertNotSame($first, $second, 'the executor itself stays per-operation');
    }

    /** @param class-string $class */
    private function property(string $class, string $name, object $object): mixed
    {
        return (new \ReflectionProperty($class, $name))->getValue($object);
    }
}
