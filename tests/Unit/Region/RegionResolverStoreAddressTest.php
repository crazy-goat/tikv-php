<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

class RegionResolverStoreAddressTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
    }

    #[DataProvider('malformedAddressProvider')]
    public function testRejectsAddressThatIsNotABareHostPort(string $address): void
    {
        $logger = new TestLogger();
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store($address));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache, logger: $logger);

        try {
            $resolver->resolveStoreAddress(1);
            $this->fail('Expected InvalidStoreAddressException');
        } catch (\Throwable $e) {
            $this->assertNotInstanceOf(StoreNotFoundException::class, $e);
            $this->assertInstanceOf(InvalidStoreAddressException::class, $e);
            $this->assertStringContainsString('expected host:port', $e->getMessage());
        }

        $this->assertNotEmpty($logger->records(LogLevel::ERROR));
    }

    /**
     * @return array<string, array{string}>
     */
    public static function malformedAddressProvider(): array
    {
        return [
            'unix socket' => ['unix:/var/run/docker.sock'],
            'unix-abstract socket' => ['unix-abstract:custom-socket-name'],
            'dns scheme' => ['dns:///tikv-0.tikv.svc.cluster.local:20160'],
            'ipv4 scheme' => ['ipv4:127.0.0.1:20160'],
            'missing port' => ['127.0.0.1'],
            'non-numeric port' => ['127.0.0.1:http'],
        ];
    }

    public function testRejectsHostOutsideAllowedStoreHosts(): void
    {
        $logger = new TestLogger();
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store('10.1.2.3:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['127.0.0.1'],
            logger: $logger,
        );

        try {
            $resolver->resolveStoreAddress(1);
            $this->fail('Expected InvalidStoreAddressException');
        } catch (\Throwable $e) {
            $this->assertNotInstanceOf(StoreNotFoundException::class, $e);
            $this->assertInstanceOf(InvalidStoreAddressException::class, $e);
            $this->assertStringContainsString('outside the allowed set', $e->getMessage());
        }

        $this->assertNotEmpty($logger->records(LogLevel::ERROR));
    }

    public function testRejectedAddressIsDistinctFromStoreNotFoundException(): void
    {
        $this->pdClient->method('getStore')->willReturn($this->store('unix:/var/run/docker.sock'));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        try {
            $resolver->resolveStoreAddress(1);
            $this->fail('Expected InvalidStoreAddressException');
        } catch (\Throwable $e) {
            $this->assertNotInstanceOf(StoreNotFoundException::class, $e);
            $this->assertInstanceOf(InvalidStoreAddressException::class, $e);
        }
    }

    public function testAllowsValidHostPortInsideAllowlist(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store('127.0.0.1:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['127.0.0.1'],
        );

        $this->assertSame('127.0.0.1:20160', $resolver->resolveStoreAddress(1));
    }

    public function testAllowsValidHostPortWhenNoPolicyConfigured(): void
    {
        // Backward compatibility: without allowedStoreHosts only the format
        // check applies, any bare host:port is accepted.
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store('10.0.0.7:20160'));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->assertSame('10.0.0.7:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDnsSuffixAllowlistMatchesSubdomains(): void
    {
        $this->pdClient->expects($this->exactly(3))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('tikv-1.store.internal:20160'),
            $this->store('tikv-1.store.internal:20160'),
            $this->store('tikv-1.other.internal:20160'),
        );

        $suffixResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['.store.internal'],
        );
        $this->assertSame('tikv-1.store.internal:20160', $suffixResolver->resolveStoreAddress(1));

        $plainResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['store.internal'],
        );
        $this->assertSame('tikv-1.store.internal:20160', $plainResolver->resolveStoreAddress(1));

        $outsideResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['.store.internal'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $outsideResolver->resolveStoreAddress(1);
    }

    public function testCidrAllowlistMatchesIpv4Range(): void
    {
        $this->pdClient->expects($this->exactly(2))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('10.0.0.7:20160'),
            $this->store('11.0.0.7:20160'),
        );

        $inside = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['10.0.0.0/8'],
        );
        $this->assertSame('10.0.0.7:20160', $inside->resolveStoreAddress(1));

        $outside = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['10.0.0.0/8'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $outside->resolveStoreAddress(1);
    }

    public function testCallablePolicyOverridesAllowlist(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store('10.0.0.7:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['127.0.0.1'],
            storeHostPolicy: static fn (string $address): bool => $address === '10.0.0.7:20160',
        );

        $this->assertSame('10.0.0.7:20160', $resolver->resolveStoreAddress(1));
    }

    public function testStoreNotFoundStillThrowsStoreNotFoundException(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn(null);

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->expectException(StoreNotFoundException::class);
        $resolver->resolveStoreAddress(99);
    }

    private function store(string $address): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress($address);

        return $store;
    }
}
