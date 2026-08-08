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

    public function testAllowsValidHostPortWhenNoPolicyAndNoEndpointsConfigured(): void
    {
        // Direct construction without pdEndpoints cannot derive the default
        // policy, so only the format check applies. The public create() path
        // always passes the configured PD endpoints (see default-policy tests
        // below), so production clients get the default policy automatically.
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store('10.0.0.7:20160'));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->assertSame('10.0.0.7:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDnsSuffixAllowlistMatchesSubdomains(): void
    {
        $this->pdClient->expects($this->exactly(3))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('tikv-1.store.internal:20160'),
            $this->store('store.internal:20160'),
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
            allowedStoreHosts: ['.store.internal'],
        );
        $this->assertSame('store.internal:20160', $plainResolver->resolveStoreAddress(1));

        $outsideResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['.store.internal'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $outsideResolver->resolveStoreAddress(1);
    }

    public function testExactAllowlistEntryRejectsSubdomainHost(): void
    {
        // A non-dotted entry is an EXACT hostname match; it must not behave
        // as a suffix, so a subdomain host is rejected.
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('evil.tikv-0.tikv.svc:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['tikv-0.tikv.svc'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->resolveStoreAddress(1);
    }

    #[DataProvider('invalidPortAddressProvider')]
    public function testRejectsOutOfRangeOrTrailingNewlinePorts(string $address): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store($address));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->resolveStoreAddress(1);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function invalidPortAddressProvider(): array
    {
        return [
            'port 0' => ['127.0.0.1:0'],
            'port 65536' => ['127.0.0.1:65536'],
            'port 99999' => ['127.0.0.1:99999'],
            'trailing newline' => ["evil:20160\n"],
            'trailing newline on valid host' => ["127.0.0.1:20160\n"],
        ];
    }

    public function testAllowsPortBoundaryValues(): void
    {
        $this->pdClient->expects($this->exactly(2))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('127.0.0.1:1'),
            $this->store('127.0.0.1:65535'),
        );

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->assertSame('127.0.0.1:1', $resolver->resolveStoreAddress(1));
        $this->assertSame('127.0.0.1:65535', $resolver->resolveStoreAddress(1));
    }

    public function testAllowsBracketedIpv6InAllowlist(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('[2001:db8::1]:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['2001:db8::1'],
        );

        $this->assertSame('[2001:db8::1]:20160', $resolver->resolveStoreAddress(1));
    }

    public function testBracketedIpv6MatchesCidrAllowlist(): void
    {
        $this->pdClient->expects($this->exactly(2))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('[2001:db8::1]:20160'),
            $this->store('[2001:db9::1]:20160'),
        );

        $inside = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['2001:db8::/32'],
        );
        $this->assertSame('[2001:db8::1]:20160', $inside->resolveStoreAddress(1));

        $outside = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['2001:db8::/32'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $outside->resolveStoreAddress(1);
    }

    public function testRejectsUnbracketedIpv6Garbage(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('2001:db8::1:20160'));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->resolveStoreAddress(1);
    }

    // ========================================================================
    // Default host policy (derived from the configured PD endpoints)
    // ========================================================================

    public function testDefaultPolicyAllowsStoreHostEqualToPdHost(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('pd-0.pd.svc:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['pd-0.pd.svc:2379'],
        );

        $this->assertSame('pd-0.pd.svc:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyAllowsSingleLabelStoreHost(): void
    {
        // E2E topology: pd:2379 + tikv1:20160 (docker-compose short names).
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('tikv1:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['pd:2379'],
        );

        $this->assertSame('tikv1:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyAllowsHostSharingLastTwoDnsLabels(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('tikv-0.tikv-hl.ns.svc:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['pd-0.pd-hl.ns.svc:2379'],
        );

        $this->assertSame('tikv-0.tikv-hl.ns.svc:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyAllowsIpInSameIpv16Subnet(): void
    {
        // Dev topology: PD on 127.0.0.1:2379, stores on 127.0.0.x:20160.
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('127.0.0.2:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['127.0.0.1:2379'],
        );

        $this->assertSame('127.0.0.2:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyRejectsUnrelatedDomain(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('attacker.example.com:443'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['pd:2379'],
        );

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->resolveStoreAddress(1);
    }

    public function testAllowedStoreHostsOverridesDefaultPolicy(): void
    {
        // Explicit allowlist wins over the default PD-derived policy.
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('10.0.0.7:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['10.0.0.7'],
            pdEndpoints: ['127.0.0.1:2379'],
        );

        $this->assertSame('10.0.0.7:20160', $resolver->resolveStoreAddress(1));
    }

    public function testStoreHostPolicyOverridesDefaultPolicy(): void
    {
        // Explicit callable policy wins over the default PD-derived policy,
        // including the documented "allow anything" escape hatch.
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('attacker.example.com:443'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            storeHostPolicy: static fn (string $address): bool => true,
            pdEndpoints: ['127.0.0.1:2379'],
        );

        $this->assertSame('attacker.example.com:443', $resolver->resolveStoreAddress(1));
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

    // ========================================================================
    // Round 2: host classification in the default policy + reserved schemes
    // ========================================================================

    /**
     * @param list<string> $pdEndpoints
     */
    #[DataProvider('defaultPolicyIpClassificationRejectionsProvider')]
    public function testDefaultPolicyRejectsMisclassifiedIpTargets(string $address, array $pdEndpoints): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store($address));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: $pdEndpoints,
        );

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->resolveStoreAddress(1);
    }

    /**
     * @return array<string, array{string, list<string>}>
     */
    public static function defaultPolicyIpClassificationRejectionsProvider(): array
    {
        return [
            'bracketed ipv6 without a matching pd ipv6' => ['[::1]:20160', ['127.0.0.1:2379']],
            'bracketed ipv6 doc literal without a matching pd ipv6' => ['[dead:beef::1]:20160', ['127.0.0.1:2379']],
            'bracketed ipv6 with zone id' => ['[fe80::1%eth0]:20160', ['127.0.0.1:2379']],
            'bracketed ipv4-mapped ipv6' => ['[::ffff:10.0.0.1]:20160', ['127.0.0.1:2379']],
            'decimal ipv4 alias' => ['2130706433:20160', ['127.0.0.1:2379']],
            'octal ipv4 alias' => ['017700000001:20160', ['127.0.0.1:2379']],
            'hex ipv4 alias' => ['0x7f000001:20160', ['127.0.0.1:2379']],
            'dotted ipv4 colliding only by textual suffix' => ['10.0.0.1:20160', ['127.0.0.1:2379']],
        ];
    }

    public function testDefaultPolicyAllowsIpv4ExactlyEqualToPdIp(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('127.0.0.1:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['127.0.0.1:2379'],
        );

        $this->assertSame('127.0.0.1:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyAllowsIpv4InSame16Subnet(): void
    {
        // /16 rule: first two octets must match; no textual suffix matching.
        $this->pdClient->expects($this->once())->method('getStore')
            ->willReturn($this->store('10.0.5.9:20160'));

        $resolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['10.0.5.1:2379'],
        );

        $this->assertSame('10.0.5.9:20160', $resolver->resolveStoreAddress(1));
    }

    public function testDefaultPolicyAllowsBracketedIpv6EqualToPdEndpoint(): void
    {
        $this->pdClient->expects($this->exactly(2))->method('getStore')->willReturnOnConsecutiveCalls(
            $this->store('[::1]:20160'),
            $this->store('[dead:beef::1]:20160'),
        );

        $loopbackResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['[::1]:2379'],
        );
        $this->assertSame('[::1]:20160', $loopbackResolver->resolveStoreAddress(1));

        $docLiteralResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['[dead:beef::1]:2379'],
        );
        $this->assertSame('[dead:beef::1]:20160', $docLiteralResolver->resolveStoreAddress(1));
    }

    #[DataProvider('reservedSchemeHostProvider')]
    public function testRejectsReservedGrpcSchemeNamesAsHosts(string $address): void
    {
        // Unconditional: rejected even without pdEndpoints (permissive
        // policy) because grpc-core interprets these hosts as URI schemes.
        $this->pdClient->expects($this->once())->method('getStore')->willReturn($this->store($address));

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        try {
            $resolver->resolveStoreAddress(1);
            $this->fail('Expected InvalidStoreAddressException');
        } catch (\Throwable $e) {
            $this->assertNotInstanceOf(StoreNotFoundException::class, $e);
            $this->assertInstanceOf(InvalidStoreAddressException::class, $e);
            $this->assertStringContainsString('reserved gRPC', $e->getMessage());
        }
    }

    /**
     * @return array<string, array{string}>
     */
    public static function reservedSchemeHostProvider(): array
    {
        return [
            'unix' => ['unix:20160'],
            'unix uppercase' => ['UNIX:20160'],
            'unix-abstract' => ['unix-abstract:20160'],
            'unix-abstract uppercase' => ['Unix-Abstract:20160'],
            'unix-gram' => ['unix-gram:20160'],
            'unix-dgram' => ['unix-dgram:20160'],
            'dns' => ['dns:20160'],
            'dns uppercase' => ['DNS:20160'],
            'ipv4' => ['ipv4:20160'],
            'ipv6' => ['ipv6:20160'],
            'ipv6 uppercase' => ['IPv6:20160'],
            'vsock' => ['vsock:20160'],
            'http' => ['http:20160'],
            'https' => ['https:20160'],
            'tcp' => ['tcp:20160'],
            'tls' => ['tls:20160'],
        ];
    }

    public function testStoreNotFoundStillThrowsStoreNotFoundException(): void
    {
        $this->pdClient->expects($this->once())->method('getStore')->willReturn(null);

        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->expectException(StoreNotFoundException::class);
        $resolver->resolveStoreAddress(99);
    }

    public function testValidateStoreAddressRejectsMalformedAddressDirectly(): void
    {
        $resolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->expectException(InvalidStoreAddressException::class);
        $resolver->validateStoreAddress('unix:/var/run/docker.sock', 1);
    }

    private function store(string $address): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress($address);

        return $store;
    }
}
