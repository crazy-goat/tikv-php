<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use Grpc\ChannelCredentials;
use PHPUnit\Framework\TestCase;

/**
 * SEC-12 (issue #315): removing `TlsConfig::close()` must not break the TLS
 * channel lifecycle.
 *
 * The transport rebuilds the credentials from the `TlsConfig` every time a
 * channel is created, including after an idle or LRU eviction. The removed
 * `close()` overwrote `clientKey` with NUL bytes, so a caller who followed its
 * docblock would have reopened the channel with a zeroed key and failed the
 * handshake. This test pins the invariant the fix relies on: the certificate
 * material handed to the credentials factory after an eviction is the original
 * configuration, untouched.
 *
 * A real handshake needs a TLS-enabled server, which the unit suites do not
 * have. The `credentialsFactory` seam records the material the channel would
 * have been built from, and returns a valid (default-roots) credentials object
 * so the channel itself is still created through the TLS branch.
 */
final class GrpcClientTlsReconnectTest extends TestCase
{
    use GrpcExtensionGate;

    private const CA_PEM = "-----BEGIN CERTIFICATE-----\nca-content\n-----END CERTIFICATE-----";
    private const CERT_PEM = "-----BEGIN CERTIFICATE-----\nclient-cert-content\n-----END CERTIFICATE-----";
    private const KEY_PEM = "-----BEGIN PRIVATE KEY-----\nclient-key-content\n-----END PRIVATE KEY-----";

    private const ADDRESS = '127.0.0.1:41300';

    /** Idle TTL of 100 s: advancing 200 s makes the channel evictable. */
    private const IDLE_TTL_MS = 100_000;

    private float $now = 1000.0;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testAnIdleTlsChannelIsRecreatedFromTheIntactCredentials(): void
    {
        /** @var list<array{?string, ?string, ?string}> $seen */
        $seen = [];

        $client = new GrpcClient(
            tlsConfig: new TlsConfig(self::CA_PEM, self::CERT_PEM, self::KEY_PEM),
            idleTtlMs: self::IDLE_TTL_MS,
            clock: fn (): float => $this->now,
            credentialsFactory: function (?string $ca, ?string $cert, ?string $key) use (&$seen): ChannelCredentials {
                $seen[] = [$ca, $cert, $key];

                return ChannelCredentials::createSsl();
            },
        );

        try {
            $first = $client->getChannel(self::ADDRESS);

            // Not idle yet: the sweep keeps the channel.
            $client->reapIdleChannels();
            $this->assertSame(1, $client->getChannelCount());

            $this->now += 200.0;
            $client->reapIdleChannels();
            $this->assertSame(0, $client->getChannelCount());

            $second = $client->getChannel(self::ADDRESS);

            $this->assertNotSame($first, $second, 'The evicted channel must be replaced by a new handle');
            $this->assertSame(1, $client->getChannelCount());
        } finally {
            $client->close();
        }

        $this->assertSame(
            [
                [self::CA_PEM, self::CERT_PEM, self::KEY_PEM],
                [self::CA_PEM, self::CERT_PEM, self::KEY_PEM],
            ],
            $seen,
            'Re-creation after eviction must reuse the intact TLS material',
        );
    }
}
