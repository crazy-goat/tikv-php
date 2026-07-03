<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Tls;

use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use PHPUnit\Framework\TestCase;

class TlsConfigTest extends TestCase
{
    public function testConstructionWithAllFields(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert-content',
            clientKey: 'client-key-content',
        );

        $this->assertSame('ca-content', $config->caCert);
        $this->assertSame('client-cert-content', $config->clientCert);
        $this->assertSame('client-key-content', $config->clientKey);
    }

    public function testConstructionWithNulls(): void
    {
        $config = new TlsConfig();

        $this->assertNull($config->caCert);
        $this->assertNull($config->clientCert);
        $this->assertNull($config->clientKey);
    }

    public function testConstructionWithCaCertOnly(): void
    {
        $config = new TlsConfig(caCert: 'ca-content');
        $this->assertSame('ca-content', $config->caCert);
        $this->assertNull($config->clientCert);
        $this->assertNull($config->clientKey);
    }

    // ========================================================================
    // isEnabled()
    // ========================================================================

    public function testIsEnabledReturnsTrueWhenCaCertPresent(): void
    {
        $config = new TlsConfig(caCert: 'ca-content');
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsTrueWhenClientCertAndKeyPresent(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert',
            clientKey: 'client-key',
        );
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsFalseWhenAllNull(): void
    {
        $config = new TlsConfig();
        $this->assertFalse($config->isEnabled());
    }

    // ========================================================================
    // isComplete()
    // ========================================================================

    public function testIsCompleteReturnsTrueWithCaCertOnly(): void
    {
        $config = new TlsConfig(caCert: 'ca-content');
        $this->assertTrue($config->isComplete());
    }

    public function testIsCompleteReturnsTrueWithAllFields(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert',
            clientKey: 'client-key',
        );
        $this->assertTrue($config->isComplete());
    }

    public function testIsCompleteReturnsFalseWithNoCaCert(): void
    {
        $config = new TlsConfig();
        $this->assertFalse($config->isComplete());
    }

    // ========================================================================
    // Constructor validation — partial config rejection
    // ========================================================================

    public function testConstructionWithClientCertOnlyThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Both clientCert and clientKey must be provided together');

        new TlsConfig(clientCert: 'cert-only');
    }

    public function testConstructionWithClientKeyOnlyThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Both clientCert and clientKey must be provided together');

        new TlsConfig(clientKey: 'key-only');
    }

    public function testConstructionWithClientCertAndKeyWithoutCaThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Partial TLS configuration');

        new TlsConfig(
            clientCert: 'client-cert',
            clientKey: 'client-key',
        );
    }

    // ========================================================================
    // close() key-zeroing
    // ========================================================================

    public function testCloseZeroesClientKey(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert-content',
            clientKey: 'client-key-content',
        );

        $config->close();

        $this->assertNotNull($config->clientKey);
        $this->assertSame(strlen('client-key-content'), strlen($config->clientKey));
        $this->assertSame(
            str_repeat("\0", strlen('client-key-content')),
            $config->clientKey,
        );
    }

    public function testCloseWithNullClientKeyDoesNotCrash(): void
    {
        $config = new TlsConfig(caCert: 'ca-content');

        // Should not throw
        $config->close();

        $this->assertNull($config->clientKey);
    }

    public function testCloseDoesNotAffectCaCertOrClientCert(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert-content',
            clientKey: 'sensitive-key',
        );

        $config->close();

        $this->assertSame('ca-content', $config->caCert);
        $this->assertSame('client-cert-content', $config->clientCert);
    }
}
