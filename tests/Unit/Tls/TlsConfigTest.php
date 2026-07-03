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

    public function testIsEnabledReturnsTrueWhenCaCertPresent(): void
    {
        $config = new TlsConfig(caCert: 'ca-content');
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsTrueWhenClientCertPresentWithoutCaCert(): void
    {
        $config = new TlsConfig(clientCert: 'client-cert');
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsTrueWhenClientKeyPresentWithoutCaCert(): void
    {
        $config = new TlsConfig(clientKey: 'client-key');
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsFalseWhenAllNull(): void
    {
        $config = new TlsConfig();
        $this->assertFalse($config->isEnabled());
    }

    public function testIsEnabledReturnsTrueForFullTlsConfig(): void
    {
        $config = new TlsConfig(
            caCert: 'ca-content',
            clientCert: 'client-cert',
            clientKey: 'client-key',
        );
        $this->assertTrue($config->isEnabled());
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
