<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Tls;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use PHPUnit\Framework\TestCase;

class TlsConfigTest extends TestCase
{
    private const CA_PEM = "-----BEGIN CERTIFICATE-----\nca-content\n-----END CERTIFICATE-----";
    private const CERT_PEM = "-----BEGIN CERTIFICATE-----\nclient-cert-content\n-----END CERTIFICATE-----";
    private const KEY_PEM = "-----BEGIN PRIVATE KEY-----\nclient-key-content\n-----END PRIVATE KEY-----";
    private const SHORT_CA_PEM = "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----";
    private const SHORT_CERT_PEM = "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----";
    private const SHORT_KEY_PEM = "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----";

    public function testConstructionWithAllFields(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM, clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);

        $this->assertSame(self::CA_PEM, $config->caCert);
        $this->assertSame(self::CERT_PEM, $config->clientCert);
        $this->assertSame(self::KEY_PEM, $config->clientKey);
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
        $config = new TlsConfig(caCert: self::CA_PEM);
        $this->assertSame(self::CA_PEM, $config->caCert);
        $this->assertNull($config->clientCert);
        $this->assertNull($config->clientKey);
    }

    // ========================================================================
    // isEnabled()
    // ========================================================================

    public function testIsEnabledReturnsTrueWhenCaCertPresent(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM);
        $this->assertTrue($config->isEnabled());
    }

    public function testIsEnabledReturnsTrueWhenClientCertAndKeyPresent(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM, clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);
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
        $config = new TlsConfig(caCert: self::CA_PEM);
        $this->assertTrue($config->isComplete());
    }

    public function testIsCompleteReturnsTrueWithAllFields(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM, clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);
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
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Both clientCert and clientKey must be provided together');

        new TlsConfig(clientCert: 'cert-only');
    }

    public function testConstructionWithClientKeyOnlyThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Both clientCert and clientKey must be provided together');

        new TlsConfig(clientKey: 'key-only');
    }

    public function testConstructionWithClientCertAndKeyWithoutCaThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Partial TLS configuration');

        new TlsConfig(clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);
    }

    // ========================================================================
    // close() key-zeroing
    // ========================================================================

    public function testCloseZeroesClientKey(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM, clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);

        $config->close();

        $this->assertNotNull($config->clientKey);
        $this->assertSame(strlen(self::KEY_PEM), strlen($config->clientKey));
        $this->assertSame(str_repeat("\0", strlen(self::KEY_PEM)), $config->clientKey);
    }

    public function testCloseWithNullClientKeyDoesNotCrash(): void
    {
        $config = new TlsConfig(caCert: self::CA_PEM);

        // Should not throw
        $config->close();

        $this->assertNull($config->clientKey);
    }

    public function testCloseDoesNotAffectCaCertOrClientCert(): void
    {
        $config = new TlsConfig(
            caCert: self::CA_PEM,
            clientCert: self::CERT_PEM,
            clientKey: "-----BEGIN PRIVATE KEY-----\nsensitive-key\n-----END PRIVATE KEY-----",
        );

        $config->close();

        $this->assertSame(self::CA_PEM, $config->caCert);
        $this->assertSame(self::CERT_PEM, $config->clientCert);
    }

    // ========================================================================
    // SEC-02: PEM validation
    // ========================================================================

    public function testNonPemCaCertThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('caCert does not contain PEM data');

        new TlsConfig(caCert: '/path/to/ca.crt');
    }

    public function testNonPemClientCertThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('clientCert does not contain PEM data');

        new TlsConfig(caCert: self::SHORT_CA_PEM, clientCert: 'not-pem', clientKey: self::SHORT_KEY_PEM);
    }

    public function testNonPemClientKeyThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('clientKey does not contain PEM data');

        new TlsConfig(caCert: self::SHORT_CA_PEM, clientCert: self::SHORT_CERT_PEM, clientKey: 'not-pem');
    }
}
