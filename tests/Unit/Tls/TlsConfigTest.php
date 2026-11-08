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
        try {
            new TlsConfig(clientCert: 'cert-only');
            $this->fail('Expected InvalidArgumentException was not thrown');
        } catch (InvalidArgumentException $e) {
            $this->assertSame(
                'TlsConfig requires $clientCert and $clientKey together; '
                . 'got $clientCert=true, $clientKey=false',
                $e->getMessage(),
            );
        }
    }

    public function testConstructionWithClientKeyOnlyThrows(): void
    {
        try {
            new TlsConfig(clientKey: 'key-only');
            $this->fail('Expected InvalidArgumentException was not thrown');
        } catch (InvalidArgumentException $e) {
            $this->assertSame(
                'TlsConfig requires $clientCert and $clientKey together; '
                . 'got $clientCert=false, $clientKey=true',
                $e->getMessage(),
            );
        }
    }

    public function testConstructionWithClientCertAndKeyWithoutCaThrows(): void
    {
        try {
            new TlsConfig(clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);
            $this->fail('Expected InvalidArgumentException was not thrown');
        } catch (InvalidArgumentException $e) {
            $this->assertSame(
                'Partial TLS configuration: $clientCert and $clientKey are set but '
                . '$caCert is not. A CA certificate is required to verify the server '
                . 'certificate. Provide one with caCertFile (or caCertPem), or remove '
                . 'the client certificate and key ($clientCert and $clientKey) if you '
                . 'intend to use an insecure connection.',
                $e->getMessage(),
            );
        }
    }

    /**
     * Issue #688: the messages must not name a bare deprecated option key.
     *
     * `caCert`, `clientCert` and `clientKey` are both the TlsConfig property
     * names and the deprecated `options['tls']` keys, so a message that names
     * one bare leaves the reader guessing which it means. The properties carry
     * a `$`, and the only option key a message names is a supported one.
     */
    public function testMessagesDoNotNameBareDeprecatedOptionKeys(): void
    {
        $messages = [];

        try {
            new TlsConfig(clientCert: self::CERT_PEM);
            $this->fail('Expected InvalidArgumentException was not thrown');
        } catch (InvalidArgumentException $e) {
            $messages[] = $e->getMessage();
        }

        try {
            new TlsConfig(clientCert: self::CERT_PEM, clientKey: self::KEY_PEM);
            $this->fail('Expected InvalidArgumentException was not thrown');
        } catch (InvalidArgumentException $e) {
            $messages[] = $e->getMessage();
        }

        $this->assertCount(2, $messages);

        foreach ($messages as $message) {
            $this->assertDoesNotMatchRegularExpression('/(?<![$\w])(caCert|clientCert|clientKey)\b/', $message);
        }

        $this->assertStringContainsString('caCertFile', implode("\n", $messages));
    }

    // ========================================================================
    // SEC-12: no in-memory scrubbing
    // ========================================================================

    /**
     * PHP strings are immutable and copy-on-write, so a "zero the key"
     * method only rebinds the property to a new string: the original buffer
     * and the copy already handed to gRPC stay in memory. Such a method also
     * breaks reconnection when a caller uses it as documented, because the
     * transport rebuilds the credentials from this object after eviction
     * (issue #315, SEC-12). Do not reintroduce one.
     */
    public function testThereIsNoCloseMethodThatClaimsToScrubCredentials(): void
    {
        $this->assertFalse(
            (new \ReflectionClass(TlsConfig::class))->hasMethod('close'),
            'TlsConfig must not expose a credential-scrubbing close(): PHP cannot zero a string, '
            . 'and zeroing clientKey would break channel re-creation after eviction',
        );
    }

    // ========================================================================
    // SEC-02: PEM validation
    // ========================================================================

    public function testNonPemCaCertThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('$caCert does not contain PEM data');

        new TlsConfig(caCert: '/path/to/ca.crt');
    }

    public function testNonPemClientCertThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('$clientCert does not contain PEM data');

        new TlsConfig(caCert: self::SHORT_CA_PEM, clientCert: 'not-pem', clientKey: self::SHORT_KEY_PEM);
    }

    public function testNonPemClientKeyThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('$clientKey does not contain PEM data');

        new TlsConfig(caCert: self::SHORT_CA_PEM, clientCert: self::SHORT_CERT_PEM, clientKey: 'not-pem');
    }
}
