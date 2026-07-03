<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Tls;

use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
use PHPUnit\Framework\TestCase;

class TlsConfigBuilderTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/tikv-test-' . uniqid();
        mkdir($this->tempDir, 0777, true);
    }

    protected function tearDown(): void
    {
        if (is_dir($this->tempDir)) {
            $files = glob($this->tempDir . '/*');
            if ($files !== false) {
                foreach ($files as $file) {
                    unlink($file);
                }
            }
            rmdir($this->tempDir);
        }
    }

    public function testWithCaCertFromFile(): void
    {
        $certContent = 'test-ca-cert-content';
        $certPath = $this->tempDir . '/ca.crt';
        file_put_contents($certPath, $certContent);

        $config = (new TlsConfigBuilder())
            ->withCaCert($certPath)
            ->build();

        $this->assertSame($certContent, $config->caCert);
    }

    public function testWithCaCertFromContent(): void
    {
        $certContent = 'inline-ca-cert-content';

        $config = (new TlsConfigBuilder())
            ->withCaCert($certContent)
            ->build();

        $this->assertSame($certContent, $config->caCert);
    }

    public function testWithClientCertFromFiles(): void
    {
        $certContent = 'test-client-cert';
        $keyContent = 'test-client-key';
        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($certPath, $certContent);
        file_put_contents($keyPath, $keyContent);

        $config = (new TlsConfigBuilder())
            ->withClientCert($certPath, $keyPath)
            ->build();

        $this->assertSame($certContent, $config->clientCert);
        $this->assertSame($keyContent, $config->clientKey);
    }

    public function testBuildReturnsEmptyConfig(): void
    {
        $config = (new TlsConfigBuilder())->build();

        $this->assertNull($config->caCert);
        $this->assertNull($config->clientCert);
        $this->assertNull($config->clientKey);
        $this->assertFalse($config->isEnabled());
    }

    // ========================================================================
    // Validation: disallowed file extension
    // ========================================================================

    public function testDisallowedExtensionThrowsInvalidArgumentException(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $path = $this->tempDir . '/ca.txt';
        file_put_contents($path, 'content');

        (new TlsConfigBuilder())->withCaCert($path);
    }

    public function testDisallowedExtensionForClientCertThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $certPath = $this->tempDir . '/client.xml';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($certPath, 'cert');
        file_put_contents($keyPath, 'key');

        (new TlsConfigBuilder())->withClientCert($certPath, $keyPath);
    }

    public function testDisallowedExtensionForClientKeyThrows(): void
    {
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.json';
        file_put_contents($certPath, 'cert');
        file_put_contents($keyPath, 'key');

        (new TlsConfigBuilder())->withClientCert($certPath, $keyPath);
    }

    // ========================================================================
    // Allowed extensions (positive tests)
    // ========================================================================

    public function testWithCaCertFromPemFile(): void
    {
        $content = 'pem-content';
        $path = $this->tempDir . '/ca.pem';
        file_put_contents($path, $content);

        $config = (new TlsConfigBuilder())->withCaCert($path)->build();

        $this->assertSame($content, $config->caCert);
    }

    public function testWithCaCertFromKeyFile(): void
    {
        $content = 'key-content';
        $path = $this->tempDir . '/ca.key';
        file_put_contents($path, $content);

        $config = (new TlsConfigBuilder())->withCaCert($path)->build();

        $this->assertSame($content, $config->caCert);
    }
}
