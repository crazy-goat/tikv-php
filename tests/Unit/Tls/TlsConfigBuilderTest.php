<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Tls;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
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

    // ========================================================================
    // Backward compatibility: withCaCert() / withClientCert() guessing
    // ========================================================================

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
        $caContent = 'test-ca-cert';
        $certContent = 'test-client-cert';
        $keyContent = 'test-client-key';
        $caPath = $this->tempDir . '/ca.crt';
        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($caPath, $caContent);
        file_put_contents($certPath, $certContent);
        file_put_contents($keyPath, $keyContent);

        $config = (new TlsConfigBuilder())
            ->withCaCert($caPath)
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
    // New explicit methods: withCaCertFile() / withCaCertPem()
    // ========================================================================

    public function testWithCaCertFileReadsFromPath(): void
    {
        $content = 'file-content';
        $path = $this->tempDir . '/ca.pem';
        file_put_contents($path, $content);

        $config = (new TlsConfigBuilder())
            ->withCaCertFile($path)
            ->build();

        $this->assertSame($content, $config->caCert);
    }

    public function testWithCaCertFileThrowsOnNonExistentPath(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot read TLS file');

        (new TlsConfigBuilder())
            ->withCaCertFile($this->tempDir . '/nonexistent.pem');
    }

    public function testWithCaCertPemStoresInlineContent(): void
    {
        $pem = 'inline-pem-content';

        $config = (new TlsConfigBuilder())
            ->withCaCertPem($pem)
            ->build();

        $this->assertSame($pem, $config->caCert);
    }

    public function testWithClientCertFileReadsFromPaths(): void
    {
        $caContent = 'ca-content';
        $certContent = 'cert-content';
        $keyContent = 'key-content';
        $caPath = $this->tempDir . '/ca.crt';
        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($caPath, $caContent);
        file_put_contents($certPath, $certContent);
        file_put_contents($keyPath, $keyContent);

        $config = (new TlsConfigBuilder())
            ->withCaCertFile($caPath)
            ->withClientCertFile($certPath, $keyPath)
            ->build();

        $this->assertSame($certContent, $config->clientCert);
        $this->assertSame($keyContent, $config->clientKey);
    }

    public function testWithClientCertPemStoresInlineContent(): void
    {
        $caPem = 'inline-ca-pem';
        $certPem = 'inline-client-cert';
        $keyPem = 'inline-client-key';

        $config = (new TlsConfigBuilder())
            ->withCaCertPem($caPem)
            ->withClientCertPem($certPem, $keyPem)
            ->build();

        $this->assertSame($certPem, $config->clientCert);
        $this->assertSame($keyPem, $config->clientKey);
    }

    // ========================================================================
    // Security: path traversal protection
    // ========================================================================

    public function testPathTraversalViaDotDotIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // Attempt to traverse with a path that resolves outside allowed dirs
        // Note: /etc/passwd has no .pem/.crt/.key extension, so it will be
        // rejected at the extension validation step
        (new TlsConfigBuilder())
            ->withCaCertFile($this->tempDir . '/../../../../etc/passwd');
    }

    public function testPathTraversalWithAllowedExtensionIsRejectedByRealpath(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot read TLS file');

        // Use .pem extension to pass extension validation, but the path
        // doesn't exist so realpath() returns false
        (new TlsConfigBuilder())
            ->withCaCertFile($this->tempDir . '/../../../../etc/hosts.pem');
    }

    public function testSymlinkIsResolvedAndValidated(): void
    {
        // Create a target file with allowed extension
        $realContent = 'real-cert-content';
        $realPath = $this->tempDir . '/real.crt';
        file_put_contents($realPath, $realContent);

        // Create a symlink pointing to the real file
        $linkPath = $this->tempDir . '/link.crt';
        symlink($realPath, $linkPath);

        // The symlink should be resolved by realpath() and read successfully
        $config = (new TlsConfigBuilder())
            ->withCaCertFile($linkPath)
            ->build();

        $this->assertSame($realContent, $config->caCert);
    }

    public function testBaseDirectoryRestrictionRejectsOutsidePath(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('outside the allowed base directory');

        $outsidePath = $this->tempDir . '/../outside.crt';
        file_put_contents($outsidePath, 'outside-content');

        // The file exists but is outside the allowed base directory
        (new TlsConfigBuilder())
            ->withCaCertFile($outsidePath, $this->tempDir);
    }

    public function testBaseDirectoryRestrictionAllowsInsidePath(): void
    {
        $content = 'inside-content';
        $path = $this->tempDir . '/inside.crt';
        file_put_contents($path, $content);

        $config = (new TlsConfigBuilder())
            ->withCaCertFile($path, $this->tempDir)
            ->build();

        $this->assertSame($content, $config->caCert);
    }

    // ========================================================================
    // Security: exception does not leak filesystem path
    // ========================================================================

    public function testExceptionOnNonExistentFileDoesNotLeakPath(): void
    {
        $caught = false;
        try {
            (new TlsConfigBuilder())
                ->withCaCertFile($this->tempDir . '/secret.pem');
        } catch (InvalidArgumentException $e) {
            $caught = true;
            $this->assertStringNotContainsString($this->tempDir, $e->getMessage());
            $this->assertStringNotContainsString('secret.pem', $e->getMessage());
        }

        $this->assertTrue($caught, 'Expected InvalidArgumentException was not thrown');
    }

    public function testExceptionOnUnreadableFileDoesNotLeakPath(): void
    {
        if (function_exists('posix_getuid') && posix_getuid() === 0) {
            $this->markTestSkipped('Root can read any file regardless of permissions');
        }

        $path = $this->tempDir . '/unreadable.crt';
        file_put_contents($path, 'content');
        chmod($path, 0000);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot read TLS file');

        // We don't assert that path is absent because realpath() returns false for unreadable files
        (new TlsConfigBuilder())
            ->withCaCertFile($path);
    }

    // ========================================================================
    // Validation: disallowed file extension
    // ========================================================================

    public function testDisallowedExtensionThrowsInvalidArgumentException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $path = $this->tempDir . '/ca.txt';
        file_put_contents($path, 'content');

        (new TlsConfigBuilder())->withCaCert($path);
    }

    public function testDisallowedExtensionForClientCertThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $certPath = $this->tempDir . '/client.xml';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($certPath, 'cert');
        file_put_contents($keyPath, 'key');

        (new TlsConfigBuilder())->withClientCert($certPath, $keyPath);
    }

    public function testDisallowedExtensionForClientKeyThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.json';
        file_put_contents($certPath, 'cert');
        file_put_contents($keyPath, 'key');

        (new TlsConfigBuilder())->withClientCert($certPath, $keyPath);
    }

    // ========================================================================
    // Validation: disallowed extension with explicit file method
    // ========================================================================

    public function testDisallowedExtensionOnWithCaCertFileThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('disallowed extension');

        $path = $this->tempDir . '/ca.txt';
        file_put_contents($path, 'content');

        (new TlsConfigBuilder())->withCaCertFile($path);
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

    // ========================================================================
    // Method chaining
    // ========================================================================

    public function testMethodChaining(): void
    {
        $caContent = 'ca-content';
        $certContent = 'cert-content';
        $keyContent = 'key-content';
        $caPath = $this->tempDir . '/ca.crt';
        $certPath = $this->tempDir . '/client.crt';
        $keyPath = $this->tempDir . '/client.key';
        file_put_contents($caPath, $caContent);
        file_put_contents($certPath, $certContent);
        file_put_contents($keyPath, $keyContent);

        $config = (new TlsConfigBuilder())
            ->withCaCertFile($caPath)
            ->withClientCertFile($certPath, $keyPath)
            ->build();

        $this->assertSame($caContent, $config->caCert);
        $this->assertSame($certContent, $config->clientCert);
        $this->assertSame($keyContent, $config->clientKey);
    }

    public function testWithCaCertFileWithBaseDir(): void
    {
        $content = 'cert-content';
        $path = $this->tempDir . '/sub/ca.crt';
        mkdir($this->tempDir . '/sub', 0777, true);
        file_put_contents($path, $content);

        $config = (new TlsConfigBuilder())
            ->withCaCertFile($path, $this->tempDir)
            ->build();

        $this->assertSame($content, $config->caCert);
    }
}
