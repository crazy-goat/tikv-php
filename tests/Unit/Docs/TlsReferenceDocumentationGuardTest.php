<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #689: the TLS reference has to answer three
 * questions a reader cannot answer from the code alone.
 *
 * `ConnectionFactory::buildTlsConfig()` accepts the same credential as a file
 * path, as inline PEM and (for backward compatibility) as a guessing key, and
 * silently picks one when several are present — so the precedence has to be
 * written down, including the fact that a file beats the matching PEM form.
 * `TlsConfigBuilder::withCaCertFile()` / `withClientCertFile()` take an
 * optional `$baseDir` argument that mirrors `caCertBaseDir` /
 * `clientCertBaseDir`, and a builder example without it hides the only
 * path-traversal guard the manual path offers.
 *
 * The pre-connection `InvalidArgumentException`s are the third gap: they are
 * raised before a channel exists, so the "Handshake failed" entry in
 * `docs/troubleshooting.md` never mentions them, and an operator grepping a
 * log for one of those messages lands nowhere. The guard pins every message
 * against the class that throws it as well as against the troubleshooting
 * entry, so a reworded message cannot leave the documentation quoting a
 * string that no longer exists.
 */
final class TlsReferenceDocumentationGuardTest extends TestCase
{
    private const CONFIGURATION = 'docs/configuration.md';
    private const TROUBLESHOOTING = 'docs/troubleshooting.md';

    /**
     * Every pre-connection TLS error message, and the class that throws it.
     *
     * @var list<array{string, string}>
     */
    private const ERROR_MESSAGES = [
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'TLS value looks like a file path but the file does not exist or is not readable',
        ],
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'TLS file has disallowed extension "%s"',
        ],
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'Cannot read TLS file: file does not exist or is not readable',
        ],
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'Invalid TLS base directory: directory does not exist',
        ],
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'TLS file path is outside the allowed base directory',
        ],
        [
            'src/Client/Tls/TlsConfigBuilder.php',
            'Cannot read TLS file: read failed',
        ],
        [
            'src/Client/Tls/TlsConfig.php',
            'Both clientCert and clientKey must be provided together',
        ],
        [
            'src/Client/Tls/TlsConfig.php',
            '%s does not contain PEM data',
        ],
        [
            'src/Client/Tls/TlsConfig.php',
            'Partial TLS configuration:',
        ],
    ];

    public function testConfigurationDocumentStatesTheTlsOptionPrecedence(): void
    {
        $contents = $this->read(self::CONFIGURATION);

        self::assertStringContainsString('### TLS Option Precedence', $contents);
        self::assertStringContainsString(
            '| CA certificate | `caCertFile` → `caCertPem` → `caCert` (deprecated) |',
            $contents,
            'docs/configuration.md must state the CA precedence: caCertFile, '
            . 'then caCertPem, then the deprecated caCert.',
        );
        self::assertStringContainsString(
            'troubleshooting.md#tls-configuration-errors',
            $contents,
            'The mixed-pair bullet must link to the troubleshooting entry that explains it.',
        );

        self::assertStringContainsString(
            '| Client certificate and key | `clientCertFile` + `clientKeyFile` → '
            . '`clientCertPem` + `clientKeyPem` → `clientCert` + `clientKey` (deprecated) |',
            $contents,
            'docs/configuration.md must state the client-pair precedence: the '
            . 'file pair, then the PEM pair, then the deprecated keys.',
        );
    }

    public function testConfigurationDocumentDocumentsTheBuilderBaseDirectoryArgument(): void
    {
        $contents = $this->read(self::CONFIGURATION);

        self::assertStringContainsString(
            'withCaCertFile($path, $baseDir = null)',
            $contents,
            'docs/configuration.md must document the $baseDir argument of withCaCertFile().',
        );
        self::assertStringContainsString(
            'withClientCertFile($certPath, $keyPath, $baseDir = null)',
            $contents,
            'docs/configuration.md must document the $baseDir argument of withClientCertFile().',
        );
    }

    public function testTroubleshootingListsEveryPreConnectionTlsError(): void
    {
        $troubleshooting = $this->read(self::TROUBLESHOOTING);
        self::assertStringContainsString('#### TLS Configuration Errors', $troubleshooting);
        self::assertStringContainsString(
            'configuration.md#tlsssl-configuration',
            $troubleshooting,
            'The troubleshooting section must link to the existing TLS/SSL Configuration heading.',
        );

        $violations = [];

        foreach (self::ERROR_MESSAGES as [$source, $message]) {
            if (!str_contains($this->read($source), $message)) {
                $violations[] = sprintf('%s no longer contains the message "%s"', $source, $message);
            }
            if (!str_contains($troubleshooting, $message)) {
                $violations[] = sprintf(
                    '%s does not explain the message "%s"',
                    self::TROUBLESHOOTING,
                    $message,
                );
            }
        }

        self::assertSame(
            [],
            $violations,
            "Pre-connection TLS errors that are not documented:\n" . implode("\n", $violations),
        );
    }

    private function read(string $relativePath): string
    {
        $path = dirname(__DIR__, 3) . '/' . $relativePath;
        self::assertFileExists($path);

        $contents = file_get_contents($path);
        self::assertNotFalse($contents, "Unable to read {$relativePath}");

        return $contents;
    }
}
