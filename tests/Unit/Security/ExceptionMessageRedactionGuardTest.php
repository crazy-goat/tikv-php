<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Security;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #269 (GRPC-10): raw user keys must never be
 * interpolated into an exception message.
 *
 * The issue asked for a static-analysis rule (PHPCS/PHPStan) so the four
 * sites cannot regress. Writing a custom PHPStan/PHPCS extension for this is
 * out of proportion, so this test is the pragmatic stand-in: it scans
 * src/Client for the two exact sprintf() message patterns the issue
 * reintroduced and fails when the format argument is a bare `$key` instead of
 * KeyRedactor::redact($key).
 *
 * The scan is deliberately narrow (it only looks at the message phrase the
 * issue named) so it cannot flip green when the redaction is removed:
 * `KeyRedactor::redact($key)` puts `(` before `$key`, so the
 * comma-then-`$key` argument shape is what identifies the raw form.
 */
class ExceptionMessageRedactionGuardTest extends TestCase
{
    public function testNoRawKeyInterpolationInExceptionMessages(): void
    {
        $violations = [];

        foreach ($this->phpFilesUnder(__DIR__ . '/../../../src/Client') as $file) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            // Raw form: the named message phrase followed, within the same
            // statement, by an argument list whose argument is a bare `$key`.
            // Redacted form: `..., KeyRedactor::redact($key))` — the comma is
            // followed by `KeyRedactor`, so this pattern does not match.
            if (
                preg_match(
                    '/(?:exhausted for key|per-pair error for key) "%s"[^;]*?,\s*\$key\b/s',
                    $contents,
                    $matches,
                ) === 1
            ) {
                $violations[] = $file . ': ' . trim($matches[0]);
            }
        }

        self::assertSame(
            [],
            $violations,
            "Raw key interpolated into an exception message:\n" . implode("\n", $violations),
        );
    }

    /**
     * @return list<string>
     */
    private function phpFilesUnder(string $dir): array
    {
        $files = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }
}
