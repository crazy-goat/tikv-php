<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #384: the documentation must recommend the
 * explicit TLS option keys and must label the legacy ones as deprecated.
 *
 * `ConnectionFactory::buildTlsConfig()` prefers `caCertFile` /
 * `clientCertFile` + `clientKeyFile` — each with an optional `*BaseDir`
 * path-traversal guard — and the inline `*Pem` variants. The legacy `caCert`,
 * `clientCert` and `clientKey` keys are the backward-compatible fallbacks:
 * they guess between a file path and inline PEM content from the value
 * itself, and they have no base-directory counterpart at all, so the
 * restriction that keeps a certificate inside one directory cannot be applied
 * to them.
 *
 * A document that teaches the legacy keys sends readers to the ambiguous path,
 * so the guard fails on three shapes:
 *
 *  - a legacy key used as an options-array key in a documented snippet
 *    (`'caCert' => …`) — the way the deprecated form gets recommended;
 *  - a legacy key named in prose without a "deprecated" label in the same
 *    paragraph — the way the legacy form gets presented as current;
 *  - a supported key that no longer appears in one of the documents the issue
 *    names, which is how the recommended form disappears silently.
 *
 * The prose rule looks at whole words only and skips fenced code blocks, so
 * `$caCert` as a PHP variable holding PEM content is not the deprecated
 * option and is not reported.
 */
class TlsOptionKeysDocumentationGuardTest extends TestCase
{
    /**
     * The deprecated TLS option keys, as they are spelled inside
     * `options['tls']`.
     */
    private const DEPRECATED_KEYS = ['caCert', 'clientCert', 'clientKey'];

    /**
     * The supported TLS option keys, as they are spelled inside
     * `options['tls']`.
     */
    private const SUPPORTED_KEYS = [
        'caCertFile',
        'caCertBaseDir',
        'caCertPem',
        'clientCertFile',
        'clientCertBaseDir',
        'clientKeyFile',
        'clientCertPem',
        'clientKeyPem',
    ];

    /**
     * The documents the issue names. Each one has to show every supported
     * key, so the recommended form cannot drop out of one of them unnoticed.
     */
    private const DOCUMENTED_FILES = [
        'README.md',
        'docs/advanced.md',
        'docs/configuration.md',
    ];

    public function testNoDocumentedSnippetUsesADeprecatedTlsOptionKey(): void
    {
        $violations = [];

        foreach ($this->documentationFiles() as $file) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            foreach (self::DEPRECATED_KEYS as $key) {
                $pattern = sprintf('/([\'"])%s\1\s*=>/', preg_quote($key, '/'));
                $matches = [];
                $found = preg_match_all($pattern, $contents, $matches, PREG_OFFSET_CAPTURE);
                self::assertIsInt($found);

                foreach ($matches[0] as $match) {
                    $offset = $match[1];
                    $violations[] = sprintf(
                        '%s:%d uses the deprecated TLS option key %s',
                        $this->relative($file),
                        substr_count(substr($contents, 0, $offset), "\n") + 1,
                        $key,
                    );
                }
            }
        }

        self::assertSame(
            [],
            $violations,
            "Documented TLS options that use a deprecated key (use the *File / "
            . "*Pem keys instead):\n" . implode("\n", $violations),
        );
    }

    public function testEveryMentionOfADeprecatedTlsOptionKeyIsLabelledDeprecated(): void
    {
        $violations = [];

        foreach ($this->documentationFiles() as $file) {
            foreach ($this->proseParagraphs($file) as $paragraph) {
                $mentions = $this->deprecatedKeysIn($paragraph['text']);
                if ($mentions === []) {
                    continue;
                }

                if (str_contains(strtolower($paragraph['text']), 'deprecated')) {
                    continue;
                }

                $violations[] = sprintf(
                    '%s:%d mentions the deprecated TLS option key(s) %s without '
                    . 'labelling them deprecated',
                    $this->relative($file),
                    $paragraph['line'],
                    implode(', ', $mentions),
                );
            }
        }

        self::assertSame(
            [],
            $violations,
            "Deprecated TLS option keys mentioned without a deprecation label:\n"
            . implode("\n", $violations),
        );
    }

    public function testEveryDocumentedFileShowsTheSupportedTlsOptionKeys(): void
    {
        $violations = [];

        foreach (self::DOCUMENTED_FILES as $documented) {
            $path = $this->repositoryRoot() . '/' . $documented;
            self::assertFileExists($path);

            $contents = file_get_contents($path);
            self::assertNotFalse($contents, "Unable to read {$path}");

            foreach (self::SUPPORTED_KEYS as $key) {
                if (preg_match('/(?<![$\w])' . preg_quote($key, '/') . '\b/', $contents) !== 1) {
                    $violations[] = sprintf('%s does not mention the TLS option key %s', $documented, $key);
                }
            }
        }

        self::assertSame(
            [],
            $violations,
            "Supported TLS option keys missing from the documentation:\n" . implode("\n", $violations),
        );
    }

    /**
     * The deprecated keys named in a piece of text, as whole words.
     *
     * A name followed by a word character is a different option
     * (`caCertFile`, `clientCertBaseDir`, …), and a name preceded by `$` is
     * a PHP variable, not an option key.
     *
     * @return list<string>
     */
    private function deprecatedKeysIn(string $text): array
    {
        $found = [];

        foreach (self::DEPRECATED_KEYS as $key) {
            $pattern = sprintf('/(?<![$\w])%s\b/', preg_quote($key, '/'));
            if (preg_match($pattern, $text) === 1) {
                $found[] = $key;
            }
        }

        return $found;
    }

    /**
     * The prose paragraphs of a markdown file, fenced code blocks excluded.
     *
     * A paragraph is a run of consecutive non-blank lines, which is also what
     * a reader perceives as one block of text — so a deprecation label at the
     * end of a wrapped sentence still counts for the whole paragraph.
     *
     * @return list<array{line: int, text: string}>
     */
    private function proseParagraphs(string $file): array
    {
        $lines = file($file, FILE_IGNORE_NEW_LINES);
        self::assertNotFalse($lines, "Unable to read {$file}");

        $paragraphs = [];
        $buffer = [];
        $bufferStart = 0;
        $inFence = false;

        foreach ($lines as $index => $line) {
            $trimmed = trim($line);

            if ($this->fenceRun($trimmed) !== null) {
                $inFence = !$inFence;
                $paragraph = $this->takeParagraph($buffer, $bufferStart);
                if ($paragraph !== null) {
                    $paragraphs[] = $paragraph;
                }
                continue;
            }

            if ($inFence) {
                continue;
            }

            if ($trimmed === '') {
                $paragraph = $this->takeParagraph($buffer, $bufferStart);
                if ($paragraph !== null) {
                    $paragraphs[] = $paragraph;
                }
                continue;
            }

            if ($buffer === []) {
                $bufferStart = $index + 1;
            }
            $buffer[] = $line;
        }

        $paragraph = $this->takeParagraph($buffer, $bufferStart);
        if ($paragraph !== null) {
            $paragraphs[] = $paragraph;
        }

        return $paragraphs;
    }

    /**
     * The buffered paragraph, and the buffer emptied.
     *
     * @param list<string> $buffer
     * @return array{line: int, text: string}|null
     */
    private function takeParagraph(array &$buffer, int $start): ?array
    {
        if ($buffer === []) {
            return null;
        }

        $paragraph = ['line' => $start, 'text' => implode("\n", $buffer)];
        $buffer = [];

        return $paragraph;
    }

    /**
     * The run of backticks or tildes with which a line opens or closes a
     * fenced block, or null when the line is not a fence at all.
     */
    private function fenceRun(string $trimmedLine): ?string
    {
        $matches = [];
        if (preg_match('/^(?<run>`{3,}|~{3,})/', $trimmedLine, $matches) !== 1) {
            return null;
        }

        return $matches['run'];
    }

    /**
     * Every markdown file a reader may copy from: the whole docs/ tree plus
     * the README. Recursed so a new docs subdirectory is covered without
     * touching this test.
     *
     * @return list<string>
     */
    private function documentationFiles(): array
    {
        $repoRoot = $this->repositoryRoot();
        $files = [$repoRoot . '/README.md'];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($repoRoot . '/docs', \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'md') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }

    private function repositoryRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    private function relative(string $file): string
    {
        $repoRoot = $this->repositoryRoot() . '/';

        return str_starts_with($file, $repoRoot) ? substr($file, strlen($repoRoot)) : $file;
    }
}
