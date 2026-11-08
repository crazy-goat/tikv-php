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
 * The supported set is read out of `ConnectionFactory::RECOGNISED_TLS_KEYS`
 * rather than copied here (issue #690): a key added to the client is demanded
 * from the documentation automatically, and a key removed from the client
 * stops being demanded. Only the deprecated subset stays a literal, because
 * "deprecated" is a documentation decision the source does not encode.
 *
 * Snippets are found with the CommonMark-aware block scanner the sibling
 * `AtomicModeDocumentationGuardTest` uses: fences close only on a run of the
 * same character at least as long as the opening one (so a 3-backtick sample
 * inside a 4-backtick fence does not end the block), and 4-space-indented
 * blocks count as code too. The prose rule looks at whole words only and skips
 * every code block, so `$caCert` as a PHP variable holding PEM content is not
 * the deprecated option and is not reported.
 */
class TlsOptionKeysDocumentationGuardTest extends TestCase
{
    /**
     * The deprecated TLS option keys, as they are spelled inside
     * `options['tls']`.
     */
    private const DEPRECATED_KEYS = ['caCert', 'clientCert', 'clientKey'];

    /**
     * The documents the issue names. Each one has to show every supported
     * key, so the recommended form cannot drop out of one of them unnoticed.
     */
    private const DOCUMENTED_FILES = [
        'README.md',
        'docs/advanced.md',
        'docs/configuration.md',
    ];

    /**
     * A fixture whose deprecated key sits in a 4-space-indented code block.
     * The block is code, so the prose rule must ignore it, but the snippet
     * rule must still report the key: an indented block is the easiest place
     * for a deprecated snippet to hide.
     */
    private const INDENTED_BLOCK_FIXTURE = 'tests/Unit/Docs/fixtures/tls-option-keys/'
        . 'deprecated-key-in-indented-block.md';

    /**
     * A fixture whose deprecated key sits inside a 4-backtick fence that
     * contains a 3-backtick fence. A bare toggle would close the outer block
     * at the inner run and turn the sample into prose; the CommonMark-aware
     * scanner keeps it in one block, so the snippet rule still sees it.
     */
    private const NESTED_FENCE_FIXTURE = 'tests/Unit/Docs/fixtures/tls-option-keys/deprecated-key-in-nested-fence.md';

    public function testNoDocumentedSnippetUsesADeprecatedTlsOptionKey(): void
    {
        $violations = $this->deprecatedKeySnippetViolations($this->documentationFiles());

        self::assertSame(
            [],
            $violations,
            "Documented TLS options that use a deprecated key (use the *File / "
            . "*Pem keys instead):\n" . implode("\n", $violations),
        );
    }

    public function testEveryMentionOfADeprecatedTlsOptionKeyIsLabelledDeprecated(): void
    {
        $violations = $this->unlabelledDeprecatedKeyViolations($this->documentationFiles());

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

            foreach ($this->supportedTlsKeys() as $key) {
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
     * The documented key set follows the client, so a key the client still
     * accepts must not silently leave the deprecated list: that would make
     * the snippet rule report a key the reader can no longer use.
     */
    public function testTheDeprecatedKeysAreStillRecognisedByTheClient(): void
    {
        $recognised = $this->recognisedTlsKeys();
        self::assertNotEmpty($recognised, 'ConnectionFactory::RECOGNISED_TLS_KEYS is empty');

        foreach (self::DEPRECATED_KEYS as $key) {
            self::assertContains(
                $key,
                $recognised,
                "{$key} is listed as deprecated here but is no longer recognised by "
                . 'ConnectionFactory; remove it from DEPRECATED_KEYS and from the documentation.',
            );
        }

        self::assertNotEmpty(
            $this->supportedTlsKeys(),
            'ConnectionFactory::RECOGNISED_TLS_KEYS minus the deprecated keys is empty',
        );
    }

    /**
     * A deprecated key in an indented block is a snippet, not prose: the
     * snippet rule reports it, the prose rule does not mistake the block for
     * a paragraph.
     */
    public function testTheSnippetRuleSeesADeprecatedKeyInAnIndentedCodeBlock(): void
    {
        $fixture = $this->repositoryRoot() . '/' . self::INDENTED_BLOCK_FIXTURE;
        self::assertFileExists($fixture);

        $snippetViolations = $this->deprecatedKeySnippetViolations([$fixture]);
        self::assertCount(1, $snippetViolations);
        self::assertStringContainsString('caCert', $snippetViolations[0]);
        self::assertStringContainsString(
            self::INDENTED_BLOCK_FIXTURE . ':5',
            $snippetViolations[0],
            'The indented block is not reported on the line it starts on',
        );

        self::assertSame(
            [],
            $this->unlabelledDeprecatedKeyViolations([$fixture]),
            'A 4-space-indented code block is read as prose',
        );
    }

    /**
     * A deprecated key inside a nested fence is still a snippet: the outer
     * 4-backtick fence does not end at the inner 3-backtick run.
     */
    public function testTheSnippetRuleSeesADeprecatedKeyInsideANestedFence(): void
    {
        $fixture = $this->repositoryRoot() . '/' . self::NESTED_FENCE_FIXTURE;
        self::assertFileExists($fixture);

        $snippetViolations = $this->deprecatedKeySnippetViolations([$fixture]);
        self::assertCount(1, $snippetViolations);
        self::assertStringContainsString('caCert', $snippetViolations[0]);
        self::assertStringContainsString(
            self::NESTED_FENCE_FIXTURE . ':5',
            $snippetViolations[0],
            'The nested fence ended at the inner 3-backtick run',
        );

        self::assertSame(
            [],
            $this->unlabelledDeprecatedKeyViolations([$fixture]),
            'A fence nested inside a longer fence is read as prose',
        );
    }

    /**
     * Every deprecated key used as an options-array key in a code block, with
     * the 1-based line it appears on.
     *
     * Only code blocks are scanned: an options-array key quoted in prose is a
     * mention, which the prose rule judges by its deprecation label. Scanning
     * the raw file here would report a labelled prose mention as a snippet.
     *
     * @param list<string> $files
     * @return list<string>
     */
    private function deprecatedKeySnippetViolations(array $files): array
    {
        $violations = [];

        foreach ($files as $file) {
            foreach ($this->codeBlocks($file) as $block) {
                foreach (self::DEPRECATED_KEYS as $key) {
                    $pattern = sprintf('/([\'"])%s\1\s*=>/', preg_quote($key, '/'));
                    $matches = [];
                    $found = preg_match_all($pattern, $block['body'], $matches, PREG_OFFSET_CAPTURE);
                    self::assertIsInt($found);

                    foreach ($matches[0] as $match) {
                        $violations[] = sprintf(
                            '%s:%d uses the deprecated TLS option key %s',
                            $this->relative($file),
                            $block['bodyStart'] + substr_count(substr($block['body'], 0, $match[1]), "\n"),
                            $key,
                        );
                    }
                }
            }
        }

        return $violations;
    }

    /**
     * Every prose paragraph that names a deprecated key without labelling it
     * deprecated, with the 1-based line the paragraph starts on.
     *
     * @param list<string> $files
     * @return list<string>
     */
    private function unlabelledDeprecatedKeyViolations(array $files): array
    {
        $violations = [];

        foreach ($files as $file) {
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

        return $violations;
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
     * The prose paragraphs of a markdown file, every code block excluded.
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

        $codeLines = [];
        foreach ($this->codeBlocks($file) as $block) {
            for ($line = $block['start']; $line <= $block['end']; $line++) {
                $codeLines[$line] = true;
            }
        }

        $paragraphs = [];
        $buffer = [];
        $bufferStart = 0;

        foreach ($lines as $index => $line) {
            $lineNumber = $index + 1;

            if (isset($codeLines[$lineNumber])) {
                $paragraph = $this->takeParagraph($buffer, $bufferStart);
                if ($paragraph !== null) {
                    $paragraphs[] = $paragraph;
                }
                continue;
            }

            if (trim($line) === '') {
                $paragraph = $this->takeParagraph($buffer, $bufferStart);
                if ($paragraph !== null) {
                    $paragraphs[] = $paragraph;
                }
                continue;
            }

            if ($buffer === []) {
                $bufferStart = $lineNumber;
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
     * Every code block in a markdown file, in order.
     *
     * Both CommonMark block forms count, because either one can hide a
     * snippet: fenced blocks (three or more backticks *or* tildes, closing
     * only on a run of the same character at least as long with nothing else
     * after it — which is what lets a 4-backtick fence document a 3-backtick
     * sample) and 4-space-indented blocks (only when they follow a blank
     * line, since CommonMark does not let an indented run interrupt a
     * paragraph).
     *
     * @return list<array{start: int, end: int, bodyStart: int, body: string}>
     *         `start`/`end` are the 1-based lines of the opening and closing
     *         fence (or the first and last body line of an indented block),
     *         `bodyStart` is the 1-based line of the first body line.
     */
    private function codeBlocks(string $file): array
    {
        $lines = file($file, FILE_IGNORE_NEW_LINES);
        self::assertNotFalse($lines, "Unable to read {$file}");

        $blocks = [];
        $body = [];
        $blockStart = 0;
        $bodyStart = 0;
        $fenceCharacter = null;
        $fenceLength = 0;
        $indented = false;
        // An indented run can only be a code block when nothing but whitespace
        // precedes it; otherwise it is a lazy continuation of the paragraph.
        $afterBlankLine = true;

        foreach ($lines as $index => $line) {
            // Fences are indented when the block sits inside a list item.
            $trimmed = trim($line);
            $lineNumber = $index + 1;
            $fenceRun = $this->fenceRun($trimmed);

            if ($fenceCharacter !== null) {
                if (
                    $fenceRun !== null
                    && $fenceRun[0] === $fenceCharacter
                    && strlen($fenceRun) >= $fenceLength
                    // An info string is allowed on the opening fence only.
                    && $fenceRun === $trimmed
                ) {
                    $blocks[] = [
                        'start' => $blockStart,
                        'end' => $lineNumber,
                        'bodyStart' => $bodyStart,
                        'body' => implode("\n", $body),
                    ];
                    $fenceCharacter = null;
                    $body = [];
                } else {
                    $body[] = $line;
                }

                $afterBlankLine = false;
                continue;
            }

            if ($fenceRun !== null) {
                $fenceCharacter = $fenceRun[0];
                $fenceLength = strlen($fenceRun);
                $blockStart = $lineNumber;
                $bodyStart = $lineNumber + 1;
                $body = [];
                $afterBlankLine = false;
                continue;
            }

            if (str_starts_with($line, '    ') || str_starts_with($line, "\t")) {
                if (!$indented) {
                    if (!$afterBlankLine) {
                        continue;
                    }

                    $indented = true;
                    $blockStart = $lineNumber;
                    $bodyStart = $lineNumber;
                    $body = [];
                }

                $body[] = $line;
                continue;
            }

            if ($indented) {
                $blocks[] = [
                    'start' => $blockStart,
                    'end' => $blockStart + count($body) - 1,
                    'bodyStart' => $bodyStart,
                    'body' => implode("\n", $body),
                ];
                $indented = false;
                $body = [];
            }

            $afterBlankLine = $trimmed === '';
        }

        self::assertNull($fenceCharacter, "Unterminated code fence in {$file}");

        if ($indented) {
            $blocks[] = [
                'start' => $blockStart,
                'end' => $blockStart + count($body) - 1,
                'bodyStart' => $bodyStart,
                'body' => implode("\n", $body),
            ];
        }

        return $blocks;
    }

    /**
     * The run of backticks or tildes with which a line opens or closes a
     * fenced block, or null when the line is not a fence at all. CommonMark
     * asks for three; the length is returned as well because only a run at
     * least as long as the opening one closes the block again.
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
     * The TLS option keys the client recognises, read out of
     * `ConnectionFactory::RECOGNISED_TLS_KEYS` rather than restated here — a
     * copy could agree with a key the client dropped while every document
     * kept demanding it (issue #690).
     *
     * @return list<string>
     */
    private function recognisedTlsKeys(): array
    {
        $file = $this->repositoryRoot() . '/src/Client/Connection/ConnectionFactory.php';
        $source = file_get_contents($file);
        self::assertNotFalse($source, "Unable to read {$file}");

        $matches = [];
        $found = preg_match(
            '/private const RECOGNISED_TLS_KEYS\s*=\s*\[(?<keys>[^\]]*)\];/',
            $source,
            $matches,
        );
        self::assertSame(1, $found, 'Unable to read ConnectionFactory::RECOGNISED_TLS_KEYS');

        $keyMatches = [];
        $keysFound = preg_match_all("/'([^']+)'/", $matches['keys'], $keyMatches);
        self::assertIsInt($keysFound);
        self::assertGreaterThan(0, $keysFound, 'ConnectionFactory::RECOGNISED_TLS_KEYS lists no keys');

        return $keyMatches[1];
    }

    /**
     * The recognised keys that are not deprecated: the ones every document
     * has to show.
     *
     * @return list<string>
     */
    private function supportedTlsKeys(): array
    {
        return array_values(array_diff($this->recognisedTlsKeys(), self::DEPRECATED_KEYS));
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
