<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Guard for the index at the top of the knowledge-base files (issue #679).
 *
 * `docs/helpers/faq.md` and `docs/helpers/decisions.md` are append-only: every
 * branch that learns something non-obvious adds a section. Without an index
 * there was no fixed insertion point, so branches appended at the end of the
 * file and any two of them conflicted there (the resolutions of #355, #354,
 * #345, #443, #278, #282 and #340 were all "keep both sides"). The last line
 * was also repeatedly left without a trailing newline, which adds a
 * "\ No newline at end of file" marker to every diff that touches the file.
 *
 * Both files now start with an index that lists every section in file order,
 * and the convention is to insert a new section directly below the index. This
 * test keeps the index in step with the headings — a stale index is worse than
 * no index, because it sends a reader to a section that was renamed or removed
 * — and pins the trailing newline that the diffs kept losing.
 *
 * The anchors are GitHub's: lower-case, every character that is not a letter, a
 * digit, a combining mark, `_`, `-` or a space removed, and every space
 * replaced by a hyphen (so "Foo — Bar" becomes `foo--bar`). The algorithm was
 * checked against GitHub's rendering of both files, so every link works in the
 * web UI. Brackets in a heading are escaped in the link text, because an
 * unescaped `]` would end the label and break the link.
 */
final class HelpersIndexGuardTest extends TestCase
{
    private const INDEX_START = '<!-- index:start -->';

    private const INDEX_END = '<!-- index:end -->';

    /**
     * @return iterable<string, array{string}>
     */
    public static function knowledgeBaseFiles(): iterable
    {
        yield 'faq.md' => ['docs/helpers/faq.md'];
        yield 'decisions.md' => ['docs/helpers/decisions.md'];
    }

    #[DataProvider('knowledgeBaseFiles')]
    public function testTheIndexListsEverySectionInFileOrder(string $file): void
    {
        $content = $this->read($file);
        $actual = $this->indexLines($content);
        $expected = $this->expectedIndex($content);

        $missing = array_values(array_diff($expected, $actual));
        $stale = array_values(array_diff($actual, $expected));

        $problems = [];

        if ($missing !== []) {
            $problems[] = "Missing entries (the index must list every \"## \" section, in file order):\n"
                . implode("\n", array_slice($missing, 0, 10));
        }

        if ($stale !== []) {
            $problems[] = "Entries that match no section (the heading was renamed or removed):\n"
                . implode("\n", array_slice($stale, 0, 10));
        }

        if ($missing === [] && $stale === [] && $actual !== $expected) {
            $problems[] = 'Every entry is present but the index is not in file order.';
        }

        self::assertSame(
            [],
            $problems,
            sprintf(
                "The index at the top of %s does not match the sections below it (issue #679).\n\n%s",
                $file,
                implode("\n\n", $problems),
            ),
        );
    }

    #[DataProvider('knowledgeBaseFiles')]
    public function testTheIndexSitsAboveEverySection(string $file): void
    {
        $content = $this->read($file);

        self::assertSame(
            1,
            substr_count($content, self::INDEX_START),
            sprintf('%s must contain exactly one "%s" marker.', $file, self::INDEX_START),
        );
        self::assertSame(
            1,
            substr_count($content, self::INDEX_END),
            sprintf('%s must contain exactly one "%s" marker.', $file, self::INDEX_END),
        );

        $end = strpos($content, self::INDEX_END);
        $firstSection = strpos($content, "\n## ");

        self::assertNotFalse($end);
        self::assertNotFalse($firstSection);
        self::assertLessThan(
            $firstSection,
            $end,
            sprintf(
                'The index in %s must precede every "## " section: a new section goes directly '
                . 'below the index, never at the end of the file (issue #679).',
                $file,
            ),
        );
    }

    #[DataProvider('knowledgeBaseFiles')]
    public function testTheFileEndsWithExactlyOneNewline(string $file): void
    {
        $content = $this->read($file);

        self::assertStringEndsWith(
            "\n",
            $content,
            sprintf(
                '%s must end with a newline, otherwise every diff that touches it adds a '
                . '"\\ No newline at end of file" marker (issue #679).',
                $file,
            ),
        );
        self::assertStringEndsNotWith(
            "\n\n",
            $content,
            sprintf('%s must not end with a blank line, only with a single newline (issue #679).', $file),
        );
    }

    /**
     * The lines between the index markers, with the surrounding blank lines
     * and the indentation of the list removed.
     *
     * @return list<string>
     */
    private function indexLines(string $content): array
    {
        $start = strpos($content, self::INDEX_START);
        $end = strpos($content, self::INDEX_END);

        self::assertNotFalse($start);
        self::assertNotFalse($end);
        self::assertLessThan($end, $start);

        $block = substr($content, $start + strlen(self::INDEX_START), $end - $start - strlen(self::INDEX_START));
        $lines = [];

        foreach (explode("\n", $block) as $line) {
            $line = rtrim($line);

            if ($line === '') {
                continue;
            }

            $lines[] = $line;
        }

        return $lines;
    }

    /**
     * The index a file should have, derived from its `## ` headings.
     *
     * @return list<string>
     */
    private function expectedIndex(string $content): array
    {
        $lines = [];
        $seen = [];

        foreach (explode("\n", $content) as $line) {
            if (!str_starts_with($line, '## ')) {
                continue;
            }

            $heading = substr($line, 3);
            $anchor = $this->anchor($heading);

            if (isset($seen[$anchor])) {
                $seen[$anchor]++;
                $anchor .= '-' . $seen[$anchor];
            } else {
                $seen[$anchor] = 0;
            }

            $lines[] = '- [' . $this->label($heading) . '](#' . $anchor . ')';
        }

        return $lines;
    }

    /**
     * GitHub's heading anchor, for the ASCII headings the knowledge base uses.
     *
     * @see https://github.com/gjtorikian/html-pipeline
     */
    private function anchor(string $heading): string
    {
        if (preg_match('/\p{Lu}/u', $heading, $match) === 1 && strlen($match[0]) > 1) {
            self::fail(sprintf(
                'The heading "%s" contains the non-ASCII upper-case letter "%s". The anchor is '
                . 'lowercased with strtolower(), which only handles ASCII, so it would disagree '
                . 'with GitHub. Lowercase the letter in the heading, or switch this helper to '
                . 'mb_strtolower() and require ext-mbstring.',
                $heading,
                $match[0],
            ));
        }

        $anchor = preg_replace('/[^\p{L}\p{N}\p{M}_\- ]/u', '', strtolower($heading));

        if ($anchor === null) {
            self::fail(sprintf('Unable to derive the anchor of the heading "%s".', $heading));
        }

        return str_replace(' ', '-', $anchor);
    }

    /**
     * The link text.
     *
     * A `]` outside a code span would end the label, so those brackets are
     * escaped (GitHub renders `\]` as `]`). A backslash escape inside a code
     * span is not processed — it renders literally — so brackets between
     * backticks are left alone; they are also ignored by the link-label
     * parser, which is why a heading like `` `[Unreleased]` `` needs no escape.
     */
    private function label(string $heading): string
    {
        $label = '';
        $inCodeSpan = false;

        foreach (str_split($heading) as $character) {
            if ($character === '`') {
                $inCodeSpan = !$inCodeSpan;
            }

            if (!$inCodeSpan && ($character === '[' || $character === ']')) {
                $label .= '\\';
            }

            $label .= $character;
        }

        return $label;
    }

    private function read(string $relativePath): string
    {
        $path = dirname(__DIR__, 3) . '/' . $relativePath;
        self::assertFileExists($path);

        $contents = file_get_contents($path);
        self::assertNotFalse($contents);

        return $contents;
    }
}
