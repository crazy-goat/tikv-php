<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #720: the relative links and the `#anchors` of
 * every markdown file of the checkout have to resolve.
 *
 * Documentation rots from the edges: a heading is renamed, a file moves, a
 * paragraph is split, and the link that pointed at it keeps looking valid
 * until a reader follows it. The README is the landing page, so a dead link
 * there is the first thing a new user sees.
 *
 * The files are enumerated from the checkout, not with `git ls-files`: the
 * project image has no `git` and `.dockerignore` drops `.git`, while
 * `make test-unit` is documented to run in that image. The walk skips
 * `vendor/`, `.git/`, the PHPUnit and agent caches, the docs-guard fixtures
 * under `tests/Unit/Docs/fixtures/` (deliberately non-conforming inputs for a
 * different guard) and the local `findings.md` / `review.md` scratch files.
 *
 * Two checks run over every file:
 *
 *  - a relative target (a markdown link, an image source, an HTML `href` /
 *    `src`, or a reference definition) must exist, resolved from the
 *    directory of the file that links it;
 *  - an anchor must name a heading or an explicit `<a name="…">` /
 *    `<a id="…">` of the target file, where a same-document `#anchor` checks
 *    the file itself.
 *
 * Links inside fenced and 4-space-indented code blocks and inline code spans
 * are samples, not links, so they are skipped. External targets (`http:`,
 * `https:`, `mailto:` and any other scheme) are not fetched — the guard is
 * offline.
 *
 * Anchor slugs follow GitHub's heading algorithm: lowercase, inline markup
 * removed, punctuation dropped, each whitespace character turned into a
 * hyphen, and a numeric suffix for a repeated heading. It is an
 * approximation of GitHub's renderer, but it matches this repository's
 * headings and fails loudly on a real rename.
 */
final class MarkdownLinksGuardTest extends TestCase
{
    /**
     * Directories that are generated, vendored, cached or local agent
     * scratch, never documentation a reader follows.
     */
    private const EXCLUDED_DIRECTORIES = [
        '.git',
        '.idea',
        '.infection-tmp',
        '.opencode',
        '.phpunit.cache',
        '.pi-subagents',
        '.tyci',
        '.worktrees',
        'coverage-html',
        'node_modules',
        'vendor',
    ];

    /**
     * Git-ignored scratch files at the repository root. They exist only on a
     * developer machine and must not make the suite depend on local notes.
     */
    private const EXCLUDED_ROOT_FILES = [
        'findings.md',
        'review.md',
    ];

    /**
     * Anchors per file, so a link that points at a document many times does
     * not re-read and re-slug it for every hit.
     *
     * @var array<string, list<string>>
     */
    private array $anchorCache = [];

    public function testEveryRelativeLinkAndAnchorResolves(): void
    {
        $violations = [];
        $links = 0;

        foreach ($this->markdownFiles() as $file) {
            $contents = $this->readFile($file);
            $anchors = $this->anchorsForFile($file);

            foreach ($this->linksIn($contents) as $link) {
                $links++;

                $target = $link['target'];
                if ($this->isExternal($target)) {
                    continue;
                }

                if (str_starts_with($target, '#')) {
                    $anchor = rawurldecode(substr($target, 1));
                    if (!in_array($anchor, $anchors, true)) {
                        $violations[] = sprintf(
                            '%s:%d: anchor #%s does not exist in the file',
                            $this->relative($file),
                            $link['line'],
                            $anchor,
                        );
                    }

                    continue;
                }

                $parts = explode('#', $target, 2);
                $path = rawurldecode($parts[0]);
                $anchor = isset($parts[1]) ? rawurldecode($parts[1]) : '';

                $resolved = $this->resolve($file, $path);
                if (!file_exists($resolved)) {
                    $violations[] = sprintf(
                        '%s:%d: relative target %s does not exist',
                        $this->relative($file),
                        $link['line'],
                        $path,
                    );

                    continue;
                }

                if ($anchor === '' || strtolower(pathinfo($resolved, PATHINFO_EXTENSION)) !== 'md') {
                    continue;
                }

                if (!in_array($anchor, $this->anchorsForFile($resolved), true)) {
                    $violations[] = sprintf(
                        '%s:%d: anchor #%s does not exist in %s',
                        $this->relative($file),
                        $link['line'],
                        $anchor,
                        $this->relative($resolved),
                    );
                }
            }
        }

        self::assertGreaterThan(0, $links, 'No markdown link was found — the guard would be vacuous.');

        self::assertSame(
            [],
            $violations,
            "Markdown links and anchors that do not resolve:\n" . implode("\n", $violations),
        );
    }

    /**
     * Every markdown file of the checkout, as an absolute path.
     *
     * @return list<string>
     */
    private function markdownFiles(): array
    {
        $files = [];
        $this->collectMarkdownFiles($this->repoRoot(), $files);
        sort($files);

        self::assertNotEmpty($files, 'No markdown file was found — the guard would be vacuous.');

        return $files;
    }

    /**
     * @param list<string> $files
     */
    private function collectMarkdownFiles(string $directory, array &$files): void
    {
        $entries = scandir($directory);
        self::assertNotFalse($entries, sprintf('Unable to read directory %s', $directory));

        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $path = $directory . '/' . $entry;

            if (is_dir($path)) {
                if (!in_array($entry, self::EXCLUDED_DIRECTORIES, true)) {
                    $this->collectMarkdownFiles($path, $files);
                }

                continue;
            }

            if (!str_ends_with(strtolower($entry), '.md')) {
                continue;
            }

            $relative = $this->relative($path);
            if (in_array($relative, self::EXCLUDED_ROOT_FILES, true)) {
                continue;
            }
            if (str_starts_with($relative, 'tests/Unit/Docs/fixtures/')) {
                continue;
            }

            $files[] = $path;
        }
    }

    /**
     * Every link of a markdown document, with the 1-based line it is on.
     *
     * Fenced and 4-space-indented code blocks are skipped entirely, and
     * inline code spans are removed before matching, so a documented
     * `[text](target)` sample is not treated as a link. Reference
     * definitions (`[id]: target`) and HTML `href` / `src` attributes are
     * links too and are included.
     *
     * @return list<array{line: int, target: string}>
     */
    private function linksIn(string $contents): array
    {
        $links = [];
        $codeLines = $this->codeLineNumbers($contents);

        foreach (explode("\n", $contents) as $index => $line) {
            if (isset($codeLines[$index + 1])) {
                continue;
            }

            $withoutCode = preg_replace('/`+[^`]*`+/', '', $line);
            $withoutCode ??= $line;

            if (preg_match('/^\s{0,3}\[[^\]]+\]:\s*(\S+)/', $withoutCode, $matches) === 1) {
                $links[] = ['line' => $index + 1, 'target' => $this->stripAngleBrackets($matches[1])];
            }

            $matches = [];
            $found = preg_match_all(
                '/!?\[[^\]]*\]\(\s*([^)\s]+?)(?:\s+"[^"]*")?\s*\)/',
                $withoutCode,
                $matches,
            );
            self::assertIsInt($found);

            foreach ($matches[1] as $target) {
                $links[] = ['line' => $index + 1, 'target' => $this->stripAngleBrackets($target)];
            }

            $matches = [];
            $found = preg_match_all(
                '/\b(?:href|src)\s*=\s*["\']([^"\']+)["\']/i',
                $withoutCode,
                $matches,
            );
            self::assertIsInt($found);

            foreach ($matches[1] as $target) {
                $links[] = ['line' => $index + 1, 'target' => $this->stripAngleBrackets($target)];
            }
        }

        return $links;
    }

    /**
     * The anchors a markdown file exposes: the GitHub slugs of its headings
     * and its explicit HTML anchors.
     *
     * Fenced and indented code blocks are skipped, so a commented
     * `# heading` sample does not invent an anchor. Repeated headings get
     * GitHub's `-1`, `-2`, … suffixes.
     *
     * @return list<string>
     */
    private function anchorsIn(string $contents): array
    {
        $anchors = [];
        $counts = [];
        $codeLines = $this->codeLineNumbers($contents);

        foreach (explode("\n", $contents) as $index => $line) {
            if (isset($codeLines[$index + 1])) {
                continue;
            }

            $matches = [];
            $found = preg_match_all('/<a\s+(?:name|id)=["\']([^"\']+)["\']/i', $line, $matches);
            self::assertIsInt($found);
            foreach ($matches[1] as $name) {
                $anchors[] = $name;
            }

            if (preg_match('/^ {0,3}#{1,6}\s+(.*?)\s*$/', $line, $matches) !== 1) {
                continue;
            }

            $heading = preg_replace('/\s+#+\s*$/', '', $matches[1]);
            $slug = $this->slug($heading ?? $matches[1]);

            $seen = $counts[$slug] ?? 0;
            $anchors[] = $seen === 0 ? $slug : $slug . '-' . $seen;
            $counts[$slug] = $seen + 1;
        }

        return $anchors;
    }

    /**
     * The 1-based lines that belong to fenced or 4-space-indented code
     * blocks.
     *
     * This mirrors the CommonMark-aware scanner of the sibling
     * `TlsOptionKeysDocumentationGuardTest`: a fence closes only on a run of
     * the same character at least as long as the opening one with nothing
     * else on the line, and an indented run is code only after a blank line
     * (otherwise it is a lazy continuation of a paragraph or list item).
     *
     * @return array<int, true>
     */
    private function codeLineNumbers(string $contents): array
    {
        $codeLines = [];
        $fenceCharacter = null;
        $fenceLength = 0;
        $indented = false;
        $afterBlankLine = true;

        foreach (explode("\n", $contents) as $index => $line) {
            $trimmed = trim($line);
            $lineNumber = $index + 1;
            $fenceRun = $this->fenceRun($trimmed);

            if ($fenceCharacter !== null) {
                $codeLines[$lineNumber] = true;
                if (
                    $fenceRun !== null
                    && $fenceRun[0] === $fenceCharacter
                    && strlen($fenceRun) >= $fenceLength
                    && $fenceRun === $trimmed
                ) {
                    $fenceCharacter = null;
                }

                $afterBlankLine = false;
                continue;
            }

            if ($fenceRun !== null) {
                $fenceCharacter = $fenceRun[0];
                $fenceLength = strlen($fenceRun);
                $codeLines[$lineNumber] = true;
                $afterBlankLine = false;
                continue;
            }

            if (str_starts_with($line, '    ') || str_starts_with($line, "\t")) {
                if (!$indented) {
                    if (!$afterBlankLine) {
                        continue;
                    }

                    $indented = true;
                }

                $codeLines[$lineNumber] = true;
                continue;
            }

            $indented = false;
            $afterBlankLine = $trimmed === '';
        }

        return $codeLines;
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
     * The anchors of a markdown file, cached by path.
     *
     * @return list<string>
     */
    private function anchorsForFile(string $path): array
    {
        $this->anchorCache[$path] ??= $this->anchorsIn($this->readFile($path));

        return $this->anchorCache[$path];
    }

    /**
     * The GitHub heading slug of a piece of heading text.
     */
    private function slug(string $heading): string
    {
        $codeSpans = [];
        $heading = preg_replace_callback(
            '/`+([^`]*)`+/',
            static function (array $matches) use (&$codeSpans): string {
                $placeholder = "\x01" . count($codeSpans) . "\x02";
                $codeSpans[$placeholder] = $matches[1];

                return $placeholder;
            },
            $heading,
        );
        $heading ??= '';

        // Inline markup is gone from the rendered heading text, but the
        // content of a code span is literal text.
        $heading = preg_replace('/!\[([^\]]*)\]\([^)]*\)/', '', $heading) ?? $heading;
        $heading = preg_replace('/\[([^\]]*)\]\([^)]*\)/', '$1', $heading) ?? $heading;
        $heading = preg_replace('/<[^>]+>/', '', $heading) ?? $heading;
        $heading = preg_replace('/\*\*(.+?)\*\*/', '$1', $heading) ?? $heading;
        $heading = preg_replace('/__(.+?)__/', '$1', $heading) ?? $heading;
        $heading = preg_replace('/(?<!\w)_([^_]+)_(?!\w)/', '$1', $heading) ?? $heading;

        foreach ($codeSpans as $placeholder => $content) {
            $heading = str_replace($placeholder, $content, $heading);
        }

        $heading = strtolower(trim($heading));
        // GitHub drops punctuation but keeps hyphens, underscores and
        // unicode letters/numbers; each whitespace character becomes a
        // hyphen (so two spaces around a removed `/` leave two hyphens).
        $heading = preg_replace('/[^\p{L}\p{N}\s_-]+/u', '', $heading) ?? $heading;

        return preg_replace('/\s/', '-', $heading) ?? $heading;
    }

    private function stripAngleBrackets(string $target): string
    {
        if (str_starts_with($target, '<') && str_ends_with($target, '>')) {
            return substr($target, 1, -1);
        }

        return $target;
    }

    private function isExternal(string $target): bool
    {
        return preg_match('#^(?:[a-z][a-z0-9+.-]*:|//)#i', $target) === 1;
    }

    /**
     * Resolve a relative link target from the directory of the linking file.
     */
    private function resolve(string $file, string $path): string
    {
        if (str_starts_with($path, '/')) {
            return $this->repoRoot() . $path;
        }

        return dirname($file) . '/' . $path;
    }

    private function readFile(string $path): string
    {
        $contents = file_get_contents($path);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }

    private function repoRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    private function relative(string $path): string
    {
        $root = $this->repoRoot() . '/';

        return str_starts_with($path, $root) ? substr($path, strlen($root)) : $path;
    }
}
