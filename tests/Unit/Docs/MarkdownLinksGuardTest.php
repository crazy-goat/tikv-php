<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #720: the relative links and the `#anchors` of
 * every tracked markdown file have to resolve.
 *
 * Documentation rots from the edges: a heading is renamed, a file moves, a
 * paragraph is split, and the link that pointed at it keeps looking valid
 * until a reader follows it. The README is the landing page, so a dead link
 * there is the first thing a new user sees.
 *
 * The guard is deliberately file-system based instead of a list of known
 * documents: `git ls-files '*.md'` covers the README, the docs tree, the
 * changelog, the workflow files and a future markdown file without touching
 * this test.
 *
 * Two checks run over every file:
 *
 *  - a relative target (a path, an image source or a reference definition)
 *    must exist, resolved from the directory of the file that links it;
 *  - an anchor must name a heading or an explicit `<a name="…">` /
 *    `<a id="…">` of the target file, where a same-document `#anchor` checks
 *    the file itself.
 *
 * Links inside fenced code blocks and inline code spans are samples, not
 * links, so they are skipped. External targets (`http:`, `https:`, `mailto:`
 * and any other scheme) are not fetched — the guard is offline.
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
     * Every tracked markdown file, as an absolute path.
     *
     * @return list<string>
     */
    private function markdownFiles(): array
    {
        $root = $this->repoRoot();
        $command = sprintf(
            'git -C %s ls-files -z -- %s',
            escapeshellarg($root),
            escapeshellarg('*.md'),
        );

        $output = [];
        $exitCode = 0;
        exec($command, $output, $exitCode);

        self::assertSame(0, $exitCode, sprintf('git ls-files failed with exit code %d', $exitCode));

        $files = [];
        foreach (explode("\0", implode("\n", $output)) as $file) {
            if ($file !== '') {
                $files[] = $root . '/' . $file;
            }
        }

        sort($files);

        self::assertNotEmpty($files, 'git ls-files returned no markdown file.');

        return $files;
    }

    /**
     * Every link of a markdown document, with the 1-based line it is on.
     *
     * Fenced code blocks are skipped entirely, and inline code spans are
     * removed before matching, so a documented `[text](target)` sample is
     * not treated as a link. Reference definitions (`[id]: target`) are
     * links too and are included.
     *
     * @return list<array{line: int, target: string}>
     */
    private function linksIn(string $contents): array
    {
        $links = [];
        $fence = null;
        $fenceLength = 0;

        foreach (explode("\n", $contents) as $index => $line) {
            $trimmed = trim($line);

            if ($fence !== null) {
                if (
                    preg_match('/^(`{3,}|~{3,})$/', $trimmed, $matches) === 1
                    && $matches[1][0] === $fence
                    && strlen($matches[1]) >= $fenceLength
                ) {
                    $fence = null;
                }

                continue;
            }

            if (preg_match('/^(`{3,}|~{3,})/', $trimmed, $matches) === 1) {
                $fence = $matches[1][0];
                $fenceLength = strlen($matches[1]);
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
        }

        return $links;
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
     * The anchors a markdown file exposes: the GitHub slugs of its headings
     * and its explicit HTML anchors.
     *
     * Fenced code blocks are skipped, so a commented `# heading` sample does
     * not invent an anchor. Repeated headings get GitHub's `-1`, `-2`, …
     * suffixes.
     *
     * @return list<string>
     */
    private function anchorsIn(string $contents): array
    {
        $anchors = [];
        $counts = [];
        $fence = null;
        $fenceLength = 0;

        foreach (explode("\n", $contents) as $line) {
            $trimmed = trim($line);

            if ($fence !== null) {
                if (
                    preg_match('/^(`{3,}|~{3,})$/', $trimmed, $matches) === 1
                    && $matches[1][0] === $fence
                    && strlen($matches[1]) >= $fenceLength
                ) {
                    $fence = null;
                }

                continue;
            }

            if (preg_match('/^(`{3,}|~{3,})/', $trimmed, $matches) === 1) {
                $fence = $matches[1][0];
                $fenceLength = strlen($matches[1]);
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
