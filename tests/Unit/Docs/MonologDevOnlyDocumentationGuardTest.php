<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #804: Monolog is a dev-only dependency, so every
 * documentation snippet that imports `Monolog\` has to say so, and the
 * logging example has to keep the guard that turns a missing Monolog into an
 * install hint instead of the `Class "Monolog\Logger" not found` fatal of
 * issue #387.
 *
 * PR #796 added the dev-only note next to the README logging snippet and the
 * `class_exists(Logger::class)` guard to `examples/logging.php`, but nothing
 * in the suite pinned either: a later edit could drop them and CI stayed
 * green. A reader who copies a snippet that imports Monolog into a project
 * that only requires `crazy-goat/tikv-client` gets a fatal, because the client
 * itself depends on `psr/log` only.
 *
 * The documentation rule is deliberately section-scoped and snippet-only:
 *
 *  - only fenced code blocks count, so the many prose mentions of Monolog
 *    (log levels, `isHandling()`, redaction caveats) do not demand a note;
 *  - the note has to be prose in the same top-level (`##`) section as the
 *    block, not a comment inside it. Position is not checked: a note after the
 *    snippet counts as well, because the rule pins presence, not layout, and
 *    the documentation places every note above its snippet anyway.
 *
 * A fenced block inside a blockquote (`> ```php`) is not recognised as code:
 * no such block exists in the documentation today, and treating a quoted
 * sample as prose keeps the rule conservative rather than silently skipping
 * it. The sibling markdown guards share the same limit.
 *  - the note has to name Monolog and call it dev-only in one paragraph, so
 *    an unrelated "dev-only" elsewhere in the section cannot stand in.
 *
 * The example is checked twice. The source assertion keeps the guard above the
 * first `new Logger(...)` so a reordering cannot make the runtime check moot,
 * and the runtime test copies the shipped example next to an empty
 * autoloader, runs it and requires exit code 1 with the install command — the
 * check a grep-based test cannot make, as the review of PR #796 noted.
 */
final class MonologDevOnlyDocumentationGuardTest extends TestCase
{
    /**
     * How every snippet that needs the note imports Monolog.
     */
    private const MONOLOG_IMPORT = 'Monolog\\';

    /**
     * The label the note has to carry. A bare "optional" is not enough: the
     * dependency is absent from a consumer's install, so the note has to say
     * that it does not ship with the library.
     */
    private const DEV_ONLY = '/\b(?:dev|development)[- ]only\b/i';

    /**
     * A fixture whose Monolog snippet has no note in its section.
     */
    private const MISSING_NOTE_FIXTURE = 'tests/Unit/Docs/fixtures/monolog-dev-only/missing-note.md';

    /**
     * A fixture with the same snippet and a dev-only note.
     */
    private const NOTE_PRESENT_FIXTURE = 'tests/Unit/Docs/fixtures/monolog-dev-only/note-present.md';

    /**
     * A fixture that mentions Monolog only in prose: the rule has to leave it
     * alone, because prose cannot be copied and run.
     */
    private const PROSE_ONLY_FIXTURE = 'tests/Unit/Docs/fixtures/monolog-dev-only/prose-only.md';

    public function testEveryDocumentedMonologSnippetIsLabelledDevOnly(): void
    {
        $violations = [];

        foreach ($this->documentationFiles() as $file) {
            $violations = array_merge($violations, $this->monologSnippetViolations($file));
        }

        self::assertSame(
            [],
            $violations,
            "Documented snippets that import Monolog without a dev-only note in "
            . "their section (a reader who copies one gets \"Class Monolog\\\\Logger "
            . "not found\"):\n" . implode("\n", $violations),
        );
    }

    /**
     * The rule is only useful if it still recognises the shape it has to
     * report, so a fixture pins both directions: a snippet without a note is
     * reported on the line its fence opens, and the same snippet with a note
     * passes.
     */
    public function testTheRuleReportsASnippetWithoutANote(): void
    {
        $violations = $this->monologSnippetViolations($this->fixture(self::MISSING_NOTE_FIXTURE));

        self::assertCount(1, $violations);
        self::assertStringContainsString(self::MISSING_NOTE_FIXTURE . ':5', $violations[0]);
        self::assertStringContainsString('Logging', $violations[0]);
    }

    public function testTheRuleAcceptsASnippetWithANote(): void
    {
        self::assertSame(
            [],
            $this->monologSnippetViolations($this->fixture(self::NOTE_PRESENT_FIXTURE)),
        );
    }

    /**
     * The guard must not turn a prose mention into a demand for a note: the
     * issue asks for fenced snippets and the example file, nothing else.
     */
    public function testAProseOnlyMentionNeedsNoNote(): void
    {
        self::assertSame(
            [],
            $this->monologSnippetViolations($this->fixture(self::PROSE_ONLY_FIXTURE)),
        );
    }

    /**
     * The source-level half of the example guard: `class_exists()` has to
     * stay, and it has to run before the first `new Logger(...)` — a guard
     * moved below the first use still exits 1 in the runtime test only if the
     * fatal is somehow avoided, and it never is.
     *
     * The match accepts the fully-qualified spellings
     * (`class_exists(\Monolog\Logger::class)`, `new \Monolog\Logger(...)`) as
     * well as the imported ones the file uses today: a refactor that keeps the
     * same guard must not fail this source check while the runtime test proves
     * the guard still works.
     */
    public function testTheExampleLoggingFileKeepsTheGuardBeforeTheFirstLoggerUse(): void
    {
        $source = $this->read('examples/logging.php');

        $guard = $this->firstMatch('/class_exists\(\s*\\\\?(?:Monolog\\\\)?Logger::class\s*\)/', $source, 'guard');
        $firstUse = $this->firstMatch('/new\s+\\\\?(?:Monolog\\\\)?Logger\s*\(/', $source, 'first Logger construction');

        self::assertStringContainsString(
            'composer require monolog/monolog',
            $source,
            'The guard in examples/logging.php no longer tells the reader how to install Monolog.',
        );

        self::assertLessThan(
            $firstUse,
            $guard,
            'The class_exists(Logger::class) guard in examples/logging.php runs after the first '
            . 'Logger use, so the fatal it exists to prevent happens first.',
        );
    }

    /**
     * The runtime half: run the shipped example with an autoloader that cannot
     * find Monolog. It has to stop at the guard with exit code 1 and the
     * install command, not reach the first `new Logger(...)`.
     *
     * The example is copied into a scratch tree so its `../vendor/autoload.php`
     * points at an empty autoloader; the source file itself is never changed.
     */
    public function testTheExampleLoggingFileExitsWithTheInstallHintWithoutMonolog(): void
    {
        $workspace = sys_get_temp_dir() . '/tikv-monolog-guard-' . bin2hex(random_bytes(8));

        try {
            $script = $this->exampleWithoutMonolog($workspace);

            $command = escapeshellarg(PHP_BINARY) . ' ' . escapeshellarg($script) . ' 2>&1';
            $output = [];
            $exitCode = 0;
            exec($command, $output, $exitCode);
            $text = implode("\n", $output);

            self::assertSame(
                1,
                $exitCode,
                "examples/logging.php must exit 1 when Monolog is not installed; it exited "
                . "{$exitCode} with:\n{$text}",
            );
            self::assertStringContainsString('requires monolog/monolog', $text);
            self::assertStringContainsString('composer require monolog/monolog', $text);
            self::assertStringNotContainsString(
                'TiKV PHP Client - PSR-3 Logging Example',
                $text,
                'The example reached its banner, so the guard did not stop it before the first '
                . 'Monolog use.',
            );
            self::assertStringNotContainsString('Class "Monolog\\Logger" not found', $text);
        } finally {
            exec('rm -rf ' . escapeshellarg($workspace));
        }
    }

    /**
     * Every Monolog snippet in a markdown file whose section carries no
     * dev-only note, with the 1-based line its fence opens on.
     *
     * @return list<string>
     */
    private function monologSnippetViolations(string $file): array
    {
        $lines = $this->lines($file);
        $blocks = $this->fencedCodeBlocks($lines, $file);
        $sections = $this->h2Sections($lines, $blocks);

        $violations = [];

        foreach ($blocks as $block) {
            if (!str_contains($block['body'], self::MONOLOG_IMPORT)) {
                continue;
            }

            $section = $this->sectionForLine($block['start'], $sections);
            if ($section !== null && $this->hasDevOnlyNote($section['prose'])) {
                continue;
            }

            $violations[] = sprintf(
                '%s:%d imports Monolog in a fenced code block, but %s',
                $this->relative($file),
                $block['start'],
                $section === null
                    ? 'the file has no `##` section to carry the dev-only note'
                    : sprintf('its section "%s" has no dev-only note', $section['title']),
            );
        }

        return $violations;
    }

    /**
     * Every top-level (`##`) section of a markdown file, its prose stripped of
     * fenced code blocks.
     *
     * A note inside the snippet would be copied along with it but would not
     * reach a reader skimming the page, so it does not count: only the prose
     * around the block does.
     *
     * @param list<string>                       $lines
     * @param list<array{start: int, end: int, body: string}> $blocks
     * @return list<array{start: int, title: string, prose: string}>
     */
    private function h2Sections(array $lines, array $blocks): array
    {
        $codeLines = [];
        foreach ($blocks as $block) {
            for ($line = $block['start']; $line <= $block['end']; $line++) {
                $codeLines[$line] = true;
            }
        }

        $heads = [];
        foreach ($lines as $index => $line) {
            if (isset($codeLines[$index + 1])) {
                continue;
            }

            $matches = [];
            if (preg_match('/^ {0,3}##(?:[ \t]+(?<title>.*?))?[ \t]*#*[ \t]*$/', $line, $matches) !== 1) {
                continue;
            }

            $heads[] = [
                'start' => $index + 1,
                'title' => trim($matches['title'] ?? ''),
            ];
        }

        $sections = [];
        foreach ($heads as $index => $head) {
            $end = isset($heads[$index + 1]) ? $heads[$index + 1]['start'] - 1 : count($lines);
            $prose = [];

            for ($line = $head['start']; $line <= $end; $line++) {
                if (!isset($codeLines[$line])) {
                    $prose[] = $lines[$line - 1];
                }
            }

            $sections[] = [
                'start' => $head['start'],
                'title' => $head['title'],
                'prose' => implode("\n", $prose),
            ];
        }

        return $sections;
    }

    /**
     * The section a line belongs to: the last `##` heading at or above it.
     *
     * @param list<array{start: int, title: string, prose: string}> $sections
     * @return array{start: int, title: string, prose: string}|null
     */
    private function sectionForLine(int $line, array $sections): ?array
    {
        $found = null;

        foreach ($sections as $section) {
            if ($section['start'] > $line) {
                break;
            }

            $found = $section;
        }

        return $found;
    }

    /**
     * Whether a section's prose has one paragraph that both names Monolog and
     * calls it dev-only.
     */
    private function hasDevOnlyNote(string $prose): bool
    {
        $paragraphs = preg_split('/\n[ \t]*\n/', $prose) ?: [];

        foreach ($paragraphs as $paragraph) {
            if (
                stripos($paragraph, 'monolog') !== false
                && preg_match(self::DEV_ONLY, $paragraph) === 1
            ) {
                return true;
            }
        }

        return false;
    }

    /**
     * Every fenced code block in a markdown file, in order.
     *
     * Only fenced blocks count: the issue targets fenced code blocks and the
     * example file, and a 4-space-indented run cannot be copied from the
     * rendered page with the same confidence. The closing fence has to be the
     * same character, at least as long as the opening one and alone on its
     * line, so a 3-backtick sample inside a 4-backtick fence does not end it.
     *
     * @param list<string> $lines
     * @return list<array{start: int, end: int, body: string}>
     *         `start`/`end` are the 1-based lines of the opening and closing
     *         fence, `body` is the raw snippet between them.
     */
    private function fencedCodeBlocks(array $lines, string $file): array
    {
        $blocks = [];
        $fenceCharacter = null;
        $fenceLength = 0;
        $start = 0;
        $body = [];

        foreach ($lines as $index => $line) {
            $trimmed = trim($line);
            $fenceRun = $this->fenceRun($trimmed);
            $lineNumber = $index + 1;

            if ($fenceCharacter === null) {
                if ($fenceRun !== null) {
                    $fenceCharacter = $fenceRun[0];
                    $fenceLength = strlen($fenceRun);
                    $start = $lineNumber;
                    $body = [];
                }

                continue;
            }

            if (
                $fenceRun !== null
                && $fenceRun[0] === $fenceCharacter
                && strlen($fenceRun) >= $fenceLength
                && $fenceRun === $trimmed
            ) {
                $blocks[] = [
                    'start' => $start,
                    'end' => $lineNumber,
                    'body' => implode("\n", $body),
                ];
                $fenceCharacter = null;
                $body = [];
                continue;
            }

            $body[] = $line;
        }

        self::assertNull($fenceCharacter, "Unterminated code fence in {$file}");

        return $blocks;
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
     * The shipped example copied next to an empty autoloader, so
     * `class_exists(Monolog\Logger::class)` is false and nothing else in the
     * tree can provide it. Returns the path of the copied script.
     */
    private function exampleWithoutMonolog(string $workspace): string
    {
        self::assertTrue(
            mkdir($workspace . '/examples', 0777, true),
            "Unable to create {$workspace}/examples",
        );
        self::assertTrue(
            mkdir($workspace . '/vendor', 0777, true),
            "Unable to create {$workspace}/vendor",
        );

        self::assertTrue(
            copy($this->repositoryRoot() . '/examples/logging.php', $workspace . '/examples/logging.php'),
            'Unable to copy examples/logging.php into the scratch tree',
        );

        $autoload = "<?php\n\n// Deliberately empty: this tree has no Monolog to autoload.\n";
        self::assertSame(
            strlen($autoload),
            file_put_contents($workspace . '/vendor/autoload.php', $autoload),
        );

        return $workspace . '/examples/logging.php';
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

    /**
     * @return list<string>
     */
    private function lines(string $file): array
    {
        $lines = file($file, FILE_IGNORE_NEW_LINES);
        self::assertNotFalse($lines, "Unable to read {$file}");

        return $lines;
    }

    /**
     * The byte offset of the first match of a pattern, or a failed assertion
     * naming what the example is missing.
     */
    private function firstMatch(string $pattern, string $subject, string $what): int
    {
        $matches = [];
        $found = preg_match($pattern, $subject, $matches, PREG_OFFSET_CAPTURE);

        self::assertSame(
            1,
            $found,
            "examples/logging.php has no {$what}; issue #387 is the fatal it prevents.",
        );

        return $matches[0][1];
    }

    private function read(string $relativePath): string
    {
        $path = $this->repositoryRoot() . '/' . $relativePath;
        self::assertFileExists($path);

        return (string) file_get_contents($path);
    }

    private function fixture(string $relativePath): string
    {
        $path = $this->repositoryRoot() . '/' . $relativePath;
        self::assertFileExists($path);

        return $path;
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
