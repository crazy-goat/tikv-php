<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Guard for issue #410: the project-structure tree in README.md has to match
 * the filesystem.
 *
 * The tree is the reader's first map of the codebase, so a stale one is worse
 * than no tree at all: it pointed at a `Dto/KeyValue` class nothing used, it
 * hid the whole transaction subsystem, the exception hierarchy and the
 * observability layer, and it predated the decomposition of `RawKvClient`.
 * Five things are checked:
 *
 *  1. every directory under `src/Client/` appears in the tree, so a new
 *     package fails the suite until the README names it;
 *  2. every test suite — every direct subdirectory of `tests/` — appears in
 *     the tree, so a new suite is announced;
 *  3. every example script appears in the tree, so a new example is
 *     announced;
 *  4. every path the tree names — a directory or a file — exists on disk, so a
 *     renamed or deleted class cannot stay in the documentation;
 *  5. every class the tree names in a comment exists under `src/Client/` or is
 *     a generated protobuf package, so the tree cannot advertise a type the
 *     API does not have.
 *
 * The completeness checks cover `src/Client/`, the test suites and the
 * example scripts, and not the whole of `src/`: the protobuf packages under
 * `src/Proto/` are generated from the proto submodule and change with it, so
 * demanding that the README list all of them would make the tree drift for a
 * reason no reader cares about. The three packages the tree does name are
 * still checked for existence by check 4.
 *
 * The tree is a map, not a file inventory: the directories below
 * `tests/Unit/` are not part of it, and the README says so.
 *
 * A word in parentheses in a comment is read as a class name when it starts
 * with an uppercase letter and contains a lowercase one; it then has to name a
 * class of `src/Client/` or a generated package of `src/Proto/`. Prose in
 * parentheses is fine as long as it is lowercase — "conflict, deadlock" is
 * prose, "(TransactionConflictException)" is a class. An acronym in
 * parentheses ("PD", "TLS", "GC") is not a class name and is ignored.
 */
class ReadmeProjectStructureTest extends TestCase
{
    /** The heading the tree lives under. */
    private const TREE_HEADING = '## Project Structure';

    /** The subtree whose directories the tree has to list completely. */
    private const SOURCE_TREE = 'src/Client';

    /** The root whose direct subdirectories the tree has to list completely. */
    private const TEST_TREE = 'tests';

    /** The root whose files the tree has to list completely. */
    private const EXAMPLE_TREE = 'examples';

    /**
     * The tree has to list every directory that exists under `src/Client/`.
     */
    public function testEveryDirectoryUnderSrcClientIsDocumented(): void
    {
        $tree = $this->readmeTree();
        $onDisk = $this->directoriesOnDisk(self::SOURCE_TREE);

        $missing = $this->missingDirectories($tree['directories'], $onDisk);

        self::assertSame(
            [],
            $missing,
            "README.md does not document every directory under src/Client/. Add the "
            . "missing ones to the project-structure tree:\n" . implode("\n", $missing),
        );
    }

    /**
     * The tree has to list every test suite — every direct subdirectory of
     * `tests/`. A suite the README does not mention is a suite nobody knows
     * about.
     */
    public function testEveryTestSuiteIsDocumented(): void
    {
        $tree = $this->readmeTree();

        $missing = $this->missingDirectories(
            $tree['directories'],
            $this->topLevelDirectories(self::TEST_TREE),
        );

        self::assertSame(
            [],
            $missing,
            "README.md does not document every test suite. Add the missing ones to the "
            . "project-structure tree:\n" . implode("\n", $missing),
        );
    }

    /**
     * The tree has to list every example script.
     */
    public function testEveryExampleIsDocumented(): void
    {
        $tree = $this->readmeTree();

        $missing = $this->missingFiles(
            $tree['files'],
            $this->topLevelFiles(self::EXAMPLE_TREE),
        );

        self::assertSame(
            [],
            $missing,
            "README.md does not document every example script. Add the missing ones to the "
            . "project-structure tree:\n" . implode("\n", $missing),
        );
    }

    /**
     * Every path the tree names has to exist, in every section of the tree —
     * `src/`, `tests/` and `examples/` alike.
     */
    public function testEveryPathTheTreeNamesExistsOnDisk(): void
    {
        $tree = $this->readmeTree();

        $missing = [];
        foreach (array_merge($tree['directories'], $tree['files']) as $path) {
            if (!file_exists($this->repoRoot() . '/' . $path)) {
                $missing[] = $path;
            }
        }

        self::assertSame(
            [],
            $missing,
            "README.md names paths that do not exist on disk:\n" . implode("\n", $missing),
        );
    }

    /**
     * A class the tree names in a comment has to exist — a reader who follows
     * the tree to a class that is not there is worse off than one who does not
     * follow it at all.
     */
    public function testEveryClassTheTreeNamesExists(): void
    {
        $tree = $this->readmeTree();

        $known = array_merge($this->clientClassNames(), $this->protoPackageNames());

        $missing = [];
        foreach (array_unique($tree['classes']) as $class) {
            if (!in_array($class, $known, true)) {
                $missing[] = $class;
            }
        }

        self::assertSame(
            [],
            $missing,
            "README.md names classes that do not exist under src/Client/ and are not "
            . "generated protobuf packages. A word in parentheses is read as a class "
            . "name:\n" . implode("\n", $missing),
        );
    }

    /**
     * The guard itself has to be sensitive: a tree that omits a directory that
     * exists on disk, and a tree that names a directory that does not exist,
     * both have to be reported. Without this test the comparison could go
     * blind — a comparison that always answers "no drift" would keep the suite
     * green while the tree rots.
     */
    public function testTheGuardReportsDriftInBothDirections(): void
    {
        $block = implode("\n", [
            'src/',
            '├── Client/',
            '│   ├── Cache/',
            '│   └── Retry/',
        ]);

        $tree = $this->parseTree($block);

        self::assertSame(
            ['src', 'src/Client', 'src/Client/Cache', 'src/Client/Retry'],
            $tree['directories'],
        );

        // A package that exists on disk but is missing from the tree.
        self::assertSame(
            ['src/Client/Observability'],
            $this->missingDirectories(
                $tree['directories'],
                [
                    'src/Client',
                    'src/Client/Cache',
                    'src/Client/Retry',
                    'src/Client/Observability',
                ],
            ),
        );

        // A package the tree names but that is not on disk.
        self::assertSame(
            ['src/Client/Retry'],
            $this->staleDirectories(
                $tree['directories'],
                ['src/Client', 'src/Client/Cache'],
                self::SOURCE_TREE,
            ),
        );

        // A test suite that exists on disk but is missing from the tree.
        $suites = $this->topLevelDirectories(self::TEST_TREE);
        self::assertNotSame([], $suites, 'tests/ has to hold at least one suite.');
        self::assertSame(
            [$suites[1]],
            $this->missingDirectories(
                array_values(array_diff($suites, [$suites[1]])),
                $suites,
            ),
        );

        // An example script that exists on disk but is missing from the tree.
        $examples = $this->topLevelFiles(self::EXAMPLE_TREE);
        self::assertNotSame([], $examples, 'examples/ has to hold at least one script.');
        self::assertSame(
            [$examples[0]],
            $this->missingFiles(
                array_values(array_diff($examples, [$examples[0]])),
                $examples,
            ),
        );
    }

    /**
     * The tree as the README documents it.
     *
     * @return array{directories: list<string>, files: list<string>, classes: list<string>}
     */
    private function readmeTree(): array
    {
        $readme = file_get_contents($this->repoRoot() . '/README.md');
        self::assertNotFalse($readme, 'Unable to read README.md');

        $block = $this->extractTreeBlock($readme);
        self::assertNotNull(
            $block,
            'README.md has no code block under the "' . self::TREE_HEADING . '" heading.',
        );

        return $this->parseTree($block);
    }

    /**
     * The code block that follows the project-structure heading.
     */
    private function extractTreeBlock(string $markdown): ?string
    {
        $lines = explode("\n", $markdown);
        $counter = count($lines);

        $heading = null;
        foreach ($lines as $index => $line) {
            if (trim($line) === self::TREE_HEADING) {
                $heading = $index;
                break;
            }
        }

        if ($heading === null) {
            return null;
        }

        $start = null;
        for ($index = $heading + 1; $index < $counter; $index++) {
            if (str_starts_with(trim($lines[$index]), '```')) {
                $start = $index;
                break;
            }
        }

        if ($start === null) {
            return null;
        }

        $block = [];
        for ($index = $start + 1; $index < $counter; $index++) {
            if (str_starts_with(trim($lines[$index]), '```')) {
                return implode("\n", $block);
            }

            $block[] = $lines[$index];
        }

        return null;
    }

    /**
     * The tree of a code block: the directories and the files it names, and
     * the class names its comments name.
     *
     * @return array{directories: list<string>, files: list<string>, classes: list<string>}
     */
    private function parseTree(string $block): array
    {
        $directories = [];
        $files = [];
        $classes = [];
        $parents = [];

        foreach ($this->blockLines($block) as $line) {
            $entry = $this->parseTreeLine($line);
            if ($entry === null) {
                continue;
            }

            [$depth, $name, $comment] = $entry;

            $isDirectory = str_ends_with($name, '/');
            $name = rtrim($name, '/');

            $parent = $depth === 0 ? '' : ($parents[$depth - 1] ?? '');
            if ($depth > 0 && $parent === '') {
                continue;
            }

            $path = $parent === '' ? $name : $parent . '/' . $name;

            $parents[$depth] = $path;
            foreach (array_keys($parents) as $known) {
                if ($known > $depth) {
                    unset($parents[$known]);
                }
            }

            if ($isDirectory) {
                $directories[] = $path;
            } else {
                $files[] = $path;
            }

            foreach ($this->classNamesIn($comment) as $class) {
                $classes[] = $class;
            }
        }

        return ['directories' => $directories, 'files' => $files, 'classes' => $classes];
    }

    /**
     * The lines of a code block, without the blank ones and without the
     * indentation the block as a whole carries.
     *
     * @return list<string>
     */
    private function blockLines(string $block): array
    {
        $lines = [];
        $indentation = null;

        foreach (explode("\n", $block) as $line) {
            $line = rtrim($line);
            if (trim($line) === '') {
                continue;
            }

            $width = strlen($line) - strlen(ltrim($line, " \t"));
            $indentation = $indentation === null ? $width : min($indentation, $width);

            $lines[] = $line;
        }

        if ($indentation === null) {
            return [];
        }

        return array_map(
            static fn (string $line): string => substr($line, $indentation),
            $lines,
        );
    }

    /**
     * One line of a tree: its depth, its name and its comment.
     *
     * @return array{int, string, string}|null
     */
    private function parseTreeLine(string $line): ?array
    {
        $child = '/^(?<indent>(?:(?:│   |    ))*)(?:├──|└──) (?<name>[^#]+?)(?:\s+#(?<comment>.*))?$/';

        if (preg_match($child, $line, $matches) === 1) {
            preg_match_all('/(?:│   |    )/', $matches['indent'], $groups);

            return [
                count($groups[0]) + 1,
                trim($matches['name']),
                $matches['comment'] ?? '',
            ];
        }

        $root = '/^(?<name>[^\s#]+)(?:\s+#(?<comment>.*))?$/';

        if (preg_match($root, $line, $matches) === 1) {
            return [0, $matches['name'], $matches['comment'] ?? ''];
        }

        return null;
    }

    /**
     * The class names a comment names: the words in parentheses that look like
     * a class name.
     *
     * @return list<string>
     */
    private function classNamesIn(string $comment): array
    {
        if ($comment === '') {
            return [];
        }

        preg_match_all('/\(([^)]*)\)/', $comment, $groups);

        $names = [];
        foreach ($groups[1] as $group) {
            foreach (preg_split('/[,\s]+/', $group) ?: [] as $token) {
                if (preg_match('/^[A-Z][A-Za-z0-9]+$/', $token) !== 1) {
                    continue;
                }

                if (preg_match('/[a-z]/', $token) !== 1) {
                    continue;
                }

                $names[] = $token;
            }
        }

        return $names;
    }

    /**
     * The directories that exist under a root, the root included.
     *
     * @return list<string>
     */
    private function directoriesOnDisk(string $root): array
    {
        $rootPath = $this->repoRoot() . '/' . $root;

        $directories = [$root];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($rootPath, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::SELF_FIRST,
        );

        foreach ($iterator as $entry) {
            if (!$entry instanceof \SplFileInfo || !$entry->isDir()) {
                continue;
            }

            $relative = substr($entry->getPathname(), strlen($rootPath) + 1);
            $directories[] = $root . '/' . str_replace('\\', '/', $relative);
        }

        sort($directories);

        return $directories;
    }

    /**
     * The direct subdirectories of a root, the root included.
     *
     * The tree is a map: it names the test suites, not the directories below
     * them, so only the first level of a root is compared.
     *
     * @return list<string>
     */
    private function topLevelDirectories(string $root): array
    {
        $rootPath = $this->repoRoot() . '/' . $root;

        $directories = [$root];

        foreach (new \DirectoryIterator($rootPath) as $entry) {
            if ($entry->isDot() || !$entry->isDir()) {
                continue;
            }

            $name = $entry->getFilename();

            // A hidden directory (`.DS_Store` and friends) is not part of the
            // project structure.
            if (str_starts_with($name, '.')) {
                continue;
            }

            $directories[] = $root . '/' . $name;
        }

        sort($directories);

        return $directories;
    }

    /**
     * The files directly under a root.
     *
     * @return list<string>
     */
    private function topLevelFiles(string $root): array
    {
        $rootPath = $this->repoRoot() . '/' . $root;

        $files = [];

        foreach (new \DirectoryIterator($rootPath) as $entry) {
            if (!$entry->isFile()) {
                continue;
            }

            $name = $entry->getFilename();

            // A hidden file (`.DS_Store` and friends) is not part of the
            // project structure.
            if (str_starts_with($name, '.')) {
                continue;
            }

            $files[] = $root . '/' . $name;
        }

        sort($files);

        return $files;
    }

    /**
     * The class names `src/Client/` defines.
     *
     * @return list<string>
     */
    private function clientClassNames(): array
    {
        return $this->phpClassNames($this->repoRoot() . '/src/Client');
    }

    /**
     * The generated protobuf packages under `src/Proto/`.
     *
     * Only the direct subdirectories are packages; the ones below them hold
     * the messages of a package.
     *
     * @return list<string>
     */
    private function protoPackageNames(): array
    {
        $names = array_map(
            basename(...),
            array_values(array_diff($this->topLevelDirectories('src/Proto'), ['src/Proto'])),
        );

        sort($names);

        return $names;
    }

    /**
     * The names of the PHP classes a directory holds, without the suffix.
     *
     * @return list<string>
     */
    private function phpClassNames(string $root): array
    {
        $names = [];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if (!$entry instanceof \SplFileInfo || !$entry->isFile()) {
                continue;
            }

            if ($entry->getExtension() !== 'php') {
                continue;
            }

            $names[] = $entry->getBasename('.php');
        }

        sort($names);

        return $names;
    }

    /**
     * The directories that exist on disk but are not in the tree.
     *
     * @param list<string> $documented
     * @param list<string> $onDisk
     * @return list<string>
     */
    private function missingDirectories(array $documented, array $onDisk): array
    {
        return array_values(array_diff($onDisk, $documented));
    }

    /**
     * The files that exist on disk but are not in the tree.
     *
     * @param list<string> $documented
     * @param list<string> $onDisk
     * @return list<string>
     */
    private function missingFiles(array $documented, array $onDisk): array
    {
        return array_values(array_diff($onDisk, $documented));
    }

    /**
     * The directories the tree names under a root that do not exist.
     *
     * The guard reports a stale path through `file_exists()` in
     * `testEveryPathTheTreeNamesExistsOnDisk()`; this is the same comparison
     * in the other direction, scoped to a root.
     *
     * @param list<string> $documented
     * @param list<string> $onDisk
     * @return list<string>
     */
    private function staleDirectories(array $documented, array $onDisk, string $root): array
    {
        return array_values(array_diff($this->documentedUnder($documented, $root), $onDisk));
    }

    /**
     * The documented directories that belong to a root.
     *
     * @param list<string> $documented
     * @return list<string>
     */
    private function documentedUnder(array $documented, string $root): array
    {
        return array_values(array_filter(
            $documented,
            static fn (string $path): bool => $path === $root
                || str_starts_with($path, $root . '/'),
        ));
    }

    private function repoRoot(): string
    {
        return dirname(__DIR__, 3);
    }
}
