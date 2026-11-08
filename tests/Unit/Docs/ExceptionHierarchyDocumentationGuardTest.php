<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #721: `docs/error-handling.md` presents itself
 * as the complete reference for every exception the client can throw, but its
 * hierarchy tree, its "What Each Class Means" table and its caller-retryability
 * table can each omit a class that is reachable from an ordinary operation.
 *
 * The set of subclasses is derived from `src/` instead of being restated here:
 * a copied list would go stale in exactly the same way the document did. Any
 * new direct subclass of `TiKvException` fails these tests until it is added
 * to the tree, described in the class table and given a caller verdict.
 *
 * Only structural parts count — a fenced tree line, a table row with the
 * expected columns. A prose mention of a class does not: deleting the row and
 * keeping the mention would otherwise leave the guard green while the
 * reference lost the class.
 */
final class ExceptionHierarchyDocumentationGuardTest extends TestCase
{
    private const DOCUMENT = 'docs/error-handling.md';

    /** The classes the tree deliberately lists outside the subclass set. */
    private const NON_SUBCLASS_TREE_NAMES = ['TiKvException', 'InvalidArgumentException'];

    public function testTheHierarchyTreeListsEveryTiKvExceptionSubclass(): void
    {
        $expected = $this->subclasses();
        $documented = $this->treeSubclasses();

        $missing = array_values(array_diff($expected, $documented));
        $unexpected = array_values(array_diff($documented, $expected));

        self::assertSame(
            [],
            $missing,
            "TiKvException subclasses missing from the hierarchy tree in "
            . self::DOCUMENT . ":\n" . implode("\n", $missing),
        );
        self::assertSame(
            [],
            $unexpected,
            "Names in the hierarchy tree of " . self::DOCUMENT
            . " that are not TiKvException subclasses in src/:\n"
            . implode("\n", $unexpected),
        );
    }

    public function testEveryTiKvExceptionSubclassHasAClassDescription(): void
    {
        $expected = $this->subclasses();
        $documented = $this->classTableSubclasses();

        $missing = array_values(array_diff($expected, $documented));

        self::assertSame(
            [],
            $missing,
            "TiKvException subclasses missing from the \"What Each Class Means\" "
            . "table of " . self::DOCUMENT . " (a row whose Parent column is "
            . "'`TiKvException`'):\n" . implode("\n", $missing),
        );
    }

    public function testEveryTiKvExceptionSubclassHasACallerRetryabilityVerdict(): void
    {
        $expected = $this->subclasses();
        $documented = $this->retryabilityRows();

        $missing = array_values(array_diff($expected, $documented));

        self::assertSame(
            [],
            $missing,
            "TiKvException subclasses missing from the caller-retryability table "
            . "of " . self::DOCUMENT . ":\n" . implode("\n", $missing),
        );
    }

    /**
     * Every direct subclass of `TiKvException` under `src/Client/`, sorted.
     *
     * @return list<string>
     */
    private function subclasses(): array
    {
        $names = [];
        $files = $this->phpFiles($this->repositoryRoot() . '/src/Client');

        foreach ($files as $file) {
            $source = file_get_contents($file);
            self::assertNotFalse($source, sprintf('Unable to read %s', $file));

            $matches = [];
            $found = preg_match_all(
                '/(?:^|\s)(?:final\s+|abstract\s+)?class\s+'
                . '([A-Za-z_][A-Za-z0-9_]*)\s+extends\s+'
                . '\\\\?(?:[A-Za-z_][A-Za-z0-9_]*\\\\)*TiKvException\b/',
                $source,
                $matches,
            );
            self::assertNotFalse($found, sprintf('Unable to scan %s for subclasses', $file));

            foreach ($matches[1] as $name) {
                $names[$name] = true;
            }
        }

        $subclasses = array_keys($names);

        // A guard that derives nothing is worse than no guard: it reports green
        // while the document could omit every class.
        self::assertGreaterThan(
            20,
            count($subclasses),
            'The derived TiKvException subclass list is suspiciously small; the '
            . 'src/ scan in ExceptionHierarchyDocumentationGuardTest is broken.',
        );

        sort($subclasses);

        return $subclasses;
    }

    /**
     * The class names of the subtree under `TiKvException` in the hierarchy
     * code block, sorted.
     *
     * Tree children are the lines indented by four spaces and opened with a
     * `├──` or `└──` connector. The roots (`\RuntimeException`,
     * `TiKvException`, `\InvalidArgumentException` and `InvalidArgumentException`)
     * are not indented, so they are not part of the result.
     *
     * @return list<string>
     */
    private function treeSubclasses(): array
    {
        $section = $this->section($this->read(self::DOCUMENT), "\n## Exception Hierarchy\n");

        $matches = [];
        $found = preg_match_all(
            '/^ {4}(?:├|└)── ([A-Za-z_][A-Za-z0-9_]*)/m',
            $section,
            $matches,
        );
        self::assertNotFalse($found, 'Unable to scan the hierarchy tree for class names');

        $names = $matches[1];

        self::assertNotEmpty($names, 'No subclass lines found in the hierarchy tree.');
        self::assertSame(
            [],
            array_values(array_intersect($names, self::NON_SUBCLASS_TREE_NAMES)),
            'The hierarchy tree indents a root class as if it were a TiKvException subclass.',
        );

        sort($names);

        return $names;
    }

    /**
     * The class names whose row in the "What Each Class Means" table names
     * `TiKvException` as the parent, sorted.
     *
     * @return list<string>
     */
    private function classTableSubclasses(): array
    {
        $section = $this->section($this->read(self::DOCUMENT), "\n### What Each Class Means\n");

        $names = [];
        foreach (explode("\n", $section) as $line) {
            $line = trim($line);
            if (!str_starts_with($line, '|')) {
                continue;
            }

            $cells = array_map(trim(...), explode('|', $line));
            $classCell = $cells[1] ?? '';
            $parentCell = $cells[2] ?? '';

            if ($parentCell !== '`TiKvException`') {
                continue;
            }

            if (preg_match('/^\[`([A-Za-z_][A-Za-z0-9_]*)`\]/', $classCell, $matches) !== 1) {
                continue;
            }

            $names[$matches[1]] = true;
        }

        $subclasses = array_keys($names);
        self::assertNotEmpty($subclasses, 'No subclass rows found in the "What Each Class Means" table.');
        sort($subclasses);

        return $subclasses;
    }

    /**
     * The class names of the caller-retryability table rows, sorted.
     *
     * @return list<string>
     */
    private function retryabilityRows(): array
    {
        $section = $this->section($this->read(self::DOCUMENT), "\n## Caller Retryability\n");

        $names = [];
        foreach (explode("\n", $section) as $line) {
            $line = trim($line);
            if (!str_starts_with($line, '|')) {
                continue;
            }

            if (preg_match('/^\|\s*`([A-Za-z_][A-Za-z0-9_]*)`/', $line, $matches) !== 1) {
                continue;
            }

            $names[$matches[1]] = true;
        }

        $classes = array_keys($names);
        self::assertNotEmpty($classes, 'No rows found in the caller-retryability table.');
        sort($classes);

        return $classes;
    }

    /**
     * Every PHP file under a directory, recursively, sorted.
     *
     * @return list<string>
     */
    private function phpFiles(string $directory): array
    {
        $files = [];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($directory, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }

    /**
     * The text of a markdown section, from its heading up to the next heading
     * of the same or a shallower level.
     */
    private function section(string $contents, string $heading): string
    {
        $start = strpos($contents, $heading);
        self::assertNotFalse($start, sprintf('No "%s" section found.', trim($heading)));

        $level = 1;
        if (preg_match('/^#+/', trim($heading), $matches) === 1) {
            $level = strlen($matches[0]);
        }

        $rest = substr($contents, $start + strlen($heading));
        $end = strlen($rest);
        if (preg_match('/\n#{1,' . $level . '} /', $rest, $matches, PREG_OFFSET_CAPTURE) === 1) {
            $end = $matches[0][1];
        }

        return substr($rest, 0, $end);
    }

    private function read(string $relativePath): string
    {
        $file = $this->repositoryRoot() . '/' . $relativePath;
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $file));

        return $contents;
    }

    private function repositoryRoot(): string
    {
        return dirname(__DIR__, 3);
    }
}
