<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Guard for the structure of `CHANGELOG.md` (issue #413, DOC-47).
 *
 * Keep a Changelog expects one heading of each type per release section, with
 * every entry of that type accumulated under it. A second `### Fixed` heading
 * is easy to add by accident — a merge conflict, a contributor appending at
 * the end of the file instead of under the existing heading — and nothing in
 * the toolchain notices: the Markdown still renders, the entries are still
 * there, and a reader or a release script that scans by heading silently sees
 * only one of the two blocks.
 *
 * The same applies to the order: the standard headings are Added, Changed,
 * Deprecated, Removed, Fixed and Security. The project adds Tests, Docs and
 * Performance after them, so the canonical order below is the standard list
 * followed by the project's own headings. Released sections keep their
 * entries but are normalised to this order as well, so the file is uniform
 * for the reader and the guard has one rule instead of two.
 *
 * The guard is a test rather than a Markdown linter because the repository
 * has no Markdown linter and every other documentation invariant is already
 * pinned by a `tests/Unit/Docs` guard that runs in the unit matrix.
 *
 * A fresh `[Unreleased]` section is deliberately empty: the release pull
 * request renames the old one to `## [vX.Y.Z]` and adds a new, empty
 * `## [Unreleased]` above it. The guard therefore requires the section to
 * exist but does not require it to have a heading — the first contributor
 * after a release adds the heading together with the entry.
 */
final class ChangelogStructureGuardTest extends TestCase
{
    /**
     * The one heading order every release section has to follow.
     *
     * Keep a Changelog's six types come first, in its documented order; the
     * three headings this project adds follow them.
     *
     * @var list<string>
     */
    private const HEADING_ORDER = [
        'Added',
        'Changed',
        'Deprecated',
        'Removed',
        'Fixed',
        'Security',
        'Tests',
        'Docs',
        'Performance',
        'Infrastructure',
    ];

    public function testEveryReleaseSectionHasAtMostOneHeadingOfEachType(): void
    {
        foreach ($this->sections() as $section => $headings) {
            $counts = array_count_values($headings);

            foreach ($counts as $heading => $count) {
                self::assertSame(
                    1,
                    $count,
                    sprintf(
                        'CHANGELOG.md section "%s" has %d "### %s" headings. Keep one heading '
                        . 'of each type and append the entries under it (issue #413).',
                        $section,
                        $count,
                        $heading,
                    ),
                );
            }
        }
    }

    public function testEveryReleaseSectionUsesTheDocumentedHeadingOrder(): void
    {
        $sections = $this->sections();

        self::assertArrayHasKey(
            '[Unreleased]',
            $sections,
            'CHANGELOG.md has no "## [Unreleased]" section.',
        );

        // A fresh `[Unreleased]` is empty until the first change after a
        // release adds its heading, so only its existence is required here;
        // the loop below checks the order of whatever headings it has.
        foreach ($sections as $section => $headings) {
            $previousPosition = -1;
            $previousHeading = null;

            foreach ($headings as $heading) {
                $position = array_search($heading, self::HEADING_ORDER, true);

                self::assertNotFalse(
                    $position,
                    sprintf(
                        'CHANGELOG.md section "%s" uses the heading "### %s", which is not one '
                        . 'of the documented types: %s.',
                        $section,
                        $heading,
                        implode(', ', self::HEADING_ORDER),
                    ),
                );

                $position = (int) $position;

                if ($previousHeading !== null) {
                    self::assertGreaterThan(
                        $previousPosition,
                        $position,
                        sprintf(
                            'CHANGELOG.md section "%s" puts "### %s" after "### %s". The headings '
                            . 'must follow this order: %s (issue #413).',
                            $section,
                            $heading,
                            $previousHeading,
                            implode(', ', self::HEADING_ORDER),
                        ),
                    );
                }

                $previousPosition = $position;
                $previousHeading = $heading;
            }
        }
    }

    /**
     * The `###` headings of every `##` section, keyed by the section title.
     *
     * @return array<string, list<string>>
     */
    private function sections(): array
    {
        $sections = [];
        $current = null;

        foreach (explode("\n", $this->changelog()) as $line) {
            if (str_starts_with($line, '## ')) {
                $current = trim(substr($line, 3));
                $sections[$current] = [];

                continue;
            }

            if ($current !== null && str_starts_with($line, '### ')) {
                $sections[$current][] = trim(substr($line, 4));
            }
        }

        return $sections;
    }

    private function changelog(): string
    {
        $contents = file_get_contents(dirname(__DIR__, 3) . '/CHANGELOG.md');
        self::assertNotFalse($contents, 'Unable to read CHANGELOG.md.');

        return $contents;
    }
}
