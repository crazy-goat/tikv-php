<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #776: the SST-ingest "Operational notes" of
 * docs/operations.md copied a partial `options['timeout']` key list as if it
 * were the complete set.
 *
 * The list came from #390 and kept the eight store-RPC keys after
 * `batchDeadlineMs` (#185 / #275) and the three metadata-plane deadlines
 * (`pdTimeoutMs`, `tsoTimeoutMs`, `lockResolveTimeoutMs`, #260) were added.
 * A reader who landed on the ingest section could conclude that the newer
 * keys do not exist. TimeoutOptionKeysDocumentationGuardTest only parses
 * docs/configuration.md, so this second copy could drift again unnoticed.
 *
 * The note now points at the canonical "Per-Operation Timeouts" table instead
 * of restating keys. This guard fails if the link disappears or if the note
 * copies a key list again — a partial copy is exactly the bug, and a full
 * copy is the third place to maintain the same list.
 */
final class OperationsIngestTimeoutDocumentationGuardTest extends TestCase
{
    private const OPERATIONS_FILE = 'docs/operations.md';

    /** Heading of the ingest section that carries the operational notes. */
    private const INGEST_HEADING = '### Ingest';

    /** Anchor of the canonical table in docs/configuration.md. */
    private const CANONICAL_TIMEOUT_LINK = 'configuration.md#per-operation-timeouts';

    public function testTheIngestOperationalNotesLinkToTheCanonicalTimeoutTable(): void
    {
        self::assertStringContainsString(
            '](' . self::CANONICAL_TIMEOUT_LINK . ')',
            $this->ingestOperationalNotes(),
            'The SST-ingest operational notes must point at the canonical '
            . '"Per-Operation Timeouts" table in docs/configuration.md; a reader who lands '
            . 'on the ingest section is otherwise left with whatever copy the note has.',
        );
    }

    public function testTheIngestOperationalNotesDoNotCopyATimeoutKeyList(): void
    {
        $notes = $this->ingestOperationalNotes();

        $keys = [];
        if (preg_match_all('/`([A-Za-z][A-Za-z0-9]*Ms)`/', $notes, $matches) > 0) {
            $keys = array_values(array_unique($matches[1]));
        }

        $copied = array_values(array_diff($keys, ['ingestTimeoutMs']));

        self::assertSame(
            [],
            $copied,
            "The SST-ingest operational notes name more timeout keys than ingestTimeoutMs:\n"
            . implode("\n", $copied)
            . "\nDo not restate the options['timeout'] map here — link to it instead, so the "
            . 'key set is maintained in one place.',
        );
    }

    /**
     * The "Operational notes" block of the ingest section.
     */
    private function ingestOperationalNotes(): string
    {
        $contents = $this->read(self::OPERATIONS_FILE);
        $section = $this->section($contents, self::INGEST_HEADING);

        $start = strpos($section, '**Operational notes:**');
        self::assertNotFalse(
            $start,
            'The SST-ingest section has no "Operational notes" block.',
        );

        return substr($section, $start);
    }

    /**
     * The text of a markdown section, from its heading up to the next heading
     * of the same or a shallower level.
     */
    private function section(string $contents, string $heading): string
    {
        $lines = explode("\n", $contents);
        $start = null;

        foreach ($lines as $index => $line) {
            if (trim($line) === $heading) {
                $start = $index;
                break;
            }
        }

        self::assertNotNull($start, sprintf('No "%s" section found.', $heading));

        $level = strlen(strstr($heading, ' ', true) ?: '#');
        $lineCount = count($lines);

        for ($index = $start + 1; $index < $lineCount; $index++) {
            if (preg_match('/^#{1,' . $level . '}\s/', $lines[$index]) === 1) {
                return implode("\n", array_slice($lines, $start + 1, $index - $start - 1));
            }
        }

        return implode("\n", array_slice($lines, $start + 1));
    }

    private function read(string $path): string
    {
        $contents = file_get_contents(dirname(__DIR__, 3) . '/' . $path);
        self::assertNotFalse($contents, 'Unable to read ' . $path);

        return $contents;
    }
}
