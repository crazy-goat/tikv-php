<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #388 (DOC-22): the README must state the real
 * scan limits and must not demonstrate a whole-prefix scan with a buffered
 * scanPrefix() call.
 *
 * On current master `scan(limit: 0)` is not truncated at one page: it pages
 * internally at MAX_SCAN_LIMIT (10240) rows per RPC and throws
 * ScanLimitExceededException once the buffered result exceeds
 * options['maxScanRows'] (default 100000). The Complete Example therefore
 * counts users with scanPrefixIterator(), which has no such cap. The checks
 * look for those facts, not for one exact wording.
 */
final class ScanLimitDocumentationGuardTest extends TestCase
{
    private const SCANNING_HEADING = "\n### Scanning\n";

    private const COMPLETE_EXAMPLE_HEADING = "\n### Complete Example\n";

    public function testReadmeScanningSectionStatesThePerRpcLimitAndTheTotalCap(): void
    {
        $section = $this->section($this->readme(), self::SCANNING_HEADING);

        self::assertStringContainsString(
            'MAX_SCAN_LIMIT',
            $section,
            'The Scanning section of README.md must name MAX_SCAN_LIMIT.',
        );
        self::assertStringContainsString(
            '10240',
            $section,
            'The Scanning section of README.md must state the 10240-row per-RPC limit.',
        );
        self::assertStringContainsString(
            'maxScanRows',
            $section,
            "The Scanning section of README.md must state the options['maxScanRows'] cap.",
        );
    }

    public function testReadmeCompleteExampleUsesTheIteratorForTheWholePrefixScan(): void
    {
        $section = $this->section($this->readme(), self::COMPLETE_EXAMPLE_HEADING);

        self::assertStringContainsString(
            'scanPrefixIterator(',
            $section,
            'The Complete Example of README.md must demonstrate scanPrefixIterator(): '
            . "a buffered scanPrefix() throws past options['maxScanRows'] instead of "
            . 'returning the whole prefix (issue #388).',
        );
    }

    private function readme(): string
    {
        $file = dirname(__DIR__, 3) . '/README.md';
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return $contents;
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
}
