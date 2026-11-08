<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

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
 *
 * Issue #726 added the docblock and reference-page checks: the scan caps must
 * name the property or option they belong to, and the GC-safe-point example
 * must not look like an unbounded transactional scan.
 */
final class ScanLimitDocumentationGuardTest extends TestCase
{
    private const SCANNING_HEADING = "\n### Scanning\n";

    private const COMPLETE_EXAMPLE_HEADING = "\n### Complete Example\n";

    private const GC_SAFE_POINT_HEADING = "\n## GC Safe Points and Long-Running Reads\n";

    private const BATCH_SCAN_HEADING = "\n### Batch Scan\n";

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

    public function testScanLimitExceptionDocblockNamesTheCapItReports(): void
    {
        $doc = (new ReflectionMethod(ScanLimitExceededException::class, 'getMaxRows'))->getDocComment();
        self::assertIsString($doc, 'getMaxRows() must keep a docblock naming the cap.');

        self::assertStringContainsString(
            "options['maxScanRows']",
            $doc,
            'getMaxRows() must name the RawKV cap, options[\'maxScanRows\'] (issue #726).',
        );
        self::assertStringContainsString(
            'MAX_SCAN_LIMIT',
            $doc,
            'getMaxRows() must name the transactional cap, Transaction::MAX_SCAN_LIMIT (issue #726).',
        );
    }

    public function testScanDocblocksDoNotReferenceInstancePropertiesAsStaticOnes(): void
    {
        // Neither class declares a static property, so `self::$name` names a
        // static property that does not exist and the reference does not
        // resolve (issue #726).
        foreach (['RawKvScanner.php', 'RawKvRangeOps.php'] as $file) {
            $path = dirname(__DIR__, 3) . '/src/Client/RawKv/' . $file;
            $contents = file_get_contents($path);
            self::assertNotFalse($contents, "Unable to read {$path}");

            self::assertStringNotContainsString(
                '{@see self::$',
                $contents,
                "{$file} must reference its instance properties with the class name, not self::\$ (issue #726).",
            );
        }
    }

    public function testGcSafePointExampleStatesTheTransactionalScanCap(): void
    {
        $section = $this->section($this->errorHandling(), self::GC_SAFE_POINT_HEADING);

        self::assertStringContainsString(
            'MAX_SCAN_LIMIT',
            $section,
            'The GC-safe-point example must name Transaction::MAX_SCAN_LIMIT: an unbounded '
            . 'transactional scan throws past it (issue #726).',
        );
        self::assertStringContainsString(
            '10240',
            $section,
            'The GC-safe-point example must state the 10240-row transactional scan cap (issue #726).',
        );
    }

    public function testBatchScanStatesThePerRangeTruncationRule(): void
    {
        $doc = (new ReflectionMethod(RawKvClient::class, 'batchScan'))->getDocComment();
        self::assertIsString($doc, 'batchScan() must keep a docblock.');
        self::assertStringContainsString('eachLimit', $doc);
        self::assertMatchesRegularExpression(
            '/truncat/i',
            $doc,
            'RawKvClient::batchScan() must state that a per-range count equal to eachLimit '
            . 'may be truncated (issue #726).',
        );

        $row = $this->tableRow($this->errorHandling(), '| `batchScan(');
        self::assertStringContainsString('eachLimit', $row);
        self::assertMatchesRegularExpression(
            '/truncat/i',
            $row,
            'docs/error-handling.md must state the batchScan() per-range truncation rule (issue #726).',
        );

        $section = $this->section($this->operations(), self::BATCH_SCAN_HEADING);
        self::assertMatchesRegularExpression(
            '/eachLimit[^\n]*may mean the range holds more rows|may mean the range holds more rows[^\n]*eachLimit/',
            $section,
            'The Batch Scan reference must state the per-range truncation rule (issue #726).',
        );
    }

    private function operations(): string
    {
        $file = dirname(__DIR__, 3) . '/docs/operations.md';
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return $contents;
    }

    private function errorHandling(): string
    {
        $file = dirname(__DIR__, 3) . '/docs/error-handling.md';
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return $contents;
    }

    /**
     * The single markdown table row that starts with `$prefix`.
     */
    private function tableRow(string $contents, string $prefix): string
    {
        foreach (explode("\n", $contents) as $line) {
            if (str_starts_with($line, $prefix)) {
                return $line;
            }
        }

        self::fail(sprintf('No table row starting with "%s" found.', $prefix));
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
