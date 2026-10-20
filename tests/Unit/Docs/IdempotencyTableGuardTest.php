<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #409: every public operation of `RawKvClient`,
 * `TxnKvClient`, `Transaction` and the `ScanIterator` they hand out carries
 * an idempotency / retry-safety verdict in `docs/error-handling.md`.
 *
 * The table answers the question the exception tables cannot: after an
 * ambiguous failure (a timeout, a connection reset, an exhausted retry
 * budget) the caller has to decide whether to repeat the *operation*. The
 * answer differs per operation — `put()` converges, `compareAndSwap()` does
 * not, a transaction must be replaced by a new one — and the client makes
 * that judgement internally (it retries a secondary-region commit and
 * deliberately does not retry the primary one), so the reasoning has to be
 * written down somewhere.
 *
 * The check is a guard rather than a behavioural test because the defect it
 * protects against is an *absent* row: a new public method added tomorrow
 * would simply have no verdict, and no test could notice. The same argument
 * is why the list of operations is derived by reflection instead of being
 * restated here — a restated list would go stale silently.
 *
 * Only a *table row* counts as a verdict. A prose mention of an operation
 * does not: deleting the row and keeping the mention would otherwise leave
 * the guard green while the table lost the operation.
 *
 * The second test pins the acceptance criterion that the table is reachable
 * from the batch partial-failure documentation, which is where a caller
 * meets the "may I repeat this?" question first.
 */
class IdempotencyTableGuardTest extends TestCase
{
    /** Heading of the section under guard, including its markdown level. */
    private const SECTION_HEADING = "\n## Idempotency and Retry Safety\n";

    /** The heading of the batch partial-failure section in docs/operations.md. */
    private const PARTIAL_FAILURE_HEADING = "\n### Partial Failure and Recovery\n";

    /** The heading of the batch partial-failure section in docs/troubleshooting.md. */
    private const TROUBLESHOOTING_HEADING = "\n#### Batch Partial Failure\n";

    /** The anchor the two documents link to each other with. */
    private const SECTION_ANCHOR = '#idempotency-and-retry-safety';

    public function testEveryPublicOperationHasAnIdempotencyVerdict(): void
    {
        $section = $this->section(
            $this->readFile('docs/error-handling.md'),
            self::SECTION_HEADING,
        );

        $verdicts = $this->verdictRows($section);

        $missing = [];
        $blank = [];
        foreach ($this->publicOperations() as $operation) {
            if (!array_key_exists($operation, $verdicts)) {
                $missing[] = $operation;
                continue;
            }

            [$idempotent, $safeToRetry] = $verdicts[$operation];
            if ($idempotent === '' || $safeToRetry === '') {
                $blank[] = $operation;
            }
        }

        self::assertSame(
            [],
            $missing,
            "Public operations without a row in the \"Idempotency and Retry Safety\" "
            . "table of docs/error-handling.md:\n"
            . implode("\n", $missing),
        );

        self::assertSame(
            [],
            $blank,
            "Public operations whose row carries no verdict:\n"
            . implode("\n", $blank),
        );
    }

    public function testTheTableIsLinkedFromTheBatchPartialFailureDocumentation(): void
    {
        $operations = $this->section(
            $this->readFile('docs/operations.md'),
            self::PARTIAL_FAILURE_HEADING,
        );

        self::assertStringContainsString(
            'error-handling.md' . self::SECTION_ANCHOR,
            $operations,
            'The "Partial Failure and Recovery" section of docs/operations.md must '
            . 'link the idempotency / retry-safety table of docs/error-handling.md.',
        );

        $errorHandling = $this->readFile('docs/error-handling.md');
        $row = $this->lineContaining($errorHandling, '| `BatchPartialFailureException` |');

        self::assertStringContainsString(
            self::SECTION_ANCHOR,
            $row,
            'The `BatchPartialFailureException` row of the caller-retryability table '
            . 'in docs/error-handling.md must link the idempotency / retry-safety table.',
        );

        $troubleshooting = $this->section(
            $this->readFile('docs/troubleshooting.md'),
            self::TROUBLESHOOTING_HEADING,
        );

        self::assertStringContainsString(
            'error-handling.md' . self::SECTION_ANCHOR,
            $troubleshooting,
            'The "Batch Partial Failure" section of docs/troubleshooting.md must '
            . 'link the idempotency / retry-safety table of docs/error-handling.md.',
        );
    }

    /**
     * Every public operation of the client-facing classes, as
     * `Class::method()`.
     *
     * Constructors and destructors are left out: they are lifecycle, not
     * operations against the cluster, and the verdict that applies to them
     * ("create a new one", "do nothing") is not the question this table
     * answers.
     *
     * @return list<string>
     */
    private function publicOperations(): array
    {
        $operations = [];

        $classes = [
            RawKvClient::class,
            TxnKvClient::class,
            Transaction::class,
            ScanIterator::class,
        ];

        foreach ($classes as $class) {
            $reflection = new \ReflectionClass($class);
            $classOperations = 0;

            foreach ($reflection->getMethods(\ReflectionMethod::IS_PUBLIC) as $method) {
                $name = $method->getName();
                if (str_starts_with($name, '__')) {
                    continue;
                }

                $operations[] = $reflection->getShortName() . '::' . $name . '()';
                $classOperations++;
            }

            self::assertGreaterThan(
                0,
                $classOperations,
                sprintf('No public operation was derived from %s.', $class),
            );
        }

        self::assertNotEmpty(
            $operations,
            'The reflection-derived operation list is empty — the guard would be vacuous.',
        );

        sort($operations);

        return $operations;
    }

    /**
     * The verdict cells of every table row of the section, keyed by the
     * operation the row documents.
     *
     * Only the first cell of a row names the operation, and only a row (a
     * line that starts with `|`) is a verdict: a mention in the prose is
     * not. A row that documents several operations carries one verdict for
     * all of them.
     *
     * @return array<string, array{string, string}> the idempotency verdict
     *                                            and the retry-safety verdict
     */
    private function verdictRows(string $section): array
    {
        $verdicts = [];

        foreach (explode("\n", $section) as $line) {
            $line = trim($line);
            if (!str_starts_with($line, '|')) {
                continue;
            }

            $cells = explode('|', $line);
            // $cells[0] is the empty string in front of the leading pipe.
            $operationCell = trim($cells[1] ?? '');
            $idempotent = trim($cells[2] ?? '');
            $safeToRetry = trim($cells[3] ?? '');

            foreach ($this->operationsIn($operationCell) as $operation) {
                $verdicts[$operation] = [$idempotent, $safeToRetry];
            }
        }

        return $verdicts;
    }

    /**
     * The operation names listed in the first cell of a table row.
     *
     * @return list<string>
     */
    private function operationsIn(string $cell): array
    {
        if (!str_starts_with($cell, '`')) {
            return [];
        }

        $operations = [];
        foreach (explode(',', $cell) as $name) {
            $name = trim(trim($name), '`');
            if ($name === '') {
                continue;
            }

            $operations[] = $name;
        }

        return $operations;
    }

    /**
     * The text of a markdown section, from its heading up to the next
     * heading of the same or a shallower level.
     *
     * The level is taken from the heading itself, so a `##` section keeps
     * its `###` subsections (the idempotency table lives in them) while a
     * `###` section still ends at the next `##`.
     */
    private function section(string $contents, string $heading): string
    {
        $start = strpos($contents, $heading);
        self::assertNotFalse(
            $start,
            sprintf('No "%s" section found.', trim($heading)),
        );

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

    /**
     * The first line of the document containing the given needle.
     */
    private function lineContaining(string $contents, string $needle): string
    {
        foreach (explode("\n", $contents) as $line) {
            if (str_contains($line, $needle)) {
                return $line;
            }
        }

        self::fail(sprintf('No line containing "%s" found.', $needle));
    }

    private function readFile(string $relativePath): string
    {
        $file = dirname(__DIR__, 3) . '/' . $relativePath;
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $file));

        return $contents;
    }
}
