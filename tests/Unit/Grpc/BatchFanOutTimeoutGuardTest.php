<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use PHPUnit\Framework\TestCase;

/**
 * Issue #775: `TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS` is pinned to the
 * per-call timeouts the batch fan-out classes actually read.
 *
 * The constant is the list the constructor uses to reject a `batchDeadlineMs`
 * below any per-call timeout used inside a fan-out, which is the invariant the
 * class docblock promises. Nothing tied it to the call sites: a new per-call
 * timeout added to `TimeoutConfig` and read inside a fan-out could be left out
 * of the list, and the constructor would then accept a deadline below it —
 * exactly the case the check exists to prevent (issue #275). Conversely a
 * field could stay in the list after the last fan-out stopped using it, and
 * no test noticed.
 *
 * A source scan is the right shape here, as in
 * `NoInfiniteDeadlineGuardTest` (issue #184) and `RpcDeadlineTest`'s
 * call-site scan: the tests that drive a fan-out can only assert the timeout
 * on the paths they happen to run, while this guard asserts the classification
 * itself.
 *
 * The scan collects every `$this->timeoutConfig-><field>` read in the six
 * fan-out classes and requires that set, minus the named non-fan-out
 * exceptions, to equal the constant exactly — in both directions. Tokenised
 * rather than regexed so a read spread over lines is still found, and so the
 * field names quoted in docblocks and comments are not mistaken for code.
 *
 * A new read in one of the scanned classes therefore has to be classified:
 * add the field to `TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS` when the read
 * happens inside a fan-out, or to {@see self::NOT_FAN_OUT} with the reason
 * when it does not. The witness test below proves the scan is not silently
 * matching nothing.
 */
class BatchFanOutTimeoutGuardTest extends TestCase
{
    /**
     * The classes the issue names as fan-out call sites, relative to the
     * repository root. Explicit rather than discovered by a glob, so a rename
     * fails the test (`assertFileExists`) instead of quietly dropping the
     * class from the scan.
     *
     * @var list<string>
     */
    private const FAN_OUT_SOURCES = [
        'src/Client/RawKv/RawKvBatch.php',
        'src/Client/RawKv/RawKvRangeOps.php',
        'src/Client/RawKv/RawKvScanner.php',
        'src/Client/RawKv/SstIngestor.php',
        'src/Client/TxnKv/TwoPhaseCommitter.php',
        'src/Client/TxnKv/TxnReader.php',
    ];

    /**
     * Reads in the scanned classes that are deliberately not per-call
     * fan-out timeouts, with the reason. Every entry must still be read
     * somewhere in the scanned sources: once a read disappears the
     * classification describes code that no longer exists and the guard
     * fails on it too.
     *
     * @var array<string, string>
     */
    private const NOT_FAN_OUT = [
        'batchDeadlineMs' => 'it is the wall-clock budget the fan-out is checked against, not a per-call timeout',
        'readTimeoutMs' => 'only synchronous point reads use it; the TxnReader fan-outs read'
            . ' batchReadTimeoutMs and scanTimeoutMs',
    ];

    public function testTheDeclaredFanOutTimeoutsAreTheOnesTheFanOutClassesRead(): void
    {
        $observed = $this->readsInFanOutClasses();

        $read = [];
        foreach (array_keys($observed) as $field) {
            if (array_key_exists($field, self::NOT_FAN_OUT)) {
                continue;
            }
            $read[] = $field;
        }
        sort($read);

        $declared = BatchFanOutTimeouts::declared();
        sort($declared);

        $missing = array_values(array_diff($read, $declared));
        $stale = array_values(array_diff($declared, $read));

        self::assertSame(
            $declared,
            $read,
            "TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS is out of step with the per-call timeouts the\n"
            . "fan-out classes read (issue #775). The constructor rejects a batchDeadlineMs below\n"
            . "every field in that list, so a fan-out timeout missing from it silently permits a\n"
            . "deadline shorter than one of its calls.\n"
            . $this->renderFields('Read by a fan-out class but not declared:', $missing, $observed)
            . $this->renderFields('Declared but no fan-out class reads it:', $stale, $observed)
            . "Add a field to TimeoutConfig::BATCH_FAN_OUT_TIMEOUTS when the read happens inside a\n"
            . "fan-out, or to NOT_FAN_OUT in this test with the reason when it does not.",
        );
    }

    /**
     * The exceptions above are only honest while the code still contains the
     * read they name. `batchDeadlineMs` disappearing from the fan-out classes
     * or `readTimeoutMs` from `TxnReader` would leave a rule that describes
     * nothing.
     */
    public function testEveryNonFanOutExclusionStillMatchesARead(): void
    {
        $observed = $this->readsInFanOutClasses();

        foreach (self::NOT_FAN_OUT as $field => $reason) {
            self::assertArrayHasKey(
                $field,
                $observed,
                sprintf(
                    'The guard excludes %s from the fan-out timeout set because "%s",'
                    . ' but no scanned fan-out class reads it any more. Remove the exclusion.',
                    $field,
                    $reason,
                ),
            );
        }
    }

    /**
     * The scan must look at the six fan-out classes. Without this, a renamed
     * or moved class would let the scan run over nothing.
     */
    public function testTheScanCoversEveryFanOutClass(): void
    {
        $root = $this->repositoryRoot();

        foreach (self::FAN_OUT_SOURCES as $relative) {
            self::assertFileExists(
                $root . '/' . $relative,
                sprintf('The fan-out scan lists %s, which no longer exists; update the list.', $relative),
            );
        }

        self::assertNotSame(
            [],
            $this->readsInFanOutClasses(),
            'The scan found no $this->timeoutConfig-><field> read in any fan-out class;'
            . ' the scan is broken, not the source.',
        );
    }

    /**
     * The scan must still find a read when there is one, and must not mistake
     * a field name inside a comment for a read.
     */
    public function testTheScanStillFindsTimeoutConfigReads(): void
    {
        $code = <<<'PHP'
        <?php
        $read = $this->timeoutConfig->batchReadTimeoutMs;
        $new = $this->timeoutConfig->brandNewFanOutTimeoutMs;
        // $this->timeoutConfig->commentedOutTimeoutMs is not a read
        /* $this->timeoutConfig->alsoCommentedOutMs */
        PHP;

        $fields = [];
        foreach ($this->timeoutConfigReads($code) as [, $field]) {
            $fields[] = $field;
        }

        self::assertSame(
            ['batchReadTimeoutMs', 'brandNewFanOutTimeoutMs'],
            $fields,
            'The source scan must find executable $this->timeoutConfig-><field> reads and ignore comments.',
        );
    }

    /**
     * A class that enforces the batch deadline is a fan-out class, so it has
     * to be in the scan above. Without this the explicit file list would be
     * the same kind of hand-maintained list the constant used to be: a new
     * fan-out executor could be left out and its per-call timeout reads would
     * never be checked against `BATCH_FAN_OUT_TIMEOUTS` (issue #775).
     */
    public function testEveryClassThatArmsTheBatchDeadlineIsScanned(): void
    {
        $root = $this->repositoryRoot();
        $unscanned = [];

        foreach ($this->phpFilesUnder($root . '/src/Client') as [$file, $relative]) {
            $code = file_get_contents($file);
            if ($code === false) {
                self::fail("Unable to read {$file}");
            }

            foreach ($this->timeoutConfigReads($code) as [, $field]) {
                if ($field !== 'batchDeadlineMs') {
                    continue;
                }

                $sourcePath = 'src/Client/' . $relative;
                if (!in_array($sourcePath, self::FAN_OUT_SOURCES, true)) {
                    $unscanned[] = $sourcePath;
                }

                break;
            }
        }

        $unscanned = array_values(array_unique($unscanned));
        sort($unscanned);

        self::assertSame(
            [],
            $unscanned,
            "These classes read \$this->timeoutConfig->batchDeadlineMs — they run a batch fan-out\n"
            . "against the deadline — but are not in FAN_OUT_SOURCES, so their per-call timeout\n"
            . "reads are not pinned (issue #775). Add them to the scan.",
        );
    }

    /**
     * Every `$this->timeoutConfig-><field>` read in the six fan-out classes,
     * as field name => "relative path:line" sites.
     *
     * @return array<string, list<string>>
     */
    private function readsInFanOutClasses(): array
    {
        $root = $this->repositoryRoot();
        $reads = [];

        foreach (self::FAN_OUT_SOURCES as $relative) {
            $file = $root . '/' . $relative;
            if (!is_file($file)) {
                continue;
            }

            $code = file_get_contents($file);
            if ($code === false) {
                self::fail("Unable to read {$file}");
            }

            foreach ($this->timeoutConfigReads($code) as [$line, $field]) {
                $reads[$field][] = sprintf('%s:%d', $relative, $line);
            }
        }

        ksort($reads);

        return $reads;
    }

    /**
     * Every executable `$this->timeoutConfig-><field>` read in a piece of
     * source, as (line, field name) pairs.
     *
     * Tokenised rather than regexed so a read spread over lines is still
     * found, and so a field name inside a comment or docblock — which is where
     * the rationale for the classification is written — is not taken for a
     * read.
     *
     * @return list<array{int, string}>
     */
    private function timeoutConfigReads(string $code): array
    {
        $tokens = token_get_all($code);
        $count = count($tokens);
        $reads = [];

        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || $token[0] !== T_STRING || $token[1] !== 'timeoutConfig') {
                continue;
            }

            $arrow = $this->nextSignificantToken($tokens, $i + 1);
            if ($arrow === null || !is_array($tokens[$arrow]) || $tokens[$arrow][0] !== T_OBJECT_OPERATOR) {
                continue;
            }

            $field = $this->nextSignificantToken($tokens, $arrow + 1);
            if ($field === null || !is_array($tokens[$field]) || $tokens[$field][0] !== T_STRING) {
                continue;
            }

            $reads[] = [$token[2], $tokens[$field][1]];
        }

        return $reads;
    }

    /**
     * Index of the next token that is not whitespace or a comment, or null at
     * the end of the source.
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function nextSignificantToken(array $tokens, int $index): ?int
    {
        $count = count($tokens);

        for ($i = $index; $i < $count; $i++) {
            if (
                is_array($tokens[$i])
                && in_array($tokens[$i][0], [T_WHITESPACE, T_COMMENT, T_DOC_COMMENT], true)
            ) {
                continue;
            }

            return $i;
        }

        return null;
    }

    /**
     * One indented block naming the fields and the sites that read them, or
     * an empty string when there is nothing to report.
     *
     * @param list<string> $fields
     * @param array<string, list<string>> $observed
     */
    private function renderFields(string $heading, array $fields, array $observed): string
    {
        if ($fields === []) {
            return '';
        }

        $lines = [$heading];
        foreach ($fields as $field) {
            $sites = $observed[$field] ?? [];
            $lines[] = $sites === []
                ? sprintf('  - %s', $field)
                : sprintf('  - %s (%s)', $field, implode(', ', $sites));
        }

        return implode("\n", $lines) . "\n";
    }

    private function repositoryRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    /**
     * @return list<array{string, string}> absolute path and path relative to $dir
     */
    private function phpFilesUnder(string $dir): array
    {
        $root = (string) realpath($dir);
        $files = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $files[] = [$entry->getPathname(), substr($entry->getPathname(), strlen($root) + 1)];
            }
        }

        usort($files, static fn (array $a, array $b): int => $a[1] <=> $b[1]);

        return $files;
    }
}
