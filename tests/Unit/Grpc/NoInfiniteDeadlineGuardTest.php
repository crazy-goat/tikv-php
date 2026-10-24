<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use PHPUnit\Framework\TestCase;

/**
 * Issue #184 (GRPC-01, PERF-07, REG-06): no code path in `src/Client` may
 * derive an **infinite** gRPC deadline.
 *
 * The bug this class exists for was never a value that was too large — it was
 * a `null` that silently meant "forever". `GrpcClient::call()` used to turn a
 * `null` `$timeoutMs` into `Timeval::infFuture()`, and the three PD/TSO/
 * lock-resolution classes passed none, so a PD that accepted a connection and
 * never answered pinned a PHP-FPM worker inside the gRPC C core, where
 * `max_execution_time` cannot reach. #260 fixed the call sites and added a
 * 30 s backstop for `null`, and the remaining `?:`-over-a-nullable shape
 * survived in `RawKvBatch`, which hand-rolls `new \Grpc\Call(...)` and
 * therefore never reaches that backstop. One `match` arm away from arming an
 * unbounded deadline, with nothing in the type system objecting.
 *
 * A guard is the right answer because the whole point of the class of bug is
 * that the *absence* of a value is what breaks: a behavioural test can only
 * assert the paths it happens to drive, and a value assertion says nothing
 * about a site added tomorrow. So this reads the sources, the same pragmatic
 * stand-in as `NoSilentRegionDropGuardTest` (issue #187) and
 * `ExceptionMessageRedactionGuardTest` (issue #269).
 *
 * The rule is stated over `Timeval::infFuture()` **occurrences**, not over
 * `?:` and ternaries: any conditional, `match`, `??` or ternary that can
 * produce the infinite deadline must mention it somewhere, so this catches
 * every shape at once, and it also catches a site that hardcodes the infinite
 * deadline with no conditional at all. Docblocks and comments are excluded by
 * construction — `token_get_all()` reports their contents as
 * `T_COMMENT`/`T_DOC_COMMENT`, never as the `T_STRING` this looks for.
 *
 * ## Scope: no exceptions at all
 *
 * Two sites used to be allowlisted by count — the unary transport's "no
 * deadline" sentinel `0` and the `BatchCommands` stream's `0`. Both are
 * gone since issue #276 (GRPC-17): a non-positive timeout is a
 * configuration error, so there is no way to ask for an unbounded call and
 * nothing left to allowlist. The scan itself is still witnessed by a
 * separate test that feeds it a snippet containing an `infFuture()`, so a
 * scan that silently stops matching anything fails too.
 */
class NoInfiniteDeadlineGuardTest extends TestCase
{
    private const CLIENT_SOURCE_DIR = __DIR__ . '/../../../src/Client';

    public function testNoCodeInTheClientDerivesAnInfiniteDeadline(): void
    {
        $violations = [];
        $total = 0;

        foreach ($this->phpFilesUnder(self::CLIENT_SOURCE_DIR) as [$file, $relative]) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            $found = $this->infiniteDeadlineSites($contents);
            if ($found === []) {
                continue;
            }

            $total += count($found);
            $violations[] = sprintf(
                "%s: %d infinite-deadline site(s) (issue #184)\n    %s",
                $relative,
                count($found),
                implode("\n    ", $found),
            );
        }

        self::assertSame(
            [],
            $violations,
            "An unbounded gRPC deadline was derived in src/Client. "
            . "Timeval::infFuture() is the one deadline the gRPC C core never expires, and "
            . "Grpc\\Call::startBatch() blocks inside the C extension where max_execution_time "
            . "cannot interrupt it, so a single slow TiKV/PD can exhaust a whole PHP-FPM worker "
            . "pool. A `null` that silently means 'forever' is what caused issue #184: resolve it "
            . "to a positive deadline instead, exactly as GrpcClient::resolveTimeoutMs() does. "
            . "There is no legitimate opt-out left: a non-positive timeout is a "
            . "configuration error (issue #276, GRPC-17).\n" . implode("\n", $violations),
        );

        // Without this the test would also pass if the scan stopped looking
        // at the right directory. The scan itself is witnessed by
        // testTheScanStillFindsAnInfiniteDeadlineSite(); this assertion
        // pins the count the scan has to find.
        self::assertSame(0, $total, 'The scan found ' . $total . ' Timeval::infFuture() site(s) '
            . 'in src/Client; there must be none — a non-positive timeout is a configuration '
            . 'error (issue #276, GRPC-17).');
    }

    /**
     * The scan must look at the client sources — the guard above asserts a
     * count of zero, so without this a scan over an empty or wrong
     * directory would pass.
     */
    public function testTheScanLooksAtTheClientSourceDirectory(): void
    {
        self::assertDirectoryExists(self::CLIENT_SOURCE_DIR);
        self::assertNotEmpty(
            $this->phpFilesUnder(self::CLIENT_SOURCE_DIR),
            'The scan must look at the client sources.',
        );
    }

    /**
     * The scan must still find an `infFuture()` when there is one — the
     * guard above asserts a count of zero, so without a witness the test
     * would also pass if the scan silently stopped matching anything.
     */
    public function testTheScanStillFindsAnInfiniteDeadlineSite(): void
    {
        $code = <<<'PHP'
        <?php
        $deadline = $timeoutMs > 0
            ? Timeval::now()->add(new Timeval($timeoutMs * 1000))
            : Timeval::infFuture();
        PHP;

        $sites = $this->infiniteDeadlineSites($code);

        self::assertCount(1, $sites, 'The scan must find an executable infFuture() site');
        self::assertStringContainsString('Timeval::infFuture()', $sites[0]);
    }

    /**
     * Every executable `Timeval::infFuture()` in a file, as
     * "line N: <the line's text>" for the failure message.
     *
     * Tokenised rather than regexed so a call that is built dynamically (or
     * spread over lines) is still found, and so the same text inside a
     * comment or a docblock — which is where the rationale for a site is
     * written — is not mistaken for code.
     *
     * @return list<string>
     */
    private function infiniteDeadlineSites(string $code): array
    {
        $lines = preg_split('/\R/', $code) ?: [];
        $sites = [];

        foreach (token_get_all($code) as $token) {
            if (!is_array($token) || $token[0] !== T_STRING || $token[1] !== 'infFuture') {
                continue;
            }

            $line = $token[2];
            $sites[] = sprintf('line %d: %s', $line, trim($lines[$line - 1] ?? ''));
        }

        return $sites;
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
