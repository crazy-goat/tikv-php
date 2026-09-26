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
 * ## Scope: two reviewed exceptions, allowlisted by count
 *
 * The count form is the same as `NoSilentRegionDropGuardTest`'s and for the
 * same reason: it is invariant under reformatting, and adding or removing one
 * of these sites fails the test until this allowlist and its reason are
 * updated — a conscious review, not a silently widened exception.
 */
class NoInfiniteDeadlineGuardTest extends TestCase
{
    private const CLIENT_SOURCE_DIR = __DIR__ . '/../../../src/Client';

    /**
     * Reviewed exceptions, as `file (relative to src/Client) => how many
     * `Timeval::infFuture()` sites that file may contain`.
     *
     *  - `Grpc/GrpcClient.php` (1): the transport's own opt-out. `null`
     *    resolves to {@see \CrazyGoat\TiKV\Client\Grpc\GrpcClient::DEFAULT_TIMEOUT_MS};
     *    the only way to ask for no deadline at all is the explicit sentinel
     *    `0`, which is the single convention the library spells "disabled"
     *    with (issue #260). A caller has to *write* the sentinel — nothing
     *    reaches it by omission.
     *  - `Grpc/BatchCommandsConnection.php` (1): a **stream** lifetime, not
     *    a unary call. The class opens one bidirectional `BatchCommands` stream
     *    per store and reuses it, so its `open()` takes a real `$deadlineMs`
     *    parameter defaulting to 60 s; `0` is the same explicit opt-out.
     *    Production never passes it (`GrpcBatchCommandsTransport::forClient()`
     *    calls `open()` with one argument), so no configured value can reach
     *    this site today.
     *
     * @var array<string, int>
     */
    private const ALLOWED = [
        'Grpc/GrpcClient.php' => 1,
        'Grpc/BatchCommandsConnection.php' => 1,
    ];

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
            $expected = self::ALLOWED[$relative] ?? 0;
            if (count($found) !== $expected) {
                $violations[] = sprintf(
                    "%s: %d infinite-deadline site(s), %d reviewed (issue #184)\n    %s",
                    $relative,
                    count($found),
                    $expected,
                    implode("\n    ", $found),
                );
            }
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
            . "If a site here is a legitimate opt-out, review it and update the ALLOWED map with "
            . "its reason.\n" . implode("\n", $violations),
        );

        // Without this the test would also pass if the scan silently stopped
        // finding anything — the two allowlisted sites below are its witness.
        self::assertSame(2, $total, 'The scan found a different number of '
            . 'Timeval::infFuture() sites than the two documented opt-outs; the scan is '
            . 'broken, not the source.');
    }

    /**
     * Every executable `Timeval::infFuture()` in a file, as
     * "line N: <the line's text>" for the failure message.
     *
     * Tokenised rather than regexed so a call that is built dynamically (or
     * spread over lines) is still found, and so the same text inside a
     * comment or a docblock — which is where the rationale for these two
     * sites is written — is not mistaken for code.
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
