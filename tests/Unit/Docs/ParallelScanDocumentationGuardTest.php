<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Tests\Unit\Docs\Support\RawKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #827: the `parallelScan()` example in
 * `docs/advanced.md` must partition the hex alphabet into contiguous,
 * half-open ranges that cover every bucket exactly once.
 *
 * The example used to end its last range at `$prefix . ";"`. `;` is `0x3B`,
 * below `0`–`9` (`0x30`–`0x39`) and `a`–`f` (`0x61`–`0x66`). With three or more
 * workers the last range (`[c, ;)` on the default four) was therefore
 * reversed, and `RawKvScanner::batchScan()` rejects the whole call with
 * `InvalidArgumentException` through `RegionRangeClipper::assertForwardRangeOrder()`
 * (added in #824); before that validation the clipper would have skipped the
 * range. With one or two workers the last range was valid but still ended at
 * `';'`, so it silently covered only the `0`–`9` buckets and omitted `a`–`f`.
 * It also computed `$step = strlen($chars) / $workers` as a float whenever the
 * worker count did not divide 16, and `$chars[$i * $step]` then emitted
 * `Warning: String offset cast occurred` and truncated the offset.
 *
 * The example is evaluated for real rather than pattern-matched: the code
 * block is read out of the document, its function is renamed and eval'd in a
 * namespace whose `RawKvClient` is the fixture stand-in, so the test exercises
 * the ranges the document actually builds. The eval'd call runs under an
 * error handler that turns warnings into exceptions, so any float offset fails
 * regardless of how it is spelled.
 */
final class ParallelScanDocumentationGuardTest extends TestCase
{
    private const DOCUMENT = 'docs/advanced.md';

    private const HEADING = '#### Parallel Scanning';

    private const HEX_BUCKETS = '0123456789abcdef';

    private const FUNCTION_NAME = 'parallelScanUnderTest';

    private const FUNCTION_NAMESPACE = 'CrazyGoat\\TiKV\\Tests\\Unit\\Docs\\Support';

    public function testEveryWorkerCountPartitionsTheHexAlphabetWithoutGapsOrOverlap(): void
    {
        $parallelScan = $this->example();

        for ($workers = 1; $workers <= strlen(self::HEX_BUCKETS); $workers++) {
            $client = new RawKvClient();
            $this->runExample($parallelScan, $client, $workers);

            $ranges = $client->ranges;

            self::assertCount(
                $workers,
                $ranges,
                sprintf('With %d workers the example must build exactly %d ranges (issue #827)', $workers, $workers),
            );
            self::assertSame(
                'user:0',
                $ranges[0][0],
                'The first range must start at the first hex bucket (issue #827)',
            );
            self::assertSame(
                'user:g',
                $ranges[$workers - 1][1],
                sprintf(
                    'The last range must end after the last hex bucket, %s, so the %d-worker '
                    . 'partition covers c-f (issue #827)',
                    "'f'",
                    $workers,
                ),
            );

            for ($i = 0; $i < $workers; $i++) {
                self::assertTrue(
                    strcmp($ranges[$i][0], $ranges[$i][1]) < 0,
                    sprintf(
                        'Range %d, [%s, %s), is empty or reversed (issue #827)',
                        $i,
                        $ranges[$i][0],
                        $ranges[$i][1],
                    ),
                );

                if ($i === 0) {
                    continue;
                }

                self::assertSame(
                    $ranges[$i - 1][1],
                    $ranges[$i][0],
                    sprintf(
                        'With %d workers, range %d must start where range %d ends (issue #827)',
                        $workers,
                        $i,
                        $i - 1,
                    ),
                );
            }

            foreach (str_split(self::HEX_BUCKETS) as $bucket) {
                $covering = $this->rangesContaining($ranges, 'user:' . $bucket);

                self::assertCount(
                    1,
                    $covering,
                    sprintf(
                        'With %d workers the bucket %s is covered %d times, not exactly once (issue #827)',
                        $workers,
                        $bucket,
                        count($covering),
                    ),
                );
            }
        }
    }

    public function testWorkerCountIsClampedToTheBucketCount(): void
    {
        $parallelScan = $this->example();

        foreach ([0, -3] as $workers) {
            $client = new RawKvClient();
            $this->runExample($parallelScan, $client, $workers);

            self::assertCount(
                1,
                $client->ranges,
                sprintf('A worker count of %d must be clamped to one range, not divide by zero (issue #827)', $workers),
            );
            self::assertSame('user:g', $client->ranges[0][1]);
        }

        $client = new RawKvClient();
        $this->runExample($parallelScan, $client, 17);

        self::assertCount(16, $client->ranges, 'A worker count above 16 must be clamped to 16 (issue #827)');
    }

    public function testTheSnippetEndsAfterTheLastBucketAndKeepsTheBoundedSampleNote(): void
    {
        $snippet = $this->snippet();

        self::assertStringContainsString(
            "'g'",
            $snippet,
            "The last range must end after 'f', for example at \$prefix . 'g' (issue #827)",
        );
        self::assertStringNotContainsString(
            '$prefix . \';\'',
            $snippet,
            "The last range must not end at ';', which sorts before '0' and every hex letter (issue #827)",
        );
        self::assertStringContainsString(
            'For a complete read',
            $snippet,
            'The bounded-sample comment must stay next to the corrected ranges (issue #827)',
        );
    }

    /**
     * Run the documented function with every PHP warning, notice and
     * deprecation turned into an exception.
     *
     * The eval'd tests would otherwise pass on the old float step for worker
     * counts that divide 16 evenly, and PHPUnit does not fail a run on a
     * warning. Any `$chars[$i * $step]` float offset then fails test 1
     * regardless of how the expression is written (issue #827).
     *
     * @param \Closure(RawKvClient, string, int, int): mixed $parallelScan
     */
    private function runExample(\Closure $parallelScan, RawKvClient $client, int $workers): void
    {
        set_error_handler(
            static function (int $severity, string $message, string $file, int $line): bool {
                throw new \ErrorException($message, 0, $severity, $file, $line);
            },
            E_WARNING | E_DEPRECATED | E_NOTICE,
        );

        try {
            $parallelScan($client, 'user:', 10, $workers);
        } finally {
            restore_error_handler();
        }
    }

    /**
     * The document's own `parallelScan()` function, eval'd in a namespace
     * whose `RawKvClient` is the fixture stand-in.
     *
     * @return \Closure(RawKvClient, string, int, int): array<array<array{key: string, value: ?string}>>
     */
    private function example(): \Closure
    {
        static $closure = null;

        if ($closure instanceof \Closure) {
            return $closure;
        }

        $function = self::FUNCTION_NAMESPACE . '\\' . self::FUNCTION_NAME;
        $code = 'namespace ' . self::FUNCTION_NAMESPACE . ";\n"
            . $this->renamedSnippet()
            . "\nreturn \\Closure::fromCallable(" . var_export($function, true) . ');';

        $evaluated = eval($code);

        if (!$evaluated instanceof \Closure) {
            self::fail(sprintf(
                'The "Parallel Scanning" example in %s must still declare parallelScan(); '
                . 'the guard renames it to %s before evaluating the block (issue #827)',
                self::DOCUMENT,
                self::FUNCTION_NAME,
            ));
        }

        $closure = $evaluated;

        return $closure;
    }

    private function renamedSnippet(): string
    {
        return str_replace(
            'function parallelScan(',
            'function ' . self::FUNCTION_NAME . '(',
            $this->snippet(),
        );
    }

    /**
     * The first PHP code block of the "Parallel Scanning" section.
     */
    private function snippet(): string
    {
        $path = dirname(__DIR__, 3) . '/' . self::DOCUMENT;
        $contents = file_get_contents($path);
        self::assertNotFalse($contents, "Unable to read {$path}");

        $heading = strpos($contents, self::HEADING);
        self::assertNotFalse($heading, sprintf('%s no longer has the "%s" section', self::DOCUMENT, self::HEADING));

        $rest = substr($contents, $heading);
        $opening = strpos($rest, "```php\n");
        self::assertNotFalse($opening, 'The "Parallel Scanning" section no longer opens a php code block');

        $bodyStart = $opening + strlen("```php\n");
        $closing = strpos($rest, '```', $bodyStart);
        self::assertNotFalse($closing, 'The "Parallel Scanning" code block is not closed');

        return substr($rest, $bodyStart, $closing - $bodyStart);
    }

    /**
     * @param list<array{0: string, 1: string}> $ranges
     * @return list<array{0: string, 1: string}>
     */
    private function rangesContaining(array $ranges, string $key): array
    {
        return array_values(array_filter(
            $ranges,
            static fn (array $range): bool => strcmp($range[0], $key) <= 0 && strcmp($key, $range[1]) < 0,
        ));
    }
}
