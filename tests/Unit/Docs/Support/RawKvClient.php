<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs\Support;

/**
 * Stand-in for `RawKvClient` used to execute the documented `parallelScan()`
 * example (issue #827).
 *
 * `ParallelScanDocumentationGuardTest` evaluates the example's own source, so
 * the type the example declares has to resolve. This fixture records the
 * ranges it is given instead of sending them to TiKV.
 */
final class RawKvClient
{
    /** @var list<array{0: string, 1: string}> */
    public array $ranges = [];

    public int $eachLimit = 0;

    /**
     * @param list<array{0: string, 1: string}> $ranges
     * @return list<array<array{key: string, value: ?string}>>
     */
    public function batchScan(array $ranges, int $eachLimit): array
    {
        $this->ranges = $ranges;
        $this->eachLimit = $eachLimit;

        return array_fill(0, count($ranges), []);
    }
}
