<?php

/**
 * Benchmark for issue #302 (PERF-15): the write-set merge of a transactional
 * scan.
 *
 * Run from the repository root with:
 *   php benchmarks/TxnScanMergeBenchmark.php
 *
 * What is measured
 * ----------------
 *
 * `TxnReader::finalizeScanResults()` merges the rows TiKV returned with the
 * transaction's own in-range writes. The `before` column reproduces the shape
 * shipped before this issue: the scanned keys are copied, the write-set keys
 * are appended, `array_unique()` de-duplicates the combined list and a full
 * `sort(SORT_STRING)` orders it again. The `after` column calls the shipped
 * `finalizeScanResults()` (via reflection: it is private) and therefore
 * measures the real code: only the write-set keys are sorted, and each one is
 * placed into the already-sorted scanned rows with a binary search, the
 * untouched runs being copied as array slices.
 *
 * The issue suggested a single linear merge pass instead. That was measured
 * too and is *slower* than the code it replaces (about 13 ms versus 7 ms for
 * the data set below): `array_unique()` and `sort()` are C, while a merge that
 * compares one row at a time in PHP pays a userland call and an array append
 * per row. The shipped shape keeps the per-row work in C and only the
 * write-set keys — at most a fraction of `MAX_SCAN_LIMIT` — are walked in PHP.
 *
 * The data set is the issue's worst case — `ROW_COUNT` scanned rows in
 * ascending byte order, as TiKV returns them, plus `WRITE_KEY_COUNT`
 * write-set keys inserted in reverse order, so the write-set sort cannot be
 * skipped. Both columns must produce identical output; the script says so
 * before printing the timings.
 *
 * The numbers are machine-dependent on purpose (never assert on them in the
 * test suite). The improvement is a constant factor on a bounded amount of
 * work: the transactional scan is capped at `MAX_SCAN_LIMIT` (10240) rows, so
 * this saves a few milliseconds per large scan and does not change how the
 * cost scales.
 */

declare(strict_types=1);

use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use CrazyGoat\TiKV\Client\Util\KeyOrder;

require dirname(__DIR__) . '/vendor/autoload.php';

/** Rows TiKV returns: the transactional scan cap (`MAX_SCAN_LIMIT`). */
const ROW_COUNT = 10240;

/** Keys the transaction has written, interleaved with the scanned rows. */
const WRITE_KEY_COUNT = 100;

/** Timed runs per column (the first run is a warm-up and is discarded). */
const ITERATIONS = 100;

/**
 * Build the scanned rows in ascending byte order, exactly as TiKV returns
 * them: pages continue from the previous region boundary and regions are
 * clipped in ascending order.
 *
 * @return list<array{key: string, value: ?string}>
 */
function scannedRows(): array
{
    $rows = [];
    for ($i = 0; $i < ROW_COUNT; $i++) {
        $rows[] = ['key' => sprintf('key%06d', $i), 'value' => 'value-' . $i];
    }

    return $rows;
}

/**
 * Build the write-set keys in the transaction's write order. Four out of five
 * override a scanned row and the rest are keys TiKV has not returned yet; the
 * list is built backwards, which is the worst case for the write-set sort.
 *
 * @return list<string>
 */
function writeKeys(): array
{
    $keys = [];
    for ($i = WRITE_KEY_COUNT - 1; $i >= 0; $i--) {
        $row = intdiv($i * ROW_COUNT, WRITE_KEY_COUNT);
        $keys[] = $i % 5 === 0
            ? sprintf('zz-new-%06d', $i)
            : sprintf('key%06d', $row);
    }

    return $keys;
}

/**
 * The pre-#302 merge shape: de-duplicate the combined key list with
 * `array_unique()` and sort all of it again.
 *
 * @param list<array{key: string, value: ?string}> $rows
 * @param list<string> $writeKeys in write order
 * @param array<string, ?string> $writeSet
 * @return list<array{key: string, value: ?string}>
 */
function mergeBefore(array $rows, array $writeKeys, array $writeSet, ?int $limit): array
{
    $tikvMap = [];
    foreach ($rows as $entry) {
        $tikvMap[$entry['key']] = $entry['value'];
    }

    $allKeys = array_map(strval(...), array_keys($tikvMap));
    foreach ($writeKeys as $key) {
        if (KeyOrder::inRange($key, '', '')) {
            $allKeys[] = $key;
        }
    }
    $allKeys = array_unique($allKeys);
    sort($allKeys, SORT_STRING);

    $merged = [];
    foreach ($allKeys as $key) {
        if ($limit !== null && count($merged) >= $limit) {
            break;
        }

        if (array_key_exists($key, $writeSet)) {
            $writeValue = $writeSet[$key];
            if ($writeValue !== null) {
                $merged[] = ['key' => $key, 'value' => $writeValue];
            }
        } elseif (array_key_exists($key, $tikvMap)) {
            $merged[] = ['key' => $key, 'value' => $tikvMap[$key]];
        }
    }

    return $merged;
}

/**
 * Time one column and return the total milliseconds for `ITERATIONS` runs.
 *
 * @param callable(): list<array{key: string, value: ?string}> $merge
 */
function timeMerge(callable $merge): float
{
    // Warm-up: the first call pays for the JIT/opcache shape of this path.
    $merge();

    $start = hrtime(true);
    for ($i = 0; $i < ITERATIONS; $i++) {
        $merge();
    }

    return (hrtime(true) - $start) / 1_000_000;
}

$rows = scannedRows();
$writeKeyList = writeKeys();

$writeSet = [];
foreach ($writeKeyList as $key) {
    $writeSet[$key] = 'local-' . $key;
}

$state = new TransactionState();
foreach ($writeKeyList as $key) {
    $state->setWrite($key, 'local-' . $key);
}

$reader = (new ReflectionClass(TxnReader::class))->newInstanceWithoutConstructor();
$finalize = new ReflectionMethod(TxnReader::class, 'finalizeScanResults');
/** @var callable(): list<array{key: string, value: ?string}> $mergeAfter */
$mergeAfter = static fn(): array => $finalize->invoke($reader, $rows, '', '', null, $state);

$before = mergeBefore($rows, $writeKeyList, $writeSet, null);
$after = $mergeAfter();

$beforeMs = timeMerge(static fn(): array => mergeBefore($rows, $writeKeyList, $writeSet, null));
$afterMs = timeMerge($mergeAfter);

printf("finalizeScanResults(): %d TiKV rows, %d write-set keys, %d runs\n", ROW_COUNT, WRITE_KEY_COUNT, ITERATIONS);
printf("  output rows:      %d (before) / %d (after)\n", count($before), count($after));
printf("  identical output: %s\n", $before === $after ? 'yes' : 'NO — the comparison is invalid');
printf("  before:           %8.2f ms total, %7.1f us/scan (array_unique + sort)\n", $beforeMs, $beforeMs * 1000 / ITERATIONS);
printf("  after:            %8.2f ms total, %7.1f us/scan (sort write keys + merge)\n", $afterMs, $afterMs * 1000 / ITERATIONS);
printf("  speed-up:         %8.2fx\n", $beforeMs / $afterMs);
