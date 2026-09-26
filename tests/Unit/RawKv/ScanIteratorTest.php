<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use PHPUnit\Framework\TestCase;

class ScanIteratorTest extends TestCase
{
    // ========================================================================
    // Basic iteration
    // ========================================================================

    public function testIteratesOverSingleBatch(): void
    {
        $scanFn = fn(string $startKey, string $endKey, int $limit): array => [
            ['key' => 'k1', 'value' => 'v1'],
            ['key' => 'k2', 'value' => 'v2'],
            ['key' => 'k3', 'value' => 'v3'],
        ];

        $iterator = new ScanIterator($scanFn, 'a', 'z', 10);
        $results = iterator_to_array($iterator);

        $this->assertSame(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3'], $results);
    }

    public function testIteratesOverMultipleBatches(): void
    {
        $callCount = 0;
        $scanFn = function (string $startKey) use (&$callCount): array {
            $callCount++;
            return match ($startKey) {
                'a' => [
                    ['key' => 'k1', 'value' => 'v1'],
                    ['key' => 'k2', 'value' => 'v2'],
                ],
                "k2\x00" => [
                    ['key' => 'k3', 'value' => 'v3'],
                    ['key' => 'k4', 'value' => 'v4'],
                ],
                "k4\x00" => [
                    ['key' => 'k5', 'value' => 'v5'],
                ],
                default => [],
            };
        };

        $iterator = new ScanIterator($scanFn, 'a', 'z', 2);
        $results = iterator_to_array($iterator);

        $this->assertSame([
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
            'k4' => 'v4',
            'k5' => 'v5',
        ], $results);
        $this->assertSame(3, $callCount);
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    public function testEmptyRangeIsExhausted(): void
    {
        $scanFn = fn(): array => [];

        $iterator = new ScanIterator($scanFn, 'a', 'z', 10);
        $results = iterator_to_array($iterator);

        $this->assertSame([], $results);
    }

    public function testExactBatchSizeTriggersNextFetch(): void
    {
        $callCount = 0;
        $scanFn = function (string $startKey) use (&$callCount): array {
            $callCount++;
            if ($startKey === 'a') {
                return [
                    ['key' => 'k1', 'value' => 'v1'],
                    ['key' => 'k2', 'value' => 'v2'],
                ];
            }

            return [];
        };

        $iterator = new ScanIterator($scanFn, 'a', 'z', 2);
        $results = iterator_to_array($iterator);

        $this->assertSame(['k1' => 'v1', 'k2' => 'v2'], $results);
        $this->assertSame(2, $callCount);
    }

    public function testKeyOnlyReturnsNullValues(): void
    {
        $scanFn = fn(string $startKey, string $endKey, int $limit, bool $keyOnly): array => [
            ['key' => 'k1', 'value' => null],
            ['key' => 'k2', 'value' => null],
        ];

        $iterator = new ScanIterator($scanFn, 'a', 'z', 10, true);
        $results = iterator_to_array($iterator);

        $this->assertSame(['k1' => null, 'k2' => null], $results);
    }

    public function testForeachWithBreakDoesNotFetchExtraBatches(): void
    {
        $callCount = 0;
        $scanFn = function () use (&$callCount): array {
            $callCount++;
            return [
                ['key' => 'k1', 'value' => 'v1'],
                ['key' => 'k2', 'value' => 'v2'],
            ];
        };

        $iterator = new ScanIterator($scanFn, 'a', 'z', 2);
        $count = 0;
        foreach ($iterator as $value) {
            $count++;
            break;
        }

        $this->assertSame(1, $count);
        $this->assertSame(1, $callCount);
    }

    public function testRewindResetsToBeginning(): void
    {
        $callCount = 0;
        $scanFn = function () use (&$callCount): array {
            $callCount++;
            return [
                ['key' => 'k1', 'value' => 'v1'],
                ['key' => 'k2', 'value' => 'v2'],
            ];
        };

        $iterator = new ScanIterator($scanFn, 'a', 'z', 10);

        $first = iterator_to_array($iterator);
        $this->assertSame(['k1' => 'v1', 'k2' => 'v2'], $first);
        $this->assertSame(1, $callCount);

        $iterator->rewind();
        $second = iterator_to_array($iterator);
        $this->assertSame(['k1' => 'v1', 'k2' => 'v2'], $second);
        $this->assertSame(2, $callCount);
    }

    public function testIterationWithNoUpperBound(): void
    {
        $callCount = 0;
        $scanFn = function (string $startKey, string $endKey) use (&$callCount): array {
            $this->assertSame('', $endKey);
            $callCount++;

            return match ($startKey) {
                'a' => [
                    ['key' => 'k1', 'value' => 'v1'],
                ],
                "k1\x00" => [
                    ['key' => 'k2', 'value' => 'v2'],
                ],
                default => [],
            };
        };

        $iterator = new ScanIterator($scanFn, 'a', '', 1);
        $results = iterator_to_array($iterator);

        $this->assertSame(['k1' => 'v1', 'k2' => 'v2'], $results);
        $this->assertSame(3, $callCount);
    }

    public function testStartKeyPastEndKeyIsImmediatelyExhausted(): void
    {
        $scanFn = function (): never {
            $this->fail('scan should not be called');
        };

        $iterator = new ScanIterator($scanFn, 'z', 'a', 10);

        $this->assertFalse($iterator->valid());
    }

    // ========================================================================
    // Numeric-string bounds (issue #261): currentStartKey and endKey are both
    // decimal strings, so PHP's own `>=` compares them as NUMBERS while TiKV
    // (and this iterator) orders keys bytewise.
    // ========================================================================

    public function testNumericRangeThatIsOnlyValidBytewiseIsStillScanned(): void
    {
        // ['20', '3') is a valid bytewise range — '2' = 0x32 < '3' = 0x33 —
        // that PHP reads as 20 >= 3, i.e. empty. A numeric termination check
        // exhausts the iterator before the first page is requested, so a scan
        // over this range returns nothing at all (silent data loss).
        $scanFn = fn(): array => [
            ['key' => '20', 'value' => 'v20'],
            ['key' => '25', 'value' => 'v25'],
        ];

        $iterator = new ScanIterator($scanFn, '20', '3', 10);

        $this->assertSame(['20' => 'v20', '25' => 'v25'], iterator_to_array($iterator));
    }

    public function testNumericRangeThatIsOnlyEmptyBytewiseTerminatesWithoutScanning(): void
    {
        // The mirror image: ['9', '11') is empty bytewise ('9' = 0x39 >
        // '1' = 0x31) but non-empty numerically (9 < 11). A numeric
        // termination check therefore issues the scan, and the caller gets
        // rows from outside the range it asked for — and a byte-order
        // consumer that paginates with the same assumption never stops.
        $scanFn = function (): never {
            $this->fail('scan must not be called for a bytewise-empty range');
        };

        $iterator = new ScanIterator($scanFn, '9', '11', 10);

        $this->assertFalse($iterator->valid());
        $this->assertSame([], iterator_to_array($iterator));
    }

    public function testNumericPaginationStopsAtTheByteOrderEndKey(): void
    {
        // Two pages over ['20', '3'). This case is NOT redundant with the two
        // above: it discriminates on the *initial* termination check, which the
        // first case also exercises, and it additionally pins the two-page
        // pagination shape (2 calls, 3 rows, short final page).
        //
        // Measured, both halves: a numeric termination check makes this test
        // fail with `[]` instead of the three rows, because
        // `KeyOrder::gte('20', '3')` must be false ('2' = 0x32 < '3' = 0x33)
        // while `'20' >= '3'` is numerically true. What it does NOT
        // discriminate is the *continuation* cursor: KeyOrder::successor()
        // appends the NUL byte, so the cursor is "25\x00" / "2z\x00" and PHP's
        // `>=` falls back to a byte comparison (a non-numeric string is never
        // compared numerically), agreeing with strcmp() on both. The second
        // page is therefore requested because the cursor sorts before '3', and
        // the short final page is what ends the iteration here.
        $calls = 0;
        $scanFn = function (string $startKey) use (&$calls): array {
            ++$calls;

            return match ($startKey) {
                '20' => [
                    ['key' => '20', 'value' => 'v20'],
                    ['key' => '25', 'value' => 'v25'],
                ],
                "25\x00" => [
                    ['key' => '2z', 'value' => 'v2z'],
                ],
                default => [],
            };
        };

        $iterator = new ScanIterator($scanFn, '20', '3', 2);

        $this->assertSame(
            ['20' => 'v20', '25' => 'v25', '2z' => 'v2z'],
            iterator_to_array($iterator),
        );
        $this->assertSame(2, $calls);
    }

    // ========================================================================
    // Validation
    // ========================================================================

    public function testBatchSizeMustBePositive(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize must be greater than 0');

        new ScanIterator(fn(): array => [], 'a', 'z', 0);
    }

    public function testBatchSizeCannotExceedMaxScanLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize (10241) exceeds maximum allowed scan limit of 10240');

        new ScanIterator(fn(): array => [], 'a', 'z', RawKvClient::MAX_SCAN_LIMIT + 1);
    }

    // ========================================================================
    // Correctness against scan-like data
    // ========================================================================

    public function testIteratorYieldsSameResultsAsScan(): void
    {
        $allResults = [];
        for ($i = 0; $i < 100; $i++) {
            $allResults[] = ['key' => sprintf('k-%03d', $i), 'value' => sprintf('v-%03d', $i)];
        }

        // The fake server answers "first key at or after $startKey", exactly
        // what a RawScan does — and it must answer it bytewise like TiKV. A
        // `>=` here is the same latent bug the E2E suite had in its `sort()`:
        // with 'k-000'..'k-099' the two orders agree, so the double would only
        // start paging from the wrong key once the keys became numeric strings
        // ('20' < '100' numerically, '100' < '20' bytewise), and the test would
        // then compare the iterator against a wrong expectation (issue #180).
        $scanFn = function (string $startKey) use ($allResults): array {
            $startIdx = 0;
            foreach ($allResults as $idx => $r) {
                if (strcmp($r['key'], $startKey) >= 0) {
                    $startIdx = $idx;
                    break;
                }
            }

            return array_slice($allResults, $startIdx, 30);
        };

        $iterator = new ScanIterator($scanFn, 'k-000', 'k-100', 30);
        $iteratorResults = iterator_to_array($iterator);

        $expected = [];
        foreach ($allResults as $r) {
            $expected[$r['key']] = $r['value'];
        }

        $this->assertSame($expected, $iteratorResults);
    }
}
