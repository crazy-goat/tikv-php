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

        $scanFn = function (string $startKey) use ($allResults): array {
            $startIdx = 0;
            foreach ($allResults as $idx => $r) {
                if ($r['key'] >= $startKey) {
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
