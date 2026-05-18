<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\TiKV\Client\RawKv\RawKvSplitter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class RawKvSplitterTest extends TestCase
{
    // ========================================================================
    // splitIntoBatches
    // ========================================================================

    public function testSplitIntoBatchesEmpty(): void
    {
        $this->assertSame([], RawKvSplitter::splitIntoBatches([], 10, 100, strlen(...)));
    }

    public function testSplitIntoBatchesSingleItem(): void
    {
        $result = RawKvSplitter::splitIntoBatches(['a'], 10, 100, strlen(...));
        $this->assertSame([['a']], $result);
    }

    public function testSplitIntoBatchesExactCount(): void
    {
        $items = ['a', 'b', 'c'];
        $result = RawKvSplitter::splitIntoBatches($items, 3, 100, strlen(...));
        $this->assertSame([['a', 'b', 'c']], $result);
    }

    public function testSplitIntoBatchesExceedsCount(): void
    {
        $items = ['a', 'b', 'c', 'd'];
        $result = RawKvSplitter::splitIntoBatches($items, 2, 100, strlen(...));
        $this->assertSame([['a', 'b'], ['c', 'd']], $result);
    }

    public function testSplitIntoBatchesExceedsBytes(): void
    {
        $items = ['aaa', 'bbb', 'ccc'];
        $result = RawKvSplitter::splitIntoBatches($items, 10, 5, strlen(...));
        $this->assertSame([['aaa'], ['bbb'], ['ccc']], $result);
    }

    public function testSplitIntoBatchesSingleOversizedItem(): void
    {
        $items = ['toolong'];
        $result = RawKvSplitter::splitIntoBatches($items, 10, 3, strlen(...));
        $this->assertSame([['toolong']], $result);
    }

    // ========================================================================
    // splitPairsIntoBatches
    // ========================================================================

    public function testSplitPairsIntoBatchesEmpty(): void
    {
        $this->assertSame([], RawKvSplitter::splitPairsIntoBatches([], 10, 100));
    }

    public function testSplitPairsIntoBatchesEmptyNoop(): void
    {
        $result = RawKvSplitter::splitPairsIntoBatches([], 10, 100, []);
        $this->assertSame([], $result);
    }

    public function testSplitPairsIntoBatchesSinglePair(): void
    {
        $pair = $this->createKvPair('key1', 'value1');
        $result = RawKvSplitter::splitPairsIntoBatches([$pair], 10, 100);
        $this->assertCount(1, $result);
        $this->assertCount(1, $result[0]['pairs']);
        $this->assertSame('key1', $result[0]['pairs'][0]->getKey());
        $this->assertSame([], $result[0]['ttls']);
    }

    public function testSplitPairsIntoBatchesExceedsCount(): void
    {
        $pairs = [
            $this->createKvPair('k1', 'v1'),
            $this->createKvPair('k2', 'v2'),
            $this->createKvPair('k3', 'v3'),
        ];
        $result = RawKvSplitter::splitPairsIntoBatches($pairs, 2, 100);
        $this->assertCount(2, $result);
        $this->assertCount(2, $result[0]['pairs']);
        $this->assertCount(1, $result[1]['pairs']);
    }

    public function testSplitPairsIntoBatchesExceedsBytes(): void
    {
        $pairs = [
            $this->createKvPair('longkey1', 'longvalue1'),
            $this->createKvPair('longkey2', 'longvalue2'),
        ];
        $result = RawKvSplitter::splitPairsIntoBatches($pairs, 10, 10, []);
        $this->assertCount(2, $result);
    }

    public function testSplitPairsIntoBatchesWithTtls(): void
    {
        $pairs = [
            $this->createKvPair('k1', 'v1'),
            $this->createKvPair('k2', 'v2'),
        ];
        $result = RawKvSplitter::splitPairsIntoBatches($pairs, 10, 100, [60, 120]);
        $this->assertCount(1, $result);
        $this->assertSame([60, 120], $result[0]['ttls']);
    }

    public function testSplitPairsIntoBatchesTtlsTrackAcrossBatches(): void
    {
        $pairs = [
            $this->createKvPair('k1', 'v1'),
            $this->createKvPair('k2', 'v2'),
            $this->createKvPair('k3', 'v3'),
            $this->createKvPair('k4', 'v4'),
        ];
        $result = RawKvSplitter::splitPairsIntoBatches($pairs, 2, 100, [10, 20, 30, 40]);
        $this->assertCount(2, $result);
        $this->assertSame([10, 20], $result[0]['ttls']);
        $this->assertSame([30, 40], $result[1]['ttls']);
    }

    public function testSplitPairsIntoBatchesWithPartialTtlsEmitsWarning(): void
    {
        $pairs = [
            $this->createKvPair('k1', 'v1'),
            $this->createKvPair('k2', 'v2'),
            $this->createKvPair('k3', 'v3'),
        ];
        // TTL array shorter than pairs — triggers undefined array key at RawKvSplitter:72
        // PHP emits a warning but continues with null values for missing indices
        $result = RawKvSplitter::splitPairsIntoBatches($pairs, 10, 100, [60]);
        $this->assertCount(1, $result);
        $this->assertCount(3, $result[0]['pairs']);
        $this->assertCount(3, $result[0]['ttls']);
        $this->assertSame(60, $result[0]['ttls'][0]);
    }

    // ========================================================================
    // calculatePrefixEndKey
    // ========================================================================

    #[DataProvider('prefixEndKeyProvider')]
    public function testCalculatePrefixEndKey(string $prefix, string $expected): void
    {
        $this->assertSame($expected, RawKvSplitter::calculatePrefixEndKey($prefix));
    }

    /** @return array<string, array{string, string}> */
    public static function prefixEndKeyProvider(): array
    {
        return [
            'empty prefix' => ['', ''],
            'single byte a' => ['a', 'b'],
            'single byte z' => ['z', '{'],
            'multiple bytes' => ['abc', 'abd'],
            'last byte ff' => ["a\xff", 'b'],
            'all ff' => ["\xff\xff\xff\xff", ''],
            'multiple ff at end' => ["abc\xff\xff", 'abd'],
            'leading ff' => ["\xff\xffa", "\xff\xffb"],
        ];
    }

    private function createKvPair(string $key, string $value): KvPair
    {
        $pair = new KvPair();
        $pair->setKey($key);
        $pair->setValue($value);
        return $pair;
    }
}
