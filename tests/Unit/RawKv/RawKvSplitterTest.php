<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

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
}
