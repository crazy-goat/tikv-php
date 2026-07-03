<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Codec;

use CrazyGoat\TiKV\Client\Codec\MemComparableCodec;
use PHPUnit\Framework\TestCase;

class MemComparableCodecTest extends TestCase
{
    private MemComparableCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new MemComparableCodec();
    }

    public function testEncodeEmptyKey(): void
    {
        $this->assertSame("\x00\x00", $this->codec->encode(''));
    }

    public function testDecodeEmptyKey(): void
    {
        $this->assertSame('', $this->codec->decode("\x00\x00"));
    }

    public function testEncodeSimpleKey(): void
    {
        $this->assertSame("hello\x00\x00", $this->codec->encode('hello'));
    }

    public function testEncodeKeyWithNullByte(): void
    {
        // 0x00 → 0x00 0xFF
        $this->assertSame("a\x00\xFFb\x00\x00", $this->codec->encode("a\x00b"));
    }

    public function testEncodeKeyWithFFByte(): void
    {
        // 0xFF → 0xFF 0x00
        $this->assertSame("a\xFF\x00b\x00\x00", $this->codec->encode("a\xFFb"));
    }

    public function testEncodeKeyWithMixedSpecialBytes(): void
    {
        // 0x00 → 0x00 0xFF, 0xFF → 0xFF 0x00
        $key = "\x00\xFF\x00\xFF";
        $expected = "\x00\xFF\xFF\x00\x00\xFF\xFF\x00\x00\x00";
        $this->assertSame($expected, $this->codec->encode($key));
    }

    public function testDecodeSimpleKey(): void
    {
        $this->assertSame('hello', $this->codec->decode("hello\x00\x00"));
    }

    public function testDecodeKeyWithNullByte(): void
    {
        $this->assertSame("a\x00b", $this->codec->decode("a\x00\xFFb\x00\x00"));
    }

    public function testDecodeKeyWithFFByte(): void
    {
        $this->assertSame("a\xFFb", $this->codec->decode("a\xFF\x00b\x00\x00"));
    }

    public function testRoundTrip(): void
    {
        $keys = [
            '',
            'a',
            'hello',
            "a\x00b",
            "a\xFFb",
            "\x00\xFF\x00\xFF",
            str_repeat("\x00", 10),
            str_repeat("\xFF", 10),
            "normal-key-with-mixed\x00bytes\xFFinside",
        ];

        foreach ($keys as $key) {
            $encoded = $this->codec->encode($key);
            $decoded = $this->codec->decode($encoded);
            $this->assertSame($key, $decoded, "Round-trip failed for key: " . bin2hex($key));
        }
    }

    public function testEncodedKeysSortCorrectly(): void
    {
        // MCE should preserve byte-wise sort order
        $keys = ['', 'a', 'b', "a\x00", "a\xFF", "b\x00", 'ab', 'abc', 'b'];

        $encoded = array_map($this->codec->encode(...), $keys);
        sort($encoded);
        $decoded = array_map($this->codec->decode(...), $encoded);

        $expected = $keys;
        sort($expected);

        $this->assertSame($expected, $decoded, 'MCE encoding must preserve sort order');
    }

    public function testDecodeThrowsOnMissingTerminator(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('terminator');
        $this->codec->decode('hello');
    }

    public function testDecodeThrowsOnTruncatedEscapeSequence(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unexpected end');
        $this->codec->decode("\x00");
    }

    public function testDecodeThrowsOnTruncatedAfterEscape(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->codec->decode("\x00\xFF\x00");
    }

    public function testDecodeWithDataAfterTerminator(): void
    {
        // Data after the terminator should be ignored
        $this->assertSame('abc', $this->codec->decode("abc\x00\x00extra-data"));
    }

    public function testEncodePreservesRegularBytes(): void
    {
        $regular = 'abcdefghijklmnopqrstuvwxyz0123456789';
        $encoded = $this->codec->encode($regular);
        $this->assertStringStartsWith($regular, $encoded);
        $this->assertStringEndsWith("\x00\x00", $encoded);
    }
}
