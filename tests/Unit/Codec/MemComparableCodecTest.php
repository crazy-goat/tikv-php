<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Codec;

use CrazyGoat\TiKV\Client\Codec\MemComparableCodec;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class MemComparableCodecTest extends TestCase
{
    private MemComparableCodec $codec;

    protected function setUp(): void
    {
        $this->codec = new MemComparableCodec();
    }

    // ========================================================================
    //  Known-answer vectors (TiDB codec.EncodeBytes/DecodeBytes)
    // ========================================================================

    /**
     * Authoritative vectors from the TiDB `codec.EncodeBytes` tests.
     *
     * @return array<string, array{string, string}> [raw key, encoded hex]
     */
    public static function encodeVectorProvider(): array
    {
        return [
            'empty' => ['', '0000000000000000f7'],
            'three bytes' => ["\x01\x02\x03", '0102030000000000fa'],
            'three bytes + trailing zero' => ["\x01\x02\x03\x00", '0102030000000000fb'],
            'exact multiple of eight' => [
                "\x01\x02\x03\x04\x05\x06\x07\x08",
                // Full group + marker 0xFF, then a trailing all-zero group
                // padded to 8 bytes (16 zeros) + marker 0xF7.
                '0102030405060708ff0000000000000000f7',
            ],
            'split boundary' => ["\x01\x00", '0100000000000000f9'],
            // 'abc' is 3 bytes, so pad = 5 and the marker is 0xFF - 5 = 0xFA.
            // Issue #252 quotes 0xF9, which is the marker of a 2-byte key.
            'abc' => ['abc', '6162630000000000fa'],
            'two bytes' => ['ab', '6162000000000000f9'],
            'hello' => ['hello', '68656c6c6f000000fc'],
            'mrr' => ['mrr', '6d72720000000000fa'],
        ];
    }

    #[DataProvider('encodeVectorProvider')]
    public function testEncodeMatchesKnownVectors(string $key, string $encodedHex): void
    {
        $this->assertSame($encodedHex, bin2hex($this->codec->encode($key)));
    }

    #[DataProvider('encodeVectorProvider')]
    public function testDecodeMatchesKnownVectors(string $key, string $encodedHex): void
    {
        $this->assertSame($key, $this->codec->decode((string) hex2bin($encodedHex)));
    }

    public function testDecodeOfSplitBoundaryIsLossless(): void
    {
        // Regression for GAP-01: splitting at "\x01\x00" yields the boundary
        // 0100000000000000f9. The old escape codec stopped at the first
        // 0x00 0x00 pair and returned "\x01", one byte below the region start,
        // which TiKV rejected as an invalid scan range. Decoding must return
        // the full user key.
        $this->assertSame("\x01\x00", $this->codec->decode((string) hex2bin('0100000000000000f9')));
    }

    // ========================================================================
    //  Round trips
    // ========================================================================

    /**
     * @return array<string, array{string}>
     */
    public static function roundTripProvider(): array
    {
        return [
            'empty' => [''],
            'single byte' => ['a'],
            'ascii' => ['hello'],
            'binary' => ["\x00\x01\x02\xfe\xff"],
            'single zero' => ["\x00"],
            'two zeros' => ["\x00\x00"],
            'trailing zero' => ["\x01\x00"],
            'consecutive zeros' => ["a\x00\x00\x00b"],
            'single ff' => ["\xff"],
            'consecutive ff' => ["\xff\xff\xff"],
            'mixed specials' => ["\x00\xff\x00\xff"],
            'exact group' => [str_repeat("\x00", 8)],
            'exact group of ff' => [str_repeat("\xff", 8)],
            'just over a group' => ["\x00" . str_repeat("\xff", 8)],
            'long zeros' => [str_repeat("\x00", 17)],
            'long ff' => [str_repeat("\xff", 17)],
            'normal mixed' => ["normal-key-with-mixed\x00bytes\xffinside"],
        ];
    }

    #[DataProvider('roundTripProvider')]
    public function testRoundTrip(string $key): void
    {
        $encoded = $this->codec->encode($key);
        $this->assertSame($key, $this->codec->decode($encoded), 'Round-trip failed for key: ' . bin2hex($key));
    }

    // ========================================================================
    //  Order preservation
    // ========================================================================

    public function testEncodedKeysSortCorrectly(): void
    {
        // MCE must preserve byte-wise sort order.
        $keys = ['', 'a', 'b', "a\x00", "a\xff", "b\x00", 'ab', 'abc', 'b'];

        $encoded = array_map($this->codec->encode(...), $keys);
        sort($encoded, SORT_STRING);
        $decoded = array_map($this->codec->decode(...), $encoded);

        $expected = $keys;
        sort($expected, SORT_STRING);

        $this->assertSame($expected, $decoded, 'MCE encoding must preserve sort order');
    }

    public function testEncodedKeysSortCorrectlyAcrossGroupBoundary(): void
    {
        $keys = [
            str_repeat("\x00", 7),
            str_repeat("\x00", 8),
            str_repeat("\x00", 9),
            str_repeat("\x00", 7) . "\xff",
            str_repeat("\xff", 8),
            str_repeat("\xff", 8) . "\x00",
            str_repeat('k', 16),
            str_repeat('k', 16) . 'k',
            '10',
            '9',
        ];

        foreach ($keys as $a) {
            foreach ($keys as $b) {
                $this->assertSame(
                    strcmp($a, $b) <=> 0,
                    strcmp($this->codec->encode($a), $this->codec->encode($b)) <=> 0,
                    bin2hex($a) . ' vs ' . bin2hex($b),
                );
            }
        }
    }

    public function testRandomBinaryKeysRoundTripAndKeepSortOrder(): void
    {
        $rng = new \Random\Randomizer(new \Random\Engine\Mt19937(252));
        $keys = [];
        for ($i = 0; $i < 400; ++$i) {
            $len = $rng->getInt(0, 20);
            $key = '';
            for ($j = 0; $j < $len; ++$j) {
                // Bias toward the bytes that matter for padding and markers.
                $key .= \chr($rng->getInt(0, 3) === 0 ? [0x00, 0xFF][$rng->getInt(0, 1)] : $rng->getInt(0, 255));
            }
            $keys[] = $key;
        }

        foreach ($keys as $key) {
            $this->assertSame($key, $this->codec->decode($this->codec->encode($key)), bin2hex($key));
        }

        $sample = \array_slice($keys, 0, 150);
        foreach ($sample as $a) {
            foreach ($sample as $b) {
                $this->assertSame(
                    strcmp($a, $b) <=> 0,
                    strcmp($this->codec->encode($a), $this->codec->encode($b)) <=> 0,
                    bin2hex($a) . ' vs ' . bin2hex($b),
                );
            }
        }
    }

    // ========================================================================
    //  Malformed input
    // ========================================================================

    public function testDecodeThrowsOnEmptyInput(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('missing its terminating group');
        $this->codec->decode('');
    }

    public function testDecodeThrowsOnTruncatedGroup(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Truncated');
        $this->codec->decode("\x01\x02\x03");
    }

    public function testDecodeThrowsWhenFullGroupsNeverTerminate(): void
    {
        // A single full group (marker 0xFF) with no terminating group after it.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('missing its terminating group');
        $this->codec->decode((string) hex2bin('0102030405060708ff'));
    }

    public function testDecodeThrowsOnInvalidMarker(): void
    {
        // Marker 0xF6 implies pad = 0xFF - 0xF6 = 9 > 8.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid MCE marker');
        $this->codec->decode(str_repeat("\x00", 8) . "\xf6");
    }

    public function testDecodeThrowsOnMarkerBelowValidRange(): void
    {
        // Marker 0x00 implies pad = 255.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid MCE marker');
        $this->codec->decode(str_repeat("\x01", 8) . "\x00");
    }

    public function testDecodeThrowsOnNonZeroPadding(): void
    {
        // Marker 0xFB implies pad = 4, but the last four bytes are not zero.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Non-zero MCE padding');
        $this->codec->decode("ABCD\x01\x02\x03\x04\xfb");
    }
}
