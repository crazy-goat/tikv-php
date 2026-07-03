<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Codec;

use CrazyGoat\Proto\Kvrpcpb\APIVersion;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\MemComparableCodec;
use CrazyGoat\TiKV\Client\Codec\Mode;
use PHPUnit\Framework\TestCase;

class CodecV1Test extends TestCase
{
    // ========================================================================
    //  RawKV mode (default, no MCE for region keys)
    // ========================================================================

    public function testRawKvEncodeKeyIsPassthrough(): void
    {
        $codec = new CodecV1(); // default: no mode = RawKV
        $this->assertSame('hello', $codec->encodeKey('hello'));
        $this->assertSame("", $codec->encodeKey(""));
        $this->assertSame("\x00\xFF", $codec->encodeKey("\x00\xFF"));
    }

    public function testRawKvDecodeKeyIsPassthrough(): void
    {
        $codec = new CodecV1();
        $this->assertSame('hello', $codec->decodeKey('hello'));
        $this->assertSame("", $codec->decodeKey(""));
    }

    public function testRawKvEncodeRegionKeyIsPassthrough(): void
    {
        $codec = new CodecV1(); // default: RawKV mode
        $this->assertSame('hello', $codec->encodeRegionKey('hello'));
    }

    public function testRawKvDecodeRegionKeyIsPassthrough(): void
    {
        $codec = new CodecV1();
        $this->assertSame('hello', $codec->decodeRegionKey('hello'));
    }

    public function testRawKvEncodeRange(): void
    {
        $codec = new CodecV1();
        [$start, $end] = $codec->encodeRange('a', 'z');
        $this->assertSame('a', $start);
        $this->assertSame('z', $end);
    }

    public function testRawKvEncodeRangeWithEmptyEnd(): void
    {
        $codec = new CodecV1();
        [$start, $end] = $codec->encodeRange('a', '');
        $this->assertSame('a', $start);
        $this->assertSame('', $end); // empty end key preserved as unbounded
    }

    public function testRawKvDecodeRange(): void
    {
        $codec = new CodecV1();
        [$start, $end] = $codec->decodeRange('a', 'z');
        $this->assertSame('a', $start);
        $this->assertSame('z', $end);
    }

    public function testRawKvDecodeRangeWithEmptyEnd(): void
    {
        $codec = new CodecV1();
        [$start, $end] = $codec->decodeRange('a', '');
        $this->assertSame('a', $start);
        $this->assertSame('', $end);
    }

    // ========================================================================
    //  TxnKV mode (MCE applied to region keys)
    // ========================================================================

    public function testTxnKvEncodeKeyIsPassthrough(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $this->assertSame('hello', $codec->encodeKey('hello'));
    }

    public function testTxnKvDecodeKeyIsPassthrough(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $this->assertSame('hello', $codec->decodeKey('hello'));
    }

    public function testTxnKvEncodeRegionKeyUsesMce(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $encoded = $codec->encodeRegionKey('hello');
        $this->assertSame("hello\x00\x00", $encoded);
    }

    public function testTxnKvDecodeRegionKeyUsesMce(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $decoded = $codec->decodeRegionKey("hello\x00\x00");
        $this->assertSame('hello', $decoded);
    }

    public function testTxnKvEncodeRange(): void
    {
        $codec = new CodecV1(Mode::Txn);
        [$start, $end] = $codec->encodeRange('a', 'z');
        $this->assertSame("a\x00\x00", $start);
        $this->assertSame("z\x00\x00", $end);
    }

    public function testTxnKvEncodeRangeWithEmptyEnd(): void
    {
        $codec = new CodecV1(Mode::Txn);
        [$start, $end] = $codec->encodeRange('a', '');
        $this->assertSame("a\x00\x00", $start);
        $this->assertSame('', $end); // empty end key preserved as unbounded
    }

    public function testTxnKvDecodeRange(): void
    {
        $codec = new CodecV1(Mode::Txn);
        [$start, $end] = $codec->decodeRange("a\x00\x00", "z\x00\x00");
        $this->assertSame('a', $start);
        $this->assertSame('z', $end);
    }

    public function testTxnKvDecodeRangeWithEmptyEnd(): void
    {
        $codec = new CodecV1(Mode::Txn);
        [$start, $end] = $codec->decodeRange("a\x00\x00", '');
        $this->assertSame('a', $start);
        $this->assertSame('', $end);
    }

    // ========================================================================
    //  API version / keyspace metadata
    // ========================================================================

    public function testApiVersion(): void
    {
        $codec = new CodecV1();
        $this->assertSame(APIVersion::V1, $codec->getApiVersion());
        $this->assertSame(0, $codec->getApiVersion());
    }

    public function testKeyspaceId(): void
    {
        $codec = new CodecV1();
        $this->assertSame(0, $codec->getKeyspaceId());
    }

    public function testKeyspaceName(): void
    {
        $codec = new CodecV1();
        $this->assertSame('', $codec->getKeyspaceName());
    }

    // ========================================================================
    //  Edge cases
    // ========================================================================

    public function testEmptyKeysInRawMode(): void
    {
        $codec = new CodecV1();
        $this->assertSame('', $codec->encodeKey(''));
        $this->assertSame('', $codec->decodeKey(''));
        $this->assertSame('', $codec->encodeRegionKey(''));
        $this->assertSame('', $codec->decodeRegionKey(''));
    }

    public function testEmptyKeysInTxnMode(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $this->assertSame('', $codec->encodeKey(''));
        $this->assertSame('', $codec->decodeKey(''));
        // Empty region key in Txn mode gets MCE-encoded to just the terminator
        $this->assertSame("\x00\x00", $codec->encodeRegionKey(''));
        $this->assertSame('', $codec->decodeRegionKey("\x00\x00"));
    }

    public function testBinaryKeysPreservedInRawMode(): void
    {
        $codec = new CodecV1();
        $binary = "\x00\x01\x02\xFE\xFF";
        $this->assertSame($binary, $codec->encodeKey($binary));
        $this->assertSame($binary, $codec->decodeKey($binary));
        $this->assertSame($binary, $codec->encodeRegionKey($binary));
        $this->assertSame($binary, $codec->decodeRegionKey($binary));
    }

    public function testCustomMemComparableCodec(): void
    {
        $custom = new MemComparableCodec();
        $codec = new CodecV1(Mode::Txn, $custom);
        $this->assertSame("hello\x00\x00", $codec->encodeRegionKey('hello'));
    }

    public function testRoundTripMixedModes(): void
    {
        $keys = ['', 'a', 'hello-world', "\x00\xFF\x00\xFF", 'key-with-binary'];

        foreach ([null, Mode::Raw, Mode::Txn] as $mode) {
            $codec = new CodecV1($mode);
            foreach ($keys as $key) {
                // User key roundtrip
                $this->assertSame($key, $codec->decodeKey($codec->encodeKey($key)));

                // Region key roundtrip
                $encodedRegion = $codec->encodeRegionKey($key);
                $decodedRegion = $codec->decodeRegionKey($encodedRegion);
                $this->assertSame($key, $decodedRegion);

                // Range roundtrip
                $endKey = $key === '' ? 'z' : '';
                [$es, $ee] = $codec->encodeRange($key, $endKey);
                [$ds, $de] = $codec->decodeRange($es, $ee);
                $this->assertSame($key, $ds);
                $this->assertSame($endKey, $de);
            }
        }
    }
}
