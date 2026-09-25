<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Codec;

use CrazyGoat\Proto\Kvrpcpb\APIVersion;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Exception\KeyOutOfBoundsException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class CodecV2Test extends TestCase
{
    #[DataProvider('keyProvider')]
    public function testRoundTripsBinaryAndTextKeys(string $key): void
    {
        foreach ([Mode::Raw, Mode::Txn] as $mode) {
            $codec = new CodecV2($mode, 0x010203, 'tenant-a');
            $this->assertSame($key, $codec->decodeKey($codec->encodeKey($key)));
            $this->assertSame($key, $codec->decodeRegionKey($codec->encodeRegionKey($key)));
        }
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function keyProvider(): iterable
    {
        yield 'empty' => [''];
        yield 'text' => ['account:alice'];
        yield 'nul and max bytes' => ["\x00\xFF\x00\xFF"];
        yield 'long binary' => [str_repeat("\x00\xFF\x01\xFE", 32)];
    }

    public function testUsesModeAndNetworkOrderPrefix(): void
    {
        $raw = new CodecV2(Mode::Raw, 0x010203, 'tenant-a');
        $txn = new CodecV2(Mode::Txn, 0x010203, 'tenant-a');

        $this->assertSame('72010203' . bin2hex('key'), bin2hex($raw->encodeKey('key')));
        $this->assertSame('78010203' . bin2hex('key'), bin2hex($txn->encodeKey('key')));
        $this->assertSame(APIVersion::V2, $raw->getApiVersion());
        $this->assertSame(0x010203, $raw->getKeyspaceId());
        $this->assertSame('tenant-a', $raw->getKeyspaceName());
    }

    public function testRejectsWrongModeKeyspaceAndMalformedKey(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');

        $this->expectException(KeyOutOfBoundsException::class);
        $codec->decodeKey("\x78\x00\x00\x2auser");
    }

    public function testRejectsAnotherKeyspace(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $other = new CodecV2(Mode::Raw, 43, 'tenant-b');

        $this->expectException(KeyOutOfBoundsException::class);
        $codec->decodeKey($other->encodeKey('key'));
    }

    public function testRejectsOutOfBoundsKeyspaceId(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new CodecV2(Mode::Raw, 0x1000000);
    }

    public function testRangePreservesUnboundedBoundaries(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        [$start, $end] = $codec->encodeRange('a', '');
        $this->assertNotSame('', $end);
        $this->assertSame('a', $codec->decodeRange($start, $end)[0]);
        $this->assertSame('', $codec->decodeRange($start, $end)[1]);
    }

    public function testRequestRangeUsesUpperBoundSentinel(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        [$start, $end] = $codec->encodeRequestRange('a', '');

        $this->assertSame($codec->encodeKey('a'), $start);
        $this->assertSame("\x72\x00\x00\x2b", $end);
    }

    public function testRejectsRegionRangeOutsideKeyspace(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $outside = (new CodecV2(Mode::Raw, 43, 'tenant-b'))->encodeRegionKey('a');

        $this->expectException(KeyOutOfBoundsException::class);
        $codec->decodeRange($outside, '');
    }

    public function testRegionKeyUsesMceAfterV2Prefix(): void
    {
        $codec = new CodecV2(Mode::Raw, 0x010203, 'tenant-a');
        $encoded = $codec->encodeRegionKey("\x00\xFF");

        $this->assertSame('7201020300ff0000fd', bin2hex($encoded));
        $this->assertSame("\x00\xFF", $codec->decodeRegionKey($encoded));
    }
}
