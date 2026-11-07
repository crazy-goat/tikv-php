<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Codec;

use CrazyGoat\TiKV\Client\Codec\Mode;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ModeTest extends TestCase
{
    public function testPrefixBytesArePinnedToTheirLiterals(): void
    {
        self::assertSame(0x72, Mode::Raw->prefixByte());
        self::assertSame(0x78, Mode::Txn->prefixByte());
    }

    /**
     * @return iterable<string, array{Mode}>
     */
    public static function modeProvider(): iterable
    {
        yield 'raw' => [Mode::Raw];
        yield 'txn' => [Mode::Txn];
    }

    #[DataProvider('modeProvider')]
    public function testFromPrefixByteRoundTrips(Mode $mode): void
    {
        self::assertSame($mode, Mode::fromPrefixByte($mode->prefixByte()));
    }

    public function testFromPrefixByteRejectsUnknownByte(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown mode prefix byte: 0x00');

        Mode::fromPrefixByte(0x00);
    }

    public function testFromPrefixByteRejectsByteOutsideSingleByteRange(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown mode prefix byte: 0x100');

        Mode::fromPrefixByte(0x100);
    }
}
