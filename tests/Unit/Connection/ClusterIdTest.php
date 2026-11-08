<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Connection\ClusterId;
use CrazyGoat\TiKV\Client\Connection\ClusterIdGuard;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The canonical uint64 cluster-ID representation (issue #818).
 *
 * PD cluster IDs are uint64; the pure-PHP protobuf decoder returns a value
 * above PHP_INT_MAX as a negative int. These tests pin the conversions that
 * keep the identity check usable across the whole uint64 range.
 */
final class ClusterIdTest extends TestCase
{
    public function testNormalizeExpectedKeepsAnIntBelowPhpIntMax(): void
    {
        self::assertSame('12345', ClusterId::normalizeExpected(12345));
    }

    public function testNormalizeExpectedAcceptsMaximumUint64(): void
    {
        self::assertSame(
            '18446744073709551615',
            ClusterId::normalizeExpected('18446744073709551615'),
        );
    }

    public function testNormalizeExpectedStripsLeadingZeros(): void
    {
        self::assertSame('12345', ClusterId::normalizeExpected('00012345'));
    }

    public function testNormalizeExpectedReturnsNullForNull(): void
    {
        self::assertNull(ClusterId::normalizeExpected(null));
    }

    /**
     * @return iterable<string, array{mixed, string}>
     */
    public static function invalidExpectedValues(): iterable
    {
        yield 'zero int' => [0, 'must be greater than 0'];
        yield 'negative int' => [-1, 'must be greater than 0'];
        yield 'zero string' => ['0', 'must be greater than 0'];
        yield 'negative string' => ['-1', 'must be a decimal string'];
        yield 'empty string' => ['', 'must be a decimal string'];
        yield 'non numeric' => ['12ab', 'must be a decimal string'];
        yield 'float string' => ['12.5', 'must be a decimal string'];
        yield 'above uint64' => ['18446744073709551616', 'must not exceed'];
        yield 'bool' => [true, 'must be an int or a decimal string'];
        yield 'float' => [1.0, 'must be an int or a decimal string'];
    }

    #[DataProvider('invalidExpectedValues')]
    public function testNormalizeExpectedRejectsInvalidValue(mixed $value, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        ClusterId::normalizeExpected($value);
    }

    public function testNormalizeExpectedUsesTheSubjectInTheMessage(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['expectedClusterId'] must be greater than 0, got 0");

        ClusterId::normalizeExpected(0, "options['expectedClusterId']");
    }

    public function testNormalizeReportedConvertsTheTwosComplementDecoding(): void
    {
        self::assertSame('9223372036854775808', ClusterId::normalizeReported(PHP_INT_MIN));
        self::assertSame('18446744073709551615', ClusterId::normalizeReported(-1));
        self::assertSame('12345', ClusterId::normalizeReported(12345));
    }

    public function testNormalizeReportedAcceptsStringsAndStripsZeros(): void
    {
        self::assertSame('18446744073709551615', ClusterId::normalizeReported('18446744073709551615'));
        self::assertSame('12345', ClusterId::normalizeReported('00012345'));
        self::assertSame('0', ClusterId::normalizeReported('0'));
    }

    public function testNormalizeReportedRejectsAnInvalidString(): void
    {
        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('PD reported an invalid cluster ID');

        ClusterId::normalizeReported('not-a-number');
    }

    public function testToPublicReturnsAnIntWhileItFits(): void
    {
        self::assertSame(12345, ClusterId::toPublic('12345'));
        self::assertSame(PHP_INT_MAX, ClusterId::toPublic((string) PHP_INT_MAX));
    }

    public function testToPublicReturnsTheCanonicalStringAbovePhpIntMax(): void
    {
        self::assertSame('9223372036854775808', ClusterId::toPublic('9223372036854775808'));
        self::assertSame('18446744073709551615', ClusterId::toPublic('18446744073709551615'));
    }

    public function testToSignedIntProducesTheTwosComplementValue(): void
    {
        self::assertSame(12345, ClusterId::toSignedInt('12345'));
        self::assertSame(PHP_INT_MAX, ClusterId::toSignedInt((string) PHP_INT_MAX));
        self::assertSame(PHP_INT_MIN, ClusterId::toSignedInt('9223372036854775808'));
        self::assertSame(-1, ClusterId::toSignedInt('18446744073709551615'));
    }

    public function testSignedIntRoundTripsThroughTheGeneratedEncoder(): void
    {
        $header = new ResponseHeader();
        $header->setClusterId(ClusterId::toSignedInt('18446744073709551615'));

        self::assertSame('08' . str_repeat('ff', 9) . '01', bin2hex($header->serializeToString()));
    }

    public function testExtractFromErrorKeepsAnOutOfRangeIdAsString(): void
    {
        self::assertSame(
            '18446744073709551615',
            ClusterIdGuard::extractFromError(
                'mismatch cluster id, need 18446744073709551615 but got 0',
            ),
        );
    }

    public function testExtractFromErrorRejectsAValueAboveUint64(): void
    {
        self::assertNull(
            ClusterIdGuard::extractFromError(
                'mismatch cluster id, need 18446744073709551616 but got 0',
            ),
        );
    }
}
