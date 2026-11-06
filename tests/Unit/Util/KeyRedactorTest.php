<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Util;

use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use CrazyGoat\TiKV\Tests\Unit\Support\KeyRedactorState;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;

class KeyRedactorTest extends TestCase
{
    protected function setUp(): void
    {
        KeyRedactorState::reset();
    }

    protected function tearDown(): void
    {
        KeyRedactorState::reset();
    }

    public function testRedactEmptyKey(): void
    {
        self::assertSame('"" (0 bytes)', KeyRedactor::redact(''));
    }

    public function testRedactShortKeyDoesNotContainTheKey(): void
    {
        $key = 'user1234';
        $result = KeyRedactor::redact($key);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(8 bytes\)\z/', $result);
        self::assertStringNotContainsString($key, $result);
        self::assertStringNotContainsString(bin2hex($key), $result);
    }

    public function testRedactExactMaxPrefixKeyDoesNotContainTheKey(): void
    {
        $key = '12345678';
        $result = KeyRedactor::redact($key);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(8 bytes\)\z/', $result);
        self::assertStringNotContainsString(bin2hex($key), $result);
    }

    public function testRedactLongKeyDoesNotContainItsPrefix(): void
    {
        $key = str_repeat('a', 100);
        $result = KeyRedactor::redact($key);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(100 bytes\)\z/', $result);
        self::assertStringNotContainsString(substr($key, 0, 8), $result);
        self::assertStringNotContainsString(bin2hex(substr($key, 0, 8)), $result);
    }

    public function testRedactBinaryKeyDoesNotContainItsBytes(): void
    {
        $key = "\x00\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8";
        $result = KeyRedactor::redact($key);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(9 bytes\)\z/', $result);
        self::assertStringNotContainsString(bin2hex(substr($key, 0, 8)), $result);
    }

    public function testSameKeyProducesTheSameTokenWithinTheProcess(): void
    {
        self::assertSame(KeyRedactor::redact('tenant-42'), KeyRedactor::redact('tenant-42'));
        self::assertNotSame(KeyRedactor::redact('tenant-42'), KeyRedactor::redact('tenant-43'));
    }

    public function testExplicitSaltProducesTheDocumentedDigest(): void
    {
        KeyRedactor::setSalt('fixed-salt');

        self::assertSame('#eeea7a862d5ed313 (8 bytes)', KeyRedactor::redact('user1234'));
    }

    public function testDifferentSaltsProduceDifferentTokens(): void
    {
        KeyRedactor::setSalt('salt-a');
        $first = KeyRedactor::redact('user1234');

        KeyRedactorState::reset();
        KeyRedactor::setSalt('salt-b');
        $second = KeyRedactor::redact('user1234');

        self::assertNotSame($first, $second);
    }

    public function testDefaultSaltIsRandomPerProcess(): void
    {
        $first = KeyRedactor::redact('user1234');

        // A reset starts a fresh process-equivalent state, so the lazily
        // generated salt must differ.
        KeyRedactorState::reset();
        $second = KeyRedactor::redact('user1234');

        self::assertNotSame($first, $second);
    }

    public function testEmptySaltIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        KeyRedactor::setSalt('');
    }

    public function testCustomRedactor(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'CUSTOM:' . strlen($key));

        self::assertSame('CUSTOM:5', KeyRedactor::redact('hello'));
    }

    public function testRedactorCanBeReplacedBeforeFirstUse(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'first');
        KeyRedactor::setRedactor(fn(string $key): string => 'second');

        self::assertSame('second', KeyRedactor::redact('hello'));
    }

    public function testRedactorCanBeClearedBeforeFirstUse(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'custom');
        KeyRedactor::setRedactor(null);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(5 bytes\)\z/', KeyRedactor::redact('hello'));
    }

    public function testSetRedactorAfterFirstUseIsRejected(): void
    {
        KeyRedactor::redact('hello');

        $this->expectException(LogicException::class);
        KeyRedactor::setRedactor(fn(string $key): string => $key);
    }

    public function testSetSaltAfterFirstUseIsRejected(): void
    {
        KeyRedactor::redact('hello');

        $this->expectException(LogicException::class);
        KeyRedactor::setSalt('fixed-salt');
    }

    public function testCustomRedactorCannotBeRemovedAfterFirstUse(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'custom');
        self::assertSame('custom', KeyRedactor::redact('hello'));

        $this->expectException(LogicException::class);
        KeyRedactor::setRedactor(null);
    }
}
