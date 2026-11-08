<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Util;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use CrazyGoat\TiKV\Tests\Unit\Support\KeyRedactorState;
use LogicException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

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
        // A full 8-byte key used to be hex-encoded in full and placed in the
        // token slot, so the keyed hash must not reproduce that encoding.
        self::assertNotSame(bin2hex($key), substr($result, 1, 16));
    }

    public function testRedactShortKeyDoesNotLeakItsHexEncoding(): void
    {
        // Issue #279 (GRPC-20): the previous default hex-encoded the whole
        // key for keys of 8 bytes or fewer, so this key was recoverable with
        // hex2bin(). The keyed hash must not contain that encoding.
        $key = 'user:42';
        $result = KeyRedactor::redact($key);

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(7 bytes\)\z/', $result);
        self::assertStringNotContainsString($key, $result);
        self::assertStringNotContainsString(bin2hex($key), $result);
    }

    public function testShortKeyTokensRemainStableAndDistinct(): void
    {
        self::assertSame(KeyRedactor::redact('user:42'), KeyRedactor::redact('user:42'));
        self::assertNotSame(KeyRedactor::redact('user:42'), KeyRedactor::redact('user:43'));
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
        KeyRedactor::setSalt('fixed-salt-0123456789');

        self::assertSame('#48427d5e17002b65 (8 bytes)', KeyRedactor::redact('user1234'));
    }

    public function testDifferentSaltsProduceDifferentTokens(): void
    {
        KeyRedactor::setSalt('salt-a-0123456789');
        $first = KeyRedactor::redact('user1234');

        KeyRedactorState::reset();
        KeyRedactor::setSalt('salt-b-0123456789');
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
        // The client exception, not the global \InvalidArgumentException:
        // callers follow docs/error-handling.md and catch the client class.
        $this->expectException(InvalidArgumentException::class);

        KeyRedactor::setSalt('');
    }

    public function testOneByteSaltIsRejected(): void
    {
        // Issue #810: '1' is the shape of a placeholder or a truncated
        // environment variable, and 256 candidate keys is no protection.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('at least 16 bytes');

        KeyRedactor::setSalt('1');
    }

    public function testSaltOneByteBelowTheMinimumIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('at least 16 bytes');

        KeyRedactor::setSalt(str_repeat('s', KeyRedactor::MIN_SALT_BYTES - 1));
    }

    public function testSaltOfTheMinimumLengthIsAccepted(): void
    {
        $salt = str_repeat('s', KeyRedactor::MIN_SALT_BYTES);
        KeyRedactor::setSalt($salt);

        self::assertSame(
            '#' . substr(hash_hmac('sha256', 'user1234', $salt), 0, 16) . ' (8 bytes)',
            KeyRedactor::redact('user1234'),
        );
    }

    public function testNullSaltIsAcceptedAndSelectsTheDefault(): void
    {
        $fixedSalt = str_repeat('s', KeyRedactor::MIN_SALT_BYTES);
        $fixedToken = '#' . substr(hash_hmac('sha256', 'user1234', $fixedSalt), 0, 16) . ' (8 bytes)';

        KeyRedactor::setSalt($fixedSalt);
        // Both calls happen before the first redact(), so the one-shot guard
        // still allows the reset to the default salt.
        KeyRedactor::setSalt(null);
        $default = KeyRedactor::redact('user1234');

        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(8 bytes\)\z/', $default);
        self::assertNotSame($fixedToken, $default);
    }

    public function testGenerateSaltProducesAValidSalt(): void
    {
        $salt = KeyRedactor::generateSalt();

        self::assertMatchesRegularExpression('/\A[0-9a-f]{64}\z/', $salt);
        self::assertGreaterThanOrEqual(KeyRedactor::MIN_SALT_BYTES, strlen($salt));

        KeyRedactor::setSalt($salt);
        self::assertMatchesRegularExpression('/\A#[0-9a-f]{16} \(8 bytes\)\z/', KeyRedactor::redact('user1234'));
    }

    public function testGenerateSaltReturnsADifferentValueEachCall(): void
    {
        self::assertNotSame(KeyRedactor::generateSalt(), KeyRedactor::generateSalt());
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

    public function testThrowingCustomRedactorStillCountsAsFirstUse(): void
    {
        KeyRedactor::setRedactor(function (string $key): string {
            throw new RuntimeException('custom redactor failed');
        });

        try {
            KeyRedactor::redact('hello');
            self::fail('the custom redactor must throw');
        } catch (RuntimeException $e) {
            self::assertSame('custom redactor failed', $e->getMessage());
        }

        $this->expectException(LogicException::class);
        KeyRedactor::setRedactor(null);
    }

    public function testSetSaltAfterFirstUseIsRejected(): void
    {
        KeyRedactor::redact('hello');

        $this->expectException(LogicException::class);
        KeyRedactor::setSalt('fixed-salt-0123456789');
    }

    public function testCustomRedactorCannotBeRemovedAfterFirstUse(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'custom');
        self::assertSame('custom', KeyRedactor::redact('hello'));

        $this->expectException(LogicException::class);
        KeyRedactor::setRedactor(null);
    }
}
