<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Util;

use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use PHPUnit\Framework\TestCase;

class KeyRedactorTest extends TestCase
{
    protected function tearDown(): void
    {
        // Reset custom redactor after each test
        KeyRedactor::setRedactor(null);
    }

    public function testRedactEmptyKey(): void
    {
        $this->assertSame('"" (0 bytes)', KeyRedactor::redact(''));
    }

    public function testRedactShortKey(): void
    {
        $result = KeyRedactor::redact('key_a');
        // 'key_a' hex = 6b65795f61, 5 bytes
        $this->assertSame('"6b65795f61" (5 bytes)', $result);
    }

    public function testRedactExactMaxPrefixKey(): void
    {
        // 8 bytes: exactly MAX_PREFIX_BYTES
        $key = '12345678';
        $hex = bin2hex($key);
        $result = KeyRedactor::redact($key);
        $this->assertSame(sprintf('"%s" (8 bytes)', $hex), $result);
    }

    public function testRedactLongKey(): void
    {
        $key = str_repeat('a', 100);
        $prefix = substr($key, 0, 8);
        $hex = bin2hex($prefix);
        $result = KeyRedactor::redact($key);
        $this->assertSame(sprintf('"%s... (100 bytes)', $hex), $result);
    }

    public function testRedactBinaryKey(): void
    {
        $key = "\x00\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8";
        $prefix = substr($key, 0, 8);
        $hex = bin2hex($prefix);
        $result = KeyRedactor::redact($key);
        $this->assertStringStartsWith('"', $result);
        $this->assertStringEndsWith(' (9 bytes)', $result);
        $this->assertStringContainsString($hex, $result);
    }

    public function testCustomRedactor(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'CUSTOM:' . strlen($key));
        $this->assertSame('CUSTOM:5', KeyRedactor::redact('hello'));
    }

    public function testCustomRedactorReset(): void
    {
        KeyRedactor::setRedactor(fn(string $key): string => 'custom');
        KeyRedactor::setRedactor(null);
        // Should use default redaction after reset
        $this->assertStringContainsString('bytes', KeyRedactor::redact('test'));
    }

    public function testCustomRedactorReturnsNullUsesDefault(): void
    {
        KeyRedactor::setRedactor(null);
        $result = KeyRedactor::redact('test');
        $this->assertStringContainsString('bytes', $result);
    }
}
