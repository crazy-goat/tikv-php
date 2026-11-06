<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use ReflectionProperty;

/**
 * Restores `KeyRedactor`'s process-global static state between tests.
 *
 * `setRedactor()` and `setSalt()` are deliberately one-shot, so a test that
 * needs to install a redactor or a fixed salt cannot rely on a public reset
 * method (and production code must not get one). Reflection is confined to
 * this helper.
 */
final class KeyRedactorState
{
    public static function reset(): void
    {
        self::set('customRedactor', null);
        self::set('salt', null);
        self::set('used', false);
    }

    private static function set(string $property, mixed $value): void
    {
        $reflection = new ReflectionProperty(KeyRedactor::class, $property);
        $reflection->setValue(null, $value);
    }
}
