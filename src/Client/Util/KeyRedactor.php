<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Util;

/**
 * Utility for redacting sensitive key data in log contexts.
 *
 * To prevent leakage of tenant IDs, user IDs, session tokens, etc. into
 * log aggregation, raw keys should never be logged directly. This class
 * provides a redaction method that replaces raw keys with a short hex
 * prefix and the total byte length.
 *
 * Integrators can replace this redactor with a custom implementation
 * by setting a custom callable via {@see setRedactor()}.
 *
 * ## Example output
 *
 * - `key_a` (5 bytes)            → `"6b65795f61" (5 bytes)`
 * - A 256-byte binary key        → `"a1b2c3d4e5... (256 bytes)`
 */
final class KeyRedactor
{
    private const MAX_PREFIX_BYTES = 8;

    /** @var (callable(string): string)|null */
    private static $customRedactor = null;

    /**
     * Set a custom redaction callable.
     *
     * The callable receives the raw key string and must return the
     * redacted representation. Set to null to restore the default.
     *
     * @param (callable(string): string)|null $callable
     */
    public static function setRedactor(?callable $callable): void
    {
        self::$customRedactor = $callable;
    }

    /**
     * Redact a raw key for safe logging.
     *
     * Returns a string like `"6b65795f61" (5 bytes)` – the first N bytes
     * as lowercase hex, followed by the total length in bytes.
     *
     * If a custom redactor is configured, it is invoked instead.
     */
    public static function redact(string $key): string
    {
        if (self::$customRedactor !== null) {
            return (self::$customRedactor)($key);
        }

        $len = strlen($key);

        if ($len === 0) {
            return '"" (0 bytes)';
        }

        $prefix = substr($key, 0, min($len, self::MAX_PREFIX_BYTES));
        $hex = bin2hex($prefix);

        if ($len <= self::MAX_PREFIX_BYTES) {
            return sprintf('"%s" (%d bytes)', $hex, $len);
        }

        return sprintf('"%s... (%d bytes)', $hex, $len);
    }
}
