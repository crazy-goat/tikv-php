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
 * - `key_a` (5 bytes)     → `"6b65795f61" (5 bytes)`
 * - an 8-byte binary key  → `"a1b2c3d4e5f6f7f8" (8 bytes)`
 * - a 256-byte key starting with those 8 bytes
 *                         → `"a1b2c3d4e5f6f7f8... (256 bytes)`
 *
 * The hex prefix is at most `MAX_PREFIX_BYTES` (8) bytes long, so it is at
 * most 16 hex characters. A key longer than the prefix is truncated to those
 * 8 bytes, and the truncated form carries **no closing quote** — the shape
 * is pinned by `tests/Unit/Util/KeyRedactorTest.php`, so do not "repair" it.
 */
final class KeyRedactor
{
    private const MAX_PREFIX_BYTES = 8;

    /** @var (callable(string): string)|null */
    private static $customRedactor;

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
     * Returns a string like `"6b65795f61" (5 bytes)` – the first
     * `min(MAX_PREFIX_BYTES, strlen($key))` bytes as lowercase hex, followed
     * by the total length of the key in bytes. `MAX_PREFIX_BYTES` is 8, so
     * the hex prefix is at most 16 characters long.
     *
     * A key of 8 bytes or fewer is hex-encoded in full and quoted on both
     * sides. A longer key is truncated to those 8 bytes, and the truncated
     * form carries **no closing quote**: `"a1b2c3d4e5f6f7f8... (12 bytes)`.
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
