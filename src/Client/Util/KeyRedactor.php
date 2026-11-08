<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Util;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use LogicException;

/**
 * Utility for redacting sensitive key data in log contexts.
 *
 * To prevent leakage of tenant IDs, user IDs, session tokens, etc. into
 * log aggregation, raw keys should never be logged directly. This class
 * replaces every non-empty key with a truncated keyed hash and the total
 * byte length.
 *
 * The digest is the first 16 hex characters of
 * `hash_hmac('sha256', $key, $salt)`. The salt is 32 random bytes generated
 * on first use unless {@see setSalt()} configures one, so the same key keeps
 * the same token within a process while different processes (or different
 * salts) produce different tokens. A configured salt must be at least
 * {@see MIN_SALT_BYTES} bytes; 32 random bytes (for example the 64 hex
 * characters returned by {@see generateSalt()}) are recommended. The token
 * contains no part of the key, including for keys of 8 bytes or fewer:
 * recovering a key requires guessing it against the secret salt.
 *
 * Integrators can replace this redactor with a custom implementation by
 * calling {@see setRedactor()} before the first key is redacted.
 *
 * ## Example output
 *
 * - `key_a` (5 bytes)     → `#3c469e9d6c5875d3 (5 bytes)`
 * - an 8-byte binary key  → `#7c9e2b41f8a0d635 (8 bytes)`
 * - an empty key (default redactor) → `"" (0 bytes)`
 *
 * The examples show the shape only: the digest depends on the process salt,
 * so it is not a fixed value for a given key. With the default redactor an
 * empty key has no content to hash and keeps the readable `"" (0 bytes)`
 * form; a custom redactor receives the empty string like any other key.
 *
 * The redactor and the salt are process-global and one-shot: both
 * {@see setRedactor()} and {@see setSalt()} throw a {@see LogicException}
 * after the first {@see redact()} call, so running code cannot silently
 * replace the redactor or change the salt once log correlation depends on
 * them.
 */
final class KeyRedactor
{
    /**
     * Minimum length, in bytes, of a caller-supplied salt.
     *
     * A shorter salt makes the keyed hash cheap to attack: an attacker who can
     * try every possible short salt recovers the key. A one-byte salt has only
     * 256 candidates, which is no protection at all for the short tenant, user
     * and session identifiers this redactor protects.
     */
    public const MIN_SALT_BYTES = 16;

    private const DIGEST_HEX_LENGTH = 16;

    private const SALT_BYTES = 32;

    /** @var (callable(string): string)|null */
    private static $customRedactor;

    private static ?string $salt = null;

    private static bool $used = false;

    /**
     * Set a custom redaction callable.
     *
     * The callable receives the raw key string and must return the redacted
     * representation. It must be installed before the first key is redacted;
     * once {@see redact()} has run, this method throws a {@see LogicException}
     * so a debug helper or a dependency cannot silently disable redaction for
     * the rest of the process. A custom redactor that throws still counts as
     * the first use: the process cannot install a replacement afterwards.
     *
     * @param (callable(string): string)|null $callable
     */
    public static function setRedactor(?callable $callable): void
    {
        if (self::$used) {
            throw new LogicException(
                'KeyRedactor::setRedactor() must be called before the first KeyRedactor::redact() call',
            );
        }

        self::$customRedactor = $callable;
    }

    /**
     * Set the salt used by the default redactor.
     *
     * A `null` salt (the default) makes the redactor generate 32 random bytes
     * on first use, so tokens are stable within the process but differ between
     * processes. Configure a high-entropy fixed salt when tokens must be
     * comparable across processes or restarts; {@see generateSalt()} returns a
     * suitable random value. A configured salt must be at least
     * {@see MIN_SALT_BYTES} bytes: a shorter one is rejected with
     * {@see InvalidArgumentException}, because it makes the keyed hash cheap to
     * attack — the empty string would turn it into an unkeyed one. 32 random
     * bytes remain the recommendation.
     *
     * Like {@see setRedactor()}, this method is one-shot: it throws a
     * {@see LogicException} once a key has been redacted, because changing
     * the salt afterwards would break the in-process correlation the token
     * exists for.
     *
     * @throws InvalidArgumentException when the salt is shorter than
     *                                  {@see MIN_SALT_BYTES} bytes
     */
    public static function setSalt(?string $salt): void
    {
        if (self::$used) {
            throw new LogicException(
                'KeyRedactor::setSalt() must be called before the first KeyRedactor::redact() call',
            );
        }

        if ($salt === null) {
            self::$salt = null;

            return;
        }

        if (strlen($salt) < self::MIN_SALT_BYTES) {
            throw new InvalidArgumentException(sprintf(
                'The key redaction salt must be at least %d bytes (128 bits), got %d',
                self::MIN_SALT_BYTES,
                strlen($salt),
            ));
        }

        self::$salt = $salt;
    }

    /**
     * Generate a random salt suitable for {@see setSalt()}.
     *
     * Returns {@see SALT_BYTES} random bytes encoded as hex — a 64-character
     * string, above the {@see MIN_SALT_BYTES} minimum. Store the value in an
     * environment variable or a secret store and pass it to {@see setSalt()}
     * at bootstrap so tokens stay comparable across processes.
     *
     * @throws \Random\RandomException when the platform CSPRNG fails
     */
    public static function generateSalt(): string
    {
        return bin2hex(random_bytes(self::SALT_BYTES));
    }

    /**
     * Redact a raw key for safe logging.
     *
     * Returns a string like `#3c469e9d6c5875d3 (5 bytes)` – the first 16 hex
     * characters of the keyed SHA-256 digest, followed by the total length of
     * the key in bytes. The digest depends on the process salt (see
     * {@see setSalt()}), so the same key produces the same token within a
     * process and different tokens across processes with different salts.
     *
     * An empty key has no content to hash; with the default redactor it
     * returns `"" (0 bytes)`. A custom redactor receives it like any other
     * key.
     *
     * If a custom redactor is configured, it is invoked instead.
     */
    public static function redact(string $key): string
    {
        self::$used = true;

        if (self::$customRedactor !== null) {
            return (self::$customRedactor)($key);
        }

        $len = strlen($key);

        if ($len === 0) {
            return '"" (0 bytes)';
        }

        $digest = substr(hash_hmac('sha256', $key, self::salt()), 0, self::DIGEST_HEX_LENGTH);

        return sprintf('#%s (%d bytes)', $digest, $len);
    }

    private static function salt(): string
    {
        self::$salt ??= random_bytes(self::SALT_BYTES);

        return self::$salt;
    }
}
