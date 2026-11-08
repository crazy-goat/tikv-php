<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * Canonical representation of a PD cluster ID (issue #818).
 *
 * PD cluster IDs are proto `uint64`. The pure-PHP protobuf runtime decodes a
 * value above `PHP_INT_MAX` into a negative PHP int (two's complement): the
 * raw varint of `2^63` becomes `PHP_INT_MIN` and `2^64-1` becomes `-1`.
 * Comparing such a value strictly against a positive `int` can never succeed,
 * which used to make `options['expectedClusterId']` unusable for that part of
 * the ID space.
 *
 * Every internal comparison therefore uses the canonical unsigned decimal
 * string: an int from a decoded header is converted with `sprintf('%u', $id)`
 * and a string is validated and stripped of leading zeros. Only the public
 * accessors convert back — an ID that still fits in an `int` is returned as
 * one, a larger one as the canonical decimal string.
 *
 * @internal
 */
final class ClusterId
{
    /** Largest value a `uint64` can carry. */
    public const MAX_UINT64_DECIMAL = '18446744073709551615';

    private function __construct()
    {
    }

    /**
     * Normalise a configured cluster ID (the `expectedClusterId` option, a
     * constructor argument or {@see PdClientInterface::setClusterId()}).
     *
     * Accepts a positive int or a canonical/leading-zero decimal string up to
     * `2^64-1`; rejects everything else. Strings are used so a cluster ID
     * above `PHP_INT_MAX` can be pinned.
     *
     * @param string $subject name used in the exception message, so the
     *        options path says `options['expectedClusterId']` while a direct
     *        setter says `clusterId`
     *
     * @throws InvalidArgumentException when the value is missing a positive
     *         numeric form or does not fit in a uint64
     */
    public static function normalizeExpected(mixed $value, string $subject = 'expectedClusterId'): ?string
    {
        if ($value === null) {
            return null;
        }

        if (is_int($value)) {
            if ($value < 1) {
                throw new InvalidArgumentException(sprintf(
                    '%s must be greater than 0, got %d',
                    $subject,
                    $value,
                ));
            }

            return (string) $value;
        }

        if (!is_string($value)) {
            throw new InvalidArgumentException(sprintf(
                '%s must be an int or a decimal string (uint64 cluster ID), %s given',
                $subject,
                get_debug_type($value),
            ));
        }

        $canonical = self::canonicalize($value);
        if ($canonical === null) {
            if (preg_match('/^\d+$/', $value) === 1) {
                throw new InvalidArgumentException(sprintf(
                    '%s must not exceed %s (uint64), %s given',
                    $subject,
                    self::MAX_UINT64_DECIMAL,
                    $value,
                ));
            }

            throw new InvalidArgumentException(sprintf(
                '%s must be a decimal string (uint64 cluster ID), %s given',
                $subject,
                $value === '' ? 'an empty string' : "'{$value}'",
            ));
        }

        if ($canonical === '0') {
            throw new InvalidArgumentException(sprintf(
                '%s must be greater than 0, got 0',
                $subject,
            ));
        }

        return $canonical;
    }

    /**
     * Normalise an ID reported by PD (a response-header getter, an error
     * string or an established-ID getter) to the canonical unsigned decimal
     * string.
     *
     * A negative int is the two's-complement decoding of a uint64 above
     * `PHP_INT_MAX`, so it is converted with `sprintf('%u', $id)`. The
     * canonical string `'0'` means "not reported" to the caller.
     *
     * @throws TiKvException when a string is not a valid uint64 decimal
     */
    public static function normalizeReported(int|string $value): string
    {
        if (is_int($value)) {
            return sprintf('%u', $value);
        }

        $canonical = self::canonicalize($value);
        if ($canonical === null) {
            throw new TiKvException(sprintf(
                'PD reported an invalid cluster ID: %s',
                $value === '' ? 'an empty string' : "'{$value}'",
            ));
        }

        return $canonical;
    }

    /**
     * Convert a canonical ID to the form the public accessors expose: an
     * `int` while it fits, otherwise the canonical unsigned decimal string.
     */
    public static function toPublic(string $canonical): int|string
    {
        if (self::fitsInPhpInt($canonical)) {
            return (int) $canonical;
        }

        return $canonical;
    }

    /**
     * Convert a canonical ID to the signed PHP int a proto `uint64` field
     * uses internally.
     *
     * The generated setter reaches `GPBUtil::checkUint64()`, which runs
     * `intval()` on a numeric string and therefore clamps anything above
     * `PHP_INT_MAX`. Passing the two's-complement value instead keeps the
     * encoder's output correct: the pure-PHP encoder writes a negative int
     * as the matching uint64 varint.
     */
    public static function toSignedInt(string $canonical): int
    {
        if (self::fitsInPhpInt($canonical)) {
            return (int) $canonical;
        }

        // 2^64 - value fits in an int for every value >= 2^63.
        $complement = self::subtract(self::MAX_UINT64_DECIMAL, $canonical);

        return -1 - (int) $complement;
    }

    private static function fitsInPhpInt(string $canonical): bool
    {
        $max = (string) PHP_INT_MAX;
        $length = strlen($canonical);
        if ($length !== strlen($max)) {
            return $length < strlen($max);
        }

        return strcmp($canonical, $max) <= 0;
    }

    /**
     * Strip leading zeros and reject non-digits or values above 2^64-1.
     * Returns null when the value is not a valid uint64 decimal.
     */
    private static function canonicalize(string $value): ?string
    {
        if (preg_match('/^[0-9]+$/', $value) !== 1) {
            return null;
        }

        $trimmed = ltrim($value, '0');
        if ($trimmed === '') {
            return '0';
        }

        $maxLength = strlen(self::MAX_UINT64_DECIMAL);
        if (strlen($trimmed) > $maxLength) {
            return null;
        }
        if (strlen($trimmed) === $maxLength && strcmp($trimmed, self::MAX_UINT64_DECIMAL) > 0) {
            return null;
        }

        return $trimmed;
    }

    /**
     * Decimal subtraction of two canonical values, where `$a >= $b` and the
     * result fits in a PHP int.
     */
    private static function subtract(string $a, string $b): string
    {
        $a = strrev($a);
        $b = strrev($b);
        $result = '';
        $borrow = 0;
        $length = strlen($a);

        for ($i = 0; $i < $length; $i++) {
            $digit = (int) $a[$i] - $borrow - (int) ($b[$i] ?? '0');
            if ($digit < 0) {
                $digit += 10;
                $borrow = 1;
            } else {
                $borrow = 0;
            }
            $result .= (string) $digit;
        }

        $result = ltrim(strrev($result), '0');

        return $result === '' ? '0' : $result;
    }
}
