<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

/**
 * Memory Comparable Encoding (MCE) for region keys.
 *
 * TiKV and PD report transactional region boundaries in an encoded key space
 * so that byte-wise lexicographic comparison of encoded keys matches the
 * ordering of the original user keys. This class implements precisely the
 * TiDB `codec.EncodeBytes` / `codec.DecodeBytes` scheme that client-go uses
 * for `Mode::Txn` region lookups (`internal/apicodec/mem_codec.go` +
 * `pkg/util/codec/bytes.go`).
 *
 * Encoding walks the key in 8-byte groups. Each group is written verbatim and
 * followed by a marker byte:
 *
 *   - full group (8 bytes)  → marker 0xFF
 *   - final partial group   → padded with 0x00 to 8 bytes, marker 0xFF - pad
 *
 * where `pad = 8 - (bytes in the group)`. The loop runs for
 * `idx = 0; idx <= len; idx += 8`, so a key whose length is an exact multiple
 * of 8 also gets a trailing all-zero group (pad = 8, marker 0xF7), and the
 * empty key encodes to a single such group.
 *
 * Decoding reads 9-byte groups (8 data bytes + marker), computes
 * `pad = 0xFF - marker`, verifies the pad bytes are zero, appends the first
 * `8 - pad` bytes, and stops after the group whose `pad != 0`. Malformed
 * input (truncated group, marker implying `pad > 8`, or non-zero padding)
 * raises an `\InvalidArgumentException`.
 */
final class MemComparableCodec
{
    private const GROUP_SIZE = 8;
    private const MARKER = 0xFF;
    private const PAD_BYTE = "\x00";

    /**
     * Encode a key using Memory Comparable Encoding.
     *
     * The returned bytes sort in the same order as the original keys, and the
     * encoding is losslessly reversible by {@see self::decode()}.
     */
    public function encode(string $key): string
    {
        $encoded = '';
        $length = \strlen($key);

        for ($idx = 0; $idx <= $length; $idx += self::GROUP_SIZE) {
            $group = substr($key, $idx, self::GROUP_SIZE);
            $pad = self::GROUP_SIZE - \strlen($group);

            $encoded .= $group . str_repeat(self::PAD_BYTE, $pad);
            $encoded .= \chr((self::MARKER - $pad) & 0xFF);
        }

        return $encoded;
    }

    /**
     * Decode a Memory Comparable Encoded key back to its original value.
     *
     * Trailing bytes after the terminating group are ignored.
     *
     * @param  string $encoded The MCE-encoded key
     * @return string The decoded key
     * @throws \InvalidArgumentException if the encoded key is malformed
     */
    public function decode(string $encoded): string
    {
        $decoded = '';
        $length = \strlen($encoded);

        for ($idx = 0; $idx < $length; $idx += self::GROUP_SIZE + 1) {
            if ($idx + self::GROUP_SIZE + 1 > $length) {
                throw new \InvalidArgumentException(
                    sprintf('Truncated MCE-encoded data at byte %d', $idx),
                );
            }

            $group = substr($encoded, $idx, self::GROUP_SIZE);
            $marker = \ord($encoded[$idx + self::GROUP_SIZE]);
            $pad = self::MARKER - $marker;

            if ($pad > self::GROUP_SIZE) {
                throw new \InvalidArgumentException(
                    sprintf('Invalid MCE marker 0x%02X at byte %d', $marker, $idx + self::GROUP_SIZE),
                );
            }

            if ($pad > 0) {
                $padding = substr($group, self::GROUP_SIZE - $pad);
                if (rtrim($padding, self::PAD_BYTE) !== '') {
                    throw new \InvalidArgumentException(
                        sprintf(
                            'Non-zero MCE padding at byte %d',
                            $idx + self::GROUP_SIZE - $pad,
                        ),
                    );
                }
            }

            $decoded .= substr($group, 0, self::GROUP_SIZE - $pad);

            if ($pad !== 0) {
                return $decoded;
            }
        }

        throw new \InvalidArgumentException(
            'MCE-encoded data is missing its terminating group',
        );
    }
}
