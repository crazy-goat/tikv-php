<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

/**
 * Memory Comparable Encoding (MCE) for region keys.
 *
 * TiKV uses MCE to encode region boundary keys so that byte-wise lexicographic
 * comparison of encoded keys produces the same ordering as the original keys.
 *
 * The encoding escapes bytes 0x00 and 0xFF with a padding scheme, then appends
 * a terminator to mark the end of the key:
 *
 *   - 0x00 → 0x00 0xFF
 *   - 0xFF → 0xFF 0x00
 *   - any other byte → passed through unchanged
 *   - terminator → 0x00 0x00
 *
 * Decoding reverses the process: the terminator 0x00 0x00 marks the end, and
 * escaped pairs are restored to their original value.
 *
 * This implementation follows the TiDB codec.EncodeBytes / DecodeBytes
 * convention used in both V1 (TxnKV) and V2 for PD region lookups.
 */
final class MemComparableCodec
{
    /**
     * Encode a key using Memory Comparable Encoding.
     *
     * The input key is escaped and terminated so that encoded keys sort in
     * the same binary order as the original keys.
     */
    public function encode(string $key): string
    {
        $encoded = '';

        for ($i = 0, $len = \strlen($key); $i < $len; $i++) {
            $byte = \ord($key[$i]);
            if ($byte === 0x00) {
                $encoded .= "\x00\xFF";
            } elseif ($byte === 0xFF) {
                $encoded .= "\xFF\x00";
            } else {
                $encoded .= \chr($byte);
            }
        }

        // Append terminator
        $encoded .= "\x00\x00";

        return $encoded;
    }

    /**
     * Decode a Memory Comparable Encoded key back to its original value.
     *
     * Reads until the terminator (0x00 0x00) is found and reverses the
     * escaping.
     *
     * @param  string $encoded The MCE-encoded key
     * @return string The decoded key
     * @throws \InvalidArgumentException if the encoded key does not contain
     *         a valid terminator
     */
    public function decode(string $encoded): string
    {
        $decoded = '';
        $len = \strlen($encoded);
        $i = 0;

        while ($i < $len) {
            if ($i + 1 >= $len) {
                throw new \InvalidArgumentException(
                    'Unexpected end of MCE-encoded data before terminator',
                );
            }

            $byte = \ord($encoded[$i]);
            $next = \ord($encoded[$i + 1]);

            // Check for terminator: 0x00 0x00
            if ($byte === 0x00 && $next === 0x00) {
                // Move past the terminator and return
                return $decoded;
            }

            // Check for escaped sequences
            if ($byte === 0x00 && $next === 0xFF) {
                $decoded .= "\x00";
                $i += 2;
                continue;
            }

            if ($byte === 0xFF && $next === 0x00) {
                $decoded .= "\xFF";
                $i += 2;
                continue;
            }

            // Regular byte
            $decoded .= \chr($byte);
            $i++;
        }

        throw new \InvalidArgumentException(
            'MCE-encoded data is missing the terminator (0x00 0x00)',
        );
    }
}
