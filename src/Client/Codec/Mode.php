<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

/**
 * Mode represents the key-space mode for API V2 key encoding.
 *
 * In V2, each key is prefixed with a mode byte followed by a 3-byte keyspace
 * ID (network byte order):
 * - RawKV: prefix byte 0x72 ('r')
 * - TxnKV: prefix byte 0x78 ('x')
 *
 * V1 does not use mode prefixes and keys are stored as-is.
 */
enum Mode: string
{
    /**
     * RawKV mode — keys are prefixed with 0x72 in V2.
     */
    case Raw = 'raw';

    /**
     * TxnKV mode — keys are prefixed with 0x78 in V2.
     */
    case Txn = 'txn';

    /**
     * Return the V2 mode prefix byte.
     *
     * @return int 0x72 for Raw, 0x78 for Txn
     */
    public function prefixByte(): int
    {
        return match ($this) {
            self::Raw => 0x72, // 'r'
            self::Txn => 0x78, // 'x'
        };
    }

    /**
     * Create a Mode from a prefix byte.
     *
     * @param  int    $byte  0x72 (Raw) or 0x78 (Txn)
     * @throws \InvalidArgumentException if the byte is not a recognised prefix
     */
    public static function fromPrefixByte(int $byte): self
    {
        return match ($byte) {
            0x72 => self::Raw,
            0x78 => self::Txn,
            default => throw new \InvalidArgumentException(
                sprintf('Unknown mode prefix byte: 0x%02x', $byte),
            ),
        };
    }
}
