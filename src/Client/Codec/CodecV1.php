<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

use CrazyGoat\Proto\Kvrpcpb\APIVersion;

/**
 * V1 codec implementation — passthrough for keys.
 *
 * In V1, keys are stored and retrieved as-is with no prefix encoding.
 * Region keys for PD lookups are encoded using Memory Comparable Encoding
 * (MCE) only for TxnKV operations; RawKV uses passthrough for all keys.
 *
 * API version: 0 (V1)
 * Keyspace ID: 0 (default)
 * Keyspace name: '' (empty)
 */
final readonly class CodecV1 implements CodecInterface
{
    /**
     * Whether to use MCE for region key encoding.
     *
     * - RawKV: region keys are passed through as-is (no MCE).
     * - TxnKV: region keys are MCE-encoded to preserve sort order.
     */
    private bool $encodeRegionKeys;

    private MemComparableCodec $mce;

    public function __construct(
        ?Mode $mode = null,
        ?MemComparableCodec $mce = null,
    ) {
        // In V1, only TxnKV uses MCE for region keys. RawKV uses passthrough.
        $this->encodeRegionKeys = $mode === Mode::Txn;
        $this->mce = $mce ?? new MemComparableCodec();
    }

    public function encodeKey(string $key): string
    {
        // V1: keys are stored as-is
        return $key;
    }

    public function decodeKey(string $encodedKey): string
    {
        // V1: keys are stored as-is
        return $encodedKey;
    }

    public function encodeRegionKey(string $key): string
    {
        if ($this->encodeRegionKeys) {
            return $this->mce->encode($key);
        }

        return $key;
    }

    public function decodeRegionKey(string $encodedKey): string
    {
        if ($this->encodeRegionKeys) {
            return $this->mce->decode($encodedKey);
        }

        return $encodedKey;
    }

    public function encodeRange(string $startKey, string $endKey): array
    {
        return [
            $this->encodeRegionKey($startKey),
            $endKey === '' ? '' : $this->encodeRegionKey($endKey),
        ];
    }

    public function decodeRange(string $encodedStart, string $encodedEnd): array
    {
        return [
            $this->decodeRegionKey($encodedStart),
            $encodedEnd === '' ? '' : $this->decodeRegionKey($encodedEnd),
        ];
    }

    public function getApiVersion(): int
    {
        return APIVersion::V1;
    }

    public function getKeyspaceId(): int
    {
        return 0;
    }

    public function getKeyspaceName(): string
    {
        return '';
    }
}
