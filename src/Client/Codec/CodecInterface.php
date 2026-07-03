<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

/**
 * CodecInterface defines the contract for key encoding/decoding across
 * different TiKV API versions.
 *
 * V1 (passthrough): keys are stored as-is, no prefix encoding.
 * V2: keys are prefixed with mode byte + 3-byte keyspace ID.
 *
 * Region keys (used for PD region lookups) are encoded using Memory Comparable
 * Encoding (MCE) in both V1 (for TxnKV) and V2, to ensure correct binary
 * sorting of region boundaries.
 */
interface CodecInterface
{
    /**
     * Encode a user key for storage/retrieval.
     *
     * For V1: returns the key unchanged.
     * For V2: returns mode_prefix + keyspace_id(3 bytes BE) + user_key.
     */
    public function encodeKey(string $key): string;

    /**
     * Decode a previously encoded key back to the original user key.
     *
     * For V1: returns the key unchanged.
     * For V2: strips the mode prefix + keyspace ID prefix and validates bounds.
     */
    public function decodeKey(string $encodedKey): string;

    /**
     * Encode a key for use in PD region lookups.
     *
     * Applies Memory Comparable Encoding (MCE) to the key so that region
     * boundary keys compare correctly as byte strings.
     *
     * For V1 TxnKV: the raw key is MCE-encoded.
     * For V2: encodeKey() is applied first, then MCE encoding.
     */
    public function encodeRegionKey(string $key): string;

    /**
     * Decode a region key that was previously encoded with encodeRegionKey().
     */
    public function decodeRegionKey(string $encodedKey): string;

    /**
     * Encode a key range [start, end) for PD scan requests.
     *
     * Returns [encodedStart, encodedEnd] where each key is processed
     * through encodeRegionKey(). An empty end key (unbounded) is preserved
     * as-is.
     *
     * @return array{0: string, 1: string} [encodedStart, encodedEnd]
     */
    public function encodeRange(string $startKey, string $endKey): array;

    /**
     * Decode a key range that was previously encoded with encodeRange().
     *
     * @return array{0: string, 1: string} [decodedStart, decodedEnd]
     */
    public function decodeRange(string $encodedStart, string $encodedEnd): array;

    /**
     * Return the API version constant (see CrazyGoat\Proto\Kvrpcpb\APIVersion).
     *
     * - V1: returns 0 (APIVersion::V1)
     * - V2: returns 2 (APIVersion::V2)
     */
    public function getApiVersion(): int;

    /**
     * Return the keyspace ID (0 for V1/default, 1-16777215 for V2).
     */
    public function getKeyspaceId(): int;

    /**
     * Return the keyspace name (empty string for V1/default).
     */
    public function getKeyspaceName(): string;
}
