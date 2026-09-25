<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Codec;

use CrazyGoat\Proto\Kvrpcpb\APIVersion;
use CrazyGoat\TiKV\Client\Exception\KeyOutOfBoundsException;

/**
 * API V2 key codec.
 *
 * User keys are prefixed with a mode byte and a three-byte, network-order
 * keyspace ID. Region keys additionally use Memory Comparable Encoding so
 * PD/TiKV region boundaries sort in the same order as user keys.
 */
final readonly class CodecV2 implements CodecInterface
{
    private const PREFIX_LENGTH = 4;
    private const MAX_KEYSPACE_ID = 0xFFFFFF;
    public const DEFAULT_KEYSPACE_NAME = 'DEFAULT';

    private MemComparableCodec $mce;

    public function __construct(
        private Mode $mode,
        private int $keyspaceId,
        private string $keyspaceName = self::DEFAULT_KEYSPACE_NAME,
        ?MemComparableCodec $mce = null,
    ) {
        if ($keyspaceId < 0 || $keyspaceId > self::MAX_KEYSPACE_ID) {
            throw new \InvalidArgumentException(sprintf(
                'keyspace ID must be between 0 and %d, got %d',
                self::MAX_KEYSPACE_ID,
                $keyspaceId,
            ));
        }

        $this->mce = $mce ?? new MemComparableCodec();
    }

    public function encodeKey(string $key): string
    {
        return chr($this->mode->prefixByte())
            . substr(pack('N', $this->keyspaceId), 1)
            . $key;
    }

    private function encodeUpperBound(string $endKey): string
    {
        return $endKey === '' ? $this->rawEndKey() : $this->encodeKey($endKey);
    }

    private function encodeRegionUpperBound(): string
    {
        return $this->mce->encode($this->rawEndKey());
    }

    private function rawEndKey(): string
    {
        $prefix = $this->encodeKey('');
        $value = unpack('N', $prefix);
        $next = (is_array($value) ? (int) ($value[1] ?? 0) : 0) + 1;

        return pack('N', $next);
    }

    public function isUpperBound(string $encodedKey): bool
    {
        return $encodedKey === '' || $encodedKey === $this->rawEndKey();
    }

    public function decodeKey(string $encodedKey): string
    {
        if (strlen($encodedKey) < self::PREFIX_LENGTH) {
            throw KeyOutOfBoundsException::forKey($encodedKey);
        }

        $prefix = ord($encodedKey[0]);
        $id = unpack('N', "\x00" . substr($encodedKey, 1, 3));
        $keyspaceId = is_array($id) ? (int) ($id[1] ?? -1) : -1;
        if ($prefix !== $this->mode->prefixByte() || $keyspaceId !== $this->keyspaceId) {
            throw KeyOutOfBoundsException::forKey($encodedKey);
        }

        return substr($encodedKey, self::PREFIX_LENGTH);
    }

    public function encodeRegionKey(string $key): string
    {
        return $this->mce->encode($this->encodeKey($key));
    }

    public function decodeRegionKey(string $encodedKey): string
    {
        $decoded = $this->mce->decode($encodedKey);
        if ($decoded === $this->rawEndKey()) {
            return '';
        }

        return $this->decodeKey($decoded);
    }

    /**
     * Encode a range carried by a TiKV request.
     *
     * Unlike PD region ranges, request ranges use the keyspace prefix
     * directly. An empty upper bound is represented by the first byte after
     * the keyspace, as required by the API V2 protocol.
     *
     * @return array{0: string, 1: string}
     */
    public function encodeRequestRange(string $startKey, string $endKey, bool $reverse = false): array
    {
        if ($reverse) {
            return [$this->encodeKey($endKey), $this->encodeKey($startKey)];
        }

        return [$this->encodeKey($startKey), $this->encodeUpperBound($endKey)];
    }

    public function encodeRange(string $startKey, string $endKey): array
    {
        return [
            $this->encodeRegionKey($startKey),
            $endKey === '' ? $this->encodeRegionUpperBound() : $this->encodeRegionKey($endKey),
        ];
    }

    public function decodeRange(string $encodedStart, string $encodedEnd): array
    {
        $decodedStart = $encodedStart === '' ? '' : $this->mce->decode($encodedStart);
        $decodedEnd = $encodedEnd === '' ? '' : $this->mce->decode($encodedEnd);
        $rawEnd = $this->rawEndKey();
        $prefix = $this->encodeKey('');
        if (
            ($decodedStart !== '' && strcmp($decodedStart, $rawEnd) >= 0)
            || ($decodedEnd !== '' && strcmp($decodedEnd, $prefix) <= 0)
        ) {
            throw KeyOutOfBoundsException::forKey($encodedStart !== '' ? $encodedStart : $encodedEnd);
        }

        return [
            $decodedStart === '' ? '' : $this->decodeKey($decodedStart),
            $decodedEnd === $rawEnd ? '' : ($decodedEnd === '' ? '' : $this->decodeKey($decodedEnd)),
        ];
    }

    public function getApiVersion(): int
    {
        return APIVersion::V2;
    }

    public function getKeyspaceId(): int
    {
        return $this->keyspaceId;
    }

    public function getKeyspaceName(): string
    {
        return $this->keyspaceName;
    }
}
