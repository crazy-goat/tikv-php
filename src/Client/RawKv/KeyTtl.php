<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

/**
 * Result of a RawKV key TTL lookup.
 *
 * Distinguishes the three states a TTL lookup can report: the key does not
 * exist, it exists without an expiry, or it exists with a remaining TTL.
 *
 * @see RawKvClient::getKeyTTLResult()
 */
final readonly class KeyTtl
{
    /**
     * @param bool $exists Whether the key exists
     * @param int|null $ttlSeconds Remaining TTL in seconds, or null when the
     *        key has no expiry (or does not exist)
     */
    public function __construct(
        public bool $exists,
        public ?int $ttlSeconds,
    ) {
    }
}
