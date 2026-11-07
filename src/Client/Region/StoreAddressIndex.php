<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

/**
 * Address-to-store index used to attribute a transport failure to the store
 * the failing RPC actually targeted (issue #424).
 *
 * `GrpcException` carries the address the call was sent to (issue #248), not
 * the store ID. The resolver records every address it validates for a store,
 * so the retry executor can look the store up from the exception instead of
 * guessing from the outer operation's provider or the cached leader.
 *
 * The index is intentionally address-keyed and never expires: a store address
 * is stable for the life of a cluster, and a re-registered address overwrites
 * the old entry. The number of entries is bounded by the number of stores the
 * client has resolved.
 */
final class StoreAddressIndex
{
    /** @var array<string, int> address => store ID */
    private array $storeIdByAddress = [];

    public function record(string $address, int $storeId): void
    {
        if ($address === '' || $storeId <= 0) {
            return;
        }

        $this->storeIdByAddress[$address] = $storeId;
    }

    public function storeIdForAddress(string $address): ?int
    {
        return $this->storeIdByAddress[$address] ?? null;
    }

    public function clear(): void
    {
        $this->storeIdByAddress = [];
    }
}
