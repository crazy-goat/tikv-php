<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * Thrown by {@see \CrazyGoat\TiKV\Client\Region\RegionResolver::resolveStoreAddress()}
 * when a store is marked unhealthy and its cooldown has not elapsed (issue
 * #424, GAP-10).
 *
 * The retry executor classifies it as a retryable transport error, so the
 * request waits out the cooldown through the normal backoff instead of
 * burning a full RPC timeout against a store already known to be bad. Once
 * the cooldown elapses the next attempt is a half-open probe.
 */
final class StoreUnavailableException extends TiKvException
{
    public function __construct(public readonly int $storeId)
    {
        parent::__construct(sprintf(
            'Store %d is marked unhealthy after repeated transport failures; '
            . 'not routing to it during its cooldown',
            $storeId,
        ));
    }
}
