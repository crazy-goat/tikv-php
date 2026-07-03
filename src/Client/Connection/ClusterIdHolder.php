<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

/**
 * Mutable holder for the PD cluster ID.
 *
 * Shared between {@see PdClient} and {@see TimestampOracle} to break the
 * reference cycle that would otherwise prevent prompt garbage collection
 * of the PdClient → TimestampOracle → PdClient closure.
 *
 * @internal Used only by the connection layer.
 */
final class ClusterIdHolder
{
    public function __construct(
        private ?int $clusterId = null,
    ) {}

    public function get(): ?int
    {
        return $this->clusterId;
    }

    public function set(int $clusterId): void
    {
        $this->clusterId = $clusterId;
    }
}
