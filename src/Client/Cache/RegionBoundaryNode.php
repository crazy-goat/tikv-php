<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

/**
 * Internal node in RegionCache's start-key ordered treap.
 *
 * The node is intentionally mutable: rotations update child links in place
 * while RegionCache owns all lifetime and eviction decisions.
 */
final class RegionBoundaryNode
{
    public function __construct(
        public string $startKey,
        public int $regionId,
        public int $priority,
        public ?self $left = null,
        public ?self $right = null,
        public ?self $parent = null,
    ) {
    }
}
