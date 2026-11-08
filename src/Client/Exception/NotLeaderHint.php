<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * The leader hint carried by a `NotLeader` region error, in the library's own
 * types (issue #734).
 *
 * {@see RegionException::$notLeader} used to expose the generated
 * `CrazyGoat\Proto\Errorpb\NotLeader`, which made a class the library
 * regenerates with every protobuf bump part of the stability promise. This
 * value object is the promised surface instead: it carries the same fields
 * the retry path reads, and the generated class stays internal to
 * {@see RegionException::fromRegionError()}.
 *
 * The leader fields are null when the error carries no leader hint, which is
 * a valid NotLeader response: the caller then has to invalidate the region
 * and resolve it again through PD.
 */
final readonly class NotLeaderHint
{
    /**
     * @param int $regionId The region that reported the error
     * @param int|null $leaderPeerId The peer ID of the hinted leader, or
     *     null when the error carries no hint
     * @param int|null $leaderStoreId The store ID of the hinted leader, or
     *     null when the error carries no hint
     */
    public function __construct(
        public int $regionId,
        public ?int $leaderPeerId = null,
        public ?int $leaderStoreId = null,
    ) {
    }
}
