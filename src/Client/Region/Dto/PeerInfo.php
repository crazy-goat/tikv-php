<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region\Dto;

final readonly class PeerInfo
{
    /** metapb.PeerRole::Voter — a full voting replica. */
    public const ROLE_VOTER = 0;

    /** metapb.PeerRole::Learner — a non-voting replica. */
    public const ROLE_LEARNER = 1;

    /** metapb.PeerRole::IncomingVoter — a learner being promoted to voter. */
    public const ROLE_INCOMING_VOTER = 2;

    /** metapb.PeerRole::DemotingVoter — a voter being demoted to learner. */
    public const ROLE_DEMOTING_VOTER = 3;

    /**
     * @param int $role one of the ROLE_* constants, mirroring metapb.PeerRole
     * @param bool $isWitness a witness replica stores no data and cannot serve
     *        reads, so it must never be selected as the leader
     */
    public function __construct(
        public int $peerId,
        public int $storeId,
        public int $role = self::ROLE_VOTER,
        public bool $isWitness = false,
    ) {
    }

    /**
     * Whether this peer may serve as the region leader (issue #258, REG-27).
     *
     * A learner never votes, and a witness stores no data and cannot serve
     * reads at all, so neither may lead. The transitional roles
     * (IncomingVoter, DemotingVoter) stay eligible: they are voters for the
     * duration of the joint configuration, and excluding them could strand a
     * region whose leader is mid-transfer. An unrecognised role is treated as
     * ineligible, so a future metapb.PeerRole cannot silently become routable.
     */
    public function canBeLeader(): bool
    {
        if ($this->isWitness) {
            return false;
        }

        return in_array(
            $this->role,
            [self::ROLE_VOTER, self::ROLE_INCOMING_VOTER, self::ROLE_DEMOTING_VOTER],
            true,
        );
    }
}
