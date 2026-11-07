<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv\Dto;

use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class PeerInfoTest extends TestCase
{
    public function testConstruction(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3);

        $this->assertSame(7, $peer->peerId);
        $this->assertSame(3, $peer->storeId);
    }

    public function testRoleAndWitnessDefaultToAVotingDataPeer(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3);

        $this->assertSame(PeerInfo::ROLE_VOTER, $peer->role);
        $this->assertFalse($peer->isWitness);
        $this->assertTrue($peer->canBeLeader());
    }

    public function testVotingDataPeerCanBeLeader(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3, role: PeerInfo::ROLE_VOTER);

        $this->assertTrue($peer->canBeLeader());
    }

    public function testLearnerCannotBeLeader(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3, role: PeerInfo::ROLE_LEARNER);

        $this->assertFalse($peer->canBeLeader());
    }

    public function testWitnessCannotBeLeaderEvenWhenVoting(): void
    {
        $peer = new PeerInfo(
            peerId: 7,
            storeId: 3,
            role: PeerInfo::ROLE_VOTER,
            isWitness: true,
        );

        $this->assertFalse($peer->canBeLeader());
    }

    /**
     * @return array<string, array{int}>
     */
    public static function transitionalRoleProvider(): array
    {
        return [
            'incoming voter' => [PeerInfo::ROLE_INCOMING_VOTER],
            'demoting voter' => [PeerInfo::ROLE_DEMOTING_VOTER],
        ];
    }

    #[DataProvider('transitionalRoleProvider')]
    public function testTransitionalVoterRoleCanStillLead(int $role): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3, role: $role);

        $this->assertTrue($peer->canBeLeader());
    }

    public function testUnknownRoleCannotLead(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3, role: 99);

        $this->assertFalse($peer->canBeLeader());
    }

    public function testIsReadonly(): void
    {
        $reflection = new \ReflectionClass(PeerInfo::class);
        $this->assertTrue($reflection->isReadOnly());
    }
}
