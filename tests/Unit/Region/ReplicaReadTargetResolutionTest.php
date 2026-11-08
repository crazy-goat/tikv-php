<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use Closure;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use PHPUnit\Framework\TestCase;

class ReplicaReadTargetResolutionTest extends TestCase
{
    /**
     * Region 42 with leader on store 3 (peer 7) and followers on stores
     * 4 (peer 8) and 5 (peer 9).
     */
    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 42,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
            peers: [
                new PeerInfo(7, 3),
                new PeerInfo(8, 4),
                new PeerInfo(9, 5),
            ],
        );
    }

    /** @param array<int, array<string, string>> $storesByLabel */
    private function storeLookup(array $storesByLabel = []): Closure
    {
        return static function (int $storeId) use ($storesByLabel): ?Store {
            if (!isset($storesByLabel[$storeId])) {
                return null;
            }

            $store = new Store();
            $store->setId($storeId);
            $labels = [];
            foreach ($storesByLabel[$storeId] as $key => $value) {
                $label = new \CrazyGoat\Proto\Metapb\StoreLabel();
                $label->setKey($key);
                $label->setValue($value);
                $labels[] = $label;
            }
            $store->setLabels($labels);

            return $store;
        };
    }

    public function testLeaderModeTargetsLeaderPeerWithNoReplicaFlags(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(),
            $this->storeLookup(),
        );

        $peer = $target->context->getPeer();
        self::assertNotNull($peer);
        self::assertSame(7, $peer->getId());
        self::assertSame(3, $peer->getStoreId());
        self::assertSame(3, $target->storeId);
        self::assertFalse($target->context->getReplicaRead());
        self::assertFalse($target->context->getStaleRead());
    }

    public function testFollowerModeTargetsNonLeaderPeerWithReplicaRead(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
        );

        $peer = $target->context->getPeer();
        self::assertNotNull($peer);
        self::assertNotSame(3, $peer->getStoreId());
        self::assertContains($peer->getStoreId(), [4, 5]);
        self::assertTrue($target->context->getReplicaRead());
        self::assertFalse($target->context->getStaleRead());
    }

    public function testStaleReadPolicySetsStaleReadFlag(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower, staleRead: true),
            $this->storeLookup(),
        );

        self::assertTrue($target->context->getStaleRead());
        self::assertTrue($target->context->getReplicaRead());
    }

    public function testLeaderTargetedStaleReadKeepsLeaderAndSetsStaleRead(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(staleRead: true),
            $this->storeLookup(),
        );

        self::assertSame(3, $target->storeId);
        self::assertFalse($target->context->getReplicaRead());
        self::assertTrue($target->context->getStaleRead());
    }

    public function testMixedModeMayTargetAnyPeer(): void
    {
        $bounds = [];
        $storeIds = [];
        // Walk every candidate index deterministically instead of relying on
        // random_int() hitting all three peers in a bounded number of draws.
        foreach ([0, 1, 2] as $index) {
            $target = RegionContextFactory::resolveTarget(
                $this->region(),
                new ReplicaReadPolicy(mode: ReplicaReadMode::Mixed),
                $this->storeLookup(),
                randomInt: static function (int $min, int $max) use ($index, &$bounds): int {
                    $bounds[] = [$min, $max];

                    return $min + $index;
                },
            );
            $storeIds[] = $target->storeId;
            self::assertTrue($target->context->getReplicaRead() || $target->storeId === 3);
        }

        // Peers are [leader 7/3, 8/4, 9/5], so each candidate index reaches
        // one distinct store, leader included.
        self::assertSame([3, 4, 5], $storeIds);
        self::assertSame([[0, 2], [0, 2], [0, 2]], $bounds);
    }

    public function testPreferLeaderKeepsLeaderWhenLabelsAllowIt(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::PreferLeader, matchStoreLabels: ['zone' => 'az1']),
            $this->storeLookup([3 => ['zone' => 'az1'], 4 => ['zone' => 'az1'], 5 => ['zone' => 'az2']]),
        );

        self::assertSame(3, $target->storeId);
        self::assertFalse($target->context->getReplicaRead());
    }

    public function testPreferLeaderFallsBackToFollowerWhenLeaderLabelsDoNotMatch(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::PreferLeader, matchStoreLabels: ['zone' => 'az1']),
            $this->storeLookup([3 => ['zone' => 'az2'], 4 => ['zone' => 'az1']]),
        );

        self::assertSame(4, $target->storeId);
        self::assertTrue($target->context->getReplicaRead());
    }

    public function testFollowerWithLabelsRestrictsSelectionToMatchingStores(): void
    {
        for ($i = 0; $i < 20; $i++) {
            $target = RegionContextFactory::resolveTarget(
                $this->region(),
                new ReplicaReadPolicy(mode: ReplicaReadMode::Follower, matchStoreLabels: ['zone' => 'az1']),
                $this->storeLookup([3 => ['zone' => 'az1'], 4 => ['zone' => 'az1'], 5 => ['zone' => 'az2']]),
            );

            self::assertSame(4, $target->storeId);
            self::assertTrue($target->context->getReplicaRead());
        }
    }

    public function testNoMatchingReplicaDegradesToLeaderWithoutReplicaRead(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower, matchStoreLabels: ['zone' => 'nowhere']),
            $this->storeLookup([3 => ['zone' => 'az1'], 4 => ['zone' => 'az1'], 5 => ['zone' => 'az2']]),
        );

        self::assertSame(3, $target->storeId);
        self::assertFalse($target->context->getReplicaRead());
    }

    public function testExcludedStoreRemovesReplicaFromSelection(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
            excludedStoreId: 4,
        );

        self::assertSame(5, $target->storeId);
    }

    public function testExcludingTheOnlyReplicaFallsBackToLeader(): void
    {
        $oneFollower = new RegionInfo(
            regionId: 43,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
            peers: [
                new PeerInfo(7, 3),
                new PeerInfo(8, 4),
            ],
        );

        $target = RegionContextFactory::resolveTarget(
            $oneFollower,
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
            excludedStoreId: 4,
        );

        // The only follower was just reported DataIsNotReady: degrade to
        // the leader instead of retrying the failing replica.
        self::assertSame(3, $target->storeId);
        self::assertFalse($target->context->getReplicaRead());
    }

    public function testContextCarriesRegionIdentityLikeLeaderPath(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
        );

        self::assertSame(42, $target->context->getRegionId());
        $epoch = $target->context->getRegionEpoch();
        self::assertNotNull($epoch);
        self::assertSame(1, $epoch->getConfVer());
        self::assertSame(10, $epoch->getVersion());
    }

    public function testStoreWithNoLookupResultNeverMatchesLabels(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower, matchStoreLabels: ['zone' => 'az1']),
            $this->storeLookup([]),
        );

        self::assertSame(3, $target->storeId);
    }

    public function testFollowerModeSkipsAnUnhealthyReplica(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
            storeAvailable: static fn (int $storeId): bool => $storeId !== 4,
        );

        self::assertSame(5, $target->storeId);
    }

    public function testFollowerModeFallsBackToLeaderWhenEveryReplicaIsUnhealthy(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Follower),
            $this->storeLookup(),
            storeAvailable: static fn (int $storeId): bool => $storeId === 3,
        );

        // No healthy replica exists: degrade to the leader, which is still
        // the best available target (and will fail fast if it is unhealthy).
        self::assertSame(3, $target->storeId);
    }

    public function testPreferLeaderFallsBackToAReplicaWhenTheLeaderIsUnhealthy(): void
    {
        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::PreferLeader),
            $this->storeLookup(),
            storeAvailable: static fn (int $storeId): bool => $storeId !== 3,
        );

        self::assertContains($target->storeId, [4, 5]);
    }
}
