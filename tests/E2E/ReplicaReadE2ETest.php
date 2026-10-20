<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests for replica reads and stale reads (issue #421).
 *
 * Requires a running TiKV cluster with at least 3 replicas per region
 * (the docker-compose cluster provides tikv1/tikv2/tikv3).
 *
 * Note: the docker-compose cluster has no region/store labels configured,
 * so label-matching tests assert the fallback-to-leader behaviour of a
 * policy whose labels no store carries — which still exercises the full
 * selection path (label filter → no candidate → leader).
 */
class ReplicaReadE2ETest extends TestCase
{
    private const PD_ENDPOINTS = 'PD_ENDPOINTS';

    private static ?RawKvClient $leaderClient = null;

    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = getenv(self::PD_ENDPOINTS)
            ? explode(',', (string) getenv(self::PD_ENDPOINTS))
            : ['pd:2379'];
        ClusterProbe::assertPdReachable($pdEndpoints);
        self::$leaderClient = RawKvClient::create($pdEndpoints);
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$leaderClient instanceof RawKvClient) {
            self::$leaderClient->close();
            self::$leaderClient = null;
        }
    }

    private function client(): RawKvClient
    {
        if (!self::$leaderClient instanceof RawKvClient) {
            $this->markTestSkipped('TiKV cluster not available');
        }

        return self::$leaderClient;
    }

    public function testFollowerReadReturnsCorrectData(): void
    {
        $leader = $this->client();
        $key = 'e2e:replica-read:follower:' . uniqid('', true);
        $value = 'follower-value-' . random_int(1, 1_000_000);

        try {
            $leader->put($key, $value);

            $follower = RawKvClient::create(
                getenv(self::PD_ENDPOINTS)
                    ? explode(',', (string) getenv(self::PD_ENDPOINTS))
                    : ['pd:2379'],
                options: ['replicaRead' => new ReplicaReadPolicy(mode: ReplicaReadMode::Follower)],
            );

            try {
                // The write is replicated to all followers; a follower read
                // must return the leader's value (replication is synchronous
                // in TiKV, so no wait is needed).
                self::assertSame($value, $follower->get($key));
            } finally {
                $follower->close();
            }
        } finally {
            try {
                $leader->delete($key);
            } catch (\Throwable) {
                // best-effort cleanup
            }
        }
    }

    public function testMixedReadReturnsCorrectData(): void
    {
        $leader = $this->client();
        $key = 'e2e:replica-read:mixed:' . uniqid('', true);
        $value = 'mixed-value-' . random_int(1, 1_000_000);

        try {
            $leader->put($key, $value);

            $mixed = RawKvClient::create(
                getenv(self::PD_ENDPOINTS)
                    ? explode(',', (string) getenv(self::PD_ENDPOINTS))
                    : ['pd:2379'],
                options: ['replicaRead' => new ReplicaReadPolicy(mode: ReplicaReadMode::Mixed)],
            );

            try {
                self::assertSame($value, $mixed->get($key));
            } finally {
                $mixed->close();
            }
        } finally {
            try {
                $leader->delete($key);
            } catch (\Throwable) {
                // best-effort cleanup
            }
        }
    }

    public function testStaleReadReturnsCorrectData(): void
    {
        $leader = $this->client();
        $key = 'e2e:replica-read:stale:' . uniqid('', true);
        $value = 'stale-value-' . random_int(1, 1_000_000);

        try {
            $leader->put($key, $value);

            // Stale read against a fresh start_ts: safe_ts on every replica
            // covers the write, so any replica (leader included) may serve it.
            $stale = RawKvClient::create(
                getenv(self::PD_ENDPOINTS)
                    ? explode(',', (string) getenv(self::PD_ENDPOINTS))
                    : ['pd:2379'],
                options: ['replicaRead' => new ReplicaReadPolicy(staleRead: true)],
            );

            try {
                self::assertSame($value, $stale->get($key));
            } finally {
                $stale->close();
            }
        } finally {
            try {
                $leader->delete($key);
            } catch (\Throwable) {
                // best-effort cleanup
            }
        }
    }

    public function testLabelPolicyWithoutMatchingStoresFallsBackToLeader(): void
    {
        $leader = $this->client();
        $key = 'e2e:replica-read:label:' . uniqid('', true);
        $value = 'label-value-' . random_int(1, 1_000_000);

        try {
            $leader->put($key, $value);

            // The compose cluster carries no zone labels: the label filter
            // finds no replica and selection must degrade to the leader.
            $labelled = RawKvClient::create(
                getenv(self::PD_ENDPOINTS)
                    ? explode(',', (string) getenv(self::PD_ENDPOINTS))
                    : ['pd:2379'],
                options: ['replicaRead' => new ReplicaReadPolicy(
                    mode: ReplicaReadMode::Follower,
                    matchStoreLabels: ['zone' => 'no-such-zone'],
                )],
            );

            try {
                self::assertSame($value, $labelled->get($key));
            } finally {
                $labelled->close();
            }
        } finally {
            try {
                $leader->delete($key);
            } catch (\Throwable) {
                // best-effort cleanup
            }
        }
    }
}
