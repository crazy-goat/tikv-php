<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #781: the two retry guards of TxnReader that had no unit coverage.
 *
 * 1. A `DataIsNotReady` region error must exclude the lagging replica store
 *    so the next attempt falls back to another replica or the leader
 *    (issue #421), both in `get()` and in the scan closure.
 * 2. A lock that is still alive must end the scan within the retry budget
 *    instead of retrying forever.
 */
final class TxnReaderRetryTest extends TestCase
{
    private const LEADER_ADDRESS = '127.0.0.1:20160';
    private const FOLLOWER_ADDRESS = '127.0.0.1:20161';

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    private RegionCacheInterface&MockObject $regionCache;

    private RegionInfo $region;

    protected function setUp(): void
    {
        // Region 42: leader on store 3 (peer 7), one follower on store 4
        // (peer 8). Follower mode therefore has exactly one candidate, so the
        // first attempt targets store 4 and the exclusion of store 4 is the
        // only way the retry can reach the leader.
        $this->region = new RegionInfo(
            regionId: 42,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
            peers: [
                new PeerInfo(7, 3),
                new PeerInfo(8, 4),
            ],
        );

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->regionCache->method('getByKey')->willReturn($this->region);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');
        $this->pdClient->method('scanRegions')->willReturn([$this->region]);
        $this->pdClient->method('getRegion')->willReturn($this->region);
        $this->pdClient->method('getStore')->willReturnCallback(
            static function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress($storeId === 3 ? self::LEADER_ADDRESS : self::FOLLOWER_ADDRESS);

                return $store;
            },
        );
    }

    private function createTransaction(
        ReplicaReadPolicy $policy,
        int $maxBackoffMs = 20000,
    ): Transaction {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-retry',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver(
                $this->grpc,
                $regionResolver,
                $this->regionCache,
                $this->pdClient,
                1000,
            ),
            regionResolver: $regionResolver,
            maxBackoffMs: $maxBackoffMs,
            replicaReadPolicy: $policy,
        );
    }

    private function followerPolicy(): ReplicaReadPolicy
    {
        return new ReplicaReadPolicy(mode: ReplicaReadMode::Follower);
    }

    public function testGetExcludesAStaleFollowerAndRetriesOnTheLeader(): void
    {
        /** @var list<string> $addresses */
        $addresses = [];
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$addresses): GetResponse {
                self::assertSame('KvGet', $method);
                $addresses[] = $address;

                if (count($addresses) === 1) {
                    // The follower's applied index is behind the read version.
                    throw new RegionException(
                        'KvGet',
                        'data is not ready',
                        errorKind: ErrorKind::DataIsNotReady,
                    );
                }

                $response = new GetResponse();
                $response->setValue('v1');

                return $response;
            },
        );

        $value = $this->createTransaction($this->followerPolicy())->get('k1');

        self::assertSame('v1', $value);
        // The second RPC must not retry the lagging follower: the excluded
        // store degrades the target to the leader (issue #421).
        self::assertSame([self::FOLLOWER_ADDRESS, self::LEADER_ADDRESS], $addresses);
    }

    public function testScanExcludesAStaleFollowerAndRetriesOnTheLeader(): void
    {
        /** @var list<string> $addresses */
        $addresses = [];
        $this->grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$addresses): ScanResponse {
                self::assertSame('KvScan', $method);
                $addresses[] = $address;

                if (count($addresses) === 1) {
                    throw new RegionException(
                        'KvScan',
                        'data is not ready',
                        errorKind: ErrorKind::DataIsNotReady,
                    );
                }

                $pair = new KvPair();
                $pair->setKey('k1');
                $pair->setValue('v1');
                $response = new ScanResponse();
                $response->setPairs([$pair]);

                return $response;
            },
        );

        $rows = $this->createTransaction($this->followerPolicy())->scan('', '', 1);

        self::assertSame(['k1'], array_column($rows, 'key'));
        self::assertSame([self::FOLLOWER_ADDRESS, self::LEADER_ADDRESS], $addresses);
    }

    public function testScanWithALiveLockTerminatesWithinTheRetryBudget(): void
    {
        $lock = new LockInfo();
        $lock->setKey('k1');
        $lock->setPrimaryLock('primary');
        $lock->setLockVersion(900);

        $keyError = new KeyError();
        $keyError->setLocked($lock);
        $lockedScan = new ScanResponse();
        $lockedScan->setError($keyError);

        // commit_version = 0 and lock_ttl > 0: the transaction is still
        // alive, so LockResolver throws TxnRetryableException instead of
        // resolving the lock.
        $status = new CheckTxnStatusResponse();
        $status->setCommitVersion(0);
        $status->setLockTtl(1000);

        $scanCalls = 0;
        /** @var list<string> $methods */
        $methods = [];
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
            ) use (
                &$scanCalls,
                &$methods,
                $lockedScan,
                $status,
            ): object {
                $methods[] = $method;

                return match ($method) {
                    'KvScan' => ++$scanCalls <= 4
                        ? $lockedScan
                        // A regression that unbounds the loop must fail the
                        // test instead of spinning until the attempt cap.
                        : throw new \RuntimeException('the scan must stop within its retry budget'),
                    'KvCheckTxnStatus' => $status,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $this->pdClient->method('getLowResolutionTimestamp')->willReturn(4000);

        // The TxnLock backoff is 100–200 ms on the first retry, so a 250 ms
        // budget allows exactly one retry and the second backoff (200–400 ms)
        // exhausts it: the scan terminates deterministically after two RPCs.
        $txn = $this->createTransaction(new ReplicaReadPolicy(), maxBackoffMs: 250);

        try {
            $txn->scan('', '', 1);
            self::fail('the scan must stop at its retry budget');
        } catch (RetryBudgetExhaustedException $e) {
            // The budget bound surfaces as the client-side give-up signal and
            // keeps the last lock error as its cause (issue #256).
            self::assertInstanceOf(TxnRetryableException::class, $e->getPrevious());
            self::assertSame(BackoffType::TxnLock, $e->getPrevious()->backoffType);
        }

        self::assertSame(2, $scanCalls);
        self::assertSame(['KvScan', 'KvCheckTxnStatus', 'KvScan', 'KvCheckTxnStatus'], $methods);
    }
}
