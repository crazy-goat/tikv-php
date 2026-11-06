<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\ServerIsBusy;
use CrazyGoat\Proto\Errorpb\StoreNotMatch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #247 (REG-16): a StoreNotMatch or a transport failure against a store
 * must drop that store's address from the store cache, so the next attempt
 * asks PD for a fresh GetStore instead of retrying the same stale address for
 * the rest of its TTL.
 *
 * These tests use a real StoreCache, RegionCache and RegionResolver and a
 * mocked PdClient: the assertion is on the observable next resolution (PD is
 * called exactly once and the second attempt sees the new address), not on a
 * mocked call.
 */
class RetryExecutorStoreInvalidationTest extends TestCase
{
    private const KEY = 'k';
    private const STORE_ID = 1;
    private const OLD_ADDRESS = 'old-store:20160';
    private const NEW_ADDRESS = 'new-store:20160';

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
    }

    private function store(string $address, int $storeId = self::STORE_ID): Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress($address);

        return $store;
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 42,
            leaderPeerId: 1,
            leaderStoreId: self::STORE_ID,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function storeNotMatch(): RegionException
    {
        $storeNotMatch = new StoreNotMatch();
        $storeNotMatch->setRequestStoreId(self::STORE_ID);
        $storeNotMatch->setActualStoreId(99);

        $error = new Error();
        $error->setMessage('store not match');
        $error->setStoreNotMatch($storeNotMatch);

        return RegionException::fromRegionError($error);
    }

    private function serverBusy(): RegionException
    {
        $error = new Error();
        $error->setMessage('server is busy');
        $error->setServerIsBusy(new ServerIsBusy());

        return RegionException::fromRegionError($error);
    }

    private function zeroSleepClassifier(): \Closure
    {
        return static fn (TiKvException $e): BackoffType => BackoffType::None;
    }

    /**
     * Give the mocked PdClient the production PdClient's store-cache
     * behaviour: a cache hit answers without a PD round trip, a miss issues
     * the GetStore (counted in $rpcCalls) and caches the fresh answer.
     * RegionResolver always goes through PdClient::getStore(); the store
     * cache is PdClient's.
     */
    /**
     * @param array<int, string> $freshAddresses address per store ID; other
     *        IDs get self::NEW_ADDRESS
     */
    private function installPdClientUsingStoreCache(
        StoreCache $storeCache,
        int &$rpcCalls,
        array $freshAddresses = [],
    ): void {
        $this->pdClient->method('getStore')
            ->willReturnCallback(function (int $storeId) use ($storeCache, &$rpcCalls, $freshAddresses): Store {
                $cached = $storeCache->get($storeId);
                if ($cached instanceof Store) {
                    return $cached;
                }

                $rpcCalls++;
                $fresh = $this->store($freshAddresses[$storeId] ?? self::NEW_ADDRESS, $storeId);
                $storeCache->put($fresh);

                return $fresh;
            });
    }

    /**
     * @return array{StoreCache, RegionCache, RegionResolver}
     */
    private function cacheWithStaleAddress(): array
    {
        $storeCache = new StoreCache();
        $storeCache->put($this->store(self::OLD_ADDRESS));

        $regionCache = new RegionCache();
        $regionCache->put($this->region());

        return [
            $storeCache,
            $regionCache,
            new RegionResolver(
                $this->pdClient,
                $regionCache,
                logger: new NullLogger(),
                storeCache: $storeCache,
            ),
        ];
    }

    private function executor(RegionCache $regionCache, RegionResolver $resolver, int $maxAttempts = 2): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $regionCache,
            grpc: $this->grpc,
            regionResolver: $resolver,
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
            deadlineMs: 0,
        );
    }

    public function testStoreNotMatchDropsTheStoreAndTheNextAttemptAsksPdAgain(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();
        $error = $this->storeNotMatch();

        // The stale address must not satisfy the second attempt: exactly one
        // fresh GetStore, and its answer is what the retry uses.
        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);

        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, $resolver, $error): string {
                $addresses[] = $resolver->resolveStoreAddress(self::STORE_ID);
                if (count($addresses) === 1) {
                    throw $error;
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            [self::OLD_ADDRESS, self::NEW_ADDRESS],
            $addresses,
            'the retry must resolve the store through PD instead of the stale cache entry',
        );
        $this->assertSame(1, $rpcCalls, 'the retry must issue exactly one fresh PD GetStore');
        $this->assertSame(
            self::NEW_ADDRESS,
            $storeCache->get(self::STORE_ID)?->getAddress(),
            'the fresh PD answer must be cached again',
        );
        $this->assertNull(
            $regionCache->getByKey(self::KEY),
            'the routing error still drops the region, as issue #245 requires',
        );
    }

    public function testStoreNotMatchDropsTheStoreEvenWhenTheRegionIsAlreadyGone(): void
    {
        // TxnKV call sites run RegionErrorHandler::check() before the
        // executor sees the exception, so the region entry is already
        // dropped. The StoreNotMatch payload must still name the store.
        $storeCache = new StoreCache();
        $storeCache->put($this->store(self::OLD_ADDRESS));
        $regionCache = new RegionCache();
        $resolver = new RegionResolver(
            $this->pdClient,
            $regionCache,
            logger: new NullLogger(),
            storeCache: $storeCache,
        );
        $error = $this->storeNotMatch();

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);

        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, $resolver, $error): string {
                $addresses[] = $resolver->resolveStoreAddress(self::STORE_ID);
                if (count($addresses) === 1) {
                    throw $error;
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            [self::OLD_ADDRESS, self::NEW_ADDRESS],
            $addresses,
            'the payload request_store_id must invalidate the store without a cached region',
        );
        $this->assertSame(1, $rpcCalls);
    }

    public function testTransportFailureDropsTheStoreAndTheNextAttemptAsksPdAgain(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);

        // The channel to the dead address is closed first (from the cache),
        // then the address is dropped.
        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with(self::OLD_ADDRESS);

        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, $resolver): string {
                $addresses[] = $resolver->resolveStoreAddress(self::STORE_ID);
                if (count($addresses) === 1) {
                    throw new GrpcException('connection reset', 14);
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            [self::OLD_ADDRESS, self::NEW_ADDRESS],
            $addresses,
            'a transport failure must force a fresh PD GetStore on the retry',
        );
        $this->assertSame(1, $rpcCalls);
        $this->assertSame(self::NEW_ADDRESS, $storeCache->get(self::STORE_ID)?->getAddress());
    }

    public function testServerIsBusyRetryKeepsTheStoreAddress(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();
        $error = $this->serverBusy();

        // Issue #245: a busy store reached the right region and the right
        // address, so neither cache may be dropped.
        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);
        $this->grpc->expects($this->never())->method('closeChannel');

        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, $resolver, $error): string {
                $addresses[] = $resolver->resolveStoreAddress(self::STORE_ID);
                if (count($addresses) === 1) {
                    throw $error;
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            [self::OLD_ADDRESS, self::OLD_ADDRESS],
            $addresses,
            'a non-routing retry must reuse the cached store address',
        );
        $this->assertSame(0, $rpcCalls, 'a non-routing retry must not ask PD for the store');
        $this->assertSame(self::OLD_ADDRESS, $storeCache->get(self::STORE_ID)?->getAddress());
        $this->assertNotNull($regionCache->getByKey(self::KEY));
    }

    public function testSuccessfulOperationKeepsTheStoreAddress(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);

        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, $resolver): string {
                $addresses[] = $resolver->resolveStoreAddress(self::STORE_ID);

                return 'ok';
            },
        );

        $this->assertSame('ok', $result);
        $this->assertSame([self::OLD_ADDRESS], $addresses);
        $this->assertSame(0, $rpcCalls);
        $this->assertSame(self::OLD_ADDRESS, $storeCache->get(self::STORE_ID)?->getAddress());
    }

    /**
     * P1 regression (issue #247): a follower read that fails at transport
     * level must evict and close the follower's address, not the leader's.
     * Evicting the leader would leave the dead follower cached (the retry
     * hits it again) and waste the healthy leader entry.
     */
    public function testFollowerTransportFailureInvalidatesTheFollowerAndKeepsTheLeader(): void
    {
        $leaderAddress = 'leader:20160';
        $followerOldAddress = 'follower-old:20160';
        $followerNewAddress = 'follower-new:20160';

        $storeCache = new StoreCache();
        $storeCache->put($this->store($leaderAddress, 1));
        $storeCache->put($this->store($followerOldAddress, 2));

        $region = new RegionInfo(
            regionId: 42,
            leaderPeerId: 10,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            peers: [
                new PeerInfo(peerId: 10, storeId: 1),
                new PeerInfo(peerId: 11, storeId: 2),
            ],
        );
        $regionCache = new RegionCache();
        $regionCache->put($region);
        $resolver = new RegionResolver(
            $this->pdClient,
            $regionCache,
            logger: new NullLogger(),
            storeCache: $storeCache,
        );

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls, [2 => $followerNewAddress]);

        // The channel to the dead follower is closed first (from the cache),
        // then that follower entry is dropped.
        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with($followerOldAddress);

        $policy = new ReplicaReadPolicy(ReplicaReadMode::Follower);
        $targetStoreId = null;
        $addresses = [];
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$addresses, &$targetStoreId, $policy, $region, $resolver): string {
                $target = RegionContextFactory::resolveTarget(
                    $region,
                    $policy,
                    $resolver->getStore(...),
                );
                $targetStoreId = $target->storeId;
                $addresses[] = $resolver->resolveStoreAddress($target->storeId);
                if (count($addresses) === 1) {
                    throw new GrpcException('connection reset', 14);
                }

                return 'ok';
            },
            targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            },
        );

        $this->assertSame('ok', $result);
        $this->assertSame(
            [$followerOldAddress, $followerNewAddress],
            $addresses,
            'the retry must re-resolve the failed follower, not replay its old address',
        );
        $this->assertSame(1, $rpcCalls, 'only the follower needs a fresh PD GetStore');
        $this->assertSame(
            $leaderAddress,
            $storeCache->get(1)?->getAddress(),
            'the healthy leader entry must survive a follower failure',
        );
        $this->assertSame($followerNewAddress, $storeCache->get(2)?->getAddress());
    }

    /**
     * Regression (issue #248, REG-17): the channel to close is the store the
     * FAILED ATTEMPT targeted, not whatever the region cache's leader is when
     * the catch block runs. A concurrent leader switch (or a range operation
     * that targeted a different store) must not make the executor tear down
     * the healthy new leader's channel.
     *
     * Note: this test already passes on the pre-#248 code because #247
     * introduced the provider; it guards that base behaviour, not this PR's
     * change. `testPutClosesTheTargetStoreChannelNotAChangedCachedLeader` and
     * `testExceptionTargetAddressWinsOverTheProvider` are the #248 regression
     * tests.
     */
    public function testTransportFailureClosesTheTargetStoreNotTheCachedLeader(): void
    {
        $leaderAddress = 'leader:20160';
        $targetOldAddress = 'target-old:20160';
        $targetNewAddress = 'target-new:20160';

        $storeCache = new StoreCache();
        $storeCache->put($this->store($leaderAddress, 1));
        $storeCache->put($this->store($targetOldAddress, 2));

        // The cache still names store 1 as the leader, while the failed
        // attempt targeted store 2.
        $regionCache = new RegionCache();
        $regionCache->put($this->region());
        $resolver = new RegionResolver(
            $this->pdClient,
            $regionCache,
            logger: new NullLogger(),
            storeCache: $storeCache,
        );

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls, [2 => $targetNewAddress]);

        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with($targetOldAddress);

        $targetStoreId = 2;
        $attempts = 0;
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$attempts, $resolver, $targetStoreId): string {
                $attempts++;
                $resolver->resolveStoreAddress($targetStoreId);
                if ($attempts === 1) {
                    throw new GrpcException('connection reset', GrpcStatusCode::Unavailable->value);
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
            targetStoreId: static fn (): int => $targetStoreId,
        );

        $this->assertSame('ok', $result);
        $this->assertSame(1, $rpcCalls, 'only the failed target store is re-resolved through PD');
        $this->assertSame(
            $leaderAddress,
            $storeCache->get(1)?->getAddress(),
            'the cached leader store must not be invalidated by a failure of another store',
        );
        $this->assertSame($targetNewAddress, $storeCache->get(2)?->getAddress());
    }

    /**
     * Regression (issue #248, P1): a GrpcException carries the address the
     * failed attempt was sent to. That address wins over the caller's target
     * provider, which describes the enclosing operation, not a nested RPC
     * such as lock resolution against another region's primary.
     */
    public function testExceptionTargetAddressWinsOverTheProvider(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);
        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with('primary-store:20160');

        $attempts = 0;
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$attempts, $resolver): string {
                $attempts++;
                $resolver->resolveStoreAddress(self::STORE_ID);
                if ($attempts === 1) {
                    throw new GrpcException(
                        'connection reset',
                        GrpcStatusCode::Unavailable->value,
                        'primary-store:20160',
                    );
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
            targetStoreId: static fn (): int => self::STORE_ID,
        );

        $this->assertSame('ok', $result);
    }

    /**
     * Regression (issue #248, REG-17): a DEADLINE_EXCEEDED is a retryable
     * transport failure on a slow but healthy connection, so it must not tear
     * the channel down and force a re-handshake.
     */
    public function testDeadlineExceededDoesNotCloseTheChannel(): void
    {
        [$storeCache, $regionCache, $resolver] = $this->cacheWithStaleAddress();

        $rpcCalls = 0;
        $this->installPdClientUsingStoreCache($storeCache, $rpcCalls);
        $this->grpc->expects($this->never())->method('closeChannel');

        $attempts = 0;
        $result = $this->executor($regionCache, $resolver)->execute(
            self::KEY,
            function () use (&$attempts, $resolver): string {
                $attempts++;
                $resolver->resolveStoreAddress(self::STORE_ID);
                if ($attempts === 1) {
                    throw new GrpcException('deadline exceeded', GrpcStatusCode::DeadlineExceeded->value);
                }

                return 'ok';
            },
            $this->zeroSleepClassifier(),
        );

        $this->assertSame('ok', $result);
    }
}
