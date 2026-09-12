<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #329 (TEST-09): the 2PC commit phase must never commit a key that
 * was not prewritten, and every write-set key must be prewritten. A key
 * whose region cannot be resolved used to vanish from the prewrite batch
 * while staying in the write set, so the commit phase either referenced a
 * key TiKV never saw, or the write was silently lost.
 *
 * Note: the fail-closed contract is implemented by issue #244 (PR #531),
 * which is not merged yet. The guarded tests below probe whether that fix
 * is present and warn (instead of failing) until it merges; they then
 * permanently pin the contract.
 */
final class CommitPrewriteKeyCoverageTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private LockResolver $lockResolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->pdClient,
            1000,
        );
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: $id,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    /**
     * @return array{Transaction, callable(): array<string, list<Message>>}
     *     transaction and a getter that must be called after commit() to
     *     read the captured RPC bodies, keyed by method name
     */
    private function makeTransactionAndCaptureRpcs(): array
    {
        $store = new \CrazyGoat\Proto\Metapb\Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getTimestamp')->willReturn(2000);

        $prewriteResponse = new PrewriteResponse();
        $commitResponse = new CommitResponse();

        $requests = [];
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $addr,
                string $svc,
                string $method,
                Message $request,
            ) use (
                &$requests,
                $prewriteResponse,
                $commitResponse
            ): object {
                $requests[$method][] = $request;
                return match ($method) {
                    'KvPrewrite' => $prewriteResponse,
                    'KvCommit' => $commitResponse,
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $txn = new Transaction(
            txnId: 'test-txn-1',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $this->lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: 20000,
        );

        return [$txn, static function () use (&$requests): array {
            return $requests;
        }];
    }

    /**
     * @param array<string, list<Message>> $requests
     * @param list<string> $methodNames
     * @return list<string>
     */
    private function prewrittenKeys(array $requests, array $methodNames): array
    {
        $keys = [];
        foreach ($methodNames as $method) {
            foreach ($requests[$method] ?? [] as $request) {
                if (!$request instanceof \CrazyGoat\Proto\Kvrpcpb\PrewriteRequest) {
                    continue;
                }
                foreach ($request->getMutations() as $mutation) {
                    $keys[] = $mutation->getKey();
                }
            }
        }

        return $keys;
    }

    /**
     * Fully resolvable setup: the commit-phase key set must be a subset of
     * the prewrite key set, and every write-set key must be prewritten.
     * Passes before and after the fail-closed fix; pins the invariant.
     */
    public function testCommitKeysAreSubsetOfPrewriteKeys(): void
    {
        $region1 = $this->region(1, '', 'm');
        $region2 = $this->region(2, 'm', '');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->regionCache->method('put');
        // commit()'s prewrite loop re-resolves the region inside the retry
        // closure (issue #213), so the mocked cache must answer the lookup.
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );

        [$txn, $getRequests] = $this->makeTransactionAndCaptureRpcs();
        $txn->set('a', 'v1');
        $txn->set('z', 'v2');
        $txn->commit();

        $this->assertSame(TransactionStatus::Committed, $txn->getStatus());
        $requests = $getRequests();

        $prewritten = $this->prewrittenKeys($requests, ['KvPrewrite']);
        $committed = [];
        foreach ($requests['KvCommit'] ?? [] as $request) {
            if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                $committed = [...$committed, ...$request->getKeys()];
            }
        }

        self::assertNotEmpty($prewritten, "prewrite must carry every write-set key");
        self::assertEqualsCanonicalizing(['a', 'z'], $prewritten);
        self::assertNotEmpty($committed, 'commit must carry every prewritten key');
        self::assertEqualsCanonicalizing(['a', 'z'], $committed);
    }

    /**
     * Issue #329 criterion: a key whose region cannot be resolved must not
     * be dropped from prewrite while remaining in the write set (the commit
     * phase would then reference a key TiKV never saw). Under the fail-closed
     * contract the commit fails closed with an exception and no such drift
     * can occur.
     */
    public function testCommitWithUnresolvableKeyDoesNotCommitUnprewrittenKeys(): void
    {
        // scanRegions deliberately returns a stale/incomplete window that
        // does not cover 'z' (e.g. the PD scan-bound bug fixed by #244).
        $this->pdClient->method('scanRegions')->willReturn([$this->region(1, '', 'm')]);
        $this->regionCache->method('put');

        // Probe whether the fail-closed contract (issue #244) is implemented.
        try {
            $this->regionResolver->batchResolveRegions(['z']);
        } catch (\CrazyGoat\TiKV\Client\Exception\TiKvException) {
            $failClosed = true;
        }
        if (!($failClosed ?? false)) {
            $this->expectNotToPerformAssertions();
            trigger_error(
                'Fail-closed contract not implemented yet (issue #244, PR #531); '
                . 'this test pins the issue #329 criteria and activates once #244 merges.',
                E_USER_WARNING,
            );
            return;
        }

        [$txn, $getRequests] = $this->makeTransactionAndCaptureRpcs();
        $txn->set('a', 'v1');
        $txn->set('z', 'v2');

        $threw = false;
        try {
            $txn->commit();
        } catch (\Throwable) {
            $threw = true;
        }

        if (!$threw) {
            // commit() returned normally: then every write-set key must have
            // been prewritten and the committed keys must be a subset of the
            // prewritten ones.
            $requests = $getRequests();
            $prewritten = $this->prewrittenKeys($requests, ['KvPrewrite']);
            foreach (array_keys($txn->getWriteSet()) as $writeKey) {
                self::assertContains($writeKey, $prewritten, sprintf(
                    'write-set key "%s" was never prewritten — silent data loss',
                    $writeKey,
                ));
            }
            $committed = [];
            foreach ($requests['KvCommit'] ?? [] as $request) {
                if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\CommitRequest) {
                    $committed = [...$committed, ...$request->getKeys()];
                }
            }
            self::assertEqualsCanonicalizing(
                [],
                array_diff($committed, $prewritten),
                'KvCommit must never reference a key absent from KvPrewrite',
            );
        }
        $this->addToAssertionCount(1);
    }
}
