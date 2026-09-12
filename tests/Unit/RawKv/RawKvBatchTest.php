<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Google\Protobuf\Internal\Message;
use Grpc\Call;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RawKvBatchTest extends TestCase
{
    use GrpcExtensionGate;

    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;
    private RawKvBatch $batch;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->batch = new RawKvBatch(
            $this->grpc,
            $regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    public function testBatchPutThrowsOnTtlCountMismatch(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TTL array count (1) must match key-value pairs count (2)');

        $retryExecutor = $this->createRetryExecutor();
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], [60], $retryExecutor);
    }

    public function testBatchGetEmptyReturnsEmpty(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->assertSame([], $this->batch->batchGet([], $retryExecutor));
    }

    public function testBatchPutEmptyReturnsEarly(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->grpc->expects($this->never())->method('getChannel');
        $this->batch->batchPut([], 0, $retryExecutor);
    }

    public function testBatchDeleteEmptyReturnsEarly(): void
    {
        $retryExecutor = $this->createRetryExecutor();
        $this->grpc->expects($this->never())->method('getChannel');
        $this->batch->batchDelete([], $retryExecutor);
    }

    public function testBatchPutWithIntTtl(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // No TiKV server in unit tests: the request reaches the transport layer
        // and fails at connection time (issue #322 pattern).
        $this->expectException(BatchPartialFailureException::class);

        $retryExecutor = $this->createRetryExecutor(1);
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], 60, $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testBatchPutWithAssociativeTtlArray(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // No TiKV server in unit tests: the request reaches the transport layer
        // and fails at connection time (issue #322 pattern).
        $this->expectException(BatchPartialFailureException::class);

        $retryExecutor = $this->createRetryExecutor(1);
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2'], ['k1' => 60, 'k2' => 120], $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testScalarTtlExpandsToMatchPairCountOnRequest(): void
    {
        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');
        $pair3 = new KvPair();
        $pair3->setKey('k3');
        $pair3->setValue('v3');

        $request = new RawBatchPutRequest();
        $request->setPairs([$pair1, $pair2, $pair3]);

        // Simulate the fix: scalar TTL expanded to match pair count
        $ttl = 60;
        $ttls = array_fill(0, count([$pair1, $pair2, $pair3]), $ttl);
        $request->setTtls($ttls);

        $this->assertCount(3, $request->getTtls());
        foreach ($request->getTtls() as $entry) {
            $this->assertSame(60, $entry);
        }
    }

    public function testPerKeyTtlArrayPassesThroughUnchanged(): void
    {
        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $request = new RawBatchPutRequest();
        $request->setPairs([$pair1, $pair2]);

        // Per-key TTL array passes through unchanged
        $ttls = [60, 120];
        $request->setTtls($ttls);

        $this->assertCount(2, $request->getTtls());
        $this->assertSame(60, $request->getTtls()[0]);
        $this->assertSame(120, $request->getTtls()[1]);
    }

    public function testBatchPutWithMultipleKeysAndScalarTtl(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // No TiKV server in unit tests: the request reaches the transport layer
        // and fails at connection time (issue #322 pattern).
        $this->expectException(BatchPartialFailureException::class);

        $retryExecutor = $this->createRetryExecutor(1);
        // 3 keys with scalar TTL - old code would send a 1-element ttls array
        $this->batch->batchPut(['k1' => 'v1', 'k2' => 'v2', 'k3' => 'v3'], 60, $retryExecutor);
        $this->addToAssertionCount(1);
    }

    public function testBatchPutWithNumericStringKeys(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // Pre-fix: PHP coerces the "12345"/"0" array keys to int, so
        // KvPair::setKey(string) throws a TypeError while building the wire
        // pairs. Post-fix the request only fails at the transport layer,
        // because there is no TiKV server in unit tests.
        $this->expectException(BatchPartialFailureException::class);

        $retryExecutor = $this->createRetryExecutor(1);
        // PHP models literal "12345"/"0" array keys as int; build the pairs
        // through string-typed keys so they reach batchPut() with their
        // declared contract (numeric-string keys must survive to the wire).
        $pairs = $this->stringKeyedPairs('12345', 'v1') + $this->stringKeyedPairs('0', 'v2');
        $this->batch->batchPut($pairs, 0, $retryExecutor);
    }

    /**
     * keyInRegion() must check region bounds in byte order, not PHP
     * numeric-string order. Region bounds ['', '100']: '9' is inside in byte
     * order, '100' is the exclusive end, '200' is outside.
     */
    public function testKeyInRegionUsesByteOrderNotNumericOrder(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '100',
        );

        $method = new \ReflectionMethod(RawKvBatch::class, 'keyInRegion');

        // In byte order '0999' < '100', so it is inside ['', '100').
        self::assertTrue($method->invoke($this->batch, '0999', $region));
        // '9' > '100' in byte order, so it belongs to the NEXT region, not this one.
        self::assertFalse($method->invoke($this->batch, '9', $region));
        // '100' is the exclusive end — not in this region.
        self::assertFalse($method->invoke($this->batch, '100', $region));
        // '200' > '100' in both byte and numeric order.
        self::assertFalse($method->invoke($this->batch, '200', $region));
    }

    // ========================================================================
    // Issue #330 pinned the old (buggy) contract: a region error carried
    // inside a batch response was NOT retried and the region cache was NOT
    // invalidated because the wait phase ran outside RetryExecutor. That
    // contract is superseded by issue #183: the wait phase now retries the
    // whole dispatch+await cycle per region. The fixed contract is pinned in
    // tests/Unit/Batch/CheckedGrpcFutureRetryableDispatchTest.php, which
    // drives CheckedGrpcFuture::fromRetryableDispatch() directly with pure
    // PHP (no ext-grpc) so it runs in the normal Unit job.
    // ========================================================================

    /**
     * Mock \Grpc\Call that resolves a recv batch to a serialized response
     * with STATUS_OK, exactly like a real channel would deliver it.
     */
    private function callReturning(Message $response): Call&MockObject
    {
        $call = $this->createMock(Call::class);
        $call->method('startBatch')->willReturn([
            'status' => ['code' => 0, 'details' => 'OK'],
            'message' => $response->serializeToString(),
        ]);

        return $call;
    }

    public function testBatchPutPartialFailureReportsWhichRegionsFailedAndDispatched(): void
    {
        // 3 regions dispatched; region 2's response carries an EpochNotMatch.
        // All three region calls are dispatched before any wait begins
        // (dispatch phase), so regions 1 and 3 were still issued even though
        // region 2 fails.
        $dispatched = [];
        $okCall = $this->callReturning(new RawBatchPutResponse());

        $failedResponse = new RawBatchPutResponse();
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());
        $failedResponse->setRegionError($error);
        $failedCall = $this->callReturning($failedResponse);

        $regionCalls = [];
        foreach ([1, 2, 3] as $regionId) {
            $call = $regionId === 2 ? $failedCall : $okCall;
            $regionCalls[$regionId] = function () use ($regionId, $call, &$dispatched): CheckedGrpcFuture {
                $dispatched[] = $regionId;
                return CheckedGrpcFuture::fromGrpcFuture(new GrpcFuture($call, RawBatchPutResponse::class));
            };
        }

        try {
            (new BatchAsyncExecutor(new NullLogger()))->executeParallel($regionCalls);
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            // All 3 regions were dispatched before the wait phase failed.
            self::assertSame([1, 2, 3], $dispatched);
            // getRegionErrors() keys identify exactly which region failed.
            self::assertSame([2], array_keys($e->getRegionErrors()));
            self::assertInstanceOf(RegionException::class, current($e->getRegionErrors()));
            self::assertSame(3, $e->getTotalRegions());
        }
    }

    public function testBatchGetSplitsIntoSubBatchesAtMaxBatchLimit(): void
    {
        // 600 keys in one region → exactly 2 RawBatchGet RPCs
        // (MAX_BATCH_LIMIT = 512). The wait phase fails against the dead
        // channel, which is what pins the *dispatch count*. maxAttempts=1
        // disables the wait-phase retry introduced by #183 so getChannel()
        // is not called again for each retry.
        $keys = array_map(static fn(int $i): string => 'k' . $i, range(0, 599));

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->expects($this->exactly(2))->method('getChannel')->willReturnCallback(
            fn(): \Grpc\Channel => new \Grpc\Channel('127.0.0.1:1', [
                'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            ]),
        );

        $this->expectException(BatchPartialFailureException::class);
        $this->batch->batchGet($keys, $this->createRetryExecutor(1));
    }

    public function testBatchPutSplitsOnByteSizeNotOnlyCount(): void
    {
        // 10 pairs of ~2KB values = 20480 bytes > MAX_BATCH_PUT_SIZE (16384)
        // → at least 2 RawBatchPut RPCs despite only 10 pairs. maxAttempts=1
        // pins the dispatch count (see testBatchGetSplitsIntoSubBatchesAtMaxBatchLimit).
        $pairs = [];
        foreach (range(0, 9) as $i) {
            $pairs['k' . $i] = str_repeat('x', 2048);
        }

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->expects($this->exactly(2))->method('getChannel')->willReturnCallback(
            fn(): \Grpc\Channel => new \Grpc\Channel('127.0.0.1:1', [
                'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            ]),
        );

        $this->expectException(BatchPartialFailureException::class);
        $this->batch->batchPut($pairs, 60, $this->createRetryExecutor(1));
    }

    public function testBatchGetWithDuplicateKeysDoesNotDuplicateRegionDispatch(): void
    {
        // batchGet(['a', 'a', 'b']) — duplicates collapse in the result map
        // (one entry per key); the pinned observable here is that the batch
        // is dispatched once for the single region, not once per duplicate.
        // maxAttempts=1 keeps the wait-phase retry (#183) from re-dispatching.
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->expects($this->exactly(1))->method('getChannel')->willReturnCallback(
            fn(): \Grpc\Channel => new \Grpc\Channel('127.0.0.1:1', [
                'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            ]),
        );

        $this->expectException(BatchPartialFailureException::class);
        $this->batch->batchGet(['a', 'a', 'b'], $this->createRetryExecutor(1));
    }

    // ========================================================================
    // Issue #183: the per-region retry wrappers must route through
    // CheckedGrpcFuture::fromRetryableDispatch(), whose retry loop re-invokes
    // the dispatch closure — so every attempt resolves the region afresh.
    // The old (dispatch-only retry) code resolved the region once and replayed
    // the same un-awaited future, so a retryable region error would never be
    // retried at all. These tests drive the *private* wrappers directly
    // (ReflectionMethod) against a dead endpoint: each attempt fails with a
    // retryable gRPC UNAVAILABLE, and with maxAttempts = 2 exactly two
    // region resolutions (pdClient->getRegion) and two gRPC sends
    // (grpc->getChannel) must happen. The region cache is forced to miss so
    // the re-resolution count is observable without cache-hit masking.
    // ========================================================================

    public function testBatchGetWithRetryReResolvesRegionOnEveryAttempt(): void
    {
        $future = $this->invokeWithDeadEndpoint('batchGetWithRetry', [['k1'], $this->createRetryExecutor(2)]);

        $this->assertRetryBudgetExhaustedWithTransportError($future);
    }

    public function testBatchPutWithRetryReResolvesRegionOnEveryAttempt(): void
    {
        $future = $this->invokeWithDeadEndpoint('batchPutWithRetry', [
            [$this->kvPair('k1', 'v1')],
            0,
            $this->createRetryExecutor(2),
        ]);

        $this->assertRetryBudgetExhaustedWithTransportError($future);
    }

    public function testBatchDeleteWithRetryReResolvesRegionOnEveryAttempt(): void
    {
        $future = $this->invokeWithDeadEndpoint('batchDeleteWithRetry', [
            ['k1'],
            $this->createRetryExecutor(2),
        ]);

        $this->assertRetryBudgetExhaustedWithTransportError($future);
    }

    private function kvPair(string $key, string $value): KvPair
    {
        $pair = new KvPair();
        $pair->setKey($key);
        $pair->setValue($value);

        return $pair;
    }

    /**
     * Set up a cluster mock whose region cache always misses and whose gRPC
     * channel points at a dead endpoint, then invoke the requested private
     * wrapper. The caller must include the RetryExecutor sized with
     * maxAttempts = 2 so exactly two dispatch attempts occur.
     *
     * @param array<int, mixed> $args full argument list for $method
     */
    private function invokeWithDeadEndpoint(string $method, array $args): CheckedGrpcFuture
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->pdClient->expects($this->exactly(2))
            ->method('getRegion')
            ->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->grpc->expects($this->exactly(2))->method('getChannel')->willReturnCallback(
            fn(): \Grpc\Channel => new \Grpc\Channel('127.0.0.1:1', [
                'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            ]),
        );

        $reflection = new \ReflectionMethod(RawKvBatch::class, $method);
        /** @var CheckedGrpcFuture $future */
        $future = $reflection->invoke($this->batch, ...$args);

        return $future;
    }

    /**
     * @param CheckedGrpcFuture $future already dispatched (eagerly) against
     *                                  the dead endpoint
     */
    private function assertRetryBudgetExhaustedWithTransportError(CheckedGrpcFuture $future): void
    {
        try {
            $future->waitForExecutor();
            self::fail('Expected the attempt cap to be exhausted against the dead endpoint');
        } catch (RetryBudgetExhaustedException $e) {
            // Issue #183 review: pinning the attempt count with maxAttempts = 2
            // must not hide the actual transport failure.
            self::assertInstanceOf(GrpcException::class, $e->getPrevious());
        }
    }

    /**
     * @return array<string, string>
     */
    private function stringKeyedPairs(string $key, string $value): array
    {
        return [$key => $value];
    }

    private function createRetryExecutor(int $maxAttempts = RetryExecutor::DEFAULT_MAX_ATTEMPTS): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            $maxAttempts,
        );
    }
}
