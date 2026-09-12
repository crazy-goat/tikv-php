<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Pins the fixed contract of issue #183: a region error returned INSIDE a
 * batch response is now retried per region through RetryExecutor, because
 * {@see CheckedGrpcFuture::fromRetryableDispatch()} issues the first attempt
 * eagerly (dispatch phase, preserving fan-out) and runs the whole
 * dispatch+await cycle in the wait phase.
 *
 * These are pure-PHP Unit tests: the future is driven via
 * {@see CheckedGrpcFuture::fromCallable()} (no mocked \Grpc\Call, no
 * ext-grpc), so they run in the normal unit job. They also join the Grpc
 * suite for the PCOV coverage run (see phpunit.xml).
 */
final class CheckedGrpcFutureRetryableDispatchTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
    }

    private function createRetryExecutor(int $maxBackoffMs = 20000): RetryExecutor
    {
        return new RetryExecutor(
            $maxBackoffMs,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
        );
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function notLeaderError(int $regionId, int $hintStoreId): Error
    {
        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId($hintStoreId);

        $notLeader = new NotLeader();
        $notLeader->setRegionId($regionId);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        return $error;
    }

    private function throwingFuture(TiKvException $error): CheckedGrpcFuture
    {
        return CheckedGrpcFuture::fromCallable(static function () use ($error): never {
            throw $error;
        });
    }

    private function regionErrorFuture(Error $error): CheckedGrpcFuture
    {
        return $this->throwingFuture(RegionException::fromRegionError($error));
    }

    private function successFuture(Message $response): CheckedGrpcFuture
    {
        return CheckedGrpcFuture::fromCallable(static fn(): Message => $response);
    }

    /**
     * A NotLeader with a leader hint must be re-dispatched: the executor
     * switches the cached leader (no invalidation) and the second, freshly
     * resolved attempt succeeds.
     */
    public function testNotLeaderWithHintSwitchesLeaderAndRetries(): void
    {
        $ok = new RawBatchGetResponse();
        $attempts = 0;

        $dispatch = function () use (&$attempts, $ok): CheckedGrpcFuture {
            $attempts++;
            if ($attempts === 1) {
                return $this->regionErrorFuture($this->notLeaderError(1, 2));
            }

            return $this->successFuture($ok);
        };

        $this->regionCache->expects($this->once())
            ->method('switchLeader')
            ->with(1, 2)
            ->willReturn(true);
        $this->regionCache->expects($this->never())->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');

        // The first attempt is dispatched eagerly at construction, i.e. during
        // the executor's dispatch phase.
        self::assertSame(1, $attempts);
        // The first attempt wraps a synthetic callable, so there is no inner
        // gRPC future to cancel (best-effort cancellation).
        self::assertNull($future->inner());

        self::assertSame($ok, $future->waitForExecutor());
        self::assertSame(2, $attempts, 'NotLeader must trigger exactly one re-dispatch');
        self::assertTrue($future->isCompleted());
    }

    /**
     * An EpochNotMatch carries no leader hint, so the executor invalidates
     * the cached region and re-dispatches; the second attempt succeeds.
     */
    public function testEpochNotMatchInvalidatesCacheAndRetries(): void
    {
        $ok = new RawBatchGetResponse();
        $attempts = 0;

        $dispatch = function () use (&$attempts, $ok): CheckedGrpcFuture {
            $attempts++;
            if ($attempts === 1) {
                $error = new Error();
                $error->setMessage('epoch not match');
                $error->setEpochNotMatch(new EpochNotMatch());

                return $this->regionErrorFuture($error);
            }

            return $this->successFuture($ok);
        };

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1, 'retry_region_error');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');

        self::assertSame(1, $attempts);
        self::assertSame($ok, $future->waitForExecutor());
        self::assertSame(2, $attempts, 'EpochNotMatch must invalidate and re-dispatch once');
    }

    /**
     * A TiKvException thrown while dispatching the first attempt (region
     * resolution / store lookup) must not escape construction: it is captured
     * and fed into RetryExecutor during the wait phase, which re-dispatches.
     */
    public function testEagerDispatchFailureIsRetriedInWaitPhase(): void
    {
        $ok = new RawBatchGetResponse();
        $attempts = 0;
        $dispatchError = new RegionException('dispatch', 'StaleCommand');

        $dispatch = function () use (&$attempts, $ok, $dispatchError): CheckedGrpcFuture {
            $attempts++;
            if ($attempts === 1) {
                throw $dispatchError;
            }

            return $this->successFuture($ok);
        };

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->expects($this->once())->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');

        self::assertSame(1, $attempts, 'the first dispatch is eager even when it throws');
        self::assertSame($ok, $future->waitForExecutor());
        self::assertSame(2, $attempts);
    }

    /**
     * A non-retryable region error must surface unchanged after exactly one
     * attempt — no re-dispatch, no cache invalidation.
     */
    public function testNonRetryableErrorPropagatesWithoutRetry(): void
    {
        $attempts = 0;
        $terminal = new RegionException('RegionError', 'RaftEntryTooLarge');

        $dispatch = function () use (&$attempts, $terminal): CheckedGrpcFuture {
            $attempts++;

            return $this->throwingFuture($terminal);
        };

        $this->regionCache->expects($this->never())->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');
        self::assertSame(1, $attempts);

        try {
            $future->waitForExecutor();
            self::fail('Expected the non-retryable error to propagate');
        } catch (RegionException $e) {
            self::assertSame($terminal, $e);
        }

        self::assertSame(1, $attempts, 'a non-retryable error must not be re-dispatched');
    }

    /**
     * Repeated retryable errors exhaust the (small) backoff budget and the
     * terminal error still propagates rather than looping forever.
     *
     * EpochNotMatch backs off with base 2 ms and equal jitter, so the
     * minimum cumulative sleep after k retries is 1, 3, 7, 15 ms; a 10 ms
     * budget therefore exhausts after 3-4 attempts under any jitter draw.
     */
    public function testBudgetExhaustionPropagatesTerminalError(): void
    {
        $attempts = 0;

        $dispatch = function () use (&$attempts): CheckedGrpcFuture {
            $attempts++;
            $error = new Error();
            $error->setMessage('epoch not match');
            $error->setEpochNotMatch(new EpochNotMatch());

            return $this->regionErrorFuture($error);
        };

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch(
            $dispatch,
            $this->createRetryExecutor(10),
            'k1',
        );

        try {
            $future->waitForExecutor();
            self::fail('Expected the retry budget to be exhausted');
        } catch (TiKvException $e) {
            self::assertInstanceOf(RegionException::class, $e);
        }

        self::assertGreaterThanOrEqual(3, $attempts);
        self::assertLessThanOrEqual(4, $attempts);
    }

    /**
     * Documents the intentional current contract behind the #183 wait-phase
     * retry. `RegionErrorHandler::check()` raises a bare
     * `RegionException('BatchRequest', $serverError)` for a top-level
     * non-region `error` string on RawBatchPut/RawBatchDelete responses —
     * with no `errorKind` — and `ErrorClassifier` falls back to
     * `BackoffType::RegionMiss` for any kind-less RegionException. A
     * permanent server string such as "ttl is not enabled …" is therefore
     * retried for up to the backoff budget. This mirrors the pre-existing
     * single-key paths (`RawKvCrud::getKeyTTL`, `RawKvAtomic::compareAndSwap`)
     * and is deliberately NOT reclassified in #183; the test pins it so a
     * future classification change is a conscious decision (see CHANGELOG).
     */
    public function testBareBatchErrorStringRegionExceptionIsRetriedAsRegionMiss(): void
    {
        $attempts = 0;
        $terminal = new RegionException('BatchRequest', 'ttl is not enabled, but get put request with ttl');
        $ok = new RawBatchGetResponse();

        $dispatch = function () use (&$attempts, $terminal, $ok): CheckedGrpcFuture {
            $attempts++;
            if ($attempts === 1) {
                return $this->throwingFuture($terminal);
            }

            return $this->successFuture($ok);
        };

        $this->regionCache->method('getByKey')->willReturn($this->region());
        // RegionMiss classification runs the standard retry_region_error
        // invalidation path once, before the second attempt succeeds.
        $this->regionCache->expects($this->once())->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');

        self::assertSame($ok, $future->waitForExecutor());
        self::assertSame(2, $attempts, 'a bare BatchRequest RegionException is retried as RegionMiss');
    }

    /**
     * Only TiKvException is captured from the eager dispatch: any other
     * throwable propagates unchanged out of construction.
     */
    public function testEagerNonTiKvThrowablePropagatesAtConstruction(): void
    {
        $dispatch = static function (): CheckedGrpcFuture {
            throw new \RuntimeException('eager boom');
        };

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('eager boom');
        CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');
    }

    /**
     * A non-TiKvException thrown while re-dispatching an attempt must escape
     * RetryExecutor unchanged (only TiKvException is classified/retried).
     */
    public function testNonTiKvThrowableFromRedispatchPropagatesUnchanged(): void
    {
        $attempts = 0;
        $dispatch = function () use (&$attempts): CheckedGrpcFuture {
            $attempts++;
            if ($attempts === 1) {
                return $this->throwingFuture(new RegionException('RegionError', 'StaleCommand'));
            }

            throw new \RuntimeException('redispatch boom');
        };

        $this->regionCache->method('getByKey')->willReturn($this->region());
        $this->regionCache->method('invalidate');

        $future = CheckedGrpcFuture::fromRetryableDispatch($dispatch, $this->createRetryExecutor(), 'k1');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('redispatch boom');
        $future->waitForExecutor();
    }
}
