<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\WriteConflict;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class RegionErrorHandlerTest extends TestCase
{
    public function testNoExceptionWhenResponseHasNoRegionError(): void
    {
        $response = new RawPutResponse();

        RegionErrorHandler::check($response);

        self::assertNull($response->getRegionError());
    }

    public function testNoExceptionWhenRegionErrorIsNull(): void
    {
        $response = new RawGetResponse();

        RegionErrorHandler::check($response);

        self::assertNull($response->getRegionError());
    }

    public function testThrowsRegionExceptionOnNotLeaderWithHint(): void
    {
        $leader = new Peer();
        $leader->setId(20);
        $leader->setStoreId(3);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            $this->assertNotNull($e->notLeader);
            $this->assertNotNull($e->notLeader->getLeader());
            $this->assertSame(3, (int) $e->notLeader->getLeader()->getStoreId());
            $this->assertSame(42, (int) $e->notLeader->getRegionId());
        }
    }

    public function testThrowsRegionExceptionOnNotLeaderWithoutHint(): void
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            $this->assertNotNull($e->notLeader);
            $this->assertNull($e->notLeader->getLeader());
        }
    }

    public function testThrowsRegionExceptionOnOtherRegionError(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('RegionError failed: epoch not match');

        RegionErrorHandler::check($response);
    }

    public function testNoExceptionForObjectWithoutGetRegionErrorMethod(): void
    {
        $response = new \stdClass();

        RegionErrorHandler::check($response);

        self::assertInstanceOf(\stdClass::class, $response);
    }

    // ========================================================================
    // Invalidation ownership (issue #474): the cache emits the
    // regionInvalidated() metric itself. At executor-owned call sites
    // (default) NotLeader oneofs are left cached for
    // RetryExecutor::handleNotLeader() — its drops alone; every other
    // region error invalidates here as 'region_error'. Un-owned sites pass
    // notLeaderOwnedByRetryExecutor: false so a NotLeader error still
    // self-invalidates ('not_leader') instead of stranding a stale entry.
    // ========================================================================

    public function testNotLeaderRegionErrorLeavesRegionCachedForHandleNotLeader(): void
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        try {
            RegionErrorHandler::check($response, $cache, 42);
            $this->fail('Expected RegionException');
        } catch (RegionException) {
            // expected — but the region must stay cached
        }
    }

    public function testNotLeaderInvalidatesWhenNotOwnedByRetryExecutor(): void
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->once())
            ->method('invalidate')
            ->with(42, 'not_leader');

        try {
            RegionErrorHandler::check($response, $cache, 42, notLeaderOwnedByRetryExecutor: false);
            $this->fail('Expected RegionException');
        } catch (RegionException) {
            // expected — invalidation happens before the throw
        }
    }

    public function testRegionErrorInvalidatesWithPlainRegionErrorReasonOtherwise(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->once())
            ->method('invalidate')
            ->with(7, 'region_error');

        try {
            RegionErrorHandler::check($response, $cache, 7);
            $this->fail('Expected RegionException');
        } catch (RegionException) {
            // expected
        }
    }

    // ========================================================================
    // Stage 1 — epoch not match invalidates BEFORE throwing (issue #335)
    // ========================================================================

    public function testRegionErrorInvalidatesCachedRegionBeforeThrowing(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $invalidatedBeforeThrow = false;
        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->once())
            ->method('invalidate')
            ->with(42, 'region_error')
            ->willReturnCallback(static function () use (&$invalidatedBeforeThrow): void {
                $invalidatedBeforeThrow = true;
            });

        try {
            RegionErrorHandler::check($response, $cache, 42);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            self::assertTrue($invalidatedBeforeThrow, 'invalidate() must be called before throw');
            self::assertStringContainsString('epoch not match', $e->getMessage());
        }
    }

    public function testRegionErrorWithoutCacheDoesNotThrowFromInvalidation(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('epoch not match');

        RegionErrorHandler::check($response, null, 42);
    }

    // ========================================================================
    // Stage 2 — top-level error string (RawBatchPut / RawBatchDelete)
    // ========================================================================

    public function testBatchErrorStringThrowsRegionException(): void
    {
        $response = new RawBatchPutResponse();
        $response->setError('server is busy');

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            self::assertStringContainsString('BatchRequest', $e->getMessage());
            self::assertStringContainsString('server is busy', $e->getMessage());
        }
    }

    public function testBatchDeleteErrorStringThrowsRegionException(): void
    {
        $response = new RawBatchDeleteResponse();
        $response->setError('server is busy');

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            self::assertStringContainsString('BatchRequest', $e->getMessage());
            self::assertStringContainsString('server is busy', $e->getMessage());
        }
    }

    public function testEmptyBatchErrorStringDoesNotThrow(): void
    {
        $response = new RawBatchPutResponse();
        $response->setError('');

        RegionErrorHandler::check($response);

        self::assertSame('', $response->getError());
    }

    public function testEmptyBatchDeleteErrorStringDoesNotThrow(): void
    {
        $response = new RawBatchDeleteResponse();
        $response->setError('');

        RegionErrorHandler::check($response);

        self::assertSame('', $response->getError());
    }

    // ========================================================================
    // Stage 3 — per-pair KeyError (RawBatchGetResponse) via DataProvider
    // ========================================================================

    #[DataProvider('keyErrorProvider')]
    public function testPerPairKeyErrorDescribesTheFailure(
        callable $buildKeyError,
        string $expectedFragment,
        string $key = 'test-key',
    ): void {
        $keyError = new KeyError();
        $buildKeyError($keyError);

        $pair = new KvPair();
        $pair->setKey($key);
        $pair->setError($keyError);
        $pair->setValue('');

        $response = new RawBatchGetResponse();
        $response->setPairs([$pair]);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException for per-pair KeyError');
        } catch (RegionException $e) {
            self::assertStringContainsString('BatchGet', $e->getMessage());
            self::assertStringContainsString($expectedFragment, $e->getMessage());
            // binary-safe: message must contain the key bytes.
            self::assertTrue(str_contains($e->getMessage(), $key));
        }
    }

    /**
     * @return iterable<string, array<int, mixed>>
     */
    public static function keyErrorProvider(): iterable
    {
        yield 'retryable' => [
            static function (KeyError $e): void {
                $e->setRetryable('too old');
            },
            'retryable: too old',
        ];

        yield 'abort' => [
            static function (KeyError $e): void {
                $e->setAbort('aborted');
            },
            'abort: aborted',
        ];

        yield 'locked with binary key' => [
            static function (KeyError $e): void {
                $lock = new LockInfo();
                $lock->setKey("\x00\xffuser");
                $lock->setLockVersion(1);
                $e->setLocked($lock);
            },
            'Locked',
            "\x00\xffuser",
        ];

        yield 'conflict' => [
            static function (KeyError $e): void {
                $e->setConflict(new WriteConflict());
            },
            'Conflict',
        ];

        yield 'deadlock' => [
            static function (KeyError $e): void {
                $e->setDeadlock(new Deadlock());
            },
            'Deadlock',
        ];

        yield 'empty key error' => [
            static function (KeyError $e): void {
                // leave $e empty — no fields set
            },
            'unknown error',
        ];
    }

    public function testPerPairWithoutErrorDoesNotThrow(): void
    {
        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        $response = new RawBatchGetResponse();
        $response->setPairs([$pair]);

        RegionErrorHandler::check($response);

        self::assertFalse($pair->hasError());
    }

    public function testPerPairKeyErrorWithNullKeyErrorDescribesNull(): void
    {
        // describeKeyError(null) is only reachable via a pair whose
        // getError() returns null while hasError() is true — that cannot be
        // constructed with the generated KvPair (setError(null) clears isset).
        // Verify the helper directly via reflection to cover the null branch.
        $method = new \ReflectionMethod(RegionErrorHandler::class, 'describeKeyError');
        $result = $method->invoke(null, "\x00\xffuser", null);

        self::assertIsString($result);
        self::assertStringContainsString('null', $result);
        self::assertTrue(str_contains($result, "\x00\xffuser"));
    }

    public function testPerPairBatchGetWithMultiplePairsThrowsOnFirstError(): void
    {
        $okPair = new KvPair();
        $okPair->setKey('ok');
        $okPair->setValue('v');

        $errPair = new KvPair();
        $errPair->setKey('bad');
        $ke = new KeyError();
        $ke->setRetryable('too old');
        $errPair->setError($ke);
        $errPair->setValue('');

        $response = new RawBatchGetResponse();
        $response->setPairs([$okPair, $errPair]);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('retryable: too old');

        RegionErrorHandler::check($response);
    }
}
