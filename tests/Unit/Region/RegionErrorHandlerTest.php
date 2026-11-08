<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Kvrpcpb\WriteConflict;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class RegionErrorHandlerTest extends TestCase
{
    public function testNoExceptionWhenResponseHasNoRegionError(): void
    {
        $response = new RawPutResponse();

        // A clean response must never drop the region from the cache: an
        // invalidation here would flush the cache on every healthy RPC.
        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

        self::assertNull($response->getRegionError());
    }

    public function testNoExceptionWhenRegionErrorIsNull(): void
    {
        $response = new RawGetResponse();

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

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

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

        self::assertInstanceOf(\stdClass::class, $response);
    }

    // ========================================================================
    // Invalidation ownership (issue #474): the cache emits the
    // regionInvalidated() metric itself. At executor-owned call sites
    // (default) NotLeader oneofs are left cached for
    // RetryExecutor::handleNotLeader() — its drops alone; every other
    // routing region error invalidates here as 'region_error:<kind>' and an
    // unclassified one as 'region_error' (issue #245). Un-owned sites pass
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

    public function testRegionErrorInvalidatesWithKindTaggedReasonOtherwise(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->once())
            ->method('invalidate')
            ->with(7, 'region_error:epoch_not_match');

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
            ->with(42, 'region_error:epoch_not_match')
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

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

        self::assertSame('', $response->getError());
    }

    public function testEmptyBatchDeleteErrorStringDoesNotThrow(): void
    {
        $response = new RawBatchDeleteResponse();
        $response->setError('');

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

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
        } catch (KeyErrorException $e) {
            self::assertStringContainsString('BatchGet', $e->getMessage());
            self::assertStringContainsString($expectedFragment, $e->getMessage());
            // Raw key bytes must never reach the exception message; only the
            // redacted representation is allowed (issue #269, GRPC-10).
            self::assertStringNotContainsString($key, $e->getMessage());
            self::assertStringContainsString(KeyRedactor::redact($key), $e->getMessage());
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

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42);

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
        self::assertStringNotContainsString("\x00\xffuser", $result);
        self::assertStringContainsString(KeyRedactor::redact("\x00\xffuser"), $result);
    }

    /**
     * Regression test for issue #269 (GRPC-10): a per-pair KeyError whose key
     * embeds personal data must surface a redacted key in the RegionException
     * message thrown by check(), never the raw key.
     */
    public function testPerPairKeyErrorRedactsRawKeyInExceptionMessage(): void
    {
        $rawKey = 'user_email:alice@example.com';

        $keyError = new KeyError();
        $keyError->setRetryable('too old');

        $pair = new KvPair();
        $pair->setKey($rawKey);
        $pair->setError($keyError);
        $pair->setValue('');

        $response = new RawBatchGetResponse();
        $response->setPairs([$pair]);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException for per-pair KeyError');
        } catch (KeyErrorException $e) {
            $message = $e->getMessage();
            $this->assertStringNotContainsString($rawKey, $message);
            $this->assertStringContainsString(KeyRedactor::redact($rawKey), $message);
            $this->assertStringContainsString('per-pair error for key', $message);
            $this->assertStringContainsString('retryable: too old', $message);
        }
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

        $this->expectException(KeyErrorException::class);
        $this->expectExceptionMessage('retryable: too old');

        RegionErrorHandler::check($response);
    }

    private function errorPair(string $key): KvPair
    {
        $keyError = new KeyError();
        $keyError->setRetryable('too old');
        $pair = new KvPair();
        $pair->setKey($key);
        $pair->setError($keyError);

        return $pair;
    }

    private function kvPair(string $key, string $value): KvPair
    {
        $pair = new KvPair();
        $pair->setKey($key);
        $pair->setValue($value);

        return $pair;
    }

    public function testTopLevelKeyErrorOnBatchGetResponseThrows(): void
    {
        $keyError = new KeyError();
        $keyError->setRetryable('conflict');
        $response = new BatchGetResponse();
        $response->setError($keyError);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('conflict');

        RegionErrorHandler::check($response);
    }

    public function testTopLevelKeyErrorIsLeftToCallerWhenHandledByCaller(): void
    {
        $keyError = new KeyError();
        $keyError->setRetryable('conflict');
        $response = new BatchGetResponse();
        $response->setError($keyError);
        $response->setPairs([$this->errorPair('k')]);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42, keyErrorsHandledByCaller: true);

        self::assertSame($keyError, $response->getError());
    }

    public function testPerPairKeyErrorOnRawScanResponseThrows(): void
    {
        $response = new RawScanResponse();
        $response->setKvs([$this->errorPair('scan-key')]);

        $this->expectException(KeyErrorException::class);
        $this->expectExceptionMessage('per-pair error');

        RegionErrorHandler::check($response);
    }

    public function testPerPairKeyErrorOnRawBatchScanResponseThrows(): void
    {
        $response = new RawBatchScanResponse();
        $response->setKvs([$this->errorPair('scan-key')]);

        $this->expectException(KeyErrorException::class);

        RegionErrorHandler::check($response);
    }

    public function testPerPairKeyErrorOnScanResponseThrows(): void
    {
        $response = new ScanResponse();
        $response->setPairs([$this->errorPair('txn-key')]);

        $this->expectException(KeyErrorException::class);
        $this->expectExceptionMessage('per-pair error');

        RegionErrorHandler::check($response);
    }

    public function testPerPairKeyErrorOnBatchGetResponseThrows(): void
    {
        $response = new BatchGetResponse();
        $response->setPairs([$this->errorPair('txn-key')]);

        $this->expectException(KeyErrorException::class);

        RegionErrorHandler::check($response);
    }

    public function testPerPairKeyErrorIsLeftToCallerWhenHandledByCaller(): void
    {
        $response = new ScanResponse();
        $response->setPairs([$this->errorPair('txn-key')]);

        $cache = $this->createMock(RegionCacheInterface::class);
        $cache->expects($this->never())->method('invalidate');

        RegionErrorHandler::check($response, $cache, 42, keyErrorsHandledByCaller: true);

        self::assertCount(1, $response->getPairs());
    }

    public function testRegionErrorStillThrowsWhenKeyErrorsAreHandledByCaller(): void
    {
        $error = new Error();
        $error->setMessage('boom');
        $response = new ScanResponse();
        $response->setRegionError($error);

        $this->expectException(RegionException::class);

        RegionErrorHandler::check($response, keyErrorsHandledByCaller: true);
    }

    // ========================================================================
    // Issue #303 (PERF-16): one traversal for the batch-get pair check and
    // value collection, and no reflection on the response path.
    // ========================================================================

    public function testCheckAndCollectBatchGetReturnsKeyValueMap(): void
    {
        $response = new RawBatchGetResponse();
        $response->setPairs([$this->kvPair('k1', 'v1'), $this->kvPair('k2', 'v2')]);

        self::assertSame(
            ['k1' => 'v1', 'k2' => 'v2'],
            RegionErrorHandler::checkAndCollectBatchGet($response),
        );
    }

    public function testCheckAndCollectBatchGetThrowsOnPerPairError(): void
    {
        $response = new RawBatchGetResponse();
        $response->setPairs([$this->kvPair('ok', 'v'), $this->errorPair('bad')]);

        $this->expectException(KeyErrorException::class);
        $this->expectExceptionMessage('BatchGet failed');

        RegionErrorHandler::checkAndCollectBatchGet($response);
    }

    public function testCheckAndCollectBatchGetChecksTheRegionErrorBeforeThePairs(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawBatchGetResponse();
        $response->setRegionError($error);
        $response->setPairs([$this->errorPair('bad')]);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('epoch not match');

        RegionErrorHandler::checkAndCollectBatchGet($response);
    }

    public function testCheckCanDeferThePairCheckToTheCollector(): void
    {
        $response = new RawBatchGetResponse();
        $response->setPairs([$this->errorPair('bad')]);

        // Deferred: the caller's collection loop owns the per-pair check.
        RegionErrorHandler::check($response, checkPairErrors: false);

        $this->expectException(KeyErrorException::class);
        RegionErrorHandler::collectBatchGetPairs($response);
    }

    public function testCollectBatchGetPairsReturnsTheValues(): void
    {
        $response = new RawBatchGetResponse();
        $response->setPairs([$this->kvPair('k1', 'v1')]);

        self::assertSame(['k1' => 'v1'], RegionErrorHandler::collectBatchGetPairs($response));
    }

    public function testRegionErrorHandlerSourceHasNoMethodExistsLookup(): void
    {
        $file = (new \ReflectionClass(RegionErrorHandler::class))->getFileName();
        self::assertIsString($file);
        $source = file_get_contents($file);
        self::assertIsString($source);

        self::assertStringNotContainsString(
            'method_exists(',
            $source,
            'Issue #303: the response path must use instanceof, not reflection.',
        );
    }

    // ========================================================================
    // Issue #303: the closed set is enumerated, so dropping a response type
    // from either match fails the suite instead of silently treating that
    // type's region/top-level error as clean.
    // ========================================================================

    /**
     * The 24 response types the region-error match in
     * RegionErrorHandler::check() covers, in the same order as the match.
     *
     * The list is typed as `class-string` and the setter is invoked through
     * reflection on purpose: the generated protobuf classes share no
     * interface, and the point of the test is the list, not a static type.
     *
     * @return iterable<string, array{object}>
     */
    public static function regionErrorResponseProvider(): iterable
    {
        $error = new Error();
        $error->setMessage('region boom');

        /** @var list<class-string> $types */
        $types = [
            RawGetResponse::class,
            RawPutResponse::class,
            RawDeleteResponse::class,
            RawGetKeyTTLResponse::class,
            RawCASResponse::class,
            RawDeleteRangeResponse::class,
            RawChecksumResponse::class,
            RawScanResponse::class,
            RawBatchGetResponse::class,
            RawBatchPutResponse::class,
            RawBatchDeleteResponse::class,
            RawBatchScanResponse::class,
            GetResponse::class,
            BatchGetResponse::class,
            ScanResponse::class,
            CommitResponse::class,
            BatchRollbackResponse::class,
            CheckTxnStatusResponse::class,
            CheckSecondaryLocksResponse::class,
            ResolveLockResponse::class,
            TxnHeartBeatResponse::class,
            PrewriteResponse::class,
            PessimisticLockResponse::class,
            PessimisticRollbackResponse::class,
        ];

        foreach ($types as $type) {
            yield $type => [self::responseWith($type, 'setRegionError', $error)];
        }
    }

    /**
     * The 18 response types that expose a singular getError(): nine carry a
     * string error and nine a KeyError.
     *
     * @return iterable<string, array{object}>
     */
    public static function topLevelErrorResponseProvider(): iterable
    {
        /** @var list<class-string> $stringTypes */
        $stringTypes = [
            RawGetResponse::class,
            RawPutResponse::class,
            RawDeleteResponse::class,
            RawGetKeyTTLResponse::class,
            RawCASResponse::class,
            RawDeleteRangeResponse::class,
            RawChecksumResponse::class,
            RawBatchPutResponse::class,
            RawBatchDeleteResponse::class,
        ];

        foreach ($stringTypes as $type) {
            yield $type . ' (string)' => [self::responseWith($type, 'setError', 'top-level boom')];
        }

        $keyError = new KeyError();
        $keyError->setRetryable('too old');

        /** @var list<class-string> $keyErrorTypes */
        $keyErrorTypes = [
            GetResponse::class,
            BatchGetResponse::class,
            ScanResponse::class,
            CommitResponse::class,
            BatchRollbackResponse::class,
            CheckTxnStatusResponse::class,
            CheckSecondaryLocksResponse::class,
            ResolveLockResponse::class,
            TxnHeartBeatResponse::class,
        ];

        foreach ($keyErrorTypes as $type) {
            yield $type . ' (KeyError)' => [self::responseWith($type, 'setError', $keyError)];
        }
    }

    /**
     * @param class-string $type
     */
    private static function responseWith(string $type, string $setter, Error|string|KeyError $value): object
    {
        $response = new $type();
        (new \ReflectionMethod($type, $setter))->invoke($response, $value);

        return $response;
    }

    #[DataProvider('regionErrorResponseProvider')]
    public function testEveryRegionErrorResponseTypeThrows(object $response): void
    {
        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('region boom');

        RegionErrorHandler::check($response);
    }

    #[DataProvider('topLevelErrorResponseProvider')]
    public function testEveryTopLevelErrorResponseTypeThrows(object $response): void
    {
        $this->expectException(RegionException::class);

        RegionErrorHandler::check($response);
    }

    public function testRegionErrorClosedSetHas24Types(): void
    {
        self::assertCount(24, iterator_to_array(self::regionErrorResponseProvider()));
    }

    public function testTopLevelErrorClosedSetHas18Types(): void
    {
        self::assertCount(18, iterator_to_array(self::topLevelErrorResponseProvider()));
    }
}
