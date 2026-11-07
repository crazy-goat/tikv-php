<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreUnavailableException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ErrorClassifierTest extends TestCase
{
    /**
     * A null backoff means the built-in classifier's fatal verdict; any other
     * value must be carried by a retry verdict (issue #534).
     */
    private function assertVerdict(?BackoffType $expected, RetryVerdict $verdict): void
    {
        if (!$expected instanceof BackoffType) {
            $this->assertTrue($verdict->isFatal(), 'a null backoff means a fatal verdict');

            return;
        }

        $this->assertTrue($verdict->isRetry(), 'a non-null backoff means a retry verdict');
        $this->assertSame($expected, $verdict->backoffType());
    }

    // ========================================================================
    // Typed path: classifyByKind() — the primary classification method
    // ========================================================================

    /** @return array<string, array{ErrorKind, BackoffType|null}> */
    public static function provideErrorKindMapping(): array
    {
        return [
            // Fatal (non-retryable)
            'RaftEntryTooLarge'        => [ErrorKind::RaftEntryTooLarge, null],
            'KeyNotInRegion'           => [ErrorKind::KeyNotInRegion, null],
            'FlashbackInProgress'      => [ErrorKind::FlashbackInProgress, null],
            'FlashbackNotPrepared'     => [ErrorKind::FlashbackNotPrepared, null],

            // Small jittered backoff (no longer zero-delay, issue #241)
            'EpochNotMatch'            => [ErrorKind::EpochNotMatch, BackoffType::EpochNotMatch],

            // Region errors with specific backoff
            'ServerIsBusy'             => [ErrorKind::ServerIsBusy, BackoffType::ServerBusy],
            'StaleCommand'             => [ErrorKind::StaleCommand, BackoffType::StaleCmd],
            'RegionNotFound'           => [ErrorKind::RegionNotFound, BackoffType::RegionMiss],
            'NotLeader'                => [ErrorKind::NotLeader, BackoffType::NotLeader],
            'DiskFull'                 => [ErrorKind::DiskFull, BackoffType::DiskFull],
            'RegionNotInitialized'     => [ErrorKind::RegionNotInitialized, BackoffType::RegionNotInitialized],
            'ReadIndexNotReady'        => [ErrorKind::ReadIndexNotReady, BackoffType::ReadIndexNotReady],
            'ProposalInMergingMode'    => [ErrorKind::ProposalInMergingMode, BackoffType::ProposalInMergingMode],
            'RecoveryInProgress'       => [ErrorKind::RecoveryInProgress, BackoffType::RecoveryInProgress],
            'IsWitness'                => [ErrorKind::IsWitness, BackoffType::IsWitness],
            'MaxTimestampNotSynced'    => [ErrorKind::MaxTimestampNotSynced, BackoffType::MaxTimestampNotSynced],

            // Unmapped kinds default to RegionMiss
            'StoreNotMatch'            => [ErrorKind::StoreNotMatch, BackoffType::RegionMiss],
            // Replica-lag signal: retried immediately with a leader fallback
            // instead of a region-miss backoff (issue #421)
            'DataIsNotReady'           => [ErrorKind::DataIsNotReady, BackoffType::None],
            'MismatchPeerId'           => [ErrorKind::MismatchPeerId, BackoffType::RegionMiss],
            'BucketVersionNotMatch'    => [ErrorKind::BucketVersionNotMatch, BackoffType::RegionMiss],
            'UndeterminedResult'       => [ErrorKind::UndeterminedResult, BackoffType::RegionMiss],
        ];
    }

    #[DataProvider('provideErrorKindMapping')]
    public function testClassifyByKind(ErrorKind $kind, ?BackoffType $expected): void
    {
        $this->assertSame($expected, ErrorClassifier::classifyByKind($kind));
    }

    // ========================================================================
    // RegionException with typed ErrorKind (uses typed path in classify())
    // ========================================================================

    #[DataProvider('provideErrorKindMapping')]
    public function testRegionExceptionWithKind(ErrorKind $kind, ?BackoffType $expected): void
    {
        $error = new Error();
        $message = 'some ' . $kind->value . ' error';

        // Set the corresponding oneof field on the Error object to make
        // RegionException::fromRegionError() detect the kind.
        $setter = 'set' . str_replace('_', '', ucwords($kind->value, '_'));
        $error->setMessage($message);
        $error->$setter(new ($this->errorClassForKind($kind))());

        $exception = RegionException::fromRegionError($error);
        $this->assertVerdict($expected, ErrorClassifier::classify($exception));
    }

    /**
     * Return a protobuf message class name for the given ErrorKind.
     */
    private function errorClassForKind(ErrorKind $kind): string
    {
        return match ($kind) {
            ErrorKind::NotLeader => \CrazyGoat\Proto\Errorpb\NotLeader::class,
            ErrorKind::RegionNotFound => \CrazyGoat\Proto\Errorpb\RegionNotFound::class,
            ErrorKind::KeyNotInRegion => \CrazyGoat\Proto\Errorpb\KeyNotInRegion::class,
            ErrorKind::EpochNotMatch => \CrazyGoat\Proto\Errorpb\EpochNotMatch::class,
            ErrorKind::ServerIsBusy => \CrazyGoat\Proto\Errorpb\ServerIsBusy::class,
            ErrorKind::StaleCommand => \CrazyGoat\Proto\Errorpb\StaleCommand::class,
            ErrorKind::StoreNotMatch => \CrazyGoat\Proto\Errorpb\StoreNotMatch::class,
            ErrorKind::RaftEntryTooLarge => \CrazyGoat\Proto\Errorpb\RaftEntryTooLarge::class,
            ErrorKind::MaxTimestampNotSynced => \CrazyGoat\Proto\Errorpb\MaxTimestampNotSynced::class,
            ErrorKind::ReadIndexNotReady => \CrazyGoat\Proto\Errorpb\ReadIndexNotReady::class,
            ErrorKind::ProposalInMergingMode => \CrazyGoat\Proto\Errorpb\ProposalInMergingMode::class,
            ErrorKind::DataIsNotReady => \CrazyGoat\Proto\Errorpb\DataIsNotReady::class,
            ErrorKind::RegionNotInitialized => \CrazyGoat\Proto\Errorpb\RegionNotInitialized::class,
            ErrorKind::DiskFull => \CrazyGoat\Proto\Errorpb\DiskFull::class,
            ErrorKind::RecoveryInProgress => \CrazyGoat\Proto\Errorpb\RecoveryInProgress::class,
            ErrorKind::FlashbackInProgress => \CrazyGoat\Proto\Errorpb\FlashbackInProgress::class,
            ErrorKind::FlashbackNotPrepared => \CrazyGoat\Proto\Errorpb\FlashbackNotPrepared::class,
            ErrorKind::IsWitness => \CrazyGoat\Proto\Errorpb\IsWitness::class,
            ErrorKind::MismatchPeerId => \CrazyGoat\Proto\Errorpb\MismatchPeerId::class,
            ErrorKind::BucketVersionNotMatch => \CrazyGoat\Proto\Errorpb\BucketVersionNotMatch::class,
            ErrorKind::UndeterminedResult => \CrazyGoat\Proto\Errorpb\UndeterminedResult::class,
        };
    }

    // ========================================================================
    // ErrorKind <-> errorpb.Error oneof correspondence (issue #608)
    // ========================================================================

    /**
     * Every ErrorKind case names a oneof field of errorpb.Error, so it must
     * have a matching `has{PascalCase}()` detector. RegionException::
     * detectErrorKind() builds that method name from the case value and calls
     * it blindly, so a case without a oneof turns every
     * RegionException::fromRegionError() into a raw `\Error` ("Call to
     * undefined method") that escapes `catch (TiKvException)` — in a test run
     * that surfaces as unrelated failures scattered across suites.
     *
     * @return array<string, array{ErrorKind}>
     */
    public static function provideErrorKindCases(): array
    {
        $cases = [];
        foreach (ErrorKind::cases() as $kind) {
            $cases[$kind->name] = [$kind];
        }

        return $cases;
    }

    #[DataProvider('provideErrorKindCases')]
    public function testEveryErrorKindHasAnErrorpbOneofDetector(ErrorKind $kind): void
    {
        // Same derivation as RegionException::detectErrorKind().
        $method = 'has' . str_replace('_', '', ucwords($kind->value, '_'));

        $this->assertTrue(
            method_exists(Error::class, $method),
            sprintf(
                'ErrorKind::%s (%s) has no matching %s::%s() oneof detector; '
                . 'add the field to errorpb.Error or drop the case',
                $kind->name,
                $kind->value,
                Error::class,
                $method,
            ),
        );
    }

    /**
     * The other direction of the same contract (issue #444, DIV-08): every
     * errorpb.Error oneof must map to an ErrorKind, so a proto update that
     * adds a variant fails here instead of silently falling into the
     * bounded-retry default for unrecognised region errors.
     *
     * @return array<string, array{string}>
     */
    public static function provideErrorpbOneofDetectors(): array
    {
        $cases = [];
        foreach (get_class_methods(Error::class) as $method) {
            if (str_starts_with($method, 'has')) {
                $cases[$method] = [$method];
            }
        }

        return $cases;
    }

    #[DataProvider('provideErrorpbOneofDetectors')]
    public function testEveryErrorpbOneofMapsToAnErrorKind(string $method): void
    {
        // hasNotLeader -> NotLeader -> not_leader
        $field = substr($method, 3);
        $value = strtolower((string) preg_replace('/(?<!^)[A-Z]/', '_$0', $field));

        $this->assertNotNull(
            ErrorKind::tryFrom($value),
            sprintf(
                '%s::%s() has no matching ErrorKind case (%s); add one and '
                . 'decide its BackoffType in ErrorClassifier::classifyByKind()',
                Error::class,
                $method,
                $value,
            ),
        );
    }

    /**
     * The classification table is an array literal, so a new ErrorKind case
     * would leave it green and untested. PHPStan's match.unhandled already
     * rejects a missing errorClassForKind() arm, but it never sees this
     * provider; the guard makes the classifyByKind() decision mandatory at
     * runtime too (issue #608).
     */
    public function testErrorKindMappingCoversEveryCase(): void
    {
        $covered = array_map(
            static fn (array $row): string => $row[0]->name,
            self::provideErrorKindMapping(),
        );
        sort($covered);

        $cases = array_map(
            static fn (ErrorKind $kind): string => $kind->name,
            ErrorKind::cases(),
        );
        sort($cases);

        $this->assertSame(
            $cases,
            $covered,
            'every ErrorKind case needs a classifyByKind() expectation (issue #608)',
        );
    }

    // ========================================================================
    // Fallback: message-text matching for exceptions without typed kind
    // ========================================================================

    public function testRaftEntryTooLargeIsFatalViaMessage(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('RaftEntryTooLarge')));
    }

    public function testKeyNotInRegionIsFatalViaMessage(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('KeyNotInRegion')));
    }

    public function testFlashbackInProgressIsFatalViaMessage(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('FlashbackInProgress')));
    }

    public function testFlashbackNotPreparedIsFatalViaMessage(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('FlashbackNotPrepared')));
    }

    public function testEpochNotMatchReturnsEpochNotMatchViaMessage(): void
    {
        $this->assertVerdict(BackoffType::EpochNotMatch, ErrorClassifier::classify(new TiKvException('EpochNotMatch')));
    }

    public function testEpochNotMatchLowercaseReturnsEpochNotMatchViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::EpochNotMatch,
            ErrorClassifier::classify(new TiKvException('epoch not match')),
        );
    }

    public function testDataIsNotReadyStaysNoneViaMessage(): void
    {
        $this->assertVerdict(BackoffType::None, ErrorClassifier::classify(new TiKvException('DataIsNotReady')));
    }

    public function testServerIsBusyReturnsServerBusyViaMessage(): void
    {
        $this->assertVerdict(BackoffType::ServerBusy, ErrorClassifier::classify(new TiKvException('ServerIsBusy')));
    }

    /**
     * The library's ServerBusy-budget message says "ServerBusy", but a
     * give-up message that names the underlying "ServerIsBusy" error would
     * match the fallback and classify as retryable. The typed exception must
     * win: an outer executor must never retry a give-up (issue #256).
     */
    public function testRetryBudgetExhaustedIsFatalDespiteServerBusyMessage(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(
            new RetryBudgetExhaustedException(
                'ServerIsBusy budget (60000 ms) exhausted for key #abc123 (3 bytes)',
                0,
                60000,
            ),
        ));
    }

    public function testStaleCommandReturnsStaleCmdViaMessage(): void
    {
        $this->assertVerdict(BackoffType::StaleCmd, ErrorClassifier::classify(new TiKvException('StaleCommand')));
    }

    public function testRegionNotFoundReturnsRegionMissViaMessage(): void
    {
        $this->assertVerdict(BackoffType::RegionMiss, ErrorClassifier::classify(new TiKvException('RegionNotFound')));
    }

    public function testNotLeaderReturnsNotLeaderViaMessage(): void
    {
        $this->assertVerdict(BackoffType::NotLeader, ErrorClassifier::classify(new TiKvException('NotLeader')));
    }

    public function testDiskFullReturnsDiskFullViaMessage(): void
    {
        $this->assertVerdict(BackoffType::DiskFull, ErrorClassifier::classify(new TiKvException('DiskFull')));
    }

    public function testRegionNotInitializedReturnsRegionNotInitializedViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::RegionNotInitialized,
            ErrorClassifier::classify(new TiKvException('RegionNotInitialized')),
        );
    }

    public function testReadIndexNotReadyReturnsReadIndexNotReadyViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::ReadIndexNotReady,
            ErrorClassifier::classify(new TiKvException('ReadIndexNotReady')),
        );
    }

    public function testProposalInMergingModeReturnsProposalInMergingModeViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::ProposalInMergingMode,
            ErrorClassifier::classify(new TiKvException('ProposalInMergingMode')),
        );
    }

    public function testRecoveryInProgressReturnsRecoveryInProgressViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::RecoveryInProgress,
            ErrorClassifier::classify(new TiKvException('RecoveryInProgress')),
        );
    }

    public function testIsWitnessReturnsIsWitnessViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::IsWitness,
            ErrorClassifier::classify(new TiKvException('IsWitness')),
        );
    }

    public function testMaxTimestampNotSyncedReturnsMaxTimestampNotSyncedViaMessage(): void
    {
        $this->assertVerdict(
            BackoffType::MaxTimestampNotSynced,
            ErrorClassifier::classify(new TiKvException('MaxTimestampNotSynced')),
        );
    }

    // ========================================================================
    // Exception type based classification (fallback)
    // ========================================================================

    public function testGrpcExceptionReturnsTiKvRpc(): void
    {
        $this->assertVerdict(
            BackoffType::TiKvRpc,
            ErrorClassifier::classify(new GrpcException('connection reset', 14)),
        );
    }

    // ========================================================================
    // gRPC status code classification (issue #240, REG-09)
    // ========================================================================

    /** @return array<string, array{int, BackoffType|null}> */
    public static function provideGrpcStatusCodes(): array
    {
        return [
            // Retryable (transient / conservative)
            'UNAVAILABLE'          => [14, BackoffType::TiKvRpc],
            'ABORTED'              => [10, BackoffType::TiKvRpc],
            'INTERNAL'             => [13, BackoffType::TiKvRpc],
            'UNKNOWN'              => [2, BackoffType::TiKvRpc],
            'DEADLINE_EXCEEDED'    => [4, BackoffType::TiKvRpc],
            // Server-busy backoff
            'RESOURCE_EXHAUSTED'   => [8, BackoffType::ServerBusy],
            // Fatal (permanent for the request — must not be retried)
            'CANCELLED'            => [1, null],
            'INVALID_ARGUMENT'     => [3, null],
            'NOT_FOUND'            => [5, null],
            'PERMISSION_DENIED'    => [7, null],
            'FAILED_PRECONDITION'  => [9, null],
            'UNIMPLEMENTED'        => [12, null],
            'UNAUTHENTICATED'      => [16, null],
            'ALREADY_EXISTS'       => [6, null],
            'OUT_OF_RANGE'         => [11, null],
            'DATA_LOSS'            => [15, null],
            // Unknown codes fail closed
            'unknown code 99'      => [99, null],
        ];
    }

    #[DataProvider('provideGrpcStatusCodes')]
    public function testGrpcExceptionClassifiedByStatusCode(int $code, ?BackoffType $expected): void
    {
        $this->assertVerdict($expected, ErrorClassifier::classify(new GrpcException('status details', $code)),);
    }

    #[DataProvider('provideGrpcStatusCodes')]
    public function testClassifyGrpcStatusDirectly(int $code, ?BackoffType $expected): void
    {
        $this->assertSame($expected, ErrorClassifier::classifyGrpcStatus($code));
    }

    public function testGrpcExceptionWithRetryKeywordInDetailsIsStillClassifiedByStatusCode(): void
    {
        // gRPC details text must not be able to smuggle a message-based
        // classification: status-code classification runs first.
        $this->assertVerdict(null, ErrorClassifier::classify(new GrpcException('NotLeader for UNAVAILABLE', 16)),);
    }

    public function testRegionExceptionWithoutKindGetsBoundedRegionMissRetry(): void
    {
        // Issue #444: an error variant the enum does not know yet is retried
        // on a bounded RegionMiss curve instead of failing immediately. The
        // retry path invalidates the cached region for a kind-less
        // RegionException, so the next attempt re-resolves it.
        $this->assertVerdict(
            BackoffType::RegionMiss,
            ErrorClassifier::classify(new RegionException('test', 'region error', fromErrorpb: true)),
        );
    }

    /**
     * The bounded retry is only reachable through the real factory, so pin
     * the flag there (issue #444). An Error with a message but no oneof is
     * the "variant this client does not know yet" case; if fromRegionError()
     * ever stops setting fromErrorpb, this test fails instead of every
     * synthetic-input test staying green.
     */
    public function testFromRegionErrorSetsFlagAndGetsBoundedRetry(): void
    {
        $error = new Error();
        $error->setMessage('a region error variant this client does not know');

        $exception = RegionException::fromRegionError($error);

        $this->assertTrue($exception->fromErrorpb);
        $this->assertNull($exception->errorKind);
        $this->assertVerdict(BackoffType::RegionMiss, ErrorClassifier::classify($exception));
    }

    public function testSyntheticRegionExceptionWithoutKindStaysFatal(): void
    {
        // A response-level error string or a top-level KeyError is wrapped in
        // a RegionException but is not a region error, so it must not inherit
        // the bounded retry (issue #251, REG-20).
        $this->assertVerdict(
            null,
            ErrorClassifier::classify(new RegionException('BatchRequest', 'ttl is not enabled')),
        );
    }

    /**
     * @return array<string, array{string}>
     */
    public static function provideRegionErrorMessageKeywords(): array
    {
        return [
            'NotLeader' => ['NotLeader'],
            'EpochNotMatch' => ['EpochNotMatch'],
            'ServerIsBusy' => ['ServerIsBusy'],
            'RaftEntryTooLarge' => ['RaftEntryTooLarge'],
            'KeyNotInRegion' => ['KeyNotInRegion'],
            'DataIsNotReady' => ['DataIsNotReady'],
        ];
    }

    /**
     * Issue #444 (DIV-08): message-string matching must not apply to a
     * RegionException. Whatever the text says, a kind-less region error is
     * the same bounded RegionMiss retry — a TiKV wording change or a key that
     * happens to contain a keyword cannot reclassify it.
     */
    #[DataProvider('provideRegionErrorMessageKeywords')]
    public function testRegionExceptionWithoutKindIgnoresMessageKeywords(string $keyword): void
    {
        $this->assertVerdict(
            BackoffType::RegionMiss,
            ErrorClassifier::classify(
                new RegionException('test', 'some ' . $keyword . ' error', fromErrorpb: true),
            ),
        );
    }

    public function testKeyErrorExceptionIsFatal(): void
    {
        $this->assertVerdict(
            null,
            ErrorClassifier::classify(new KeyErrorException('per-pair error for key k: retryable: too old')),
        );
    }

    public function testCasIndeterminateExceptionIsFatalRegardlessOfMessage(): void
    {
        // The message embeds a redacted key, and KeyRedactor::setRedactor() is
        // a public extension point: a custom redactor can put any text there.
        // "Never retried" must be carried by the type, not the message.
        $this->assertVerdict(null, ErrorClassifier::classify(new CasIndeterminateException(
            'RawCompareAndSwap failed for key EpochNotMatch NotLeader ServerIsBusy;'
                . ' the swap outcome is indeterminate',
            new GrpcException('Deadline Exceeded', 4),
        )),);
    }

    // ========================================================================
    // Unknown errors
    // ========================================================================

    public function testUnknownErrorMessageReturnsNull(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('SomeUnknownError')));
    }

    public function testEmptyMessageReturnsNull(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(new TiKvException('')));
    }

    // ========================================================================
    // Invalid store address: always fatal, before any message matching
    // ========================================================================

    public function testInvalidStoreAddressExceptionIsAlwaysFatal(): void
    {
        $this->assertVerdict(null, ErrorClassifier::classify(
            new InvalidStoreAddressException('PD returned store address "127.0.0.1:20160" outside the allowed set'),
        ));
    }

    public function testStoreUnavailableExceptionIsRetriedAsTransportError(): void
    {
        $this->assertVerdict(
            BackoffType::TiKvRpc,
            ErrorClassifier::classify(new StoreUnavailableException(7)),
        );
    }

    /**
     * The raw address inside the exception message may contain retry
     * keywords; the early instanceof check must win over message matching so
     * the rejection is never retried.
     *
     * @return array<string, array{string}>
     */
    public static function provideRetryKeywords(): array
    {
        return [
            'EpochNotMatch' => ['EpochNotMatch'],
            'epoch not match lowercase' => ['epoch not match'],
            'ServerIsBusy' => ['ServerIsBusy'],
            'StaleCommand' => ['StaleCommand'],
            'RegionNotFound' => ['RegionNotFound'],
            'NotLeader' => ['NotLeader'],
            'DiskFull' => ['DiskFull'],
            'RegionNotInitialized' => ['RegionNotInitialized'],
            'ReadIndexNotReady' => ['ReadIndexNotReady'],
            'ProposalInMergingMode' => ['ProposalInMergingMode'],
            'RecoveryInProgress' => ['RecoveryInProgress'],
            'IsWitness' => ['IsWitness'],
            'MaxTimestampNotSynced' => ['MaxTimestampNotSynced'],
            'RaftEntryTooLarge' => ['RaftEntryTooLarge'],
            'KeyNotInRegion' => ['KeyNotInRegion'],
        ];
    }

    #[DataProvider('provideRetryKeywords')]
    public function testInvalidStoreAddressContainingRetryKeywordIsFatal(string $keyword): void
    {
        $exception = new InvalidStoreAddressException(sprintf(
            'PD returned store address "%s-host:20160" for store 1 outside the allowed set',
            $keyword,
        ));

        $this->assertVerdict(null, ErrorClassifier::classify($exception));
    }
}
