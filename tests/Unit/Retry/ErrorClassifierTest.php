<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use PHPUnit\Framework\TestCase;

class ErrorClassifierTest extends TestCase
{
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

            // Immediate retry (no delay)
            'EpochNotMatch'            => [ErrorKind::EpochNotMatch, BackoffType::None],

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
            'DataIsNotReady'           => [ErrorKind::DataIsNotReady, BackoffType::RegionMiss],
            'MismatchPeerId'           => [ErrorKind::MismatchPeerId, BackoffType::RegionMiss],
            'BucketVersionNotMatch'    => [ErrorKind::BucketVersionNotMatch, BackoffType::RegionMiss],
            'UndeterminedResult'       => [ErrorKind::UndeterminedResult, BackoffType::RegionMiss],
        ];
    }

    /** @dataProvider provideErrorKindMapping */
    public function testClassifyByKind(ErrorKind $kind, ?BackoffType $expected): void
    {
        $this->assertSame($expected, ErrorClassifier::classifyByKind($kind));
    }

    // ========================================================================
    // RegionException with typed ErrorKind (uses typed path in classify())
    // ========================================================================

    /** @dataProvider provideErrorKindMapping */
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
        $this->assertSame($expected, ErrorClassifier::classify($exception));
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
    // Fallback: message-text matching for exceptions without typed kind
    // ========================================================================

    public function testRaftEntryTooLargeIsFatalViaMessage(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('RaftEntryTooLarge')));
    }

    public function testKeyNotInRegionIsFatalViaMessage(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('KeyNotInRegion')));
    }

    public function testFlashbackInProgressIsFatalViaMessage(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('FlashbackInProgress')));
    }

    public function testFlashbackNotPreparedIsFatalViaMessage(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('FlashbackNotPrepared')));
    }

    public function testEpochNotMatchReturnsNoneViaMessage(): void
    {
        $this->assertSame(BackoffType::None, ErrorClassifier::classify(new TiKvException('EpochNotMatch')));
    }

    public function testEpochNotMatchLowercaseReturnsNoneViaMessage(): void
    {
        $this->assertSame(BackoffType::None, ErrorClassifier::classify(new TiKvException('epoch not match')));
    }

    public function testServerIsBusyReturnsServerBusyViaMessage(): void
    {
        $this->assertSame(BackoffType::ServerBusy, ErrorClassifier::classify(new TiKvException('ServerIsBusy')));
    }

    public function testStaleCommandReturnsStaleCmdViaMessage(): void
    {
        $this->assertSame(BackoffType::StaleCmd, ErrorClassifier::classify(new TiKvException('StaleCommand')));
    }

    public function testRegionNotFoundReturnsRegionMissViaMessage(): void
    {
        $this->assertSame(BackoffType::RegionMiss, ErrorClassifier::classify(new TiKvException('RegionNotFound')));
    }

    public function testNotLeaderReturnsNotLeaderViaMessage(): void
    {
        $this->assertSame(BackoffType::NotLeader, ErrorClassifier::classify(new TiKvException('NotLeader')));
    }

    public function testDiskFullReturnsDiskFullViaMessage(): void
    {
        $this->assertSame(BackoffType::DiskFull, ErrorClassifier::classify(new TiKvException('DiskFull')));
    }

    public function testRegionNotInitializedReturnsRegionNotInitializedViaMessage(): void
    {
        $this->assertSame(
            BackoffType::RegionNotInitialized,
            ErrorClassifier::classify(new TiKvException('RegionNotInitialized')),
        );
    }

    public function testReadIndexNotReadyReturnsReadIndexNotReadyViaMessage(): void
    {
        $this->assertSame(
            BackoffType::ReadIndexNotReady,
            ErrorClassifier::classify(new TiKvException('ReadIndexNotReady')),
        );
    }

    public function testProposalInMergingModeReturnsProposalInMergingModeViaMessage(): void
    {
        $this->assertSame(
            BackoffType::ProposalInMergingMode,
            ErrorClassifier::classify(new TiKvException('ProposalInMergingMode')),
        );
    }

    public function testRecoveryInProgressReturnsRecoveryInProgressViaMessage(): void
    {
        $this->assertSame(
            BackoffType::RecoveryInProgress,
            ErrorClassifier::classify(new TiKvException('RecoveryInProgress')),
        );
    }

    public function testIsWitnessReturnsIsWitnessViaMessage(): void
    {
        $this->assertSame(
            BackoffType::IsWitness,
            ErrorClassifier::classify(new TiKvException('IsWitness')),
        );
    }

    public function testMaxTimestampNotSyncedReturnsMaxTimestampNotSyncedViaMessage(): void
    {
        $this->assertSame(
            BackoffType::MaxTimestampNotSynced,
            ErrorClassifier::classify(new TiKvException('MaxTimestampNotSynced')),
        );
    }

    // ========================================================================
    // Exception type based classification (fallback)
    // ========================================================================

    public function testGrpcExceptionReturnsTiKvRpc(): void
    {
        $this->assertSame(
            BackoffType::TiKvRpc,
            ErrorClassifier::classify(new GrpcException('connection reset', 14)),
        );
    }

    public function testRegionExceptionWithoutKindReturnsRegionMiss(): void
    {
        $this->assertSame(
            BackoffType::RegionMiss,
            ErrorClassifier::classify(new RegionException('test', 'region error')),
        );
    }

    // ========================================================================
    // Unknown errors
    // ========================================================================

    public function testUnknownErrorMessageReturnsNull(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('SomeUnknownError')));
    }

    public function testEmptyMessageReturnsNull(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('')));
    }
}
