<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\TestCase;

class ErrorClassifierTest extends TestCase
{
    // ========================================================================
    // Fatal errors (non-retryable) — should return null
    // ========================================================================

    public function testRaftEntryTooLargeIsFatal(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('RaftEntryTooLarge')));
    }

    public function testKeyNotInRegionIsFatal(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('KeyNotInRegion')));
    }

    public function testFlashbackInProgressIsFatal(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('FlashbackInProgress')));
    }

    public function testFlashbackNotPreparedIsFatal(): void
    {
        $this->assertNull(ErrorClassifier::classify(new TiKvException('FlashbackNotPrepared')));
    }

    // ========================================================================
    // Immediate retry (no backoff delay)
    // ========================================================================

    public function testEpochNotMatchReturnsNone(): void
    {
        $this->assertSame(BackoffType::None, ErrorClassifier::classify(new TiKvException('EpochNotMatch')));
    }

    public function testEpochNotMatchLowercaseReturnsNone(): void
    {
        $this->assertSame(BackoffType::None, ErrorClassifier::classify(new TiKvException('epoch not match')));
    }

    // ========================================================================
    // Region errors with specific backoff types
    // ========================================================================

    public function testServerIsBusyReturnsServerBusy(): void
    {
        $this->assertSame(BackoffType::ServerBusy, ErrorClassifier::classify(new TiKvException('ServerIsBusy')));
    }

    public function testStaleCommandReturnsStaleCmd(): void
    {
        $this->assertSame(BackoffType::StaleCmd, ErrorClassifier::classify(new TiKvException('StaleCommand')));
    }

    public function testRegionNotFoundReturnsRegionMiss(): void
    {
        $this->assertSame(BackoffType::RegionMiss, ErrorClassifier::classify(new TiKvException('RegionNotFound')));
    }

    public function testNotLeaderReturnsNotLeader(): void
    {
        $this->assertSame(BackoffType::NotLeader, ErrorClassifier::classify(new TiKvException('NotLeader')));
    }

    public function testDiskFullReturnsDiskFull(): void
    {
        $this->assertSame(BackoffType::DiskFull, ErrorClassifier::classify(new TiKvException('DiskFull')));
    }

    public function testRegionNotInitializedReturnsRegionNotInitialized(): void
    {
        $this->assertSame(BackoffType::RegionNotInitialized, ErrorClassifier::classify(new TiKvException('RegionNotInitialized')));
    }

    public function testReadIndexNotReadyReturnsReadIndexNotReady(): void
    {
        $this->assertSame(BackoffType::ReadIndexNotReady, ErrorClassifier::classify(new TiKvException('ReadIndexNotReady')));
    }

    public function testProposalInMergingModeReturnsProposalInMergingMode(): void
    {
        $this->assertSame(BackoffType::ProposalInMergingMode, ErrorClassifier::classify(new TiKvException('ProposalInMergingMode')));
    }

    public function testRecoveryInProgressReturnsRecoveryInProgress(): void
    {
        $this->assertSame(BackoffType::RecoveryInProgress, ErrorClassifier::classify(new TiKvException('RecoveryInProgress')));
    }

    public function testIsWitnessReturnsIsWitness(): void
    {
        $this->assertSame(BackoffType::IsWitness, ErrorClassifier::classify(new TiKvException('IsWitness')));
    }

    public function testMaxTimestampNotSyncedReturnsMaxTimestampNotSynced(): void
    {
        $this->assertSame(BackoffType::MaxTimestampNotSynced, ErrorClassifier::classify(new TiKvException('MaxTimestampNotSynced')));
    }

    // ========================================================================
    // Exception type based classification
    // ========================================================================

    public function testGrpcExceptionReturnsTiKvRpc(): void
    {
        $this->assertSame(BackoffType::TiKvRpc, ErrorClassifier::classify(new GrpcException('connection reset', 14)));
    }

    public function testRegionExceptionReturnsRegionMiss(): void
    {
        $this->assertSame(BackoffType::RegionMiss, ErrorClassifier::classify(new RegionException('test', 'region error')));
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
