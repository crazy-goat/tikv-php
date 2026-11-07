<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\BatchCallId;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\TestCase;

/**
 * Issue #285 (GRPC-26): the executor must report the real TiKV region ID and
 * the sub-batch ordinal supplied through the `$labels` map, never the bare
 * fan-out slot index. Two sub-batches of one region must stay distinct.
 */
final class BatchAsyncExecutorLabelsTest extends TestCase
{
    public function testFailureReportsTheRealRegionIdAndSubBatchIndex(): void
    {
        $logger = new RecordingLogger();
        $executor = new BatchAsyncExecutor($logger);

        $calls = [
            0 => static fn(): string => 'ok',
            1 => static fn() => throw new TiKvException('region 11 is down'),
        ];
        $labels = [
            0 => new BatchCallId(4, 0),
            1 => new BatchCallId(11, 0),
        ];

        try {
            $executor->executeParallel($calls, 0, $labels);
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            // The historical map stays keyed by the fan-out slot...
            self::assertSame([1], array_keys($e->getRegionErrors()));
            // ...while the new accessors name the real region.
            self::assertSame([11], $e->getFailedRegionIds());
            self::assertSame([1], array_keys($e->getFailedCallIds()));
            self::assertSame(11, $e->getFailedCallIds()[1]->regionId);
            self::assertSame(0, $e->getFailedCallIds()[1]->subBatchIndex);
        }

        $failure = $this->warning($logger, 'Region failed during dispatch');
        self::assertSame(11, $failure['context']['regionId']);
        self::assertSame(0, $failure['context']['subBatchIndex']);
    }

    public function testTwoSubBatchesOfOneRegionDoNotCollide(): void
    {
        $logger = new RecordingLogger();
        $executor = new BatchAsyncExecutor($logger);

        $calls = [
            0 => static fn() => throw new TiKvException('first sub-batch failed'),
            1 => static fn() => throw new TiKvException('second sub-batch failed'),
        ];
        $labels = [
            0 => new BatchCallId(7, 0),
            1 => new BatchCallId(7, 1),
        ];

        try {
            $executor->executeParallel($calls, 0, $labels);
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            self::assertCount(2, $e->getRegionErrors());
            self::assertSame([7], $e->getFailedRegionIds(), 'one region, reported once');
            $callIds = $e->getFailedCallIds();
            self::assertSame([0, 1], array_keys($callIds));
            self::assertSame(7, $callIds[0]->regionId);
            self::assertSame(0, $callIds[0]->subBatchIndex);
            self::assertSame(7, $callIds[1]->regionId);
            self::assertSame(1, $callIds[1]->subBatchIndex);
        }

        $warnings = $this->warnings($logger, 'Region failed during dispatch');
        self::assertCount(2, $warnings);
        self::assertSame(0, $warnings[0]['context']['subBatchIndex']);
        self::assertSame(1, $warnings[1]['context']['subBatchIndex']);
    }

    public function testUnlabelledCallsLogAnHonestCallKeyInsteadOfARegionId(): void
    {
        $logger = new RecordingLogger();
        $executor = new BatchAsyncExecutor($logger);

        $calls = [
            0 => static fn(): string => 'ok',
            3 => static fn() => throw new TiKvException('store fan-out failed'),
        ];

        try {
            $executor->executeParallel($calls, 0);
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            self::assertSame([], $e->getFailedRegionIds());
            self::assertSame([], $e->getFailedCallIds());
        }

        $failure = $this->warning($logger, 'Region failed during dispatch');
        self::assertArrayNotHasKey('regionId', $failure['context']);
        self::assertSame(3, $failure['context']['callKey']);
    }

    public function testDeadlineContextNamesRealRegionIds(): void
    {
        $logger = new RecordingLogger();
        $executor = new BatchAsyncExecutor($logger);

        $calls = [
            0 => static function (): string {
                usleep(20_000);

                return 'slow';
            },
            1 => static fn(): string => 'never dispatched',
        ];
        $labels = [
            0 => new BatchCallId(4, 0),
            1 => new BatchCallId(11, 2),
        ];

        try {
            $executor->executeParallel($calls, 10, $labels);
            self::fail('Expected BatchDeadlineExceededException');
        } catch (BatchDeadlineExceededException $e) {
            $context = $e->getContext();
            self::assertSame([0], $context['dispatchedRegions'], 'slot keys stay for backward compatibility');
            self::assertSame([1], $context['pendingRegions']);
            self::assertSame([4], $context['dispatchedRegionIds']);
            self::assertSame([11], $context['pendingRegionIds']);
            self::assertSame(['region=4,batch=0'], $context['dispatchedCalls']);
            self::assertSame(['region=11,batch=2'], $context['pendingCalls']);
        }
    }

    public function testBatchCallIdStringFormNamesBothCoordinates(): void
    {
        self::assertSame('region=23,batch=4', (string) new BatchCallId(23, 4));
        self::assertSame('region=23,batch=0', (string) new BatchCallId(23));
    }

    /**
     * @return array{level: mixed, message: string, context: array<string, mixed>}
     */
    private function warning(RecordingLogger $logger, string $message): array
    {
        $matches = $this->warnings($logger, $message);
        self::assertCount(1, $matches);

        return $matches[0];
    }

    /**
     * @return list<array{level: mixed, message: string, context: array<string, mixed>}>
     */
    private function warnings(RecordingLogger $logger, string $message): array
    {
        return array_values(array_filter(
            $logger->records,
            static fn(array $record): bool => $record['message'] === $message,
        ));
    }
}
