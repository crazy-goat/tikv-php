<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsCorrelator;
use CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException;
use PHPUnit\Framework\TestCase;

final class BatchCommandsCorrelatorTest extends TestCase
{
    /**
     * Build one wire message carrying the given id=>response pairs.
     *
     * @param array<int, string> $markerById request_id => marker payload
     */
    private static function wireResponse(array $markerById): BatchCommandsResponse
    {
        $message = new BatchCommandsResponse();
        $messages = [];
        foreach ($markerById as $marker) {
            $inner = new \CrazyGoat\Proto\Tikvpb\BatchCommandsResponse\Response();
            $rawGet = new \CrazyGoat\Proto\Kvrpcpb\RawGetResponse();
            $rawGet->setValue($marker);
            $inner->setRawGet($rawGet);
            $messages[] = $inner;
        }
        $message->setRequestIds(array_keys($markerById));
        $message->setResponses($messages);

        return $message;
    }

    public function testDrainReturnsWhenAllIdsAnsweredInOrder(): void
    {
        $queue = [self::wireResponse([1 => 'a', 2 => 'b', 3 => 'c'])];

        $received = BatchCommandsCorrelator::drain(
            [1, 2, 3],
            static function () use (&$queue): ?BatchCommandsResponse {
                return array_shift($queue);
            },
        );

        self::assertCount(1, $received);
    }

    public function testDrainToleratesOutOfOrderDeliveryAcrossMessages(): void
    {
        // Three wire messages, responses for ids 2, 3 and 1 arriving in
        // exactly this (reversed) order — the common out-of-order case.
        $queue = [
            self::wireResponse([2 => 'b']),
            self::wireResponse([3 => 'c']),
            self::wireResponse([1 => 'a']),
        ];

        $received = BatchCommandsCorrelator::drain(
            [1, 2, 3],
            static function () use (&$queue): ?BatchCommandsResponse {
                return array_shift($queue);
            },
        );

        // All three wire messages were consumed before the drain completed.
        self::assertCount(3, $received);
        self::assertSame([], $queue);
    }

    public function testDrainToleratesOutOfOrderDeliveryWithinOneMessage(): void
    {
        // One wire message whose parallel request_ids array is reversed
        // relative to the order the requests were sent in.
        $queue = [self::wireResponse([3 => 'c', 1 => 'a', 2 => 'b'])];

        $received = BatchCommandsCorrelator::drain(
            [1, 2, 3],
            static fn (): \CrazyGoat\Proto\Tikvpb\BatchCommandsResponse => array_shift($queue),
        );

        self::assertCount(1, $received);
    }

    public function testDrainThrowsWhenStreamClosesEarly(): void
    {
        $queue = [self::wireResponse([1 => 'a']), null];

        $this->expectException(BatchCommandsStreamException::class);
        $this->expectExceptionMessage('stream closed with 2 of 3 request_ids unanswered');

        BatchCommandsCorrelator::drain(
            [1, 2, 3],
            static function () use (&$queue): ?BatchCommandsResponse {
                return array_shift($queue);
            },
        );
    }

    public function testDrainThrowsWhenDeadlineExpires(): void
    {
        // A stream that keeps answering request_ids that were never sent
        // never satisfies the drain — the 1 ms wall-clock deadline must
        // fire deterministically (deadline is checked before each recv).
        $this->expectException(BatchCommandsStreamException::class);
        $this->expectExceptionMessage('drain deadline exceeded');

        BatchCommandsCorrelator::drain(
            [1],
            static fn (): \CrazyGoat\Proto\Tikvpb\BatchCommandsResponse => self::wireResponse([999 => 'unsolicited']),
            1,
        );
    }

    public function testDrainRejectsNegativeDeadline(): void
    {
        // A negative deadline would be treated as "no deadline" by the
        // loop, allowing an unbounded drain — it must be rejected outright.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('$deadlineMs must be >= 0');

        BatchCommandsCorrelator::drain(
            [1],
            static fn (): \CrazyGoat\Proto\Tikvpb\BatchCommandsResponse => self::wireResponse([1 => 'a']),
            -5,
        );
    }
}
