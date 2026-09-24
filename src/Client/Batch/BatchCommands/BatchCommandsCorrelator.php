<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException;

/**
 * Correlation layer for the BatchCommands stream (issue #418, GAP-04).
 *
 * TiKV delivers responses for a BatchCommandsRequest possibly out of order
 * and possibly split across several wire messages (client-go models this
 * with its batchRecvLoop). This class is the synchronous, pure-PHP
 * equivalent: it keeps reading wire messages via the $recv closure until
 * every pending request_id has been answered, tolerating out-of-order
 * delivery, and returns the collected wire responses in arrival order.
 *
 * The wire layer stays thin ({@see \CrazyGoat\TiKV\Client\Grpc\BatchCommandsConnection});
 * all stop-condition and deadline logic lives here so it is unit-testable
 * without the grpc extension.
 */
final class BatchCommandsCorrelator
{
    /**
     * Read wire responses until all $pendingIds are accounted for.
     *
     * @param list<int> $pendingIds request_ids sent on this round trip
     * @param callable(): ?BatchCommandsResponse $recv returns the next wire
     *        message, or null when the stream was closed (half-close or
     *        transport failure)
     * @param int $deadlineMs wall-clock deadline for the whole drain in
     *        milliseconds; 0 disables the deadline
     * @return list<BatchCommandsResponse> wire responses in arrival order
     *         (may be out of request_id order; each carries its own
     *         request_ids)
     *
     * @throws BatchCommandsStreamException when the stream closes before all
     *         ids are answered, or when the deadline expires
     */
    public static function drain(
        array $pendingIds,
        callable $recv,
        int $deadlineMs = 0,
    ): array {
        $remaining = array_fill_keys($pendingIds, true);
        $received = [];

        $startMs = $deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;

        while ($remaining !== []) {
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
                if ($elapsedMs >= $deadlineMs) {
                    throw new BatchCommandsStreamException(sprintf(
                        'BatchCommands drain deadline exceeded: %d of %d request_ids unanswered after %d ms',
                        count($remaining),
                        count($pendingIds),
                        $deadlineMs,
                    ));
                }
            }

            $response = $recv();
            if ($response === null) {
                throw new BatchCommandsStreamException(sprintf(
                    'BatchCommands stream closed with %d of %d request_ids unanswered',
                    count($remaining),
                    count($pendingIds),
                ));
            }

            // Responses arrive as parallel arrays: responses[i] belongs to
            // request_ids[i]. Both the grouping and the order inside one
            // message are server-controlled (out-of-order tolerated).
            $ids = $response->getRequestIds();
            $matched = 0;
            foreach ($response->getResponses() as $i => $one) {
                $id = $ids[$i] ?? null;
                if ($id === null) {
                    throw new BatchCommandsStreamException(
                        'BatchCommands wire message contains a response without a matching request_id',
                    );
                }
                unset($remaining[(int) $id]);
                ++$matched;
            }

            if ($matched > 0) {
                $received[] = $response;
            }
        }

        return $received;
    }
}
