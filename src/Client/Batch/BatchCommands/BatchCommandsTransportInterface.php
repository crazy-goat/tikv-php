<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;

/**
 * Transport seam between the pure multiplexing logic and the wire layer.
 *
 * The real implementation ({@see \CrazyGoat\TiKV\Client\Grpc\GrpcBatchCommandsTransport})
 * sends one BatchCommandsRequest message on the per-store BatchCommands
 * stream and keeps reading until every $requestIds entry has been answered.
 * Unit tests inject a fake to exercise correlation, grouping and fallback
 * without the grpc extension.
 *
 * @internal
 */
interface BatchCommandsTransportInterface
{
    /**
     * @param string $address target store address
     * @param BatchCommandsRequest $request pre-built outer message (inner
     *        requests already carry their request_ids)
     * @param list<int> $requestIds the ids assigned to the inner requests
     * @param int $timeoutMs wall-clock deadline for the round trip in
     *        milliseconds; 0 disables it
     * @return list<\CrazyGoat\Proto\Tikvpb\BatchCommandsResponse> wire
     *         responses in arrival order (out-of-order tolerated)
     *
     * @throws \CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException when the
     *         stream fails or the deadline expires
     */
    public function exchange(
        string $address,
        BatchCommandsRequest $request,
        array $requestIds,
        int $timeoutMs,
    ): array;
}
