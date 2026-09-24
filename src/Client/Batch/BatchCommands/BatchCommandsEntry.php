<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use Google\Protobuf\Internal\Message;

/**
 * One operation to be multiplexed over the BatchCommands stream.
 *
 * The entry wraps the *unary* request message (e.g. a RawBatchGetRequest);
 * the multiplexer wraps it into the BatchCommandsRequest.Request oneof and
 * assigns it a unique request_id. Which oneof field the message maps to is
 * decided by {@see BatchCommandsMultiplexer}; messages absent from the map
 * (RawCompareAndSwap, RawGetKeyTTL, RawChecksum, KvGC, KvImport, …) can
 * never be multiplexed and are reported as fallback entries.
 *
 * @see BatchCommandsMultiplexer
 */
final readonly class BatchCommandsEntry
{
    public function __construct(
        /** Target store address ("host:port") — one BatchCommands stream per address. */
        public string $address,
        /** The unary request message (e.g. RawBatchGetRequest) to multiplex. */
        public Message $request,
    ) {
    }
}
