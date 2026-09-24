<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use Google\Protobuf\Internal\Message;

/**
 * Outcome of one multiplexed dispatch: entries answered on the stream have
 * their inner response message under their key; entries that could not be
 * multiplexed (request type absent from the BatchCommands oneof) or whose
 * response must be recovered on the unary path (region error, missing
 * response) are reported as fallback keys.
 *
 * @internal
 */
final readonly class BatchCommandsDispatchResult
{
    /**
     * @param array<array-key, Message> $responses entry key => inner response
     *        message (e.g. RawBatchGetResponse)
     * @param array<array-key, true> $fallbackKeys keys of the entries that must be
     *        re-dispatched on the unary path
     */
    public function __construct(
        public array $responses = [],
        public array $fallbackKeys = [],
    ) {
    }

    /**
     * @param array<array-key, Message> $responses
     * @param list<array-key> $fallbackKeys
     */
    public static function from(array $responses, array $fallbackKeys): self
    {
        return new self($responses, array_fill_keys($fallbackKeys, true));
    }
}
