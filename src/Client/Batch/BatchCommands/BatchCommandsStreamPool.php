<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

/**
 * Cache of open BatchCommands streams keyed by store address (issue #418).
 *
 * One stream per store is opened lazily on first use and reused across
 * round trips; a stream that failed (closed / deadline exceeded) is
 * discarded by the transport and silently re-opened on the next exchange.
 *
 * The factory closure is the ext-grpc seam: it returns the real
 * BatchCommandsConnection (opened on a channel from the GrpcClient pool) in
 * production and a fake in tests.
 *
 * @internal
 */
final class BatchCommandsStreamPool
{
    /** @var array<string, BatchCommandsStreamInterface> */
    private array $streams = [];

    /** @param \Closure(string): BatchCommandsStreamInterface $factory */
    public function __construct(
        private readonly \Closure $factory,
    ) {
    }

    /**
     * Get the open stream for $address, opening one on first use.
     */
    public function get(string $address): BatchCommandsStreamInterface
    {
        return $this->streams[$address] ??= ($this->factory)($address);
    }

    /**
     * Close and forget the stream for $address (dead-stream recovery).
     * Closing is best-effort: a stream whose transport is already gone
     * must not prevent the pool from serving the next exchange.
     */
    public function discard(string $address): void
    {
        $stream = $this->streams[$address] ?? null;
        unset($this->streams[$address]);
        if ($stream === null) {
            return;
        }
        try {
            $stream->close();
        } catch (\Throwable) {
            // best-effort: the stream is already unusable
        }
    }

    /**
     * Close every open stream (client shutdown).
     */
    public function close(): void
    {
        foreach (array_keys($this->streams) as $address) {
            $this->discard($address);
        }
    }
}
