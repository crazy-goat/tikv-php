<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsStreamInterface;

/**
 * Minimal fake stream: records sends and close; recv() always returns null
 * (closed) — the pool never recvs, only the transport does.
 */
final class FakeBatchCommandsStream implements BatchCommandsStreamInterface
{
    /** @var list<BatchCommandsRequest> */
    public array $sent = [];

    public bool $closed = false;

    public function send(BatchCommandsRequest $request): void
    {
        $this->sent[] = $request;
    }

    public function recv(): ?BatchCommandsResponse
    {
        return null;
    }

    public function close(): void
    {
        $this->closed = true;
    }
}
