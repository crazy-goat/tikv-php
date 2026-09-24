<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;

/**
 * A single open BatchCommands bidirectional stream to one store.
 *
 * The real implementation is {@see \CrazyGoat\TiKV\Client\Grpc\BatchCommandsConnection};
 * tests inject fakes. Semantics: send() pushes one BatchCommandsRequest
 * message; recv() blocks until the next BatchCommandsResponse arrives and
 * returns null once the stream is closed (half-close, deadline or transport
 * failure). The stream is reused across round trips — the HTTP/2 stream is
 * NOT half-closed between exchanges.
 *
 * @internal
 */
interface BatchCommandsStreamInterface
{
    public function send(BatchCommandsRequest $request): void;

    /**
     * @return BatchCommandsResponse|null null when the stream is closed/exhausted
     */
    public function recv(): ?BatchCommandsResponse;

    public function close(): void;
}
