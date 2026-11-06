<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

final class GrpcException extends TiKvException
{
    /**
     * @param string|null $targetAddress The address the failed attempt was
     *        sent to, when known. `GrpcClient`/`GrpcFuture` fill it in at
     *        throw time so a caller can close exactly that channel even when
     *        the failure comes from a nested RPC (for example a lock
     *        resolution whose primary lives in another region). Issue #248.
     */
    public function __construct(
        string $details,
        public readonly int $grpcStatusCode,
        public readonly ?string $targetAddress = null,
    ) {
        parent::__construct("gRPC error: {$details}", $grpcStatusCode);
    }
}
