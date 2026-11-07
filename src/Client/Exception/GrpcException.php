<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\TiKV\Client\Util\ServerText;

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
        // $details comes straight off the wire; sanitise it before it
        // reaches a log sink (SEC-11, issue #314).
        parent::__construct('gRPC error: ' . ServerText::sanitise($details), $grpcStatusCode);
    }
}
