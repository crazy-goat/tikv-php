<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * A Compare-And-Swap (CAS) request failed in a way that leaves its outcome
 * unknown: the swap may or may not have been applied by TiKV.
 *
 * This is the typed counterpart of a transport-level failure (a deadline
 * exceeded, a connection reset, an UNAVAILABLE status) on
 * `RawKvClient::compareAndSwap()` and `RawKvClient::putIfAbsent()`. A CAS is
 * not idempotent, so the client deliberately does not retry those failures
 * (issue #239); it raises this exception instead of letting a bare
 * `GrpcException` escape so that callers can tell "definitely not swapped"
 * apart from "outcome unknown" without catching the generic transport type.
 *
 * The original `GrpcException` — including its `grpcStatusCode` — is
 * preserved as `getPrevious()`. Re-read the key to determine the actual
 * outcome: if the value is already the one the caller wanted to write, the
 * swap was applied.
 *
 * A failure that happens *before* the request is sent (for example PD being
 * unreachable while the region is resolved), and an application-level gRPC
 * rejection of the request itself (`UNAUTHENTICATED`, `PERMISSION_DENIED`,
 * `INVALID_ARGUMENT`, `RESOURCE_EXHAUSTED`, …), is definitive: no swap took
 * place, and it still surfaces as a plain `GrpcException` /
 * `RegionException` / `RetryBudgetExhaustedException`.
 *
 * @see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::compareAndSwap()
 */
final class CasIndeterminateException extends TiKvException
{
    public function __construct(
        string $message,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    /**
     * The gRPC status code of the underlying transport failure, when the
     * previous exception carries one.
     */
    public function getGrpcStatusCode(): ?int
    {
        $previous = $this->getPrevious();

        return $previous instanceof GrpcException ? $previous->grpcStatusCode : null;
    }
}
