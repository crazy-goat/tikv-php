<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Kvrpcpb\IsolationLevel;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

/**
 * The optional `kvrpcpb.Context` control fields shared by every request a
 * client sends (issue #426, GAP-12).
 *
 * `priority` is not part of this object: it is already a separate,
 * transaction-scoped option (`TxnKvClient::begin(['priority' => ...])`) and
 * keeps its own parameter on {@see RegionContextFactory} so the existing
 * positional API does not change.
 *
 * Every field defaults to the proto3 default (SI, cache filling enabled, no
 * server-side execution bound, no request source, no resource group, no busy
 * threshold), so an unset option leaves the serialized `Context` byte-for-byte
 * identical to a request built before this class existed. A field is applied
 * to the wire only when it differs from that default.
 *
 * The object is immutable: a client holds one instance and a transaction may
 * replace it through `TxnKvClient::begin(['requestContext' => ...])`, which
 * overrides the client-level value for that transaction.
 */
final readonly class RequestContextOptions
{
    /**
     * A stable, identifiable default request source for applications that
     * want TiKV's own metrics to attribute their load (issue #426).
     *
     * It is deliberately **not** applied unless the caller opts in: sending it
     * by default would add `request_source` to every `Context` and change the
     * bytes of requests that never asked for it. Pass
     * `new RequestContextOptions(requestSource: self::DEFAULT_REQUEST_SOURCE)`
     * (or any other label) to enable the attribution.
     */
    public const DEFAULT_REQUEST_SOURCE = 'tikv-php';

    /**
     * Largest accepted value of `max_execution_duration_ms` and
     * `busy_threshold_ms`: one hour, far beyond any sensible per-RPC bound
     * and small enough to stay a positive int32.
     */
    public const MAX_DURATION_MS = 3_600_000;

    /**
     * @param int $isolationLevel Kvrpcpb IsolationLevel: SI (0, default),
     *        RC (1) or RCCheckTS (2). Only transactional reads use it; RawKV
     *        accepts it too because the field lives on the shared Context.
     * @param bool $notFillCache Keep the request out of TiKV's block cache
     *        (`not_fill_cache`), so a full scan does not evict the working
     *        set. Default false.
     * @param int $maxExecutionDurationMs Server-side execution bound in
     *        milliseconds (`max_execution_duration_ms`). 0 (default) leaves
     *        the field unset and TiKV applies its own bound. Must be between
     *        0 and {@see self::MAX_DURATION_MS}.
     * @param string $requestSource Label TiKV records against the request
     *        (`request_source`), used for load attribution. '' (default)
     *        leaves the field unset. See {@see self::DEFAULT_REQUEST_SOURCE}.
     * @param string $resourceGroupTag Resource group tag
     *        (`resource_group_tag`) used by the resource manager. '' (default)
     *        leaves the field unset.
     * @param int $busyThresholdMs Server-side busy threshold
     *        (`busy_threshold_ms`) in milliseconds. 0 (default) leaves the
     *        field unset. Must be between 0 and {@see self::MAX_DURATION_MS}.
     * @param string $resourceGroupName Resource manager group name carried in
     *        `resource_control_context.resource_group_name`. '' (default)
     *        leaves the message unset.
     *
     * @throws InvalidArgumentException When a value is outside its defined range
     */
    public function __construct(
        public int $isolationLevel = IsolationLevel::SI,
        public bool $notFillCache = false,
        public int $maxExecutionDurationMs = 0,
        public string $requestSource = '',
        public string $resourceGroupTag = '',
        public int $busyThresholdMs = 0,
        public string $resourceGroupName = '',
    ) {
        if (
            !in_array(
                $this->isolationLevel,
                [IsolationLevel::SI, IsolationLevel::RC, IsolationLevel::RCCheckTS],
                true,
            )
        ) {
            throw new InvalidArgumentException(sprintf(
                'isolationLevel must be one of SI (0), RC (1) or RCCheckTS (2), %d given',
                $this->isolationLevel,
            ));
        }
        foreach (
            [
                'maxExecutionDurationMs' => $this->maxExecutionDurationMs,
                'busyThresholdMs' => $this->busyThresholdMs,
            ] as $name => $value
        ) {
            if ($value < 0 || $value > self::MAX_DURATION_MS) {
                throw new InvalidArgumentException(sprintf(
                    '%s must be between 0 and %d',
                    $name,
                    self::MAX_DURATION_MS,
                ));
            }
        }
    }
}
