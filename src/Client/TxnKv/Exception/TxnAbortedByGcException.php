<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * Thrown when a transaction's start timestamp is no longer readable because
 * the cluster's GC safe point has advanced past it.
 *
 * Two distinct situations produce this exception:
 *
 * 1. Read-side validation (issue #422): a client with safe-point validation
 *    enabled fetched a start timestamp below the current GC safe point
 *    (detected locally via {@see \CrazyGoat\TiKV\Client\Connection\SafePointCache}).
 *
 * 2. Server-side rejection: TiKV returned a "GC life time is shorter than
 *    transaction duration" error for a read or commit against a start
 *    timestamp that GC has already passed. This is surfaced as this typed
 *    exception instead of an untyped transaction conflict so callers can
 *    catch it and handle it (fail the job, restart with a fresh timestamp,
 *    or register a service safe point to hold GC back — see
 *    PdClientInterface::updateServiceGCSafePoint()).
 *
 * The exception is deliberately NOT retryable: retrying the same start
 * timestamp cannot succeed, only a new transaction (new start timestamp)
 * or holding GC back can.
 */
final class TxnAbortedByGcException extends TiKvException
{
}
