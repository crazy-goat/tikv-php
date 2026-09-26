<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\Proto\Pdpb\ErrorType;

/**
 * PD reported an application-level error in a response header.
 *
 * PD answers most failures with a gRPC `OK` status and an error inside
 * `pdpb.ResponseHeader.error` (issue #234). Such a response carries no
 * payload, so reading it as a success produced a *silently wrong* result
 * rather than a visible failure — `getRegion()` threw "no region",
 * `getStore()` returned `null` (a `StoreNotFoundException` for a store that
 * exists) and `scanRegions()` returned `[]`, which made
 * `RawKvRangeOps::deleteRange()` report success having deleted nothing.
 *
 * {@see $errorType} carries the raw `pdpb.ErrorType` value so a caller — and
 * the client's own endpoint rotation — can tell a *not leader* rejection
 * (rotate and retry against another endpoint) from a *not bootstrapped* or
 * *invalid value* rejection (retrying cannot help; fail closed).
 *
 * @see \CrazyGoat\TiKV\Client\Connection\PdClient::checkHeader()
 */
final class PdException extends TiKvException
{
    /**
     * Substrings PD uses when it refuses a request because the endpoint is
     * not the PD leader. Matched case-insensitively against the header
     * error text: `pdpb.ErrorType` has no not-leader member, so the type
     * alone cannot answer this question.
     *
     * @var list<string>
     */
    private const NOT_LEADER_MARKERS = [
        'not leader',
        'not the leader',
        'no leader',
        'leader has changed',
    ];

    /**
     * Message rendered in place of an empty header-error text. A typed error
     * with an empty message is still an error, and it must still say
     * something an operator can act on.
     */
    public const EMPTY_MESSAGE = 'unknown PD error (header error with empty message)';

    /**
     * @param string $method the PD RPC that failed, e.g. `ScanRegions`
     * @param int $errorType raw `pdpb.ErrorType` value from the header
     * @param string $pdErrorMessage the header error's own text, verbatim
     *        and unredacted (it never contains a TiKV key; it is PD's own
     *        error prose)
     */
    public function __construct(
        public readonly string $method,
        public readonly int $errorType,
        public readonly string $pdErrorMessage,
    ) {
        parent::__construct(sprintf(
            'PD %s failed: %s',
            $method,
            $pdErrorMessage !== '' ? $pdErrorMessage : self::EMPTY_MESSAGE,
        ));
    }

    /**
     * The `pdpb.ErrorType` value as its protobuf name (`NOT_BOOTSTRAPPED`,
     * `REGION_NOT_FOUND`, …), or the numeric value when it is not a member
     * of the enum this build was generated from.
     */
    public function getErrorTypeName(): string
    {
        try {
            return ErrorType::name($this->errorType);
        } catch (\UnexpectedValueException) {
            return (string) $this->errorType;
        }
    }

    /**
     * True when PD refused the request because the endpoint is not the
     * leader — the one header error that a *different* endpoint can satisfy,
     * so `PdClient` rotates rather than failing the call.
     *
     * Text-matched, not type-matched, because `pdpb.ErrorType` has no
     * not-leader member; a PD that types the error as `UNKNOWN` (its
     * catch-all) must still rotate.
     */
    public function isNotLeader(): bool
    {
        $message = strtolower($this->pdErrorMessage);
        foreach (self::NOT_LEADER_MARKERS as $marker) {
            if (str_contains($message, $marker)) {
                return true;
            }
        }

        return false;
    }
}
