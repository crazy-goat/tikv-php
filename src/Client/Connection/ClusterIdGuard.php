<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Single implementation of PD cluster-ID learning and verification
 * (issue #250, REG-19).
 *
 * PD reports its cluster ID in every response header. The first non-zero ID
 * is adopted; every later report must equal it, so a PD endpoint that is
 * repointed at another cluster (a VIP move, a restored backup, a staging
 * configuration pointed at production) cannot be followed silently.
 *
 * A reported ID of zero means "not reported": proto3 omits an unset scalar
 * and its getter returns `0`, so zero is never adopted and never treated as
 * a mismatch. Without that rule a header carrying `cluster_id = 0` would pin
 * the client to cluster 0 forever, because a stored 0 is not `null`.
 *
 * Cluster IDs are `uint64`; comparisons happen on the canonical unsigned
 * decimal string through {@see ClusterId} so an ID above `PHP_INT_MAX`
 * (decoded by the pure-PHP protobuf runtime as a negative int) can be learned,
 * pinned and compared (issue #818).
 *
 * {@see PdClient} and {@see TimestampOracle} both use this guard, so the
 * header path, the error-string path and the expected-ID check cannot drift
 * apart.
 */
final readonly class ClusterIdGuard
{
    /** Canonical unsigned decimal expected ID, or null to learn and adopt. */
    private ?string $expectedClusterId;

    /**
     * @param \Closure(): (int|string|null) $getClusterId established cluster ID, or null
     * @param \Closure(int|string): void $setClusterId adopts a verified cluster ID
     * @param int|string|null $expectedClusterId cluster ID every report must equal;
     *        null = learn and adopt the first non-zero report. Accepts the
     *        canonical unsigned decimal form so a uint64 above `PHP_INT_MAX`
     *        can be pinned
     */
    public function __construct(
        private \Closure $getClusterId,
        private \Closure $setClusterId,
        int|string|null $expectedClusterId = null,
        private LoggerInterface $logger = new NullLogger(),
    ) {
        $this->expectedClusterId = ClusterId::normalizeExpected($expectedClusterId);
    }

    /**
     * Verify the cluster ID reported by a response header.
     *
     * @param int|string|null $reportedClusterId the header's cluster ID, or
     *        null when the response carried no cluster-ID header at all. A
     *        negative int is the uint64 decoding of an ID above `PHP_INT_MAX`
     *        and is normalised, not rejected
     *
     * @throws TiKvException when the ID is missing but an expected ID is
     *         configured, when it is not a valid uint64, or when it
     *         contradicts the expected or the already-established ID
     */
    public function check(int|string|null $reportedClusterId): void
    {
        $reported = $reportedClusterId === null
            ? null
            : ClusterId::normalizeReported($reportedClusterId);

        if ($reported === null || $reported === '0') {
            if ($this->expectedClusterId !== null) {
                throw new TiKvException(sprintf(
                    'Cluster ID missing: expected %s, response carried none',
                    $this->expectedClusterId,
                ));
            }

            return;
        }

        $established = $this->establishedClusterId();
        self::assertMatches($this->expectedClusterId, $established, $reported);

        if ($established !== null) {
            return;
        }

        $public = ClusterId::toPublic($reported);
        ($this->setClusterId)($public);
        $this->logger->info('Learned cluster ID', ['clusterId' => $public]);
    }

    /**
     * Verify a cluster ID parsed out of PD's cluster-id mismatch error before
     * it is adopted.
     *
     * The error text is controlled by whichever server answers the socket, so
     * it may not override a configured expected ID or an already-established
     * one.
     *
     * @throws TiKvException when the ID contradicts the expected or the
     *         already-established ID
     */
    public function checkReportedInError(int|string $reportedClusterId): void
    {
        self::assertMatches(
            $this->expectedClusterId,
            $this->establishedClusterId(),
            ClusterId::normalizeReported($reportedClusterId),
        );
    }

    /**
     * @throws TiKvException when the reported ID contradicts the expected or
     *         the already-established ID
     */
    public static function assertMatches(
        ?string $expectedClusterId,
        ?string $establishedClusterId,
        string $reportedClusterId,
    ): void {
        if ($expectedClusterId !== null && $reportedClusterId !== $expectedClusterId) {
            throw new TiKvException(sprintf(
                'Cluster ID mismatch: expected %s, server reported %s',
                $expectedClusterId,
                $reportedClusterId,
            ));
        }

        if ($establishedClusterId !== null && $establishedClusterId !== $reportedClusterId) {
            throw new TiKvException(sprintf(
                'Cluster ID mismatch: established %s, server reported %s',
                $establishedClusterId,
                $reportedClusterId,
            ));
        }
    }

    /**
     * Extract the cluster ID from PD's `mismatch cluster id, need X but got Y`
     * gRPC error.
     *
     * `pdpb.ErrorType` has no cluster-mismatch member: PD rejects this at the
     * gRPC layer, not through the typed header error, so the message is the
     * only signal available. It is parsed here, once, instead of in both
     * {@see PdClient} and {@see TimestampOracle}.
     *
     * Returns null when the message is not a cluster-id mismatch, when the
     * reported ID is not positive (a `need 0` error is not a usable identity),
     * or when it does not fit in a uint64. The returned form is an int while
     * it fits, the canonical decimal string above `PHP_INT_MAX`.
     */
    public static function extractFromError(string $message): int|string|null
    {
        if (preg_match('/mismatch cluster id.*?need (\d+) but got/', $message, $matches) !== 1) {
            return null;
        }

        try {
            $canonical = ClusterId::normalizeReported($matches[1]);
        } catch (TiKvException) {
            // A value outside uint64 is server-controlled nonsense, not an
            // identity the client may adopt.
            return null;
        }

        if ($canonical === '0') {
            return null;
        }

        return ClusterId::toPublic($canonical);
    }

    /**
     * Read the established ID through the closure and normalise it.
     *
     * The closure may return an int (the legacy contract, still used by
     * tests and mocks) or the canonical string; a reported zero means
     * "not established".
     */
    private function establishedClusterId(): ?string
    {
        $value = ($this->getClusterId)();
        if (!is_int($value) && !is_string($value)) {
            return null;
        }

        $canonical = ClusterId::normalizeReported($value);

        return $canonical === '0' ? null : $canonical;
    }
}
