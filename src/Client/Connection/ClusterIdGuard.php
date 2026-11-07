<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
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
 * {@see PdClient} and {@see TimestampOracle} both use this guard, so the
 * header path, the error-string path and the expected-ID check cannot drift
 * apart.
 */
final readonly class ClusterIdGuard
{
    /**
     * @param \Closure(): ?int $getClusterId established cluster ID, or null
     * @param \Closure(int): void $setClusterId adopts a verified cluster ID
     * @param int|null $expectedClusterId cluster ID every report must equal;
     *        null = learn and adopt the first non-zero report
     */
    public function __construct(
        private \Closure $getClusterId,
        private \Closure $setClusterId,
        private ?int $expectedClusterId = null,
        private LoggerInterface $logger = new NullLogger(),
    ) {
        if ($expectedClusterId !== null && $expectedClusterId < 1) {
            throw new InvalidArgumentException(sprintf(
                'expectedClusterId must be greater than 0, got %d',
                $expectedClusterId,
            ));
        }
    }

    /**
     * Verify the cluster ID reported by a response header.
     *
     * @param int|null $reportedClusterId the header's cluster ID, or null
     *        when the response carried no cluster-ID header at all
     *
     * @throws TiKvException when the ID is missing but an expected ID is
     *         configured, or when it contradicts the expected or the
     *         already-established ID
     */
    public function check(?int $reportedClusterId): void
    {
        if ($reportedClusterId === null || $reportedClusterId < 1) {
            if ($this->expectedClusterId !== null) {
                throw new TiKvException(sprintf(
                    'Cluster ID missing: expected %d, response carried none',
                    $this->expectedClusterId,
                ));
            }

            return;
        }

        $established = ($this->getClusterId)();
        self::assertMatches($this->expectedClusterId, $established, $reportedClusterId);

        if ($established !== null && $established > 0) {
            return;
        }

        ($this->setClusterId)($reportedClusterId);
        $this->logger->info('Learned cluster ID', ['clusterId' => $reportedClusterId]);
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
    public function checkReportedInError(int $reportedClusterId): void
    {
        self::assertMatches(
            $this->expectedClusterId,
            ($this->getClusterId)(),
            $reportedClusterId,
        );
    }

    /**
     * @throws TiKvException when the reported ID contradicts the expected or
     *         the already-established ID
     */
    public static function assertMatches(
        ?int $expectedClusterId,
        ?int $establishedClusterId,
        int $reportedClusterId,
    ): void {
        if ($expectedClusterId !== null && $reportedClusterId !== $expectedClusterId) {
            throw new TiKvException(sprintf(
                'Cluster ID mismatch: expected %d, server reported %d',
                $expectedClusterId,
                $reportedClusterId,
            ));
        }

        if (
            $establishedClusterId !== null
            && $establishedClusterId > 0
            && $establishedClusterId !== $reportedClusterId
        ) {
            throw new TiKvException(sprintf(
                'Cluster ID mismatch: established %d, server reported %d',
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
     * Returns null when the message is not a cluster-id mismatch, or when the
     * reported ID is not positive (a `need 0` error is not a usable identity).
     */
    public static function extractFromError(string $message): ?int
    {
        if (preg_match('/mismatch cluster id.*?need (\d+) but got/', $message, $matches) !== 1) {
            return null;
        }

        $clusterId = (int) $matches[1];

        return $clusterId > 0 ? $clusterId : null;
    }
}
