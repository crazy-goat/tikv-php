<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use RuntimeException;

/**
 * Fail-fast probe for the end-to-end suites.
 *
 * The end-to-end tests talk to a real cluster. Without one, every single
 * test used to spend the client's whole retry budget (measured: 18.8 s)
 * before it reported the connection error, so the 242 end-to-end tests
 * needed about an hour to print their summary (issue #361).
 *
 * The probe asks PD for its member list once per process and fails the
 * test class right away when PD does not answer, so a missing cluster is
 * reported in seconds. It fails instead of skipping on purpose: a skipped
 * suite looks exactly like a suite that tested nothing, which is the
 * problem issue #361 is about.
 */
final class ClusterProbe
{
    /**
     * How often the probe asks PD before it gives up. A cluster that is
     * starting up answers late, so one failed attempt is not proof that it
     * is not there.
     */
    private const PROBE_ATTEMPTS = 3;

    /** The deadline of one probe attempt, in milliseconds. */
    private const PROBE_TIMEOUT_MS = 5000;

    /**
     * The endpoint lists that already answered, so the probe runs once per
     * process and not once per test class.
     *
     * @var array<string, true>
     */
    private static array $pdReachable = [];

    /**
     * Fail when PD does not answer at any of the given endpoints.
     *
     * @param list<string> $pdEndpoints
     *
     * @throws RuntimeException when the cluster is not reachable
     */
    public static function assertPdReachable(array $pdEndpoints): void
    {
        $key = implode(',', $pdEndpoints);

        if (isset(self::$pdReachable[$key])) {
            return;
        }

        $failure = null;
        $message = 'unknown error';

        for ($attempt = 1; $attempt <= self::PROBE_ATTEMPTS; $attempt++) {
            try {
                $bundle = ConnectionFactory::create($pdEndpoints, null, [
                    'timeout' => ['pdTimeoutMs' => self::PROBE_TIMEOUT_MS],
                ]);

                try {
                    $bundle->pdClient->ping();
                } finally {
                    $bundle->grpc->close();
                }

                self::$pdReachable[$key] = true;

                return;
            } catch (\Throwable $e) {
                $failure = $e;
                $message = $e->getMessage();
            }
        }

        throw new RuntimeException(
            sprintf(
                'The TiKV cluster is not reachable at %s (%s). The end-to-end suites need a '
                . 'running cluster and reach PD through the PD_ENDPOINTS environment variable.' . "\n"
                . '  - On the host, PD_ENDPOINTS has to point at the published PD port, for '
                . 'example "PD_ENDPOINTS=localhost:2379": the default "pd:2379" is a Docker '
                . 'Compose service name and resolves only inside the compose network.' . "\n"
                . '  - Inside the compose network, start the cluster the suite needs: "make up" '
                . '(V1TTL cluster, RawKV suite) or "docker compose -f docker-compose.yml '
                . '-f docker-compose.txnkv.yml up -d" (V1 cluster, TxnKV suite).',
                $key,
                $message,
            ),
            0,
            $failure,
        );
    }

    /**
     * Fail when nothing accepts connections at the given store address.
     *
     * @param string $address host:port of a TiKV store
     *
     * @throws RuntimeException when the store is not reachable
     */
    public static function assertStoreReachable(string $address): void
    {
        $host = $address;
        $port = 20160;

        $separator = strrpos($address, ':');
        if ($separator !== false) {
            $host = substr($address, 0, $separator);
            $port = (int) substr($address, $separator + 1);
        }

        $errorCode = 0;
        $errorMessage = '';
        // The error suppression keeps the failed connection attempt out of
        // the test output; the failure itself is reported below.
        $socket = @fsockopen($host, $port, $errorCode, $errorMessage, 2.0);

        if ($socket === false) {
            throw new RuntimeException(
                sprintf(
                    'The TiKV store is not reachable at %s (%s). The end-to-end suites need a '
                    . 'running cluster.',
                    $address,
                    $errorMessage !== '' ? $errorMessage : 'connection failed',
                ),
            );
        }

        fclose($socket);
    }
}
