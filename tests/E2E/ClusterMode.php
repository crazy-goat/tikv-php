<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use InvalidArgumentException;
use Throwable;

/**
 * The storage mode an end-to-end suite needs, and how to read it off a cluster.
 *
 * Every end-to-end suite needs a cluster in one of TiKV's three storage modes:
 * V1TTL for `E2E-RawKV`, V1 for `E2E-TxnKV`, API V2 for `E2E-ApiV2` (see
 * `docs/contributing.md` and the TTL/TxnKV entry in `docs/helpers/faq.md`).
 * A suite pointed at the wrong mode fails deep inside the client with a
 * server message about TTL or about the request's API version, which reads
 * like a client bug; `scripts/test-e2e-suite.sh` asks this class before
 * PHPUnit starts and stops with "wrong cluster mode" instead (issue #730).
 *
 * Detection is one read that writes nothing:
 *
 *  - an API V2 server serves the V2 read and the call returns normally;
 *  - a V1TTL server refuses it with "Api version in request does not match
 *    with TiKV storage, cmd: raw_get, storage: V1ttl, request: V2";
 *  - a V1 server refuses it with the same message naming "storage: V1".
 *
 * The refusal names the server's storage mode, and no cluster-level mode
 * endpoint exposes it otherwise, so the mode is read out of that message.
 *
 * A V1 raw TTL request is not a substitute: on a V1 server it does not fail
 * with the documented "non-ttl store" message, it hangs until the deadline
 * (measured against TiKV 8.5.5), so probing a V1 cluster that way would cost
 * a timeout. A V1TTL/API V2 server answers it, but that only separates V1
 * from the other two.
 */
final class ClusterMode
{
    /** Default V1 mode: no `[storage] enable-ttl`, serves TxnKV. */
    public const V1 = 'v1';

    /** V1TTL mode: `[storage] enable-ttl = true`, serves RawKV with TTL. */
    public const V1TTL = 'v1ttl';

    /** API V2 mode: `[storage] api-version = 2`, keyspace-aware. */
    public const APIV2 = 'apiv2';

    /**
     * The deadline of the probe, in milliseconds. The probe only runs against
     * a cluster that is already up, so a short deadline keeps a wrong or
     * unreachable PD from delaying the run by much before the better error
     * of `tests/E2E/ClusterProbe.php` fires.
     */
    private const PROBE_TIMEOUT_MS = 2000;

    /** The key every probe reads. It is never written. */
    private const PROBE_KEY = 'php-e2e-cluster-mode-probe';

    /** The TiKV error prefix of an API version mismatch. */
    private const API_VERSION_MISMATCH = 'Api version in request does not match';

    /**
     * The human-readable description of each mode, used in the runner output
     * and in the wrong-mode message.
     *
     * @var array<string, string>
     */
    private const DESCRIPTIONS = [
        self::V1 => 'V1 ([storage] enable-ttl unset; serves TxnKV)',
        self::V1TTL => 'V1TTL ([storage] enable-ttl = true; serves RawKV with TTL)',
        self::APIV2 => 'API V2 ([storage] api-version = 2; keyspace-aware RawKV and TxnKV)',
    ];

    /**
     * @throws InvalidArgumentException when the mode is not one of the constants
     */
    public static function describe(string $mode): string
    {
        if (!isset(self::DESCRIPTIONS[$mode])) {
            throw new InvalidArgumentException(sprintf('Unknown cluster mode "%s"', $mode));
        }

        return self::DESCRIPTIONS[$mode];
    }

    /**
     * The storage mode of the cluster behind the given PD endpoints, or null
     * when it cannot be determined (an unreachable cluster, a PD without the
     * keyspaces API, an unrecognised error).
     *
     * @param list<string> $pdEndpoints
     */
    public static function detect(array $pdEndpoints): ?string
    {
        $client = null;

        try {
            $client = RawKvClient::create($pdEndpoints, options: [
                'apiVersion' => 2,
                'keyspace' => 'DEFAULT',
                'timeout' => [
                    'readTimeoutMs' => self::PROBE_TIMEOUT_MS,
                    'writeTimeoutMs' => self::PROBE_TIMEOUT_MS,
                    'pdTimeoutMs' => self::PROBE_TIMEOUT_MS,
                ],
                'retryDeadlineMs' => self::PROBE_TIMEOUT_MS,
            ]);

            $client->get(self::PROBE_KEY);

            return self::APIV2;
        } catch (RegionException $e) {
            return self::fromApiVersionMismatch($e->getMessage());
        } catch (Throwable) {
            return null;
        } finally {
            $client?->close();
        }
    }

    /**
     * The mode named by TiKV's API-version mismatch error, or null when the
     * message is not that error.
     *
     * @internal exposed so the parsing can be tested without a cluster
     */
    public static function fromApiVersionMismatch(string $message): ?string
    {
        if (!str_contains($message, self::API_VERSION_MISMATCH)) {
            return null;
        }

        if (preg_match('/storage:\s*(v1ttl|v1|v2)\b/i', $message, $matches) !== 1) {
            return null;
        }

        return match (strtolower($matches[1])) {
            'v1' => self::V1,
            'v1ttl' => self::V1TTL,
            'v2' => self::APIV2,
            default => null,
        };
    }

    /**
     * Decide whether a detected mode satisfies the suite's expectation.
     *
     * A null detection passes with a warning instead of blocking the run: an
     * unreachable cluster is reported by `tests/E2E/ClusterProbe.php` with a
     * better message, and a probe that cannot tell must not turn a working
     * setup red.
     *
     * @return array{ok: bool, message: string}
     */
    public static function verdict(string $expected, ?string $detected): array
    {
        if ($detected === null) {
            return [
                'ok' => true,
                'message' => sprintf(
                    'Could not determine the cluster mode; continuing. The suite expects %s.',
                    self::describe($expected),
                ),
            ];
        }

        if ($detected === $expected) {
            return [
                'ok' => true,
                'message' => sprintf('Cluster mode: %s.', self::describe($detected)),
            ];
        }

        return [
            'ok' => false,
            'message' => sprintf(
                "Wrong cluster mode: the cluster is %s, but the suite needs %s.\n"
                . 'Each end-to-end suite needs a cluster mode of its own (V1TTL for E2E-RawKV, '
                . 'V1 for E2E-TxnKV, API V2 for E2E-ApiV2); see docs/contributing.md.',
                self::describe($detected),
                self::describe($expected),
            ),
        ];
    }
}
