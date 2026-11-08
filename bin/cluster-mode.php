<?php

/**
 * Cluster-mode check for the per-mode end-to-end runners (issue #730).
 *
 * Every end-to-end suite needs a cluster in its own storage mode (V1TTL for
 * `E2E-RawKV`, V1 for `E2E-TxnKV`, API V2 for `E2E-ApiV2`). Without this
 * check `composer test:e2e:txn` against a V1TTL cluster failed with the
 * confusing TTL error from deep inside the client instead of saying that the
 * cluster is in the wrong mode.
 *
 * The cluster is read from PD_ENDPOINTS, with the same default as the suites
 * ("pd:2379"). When the mode cannot be determined the check warns and lets
 * the run continue: an unreachable cluster is reported by
 * `tests/E2E/ClusterProbe.php` with a better message.
 *
 * Usage:
 *   php bin/cluster-mode.php <v1|v1ttl|apiv2>
 *
 * Exit codes:
 *   0 — the cluster mode matches the expectation, or could not be determined
 *   2 — the cluster is in a different mode, or the arguments are wrong
 */

declare(strict_types=1);

use CrazyGoat\TiKV\Tests\E2E\ClusterMode;

const EXIT_OK = 0;
const EXIT_WRONG_MODE = 2;

require dirname(__DIR__) . '/vendor/autoload.php';

// The arguments of a CLI script come from $_SERVER['argv']; the script is
// analysed by PHPStan, which does not assume the CLI SAPI.
$rawArguments = $_SERVER['argv'] ?? [];
$arguments = [];

if (is_array($rawArguments)) {
    foreach ($rawArguments as $rawArgument) {
        if (is_string($rawArgument)) {
            $arguments[] = $rawArgument;
        }
    }
}

$expected = $arguments[1] ?? '';

try {
    ClusterMode::describe($expected);
} catch (InvalidArgumentException) {
    fwrite(STDERR, "Usage: php bin/cluster-mode.php <v1|v1ttl|apiv2>\n");

    exit(EXIT_WRONG_MODE);
}

$rawEndpoints = getenv('PD_ENDPOINTS');
$endpoints = [];

if (is_string($rawEndpoints) && $rawEndpoints !== '') {
    foreach (explode(',', $rawEndpoints) as $endpoint) {
        $endpoint = trim($endpoint);

        if ($endpoint !== '') {
            $endpoints[] = $endpoint;
        }
    }
}

if ($endpoints === []) {
    $endpoints = ['pd:2379'];
}

$verdict = ClusterMode::verdict($expected, ClusterMode::detect($endpoints));

if (!$verdict['ok']) {
    fwrite(STDERR, $verdict['message'] . "\n");

    exit(EXIT_WRONG_MODE);
}

echo $verdict['message'] . "\n";

exit(EXIT_OK);
