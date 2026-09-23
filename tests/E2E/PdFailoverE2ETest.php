<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * E2E test for PD failover under a mid-workload PD outage (issue #416).
 *
 * LIMITATION: the local docker-compose cluster runs a SINGLE PD node, so
 * this test cannot kill a PD *leader* while other PD followers take over.
 * Instead it restarts that single PD node in the middle of a workload and
 * asserts the workload completes once PD comes back — i.e. the client
 * survives a full PD outage of its only endpoint. A true leader-kill
 * failover (multi-PD compose, kill the leader, followers re-elect) needs a
 * 3-PD compose file; the failover logic itself is covered by
 * tests/Unit/Connection/PdClientFailoverTest.php.
 *
 * Restarting PD needs access to the Docker Engine: either the `docker` CLI
 * (host runs) or the Docker socket mounted into the container the test runs
 * in (recommended — the test must run on the compose network, because the
 * TiKV store addresses PD advertises, e.g. `tikv1:20160`, only resolve
 * there):
 *
 *   make up
 *   docker compose run --rm --no-deps -v /var/run/docker.sock:/var/run/docker.sock \
 *     -e PD_ENDPOINTS=pd:2379 php-client \
 *     vendor/bin/phpunit tests/E2E/PdFailoverE2ETest.php
 *
 * The test self-skips when neither the CLI nor a mounted socket is
 * available (e.g. the plain `make test-e2e` run).
 */
final class PdFailoverE2ETest extends TestCase
{
    private static ?TxnKvClient $client = null;

    /** @var list<string> */
    private array $keysToCleanup = [];

    /** @var list<string> PD endpoints (single PD node in the local compose) */
    private static array $pdEndpoints = ['pd:2379'];

    /** Lightweight PD probe used for the cluster-availability skip. */
    private static ?PdClientInterface $pdProbe = null;

    public static function setUpBeforeClass(): void
    {
        $env = getenv('PD_ENDPOINTS');
        if ($env !== false && $env !== '') {
            self::$pdEndpoints = explode(',', (string) $env);
        }

        self::$client = TxnKvClient::create(self::$pdEndpoints);
        self::$pdProbe = ConnectionFactory::create(self::$pdEndpoints)->pdClient;
    }

    public static function tearDownAfterClass(): void
    {
        self::$client?->close();
        self::$client = null;
        self::$pdProbe = null;
    }

    protected function setUp(): void
    {
        if (!self::$client instanceof TxnKvClient) {
            $this->markTestSkipped('TiKV cluster not available');
        }

        // Consistent with the other E2E tests: skip when the cluster is
        // unreachable instead of failing.
        try {
            self::pdClient()->getTimestamp();
        } catch (\Throwable) {
            $this->markTestSkipped('TiKV cluster not available');
        }

        // Restarting PD needs Docker access (CLI or mounted socket); skip
        // when neither is available instead of failing.
        if (!$this->dockerAvailable()) {
            $this->markTestSkipped(
                'Docker access not available in the test environment; run '
                . 'this test with the Docker socket mounted (see class docblock)',
            );
        }
    }

    protected function tearDown(): void
    {
        if (!self::$client instanceof TxnKvClient) {
            return;
        }
        foreach ($this->keysToCleanup as $key) {
            try {
                $txn = self::$client->begin(['pessimistic' => false]);
                $txn->delete($key);
                $txn->commit();
            } catch (\Exception) {
                // Ignore errors during cleanup
            }
        }
        $this->keysToCleanup = [];
    }

    public function testWorkloadCompletesAfterPdRestart(): void
    {
        $client = self::$client;
        self::assertInstanceOf(TxnKvClient::class, $client);
        $key = 'pd-failover-' . uniqid();
        $this->keysToCleanup[] = $key;

        // Phase 1: normal workload before the outage.
        $txn = $client->begin(['pessimistic' => false]);
        $txn->set($key, 'before-restart');
        $txn->commit();
        $txn = $client->begin(['pessimistic' => false]);
        $this->assertSame('before-restart', $txn->get($key));
        $txn->rollback();

        // Phase 2: restart the (single) PD node mid-workload.
        $this->restartPd();
        $this->waitUntilPdHealthy(60);

        // Phase 3: the workload must complete after PD is back — the client
        // reconnects (and re-runs leader discovery via the failover path)
        // instead of being stuck on the pre-restart state.
        //
        // Right after PD's health endpoint answers, its store registry is
        // not fully repopulated yet (region lookups can briefly resolve to
        // store 0 and TiKV stores are still re-registering), so the phase-3
        // workload is retried until the cluster actually serves again.
        $deadline = time() + 60;
        $last = null;
        while (time() < $deadline) {
            try {
                $txn = $client->begin(['pessimistic' => false]);
                $txn->set($key, 'after-restart');
                $txn->commit();
                $txn = $client->begin(['pessimistic' => false]);
                $this->assertSame('after-restart', $txn->get($key));
                $txn->rollback();

                return;
            } catch (\Throwable $e) {
                $last = $e;
                sleep(2);
            }
        }

        self::fail(
            'Workload did not complete within 60s after PD came back'
            . ($last !== null ? ': ' . $last->getMessage() : ''),
        );
    }

    /**
     * True when Docker can be reached: either the `docker` CLI is on PATH
     * or the Docker socket is mounted into this container.
     */
    private function dockerAvailable(): bool
    {
        exec('command -v docker 2>/dev/null', $output, $exitCode);
        if ($exitCode === 0) {
            return true;
        }

        // Note: file_exists(), not is_file() — is_file() is false for sockets.
        return file_exists('/var/run/docker.sock');
    }

    private static function pdClient(): PdClientInterface
    {
        self::assertInstanceOf(PdClientInterface::class, self::$pdProbe);

        return self::$pdProbe;
    }

    /**
     * Restart the PD container used by the local compose cluster.
     *
     * Prefers the `docker` CLI (host runs, `docker compose restart pd`,
     * falling back to restarting any running PD container by image); when
     * no CLI is available, talks to the Docker Engine HTTP API over the
     * unix socket directly (container runs with the socket mounted).
     */
    private function restartPd(): void
    {
        exec('docker compose restart pd 2>/dev/null', $output, $exitCode);
        if ($exitCode !== 0) {
            exec(
                "docker restart \$(docker ps -q --filter ancestor=pingcap/pd)",
                $output,
                $exitCode,
            );
        }

        if ($exitCode === 0) {
            return;
        }

        self::assertTrue($this->restartPdViaSocket(), 'Failed to restart the PD container');
    }

    /**
     * Restart the PD container through the Docker Engine HTTP API on the
     * mounted unix socket: list containers, pick the one running the
     * pingcap/pd image, POST /containers/{id}/restart.
     */
    private function restartPdViaSocket(): bool
    {
        if (!file_exists('/var/run/docker.sock')) {
            return false;
        }

        $containers = $this->dockerApi('GET', '/containers/json?all=1');
        if (!is_array($containers)) {
            return false;
        }

        foreach ($containers as $container) {
            $image = is_array($container) ? ($container['Image'] ?? null) : null;
            $id = is_array($container) ? ($container['Id'] ?? null) : null;
            if (!is_string($image) || !is_string($id) || $id === '') {
                continue;
            }

            if (!str_starts_with($image, 'pingcap/pd:')) {
                continue;
            }

            $response = $this->dockerApi(
                'POST',
                '/containers/' . rawurlencode($id) . '/restart?t=5',
            );

            return $response !== null;
        }

        return false;
    }

    /**
     * Minimal Docker Engine API client over the unix socket (cURL).
     *
     * @return mixed decoded JSON response, or null on any failure
     */
    private function dockerApi(string $method, string $path): mixed
    {
        if (!extension_loaded('curl')) {
            return null;
        }

        $handle = curl_init('http://localhost' . $path);
        if (!$handle instanceof \CurlHandle) {
            return null;
        }

        curl_setopt($handle, CURLOPT_UNIX_SOCKET_PATH, '/var/run/docker.sock');
        \assert($method !== '');
        curl_setopt($handle, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($handle, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($handle, CURLOPT_TIMEOUT, 30);

        $body = curl_exec($handle);
        $status = (int) curl_getinfo($handle, CURLINFO_RESPONSE_CODE);
        curl_close($handle);

        if ($status >= 300 || !is_string($body) || $body === '') {
            return $status < 300 ? '' : null;
        }

        return json_decode($body, true);
    }

    /**
     * Poll the PD HTTP health endpoint (PD serves it on the client port)
     * until it answers, or time out.
     */
    private function waitUntilPdHealthy(int $timeoutSeconds): void
    {
        $deadline = time() + $timeoutSeconds;
        $lastException = null;

        while (time() < $deadline) {
            foreach (self::$pdEndpoints as $endpoint) {
                $host = (string) (parse_url(
                    str_contains($endpoint, '://') ? $endpoint : 'http://' . $endpoint,
                    PHP_URL_HOST,
                ) ?: $endpoint);
                $port = parse_url(
                    str_contains($endpoint, '://') ? $endpoint : 'http://' . $endpoint,
                    PHP_URL_PORT,
                );
                $url = sprintf(
                    'http://%s:%d/pd/api/v1/health',
                    $host,
                    $port === false || $port === null ? 2379 : (int) $port,
                );

                try {
                    $health = @file_get_contents($url);
                    if ($health !== false) {
                        return;
                    }
                } catch (\Throwable $e) {
                    $lastException = $e;
                }
            }
            sleep(1);
        }

        self::fail(
            'PD did not become healthy within ' . $timeoutSeconds . 's'
            . ($lastException !== null ? ': ' . $lastException->getMessage() : ''),
        );
    }
}
