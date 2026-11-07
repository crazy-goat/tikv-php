<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Readiness gate of the end-to-end runners (issue #514).
 *
 * `scripts/test-e2e.sh` used to accept the cluster as soon as *any* container
 * reported healthy — in practice PD, while the TiKV stores were still
 * booting — and the test container then failed with "Name does not resolve
 * tikv1:20160". The gate lives in `scripts/lib/wait-for-cluster.sh`; these
 * tests drive it through a fake `compose` function, so they need neither
 * Docker nor a cluster.
 */
final class E2eRunnerReadinessTest extends TestCase
{
    public function testAClusterWithOnlyPdRunningIsNotReady(): void
    {
        [$exitCode] = $this->runClusterReady(
            ['pd' => 'running', 'tikv1' => 'created', 'tikv2' => 'created', 'tikv3' => 'created'],
            0,
        );

        self::assertSame(1, $exitCode, 'A cluster whose TiKV containers are still being created is not ready');
    }

    public function testAClusterWithUnregisteredStoresIsNotReady(): void
    {
        [$exitCode] = $this->runClusterReady($this->allRunning(), 2);

        self::assertSame(1, $exitCode, 'Running TiKV containers are not ready until PD sees every store Up');
    }

    public function testAClusterWithAStoppedServiceIsNotReady(): void
    {
        [$exitCode] = $this->runClusterReady(
            ['pd' => 'running', 'tikv1' => 'running', 'tikv2' => 'exited', 'tikv3' => 'running'],
            3,
        );

        self::assertSame(1, $exitCode, 'One stopped TiKV container makes the whole cluster unusable');
    }

    public function testAClusterWithEveryServiceRunningAndEveryStoreUpIsReady(): void
    {
        [$exitCode] = $this->runClusterReady($this->allRunning(), 3);

        self::assertSame(0, $exitCode, 'All four containers running and three stores Up is a ready cluster');
    }

    public function testWaitForClusterReportsATimeout(): void
    {
        [$exitCode, $output] = $this->runWaitForCluster(
            ['pd' => 'running', 'tikv1' => 'created', 'tikv2' => 'created', 'tikv3' => 'created'],
            0,
            'V1 cluster',
            1,
        );

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('Timeout waiting for V1 cluster', $output);
    }

    public function testWaitForClusterReturnsAsSoonAsTheClusterIsReady(): void
    {
        [$exitCode, $output] = $this->runWaitForCluster($this->allRunning(), 3, 'cluster', 3);

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('cluster is ready!', $output);
    }

    /**
     * The runner must go through the shared gate instead of the old
     * "any container says healthy" check, and declare the TxnKV override
     * once so every invocation after the mode switch uses the same files.
     */
    public function testTheE2eRunnerUsesTheSharedReadinessGate(): void
    {
        $script = $this->readScript('scripts/test-e2e.sh');

        self::assertStringContainsString('lib/wait-for-cluster.sh', $script);
        self::assertStringContainsString('wait_for_cluster "cluster"', $script);
        self::assertStringContainsString('wait_for_cluster "V1 cluster"', $script);
        self::assertStringNotContainsString('grep -q "healthy"', $script);
        self::assertSame(
            1,
            substr_count($script, 'docker-compose.txnkv.yml'),
            'The TxnKV override belongs in the one COMPOSE_FILES definition, '
            . 'so no invocation can miss it',
        );
    }

    /**
     * @return array<string, string>
     */
    private function allRunning(): array
    {
        return ['pd' => 'running', 'tikv1' => 'running', 'tikv2' => 'running', 'tikv3' => 'running'];
    }

    /**
     * Runs `cluster_ready` against a fake compose command.
     *
     * @param array<string, string> $services service name => container state
     *
     * @return array{int, string} the exit code and the combined output
     */
    private function runClusterReady(array $services, int $stores): array
    {
        return $this->runHarness($this->harness($services, $stores) . "\ncluster_ready");
    }

    /**
     * Runs `wait_for_cluster` against a fake compose command.
     *
     * @param array<string, string> $services service name => container state
     *
     * @return array{int, string} the exit code and the combined output
     */
    private function runWaitForCluster(array $services, int $stores, string $label, int $retries): array
    {
        $tail = sprintf('wait_for_cluster %s %d', escapeshellarg($label), $retries);

        return $this->runHarness($this->harness($services, $stores) . "\n" . $tail);
    }

    /**
     * The bash harness: a fake `compose`, no real sleeping, and the library
     * under test. `compose ps` prints the service states, `compose exec`
     * answers PD's store list.
     *
     * @param array<string, string> $services service name => container state
     */
    private function harness(array $services, int $stores): string
    {
        $ps = '';
        foreach ($services as $service => $state) {
            $ps .= $service . ' ' . $state . "\n";
        }

        $storeEntries = [];
        for ($index = 0; $index < $stores; $index++) {
            $storeEntries[] = '{"store":{"state_name":"Up"}}';
        }
        $storesJson = '{"stores":[' . implode(',', $storeEntries) . ']}';

        $library = escapeshellarg(dirname(__DIR__, 3) . '/scripts/lib/wait-for-cluster.sh');

        return <<<BASH
set -e
GREEN=''
RED=''
sleep() { :; }
compose() {
    case "\$1" in
        ps)
            cat <<'PS'
{$ps}PS
            ;;
        exec)
            printf '%s' '{$storesJson}'
            ;;
        *)
            return 1
            ;;
    esac
}
. {$library}
BASH;
    }

    /**
     * @return array{int, string} the exit code and the combined output
     */
    private function runHarness(string $script): array
    {
        $output = [];
        $exitCode = 0;
        exec('bash -c ' . escapeshellarg($script) . ' 2>&1', $output, $exitCode);

        return [$exitCode, implode("\n", $output)];
    }

    private function readScript(string $path): string
    {
        $contents = file_get_contents(dirname(__DIR__, 3) . '/' . $path);
        self::assertIsString($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }
}
