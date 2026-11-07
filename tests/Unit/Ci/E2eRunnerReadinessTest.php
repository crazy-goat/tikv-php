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
 * tests drive it through a fake `docker` command, so they need neither
 * Docker nor a cluster.
 *
 * The fake answers the v1-compatible `ps --services --filter
 * status=running` shape and rejects `ps --format`, which Compose v1 does not
 * understand: a gate that regressed to the v2-only option would time out
 * here.
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

    public function testAClusterIsNotReadyWhenComposePsFails(): void
    {
        [$exitCode] = $this->runClusterReady($this->allRunning(), 3, true);

        self::assertSame(1, $exitCode, 'A failing `compose ps` means "cannot tell", which is not ready');
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
        self::assertStringContainsString("trap 'exit 130' INT", $script);
        self::assertStringContainsString("trap 'exit 143' TERM", $script);
        self::assertStringNotContainsString('grep -q "healthy"', $script);
        self::assertSame(
            1,
            substr_count($script, 'docker-compose.txnkv.yml'),
            'The TxnKV override belongs in the one COMPOSE_FILES definition, '
            . 'so no invocation can miss it',
        );
    }

    /**
     * `ps --format` is a Compose v2 option; v1's `ps` knows only
     * `-q`, `--services` and `--filter`, so the gate has to stay on those.
     */
    public function testTheReadinessGateUsesOnlyComposeV1CompatibleOptions(): void
    {
        $library = $this->readScript('scripts/lib/wait-for-cluster.sh');

        self::assertStringContainsString('compose_running_services', $library);
        self::assertStringNotContainsString('--format', $library);
    }

    /**
     * The Makefile targets have to follow the same detection as the scripts;
     * calling the v1 binary directly fails on a v2-only host.
     */
    public function testTheMakefileUsesTheSharedComposeDetection(): void
    {
        $makefile = $this->readScript('Makefile');

        self::assertStringContainsString('COMPOSE := $(shell docker compose version', $makefile);
        self::assertStringContainsString('$(COMPOSE)', $makefile);
        self::assertSame(
            0,
            preg_match('/^\tdocker-compose\b/m', $makefile),
            'No Makefile recipe may call the v1 binary directly',
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
    private function runClusterReady(array $services, int $stores, bool $psFails = false): array
    {
        return $this->runHarness($this->harness($services, $stores, $psFails) . "\ncluster_ready");
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
     * The bash harness: a fake `docker`, no real sleeping, and the libraries
     * under test. `compose ps` answers the v1-compatible service list (and
     * rejects `--format`), `compose exec` answers PD's store list.
     *
     * @param array<string, string> $services service name => container state
     */
    private function harness(array $services, int $stores, bool $psFails = false): string
    {
        $running = [];
        foreach ($services as $service => $state) {
            if ($state === 'running') {
                $running[] = escapeshellarg($service);
            }
        }

        $psBody = $psFails
            ? 'return 1'
            : 'case " $* " in' . "\n"
                . '    *" --format "*) return 1 ;;' . "\n"
                . '    *" --services "*) printf \'%s\\n\' ' . implode(' ', $running) . ' ;;' . "\n"
                . '    *) return 1 ;;' . "\n"
                . 'esac';

        $storeEntries = [];
        for ($index = 0; $index < $stores; $index++) {
            $storeEntries[] = '{"store":{"state_name":"Up"}}';
        }
        $storesJson = '{"stores":[' . implode(',', $storeEntries) . ']}';

        $root = dirname(__DIR__, 3);
        $composeLibrary = escapeshellarg($root . '/scripts/lib/compose.sh');
        $readinessLibrary = escapeshellarg($root . '/scripts/lib/wait-for-cluster.sh');

        return <<<BASH
set -e
GREEN=''
RED=''
sleep() { :; }
docker() {
    if [ "\$1" = "compose" ]; then
        shift
    fi
    case "\$1" in
        version) return 0 ;;
        ps)
            {$psBody}
            ;;
        exec)
            printf '%s' '{$storesJson}'
            ;;
        *)
            return 1
            ;;
    esac
}
. {$composeLibrary}
. {$readinessLibrary}
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
