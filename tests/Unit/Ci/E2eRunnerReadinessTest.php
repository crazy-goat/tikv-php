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
 * The gate also waits for the region layout to settle (issue #713): a fresh
 * cluster splits its bootstrap region and grows every region to its full
 * replica set a few seconds *after* the stores report Up, and each step bumps
 * the region epoch. A run that starts in between hands the client a region
 * whose epoch TiKV has already rejected; a commit that straddles the bump is
 * not retried, so the transaction fails with `EpochNotMatch`.
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

    /**
     * A fresh cluster splits its bootstrap region and adds the remaining
     * replicas several seconds after every store is Up. Both steps bump the
     * region epoch, so a run that starts in between hands the client a region
     * it will see rejected (issue #713).
     */
    public function testAClusterWhoseRegionPeersAreStillBeingAddedIsNotReady(): void
    {
        [$exitCode] = $this->runClusterReady($this->allRunning(), 3, false, [1, 1, 1, 1, 1]);

        self::assertSame(
            1,
            $exitCode,
            'A region that has not grown its full replica set can bump its epoch under the client',
        );
    }

    public function testAClusterWithNoRegionsIsNotReady(): void
    {
        [$exitCode] = $this->runClusterReady($this->allRunning(), 3, false, []);

        self::assertSame(1, $exitCode, 'A PD that reports no region has not bootstrapped the cluster');
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
     * @param list<int> $regionPeerCounts the peer count of every region PD reports
     *
     * @return array{int, string} the exit code and the combined output
     */
    private function runClusterReady(
        array $services,
        int $stores,
        bool $psFails = false,
        array $regionPeerCounts = [3],
    ): array {
        return $this->runHarness(
            $this->harness($services, $stores, $psFails, $regionPeerCounts) . "\ncluster_ready",
        );
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
     * rejects `--format`), `compose exec` answers PD's store and region lists.
     *
     * @param array<string, string> $services service name => container state
     * @param list<int> $regionPeerCounts the peer count of every region PD reports
     */
    private function harness(
        array $services,
        int $stores,
        bool $psFails = false,
        array $regionPeerCounts = [3],
    ): string {
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

        $regionEntries = [];
        foreach ($regionPeerCounts as $regionIndex => $peerCount) {
            $peers = [];
            for ($peer = 1; $peer <= $peerCount; $peer++) {
                $peers[] = sprintf('{"role_name":"Voter","id":%d,"store_id":%d}', $peer, $peer);
            }
            $regionEntries[] = sprintf(
                '{"id":%d,"peers":[%s],"leader":{"role_name":"Voter","id":1,"store_id":1}}',
                $regionIndex + 1,
                implode(',', $peers),
            );
        }
        $regionsJson = sprintf(
            '{"count":%d,"regions":[%s]}',
            count($regionEntries),
            implode(',', $regionEntries),
        );

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
            case "\$*" in
                *regions*) printf '%s' '{$regionsJson}' ;;
                *) printf '%s' '{$storesJson}' ;;
            esac
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
