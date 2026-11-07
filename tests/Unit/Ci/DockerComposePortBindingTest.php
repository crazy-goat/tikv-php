<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #318 (SEC-15): the development compose stack
 * published PD and the three TiKV ports on every host interface.
 *
 * Docker's short `"HOST:CONTAINER"` port syntax binds to `0.0.0.0` when the
 * host address is omitted, so `make up` on a shared build host or a cloud
 * development VM served an unauthenticated, unencrypted cluster to every
 * host that could route to it. Every published port now names `127.0.0.1`
 * explicitly. Inter-container traffic uses the compose network and the CI
 * readiness probes dial `localhost`, so the loopback binding changes
 * nothing for them; a workflow that reached the cluster from another host
 * needs an SSH tunnel.
 *
 * The compose files are read as text: the Unit suite runs without the
 * project's dev extensions, and no YAML parser is a dependency. Only the
 * short `- "IP:HOST:CONTAINER"` form is understood; any other shape fails
 * the test instead of being silently skipped, so a rewrite cannot drop the
 * guard without a red build.
 */
final class DockerComposePortBindingTest extends TestCase
{
    /**
     * The container ports the development cluster must keep publishing.
     *
     * PD is 2379; the three TiKV stores listen on 20160-20162. The E2E
     * runners reach the cluster through the compose network, but the manual
     * `PD_ENDPOINTS=localhost:${PD_PORT}` workflow and the CI readiness
     * probes use the published ports.
     */
    private const EXPECTED_CONTAINER_PORTS = [2379, 20160, 20161, 20162];

    public function testEveryPublishedPortIsBoundToLoopback(): void
    {
        $published = 0;
        $violations = [];

        foreach ($this->composeFiles() as $file) {
            foreach ($this->publishedPorts($this->readFile($file)) as [$line, $entry]) {
                $published++;

                if (!str_starts_with($entry, '127.0.0.1:') && !str_starts_with($entry, '[::1]:')) {
                    $violations[] = sprintf(
                        '%s:%d publishes "%s" on every host interface; prefix the host address '
                        . 'with 127.0.0.1 (or [::1]) so only the local host can reach the '
                        . 'unauthenticated development cluster (issue #318, SEC-15)',
                        $this->relative($file),
                        $line,
                        $entry,
                    );
                }
            }
        }

        // Without this witness the rule would pass vacuously if the scan
        // stopped finding a ports block at all.
        self::assertGreaterThan(
            0,
            $published,
            'No published port was found in the compose files, so this guard no longer guards anything.',
        );

        self::assertSame(
            [],
            $violations,
            "Published ports that are not loopback-bound:\n" . implode("\n", $violations),
        );
    }

    public function testTheDevelopmentClusterKeepsPublishingItsFourPorts(): void
    {
        $file = $this->repoRoot() . '/docker-compose.yml';
        $containerPorts = [];

        foreach ($this->publishedPorts($this->readFile($file)) as [$line, $entry]) {
            $containerPorts[] = $this->containerPort($entry, $this->relative($file), $line);
        }

        sort($containerPorts);

        self::assertSame(
            self::EXPECTED_CONTAINER_PORTS,
            $containerPorts,
            'docker-compose.yml must keep publishing PD (2379) and the three TiKV stores '
            . '(20160-20162); the manual host workflow and the CI readiness probes depend on them.',
        );
    }

    /**
     * Every `docker-compose*.yml` in the repository root.
     *
     * @return list<string>
     */
    private function composeFiles(): array
    {
        $files = glob($this->repoRoot() . '/docker-compose*.yml') ?: [];
        sort($files);

        return $files;
    }

    /**
     * The entries of every `ports:` block in a compose file.
     *
     * @return list<array{int, string}> the line the entry is on and the entry
     */
    private function publishedPorts(string $text): array
    {
        $lines = preg_split('/\R/', $text);
        self::assertIsArray($lines, 'Unable to split a compose file into lines');

        $entries = [];
        $inPorts = false;
        $portsIndent = 0;

        foreach ($lines as $index => $line) {
            $number = $index + 1;

            $matches = [];
            if (preg_match('/^(\s*)ports:\s*(.*)$/', $line, $matches) === 1) {
                $rest = trim($matches[2]);

                self::assertSame(
                    '',
                    $rest,
                    sprintf(
                        'docker-compose.yml line %d uses a "ports:" syntax this guard does not '
                        . 'understand (%s); use one short "- "127.0.0.1:HOST:CONTAINER"" entry per '
                        . 'line so the loopback binding stays verifiable (issue #318, SEC-15)',
                        $number,
                        $rest,
                    ),
                );

                $inPorts = true;
                $portsIndent = strlen($matches[1]);

                continue;
            }

            if (!$inPorts || trim($line) === '' || str_starts_with(ltrim($line), '#')) {
                continue;
            }

            $indent = strlen($line) - strlen(ltrim($line));

            if ($indent <= $portsIndent) {
                $inPorts = false;

                continue;
            }

            $matches = [];
            if (preg_match('/^\s*-\s*(.+)$/', $line, $matches) !== 1) {
                self::fail(sprintf(
                    'docker-compose.yml line %d is not a list entry this guard understands: %s',
                    $number,
                    trim($line),
                ));
            }

            $entry = trim(trim($matches[1]), "\"'");

            self::assertDoesNotMatchRegularExpression(
                '/^(?:target|published|host_ip|protocol|mode)\s*:/',
                $entry,
                sprintf(
                    'docker-compose.yml line %d uses the long port syntax, which this guard does '
                    . 'not verify; use the short "- "127.0.0.1:HOST:CONTAINER"" form instead '
                    . '(issue #318, SEC-15)',
                    $number,
                ),
            );

            $entries[] = [$number, $entry];
        }

        return $entries;
    }

    /**
     * The container-side port of a short-syntax port entry.
     *
     * The entry can carry a variable with a colon inside it
     * (`127.0.0.1:${PD_PORT:-2379}:2379`), so the container port is read
     * from the last colon rather than from a split.
     */
    private function containerPort(string $entry, string $file, int $line): int
    {
        $separator = strrpos($entry, ':');

        self::assertNotFalse(
            $separator,
            sprintf('%s:%d publishes "%s" without a container port', $file, $line, $entry),
        );

        $port = substr($entry, $separator + 1);

        self::assertMatchesRegularExpression(
            '/^\d+$/',
            $port,
            sprintf('%s:%d publishes "%s" with a container port that is not a number', $file, $line, $entry),
        );

        return (int) $port;
    }

    private function readFile(string $path): string
    {
        self::assertFileExists($path, sprintf('Expected %s to exist', $path));

        $contents = file_get_contents($path);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }

    private function repoRoot(): string
    {
        return dirname(__DIR__, 3);
    }

    private function relative(string $file): string
    {
        $repoRoot = $this->repoRoot() . '/';

        return str_starts_with($file, $repoRoot) ? substr($file, strlen($repoRoot)) : $file;
    }
}
