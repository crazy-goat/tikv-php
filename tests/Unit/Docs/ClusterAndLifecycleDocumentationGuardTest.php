<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #397 (DOC-31): the cluster-level and client-local
 * methods of RawKvClient must stay documented in docs/operations.md, and
 * docs/advanced.md must not recommend a write-based health probe.
 *
 * The defect the guard protects against is absent documentation: a public
 * method can be added or renamed without any behavioural test noticing, because
 * no test calls the prose. The signature list is deliberately restated here
 * rather than derived by reflection: the guard also pins that the documented
 * signature still matches what a reader copies, and a renamed parameter or a
 * changed return type is exactly the drift a docs guard is for.
 */
final class ClusterAndLifecycleDocumentationGuardTest extends TestCase
{
    /** The heading of the section under guard. */
    private const SECTION_HEADING = "\n## Cluster and lifecycle\n";

    /** The health-check heading in docs/advanced.md. */
    private const ADVANCED_HEALTH_HEADING = "\n### Health Checks\n";

    public function testOperationsGuideDocumentsEveryClusterAndLifecycleSignature(): void
    {
        $section = $this->section(
            $this->readFile('docs/operations.md'),
            self::SECTION_HEADING,
        );

        $signatures = [
            'public function healthCheck(): ?int',
            'public function getClusterId(): ?int',
            'public function getMetrics(): MetricsInterface',
            'public function setColumnFamily(string $cf): self',
            'public function getColumnFamily(): string',
            'public function isAtomicForCAS(): bool',
        ];

        foreach ($signatures as $signature) {
            self::assertStringContainsString(
                $signature,
                $section,
                sprintf(
                    'The "Cluster and lifecycle" section of docs/operations.md '
                    . 'must document the signature "%s".',
                    $signature,
                ),
            );
        }
    }

    public function testOperationsGuideStatesTheScopeAndLimits(): void
    {
        $section = $this->section(
            $this->readFile('docs/operations.md'),
            self::SECTION_HEADING,
        );

        $statements = [
            'GetMembers',
            'HealthCheckException',
            'ClientClosedException',
            'does not prove',
            'per client instance',
            "''",
            'API V2',
            'TxnKvClient',
        ];

        foreach ($statements as $statement) {
            self::assertStringContainsString(
                $statement,
                $section,
                sprintf(
                    'The "Cluster and lifecycle" section of docs/operations.md '
                    . 'must state "%s".',
                    $statement,
                ),
            );
        }
    }

    public function testAdvancedGuideDoesNotRecommendAWriteBasedHealthProbe(): void
    {
        $section = $this->section(
            $this->readFile('docs/advanced.md'),
            self::ADVANCED_HEALTH_HEADING,
        );

        self::assertStringContainsString(
            'healthCheck()',
            $section,
            'The "Health Checks" section of docs/advanced.md must document healthCheck().',
        );

        self::assertStringNotContainsString(
            'TiKvHealthChecker',
            $section,
            'docs/advanced.md must not bring back the write-based TiKvHealthChecker example.',
        );

        self::assertStringNotContainsString(
            'health:check:',
            $section,
            'docs/advanced.md must not recommend writing a health-check key on every probe.',
        );
    }

    /**
     * The text of a markdown section, from its heading up to the next heading
     * of the same or a shallower level.
     */
    private function section(string $contents, string $heading): string
    {
        $start = strpos($contents, $heading);
        self::assertNotFalse(
            $start,
            sprintf('No "%s" section found.', trim($heading)),
        );

        $level = 1;
        if (preg_match('/^#+/', trim($heading), $matches) === 1) {
            $level = strlen($matches[0]);
        }

        $rest = substr($contents, $start + strlen($heading));
        $end = strlen($rest);
        if (preg_match('/\n#{1,' . $level . '} /', $rest, $matches, PREG_OFFSET_CAPTURE) === 1) {
            $end = $matches[0][1];
        }

        return substr($rest, 0, $end);
    }

    private function readFile(string $relativePath): string
    {
        $file = dirname(__DIR__, 3) . '/' . $relativePath;
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $file));

        return $contents;
    }
}
