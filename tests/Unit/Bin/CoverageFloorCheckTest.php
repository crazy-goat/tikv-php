<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Bin;

use PHPUnit\Framework\TestCase;

/**
 * The coverage floor gate of CI (issue #362).
 *
 * The check itself lives in bin/check-coverage.php because it is a build
 * tool, not library code; these tests drive it as a subprocess, which is
 * exactly how CI calls it.
 */
final class CoverageFloorCheckTest extends TestCase
{
    /**
     * The ratcheted floor, read out of the script itself.
     *
     * The script owns the value (raising it is a deliberate act, and the
     * script is the only place that enforces it). This test reads the constant
     * out of the script instead of repeating it, so the two can never drift
     * apart — and the "a tenth below the floor has to fail" test below uses
     * the real value, so even a fractional lowering is caught.
     */

    private string $reportPath = '';

    protected function tearDown(): void
    {
        if ($this->reportPath !== '' && is_file($this->reportPath)) {
            unlink($this->reportPath);
        }

        $this->reportPath = '';
    }

    public function testCoverageBelowTheFloorFails(): void
    {
        [$exitCode, $output] = $this->runCheck($this->writeReport(60, 100));

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('60.00%', $output);
        self::assertStringContainsString(sprintf('floor: %.2f%%', $this->defaultFloor()), $output);
    }

    public function testCoverageAboveTheFloorPasses(): void
    {
        [$exitCode, $output] = $this->runCheck($this->writeReport(95, 100));

        self::assertSame(0, $exitCode);
        self::assertStringContainsString('95.00%', $output);
        self::assertStringContainsString('Coverage floor satisfied.', $output);
    }

    public function testCoverageExactlyAtTheFloorPasses(): void
    {
        [$exitCode] = $this->runCheck($this->writeReport($this->defaultFloor(), 100));

        self::assertSame(0, $exitCode);
    }

    public function testTheDefaultFloorIsTheRatchetedOne(): void
    {
        // Without an explicit --floor the script uses DEFAULT_COVERAGE_FLOOR,
        // so a report a tenth of a point below it has to fail — the floor is
        // not allowed to be lowered silently, not even by a fraction.
        // 1 000 lines, so a tenth of a point is representable in the report.
        [$exitCode] = $this->runCheck($this->writeReport($this->defaultFloor() - 0.1, 1000));

        self::assertSame(1, $exitCode);
    }

    public function testAnExplicitFloorOverridesTheDefault(): void
    {
        [$exitCode] = $this->runCheck($this->writeReport(60, 100), '--floor=50.0');

        self::assertSame(0, $exitCode);
    }

    public function testAFloorThatIsNotAPercentageIsAnError(): void
    {
        [$exitCode, $output] = $this->runCheck($this->writeReport(60, 100), '--floor=nonsense');

        self::assertSame(2, $exitCode);
        self::assertStringContainsString('is not a percentage', $output);
    }

    public function testAMissingReportIsAnError(): void
    {
        [$exitCode, $output] = $this->runCheck('/nonexistent/coverage.xml');

        self::assertSame(2, $exitCode);
        self::assertStringContainsString('does not exist', $output);
    }

    public function testAnUnreadableReportIsAnError(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'coverage-');
        self::assertIsString($path);
        file_put_contents($path, 'not xml at all');
        $this->reportPath = $path;

        [$exitCode, $output] = $this->runCheck($path);

        self::assertSame(2, $exitCode);
        self::assertStringContainsString('not a readable XML file', $output);
    }

    /**
     * The floor the script enforces by default.
     */
    private function defaultFloor(): float
    {
        $script = (string) file_get_contents(dirname(__DIR__, 3) . '/bin/check-coverage.php');
        $matched = preg_match('/const DEFAULT_COVERAGE_FLOOR = ([0-9.]+);/', $script, $matches);
        if ($matched !== 1) {
            self::fail('bin/check-coverage.php must declare DEFAULT_COVERAGE_FLOOR');
        }

        return (float) $matches[1];
    }

    /**
     * Runs bin/check-coverage.php the way CI does.
     *
     * @return array{int, string} the exit code and the combined output
     */
    private function runCheck(string $reportPath, string ...$arguments): array
    {
        $command = escapeshellarg(PHP_BINARY)
            . ' '
            . escapeshellarg(dirname(__DIR__, 3) . '/bin/check-coverage.php')
            . ' '
            . escapeshellarg($reportPath);

        foreach ($arguments as $argument) {
            $command .= ' ' . escapeshellarg($argument);
        }

        $output = [];
        $exitCode = 0;
        exec($command . ' 2>&1', $output, $exitCode);

        return [$exitCode, implode("\n", $output)];
    }

    /**
     * Writes a Clover report with the given coverage.
     *
     * @param float $coverage the coverage percentage to encode
     * @param int   $total    the number of executable lines
     */
    private function writeReport(float $coverage, int $total): string
    {
        $covered = (int) round($total * $coverage / 100);

        $path = tempnam(sys_get_temp_dir(), 'coverage-');
        self::assertIsString($path);
        $this->reportPath = $path;

        file_put_contents(
            $path,
            sprintf(
                '<?xml version="1.0" encoding="UTF-8"?>' . "\n"
                . '<coverage generated="0">' . "\n"
                . '  <project timestamp="0" name="Clover Coverage">' . "\n"
                . '    <metrics files="1" loc="10" ncloc="10" classes="1" methods="1"'
                . ' coveredmethods="1" statements="%d" coveredstatements="%d"'
                . ' elements="2" coveredelements="2"/>' . "\n"
                . '  </project>' . "\n"
                . '</coverage>' . "\n",
                $total,
                $covered
            )
        );

        return $path;
    }
}
