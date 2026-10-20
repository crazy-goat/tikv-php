<?php

/**
 * Coverage floor check (issue #362).
 *
 * PHPUnit writes a Clover XML report; this script reads the line coverage
 * from that report and fails when the coverage dropped below the floor.
 *
 * The floor is a ratchet: it is set a little below the measured coverage and
 * only ever moves up. Both the CI job and `composer test:coverage` call this
 * script, so the threshold lives in exactly one place.
 *
 * Usage:
 *   php bin/check-coverage.php [report.xml] [--floor=<percent>]
 *
 * The report path defaults to "coverage.xml" (the file PHPUnit writes, see the
 * <coverage> section of phpunit.xml) and the floor to DEFAULT_COVERAGE_FLOOR.
 *
 * Exit codes:
 *   0 — the coverage is at or above the floor
 *   1 — the coverage is below the floor
 *   2 — the arguments are wrong or the report is missing or unreadable
 */

declare(strict_types=1);

const DEFAULT_COVERAGE_FLOOR = 87.0;

const EXIT_OK = 0;
const EXIT_BELOW_FLOOR = 1;
const EXIT_USAGE_ERROR = 2;

$reportPath = 'coverage.xml';
$floor = DEFAULT_COVERAGE_FLOOR;

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

foreach (array_slice($arguments, 1) as $argument) {
    if (str_starts_with($argument, '--floor=')) {
        $floorArgument = substr($argument, strlen('--floor='));

        if (!is_numeric($floorArgument) || (float) $floorArgument < 0.0 || (float) $floorArgument > 100.0) {
            fwrite(STDERR, "The floor '{$floorArgument}' is not a percentage between 0 and 100.\n");

            exit(EXIT_USAGE_ERROR);
        }

        $floor = (float) $floorArgument;

        continue;
    }

    $reportPath = $argument;
}

if (!is_file($reportPath)) {
    fwrite(STDERR, "Coverage report '{$reportPath}' does not exist.\n");

    exit(EXIT_USAGE_ERROR);
}

$report = @simplexml_load_file($reportPath);

if ($report === false) {
    fwrite(STDERR, "Coverage report '{$reportPath}' is not a readable XML file.\n");

    exit(EXIT_USAGE_ERROR);
}

if (!isset($report->project->metrics)) {
    fwrite(STDERR, "Coverage report '{$reportPath}' has no project metrics.\n");

    exit(EXIT_USAGE_ERROR);
}

$metrics = $report->project->metrics;
$totalLines = (int) (string) $metrics['statements'];
$coveredLines = (int) (string) $metrics['coveredstatements'];

if ($totalLines <= 0) {
    fwrite(STDERR, "Coverage report '{$reportPath}' contains no executable lines.\n");

    exit(EXIT_USAGE_ERROR);
}

$coverage = $coveredLines / $totalLines * 100;

printf(
    "Line coverage: %.2f%% (%d/%d lines), floor: %.2f%%\n",
    $coverage,
    $coveredLines,
    $totalLines,
    $floor
);

if ($coverage + 0.000001 < $floor) {
    fwrite(
        STDERR,
        sprintf(
            "Coverage floor violated: %.2f%% is below the floor of %.2f%%. "
            . "Raise the floor only together with the tests that cover the new code.\n",
            $coverage,
            $floor
        )
    );

    exit(EXIT_BELOW_FLOOR);
}

echo "Coverage floor satisfied.\n";

exit(EXIT_OK);
