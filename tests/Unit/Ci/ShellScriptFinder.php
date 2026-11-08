<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

/**
 * Every shell script under a scripts directory, recursively (issue #730).
 *
 * `TestSuiteSelectorGuardTest` scanned `scripts/*.sh` only, so a PHPUnit
 * invocation in a helper under `scripts/lib/` escaped rule 2 ("every PHPUnit
 * invocation carries --fail-on-empty-test-suite"). `glob('scripts/**\/*.sh')`
 * is not a fix: PHP's glob has no `**` recursion and would then miss the
 * top-level scripts instead. This walks the tree.
 */
final class ShellScriptFinder
{
    /**
     * @param string $directory absolute path of the scripts directory
     *
     * @return list<string> absolute paths, sorted
     */
    public static function under(string $directory): array
    {
        if (!is_dir($directory)) {
            return [];
        }

        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS),
        );

        $files = [];

        foreach ($iterator as $entry) {
            if ($entry instanceof SplFileInfo && $entry->isFile() && $entry->getExtension() === 'sh') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }
}
