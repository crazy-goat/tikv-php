<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Ci;

use PHPUnit\Framework\TestCase;

/**
 * Recursive shell-script discovery of the runner guard (issue #730).
 *
 * The guard has to see `scripts/*.sh` and the helpers under `scripts/lib/`
 * alike; a single-level glob silently skipped the helpers.
 */
final class ShellScriptFinderTest extends TestCase
{
    private string $root = '';

    protected function setUp(): void
    {
        $this->root = sys_get_temp_dir() . '/tikv-shell-scripts-' . bin2hex(random_bytes(6));

        mkdir($this->root . '/lib/nested', 0777, true);
        file_put_contents($this->root . '/run.sh', "#!/bin/bash\n");
        file_put_contents($this->root . '/lib/helper.sh', "#!/bin/bash\n");
        file_put_contents($this->root . '/lib/nested/deep.sh', "#!/bin/bash\n");
        file_put_contents($this->root . '/lib/notes.md', "not a shell script\n");
    }

    protected function tearDown(): void
    {
        if ($this->root === '' || !is_dir($this->root)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($this->root, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );

        foreach ($iterator as $entry) {
            if (!$entry instanceof \SplFileInfo) {
                continue;
            }

            if ($entry->isDir()) {
                rmdir($entry->getPathname());

                continue;
            }

            unlink($entry->getPathname());
        }

        rmdir($this->root);
    }

    public function testItFindsTopLevelAndNestedShellScripts(): void
    {
        self::assertSame(
            [
                $this->root . '/lib/helper.sh',
                $this->root . '/lib/nested/deep.sh',
                $this->root . '/run.sh',
            ],
            ShellScriptFinder::under($this->root),
        );
    }

    public function testAMissingScriptsDirectoryHasNoScripts(): void
    {
        self::assertSame([], ShellScriptFinder::under($this->root . '/missing'));
    }
}
