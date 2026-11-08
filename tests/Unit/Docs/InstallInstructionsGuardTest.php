<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #733: the install instructions named
 * `crazy-goat/tikv-client` on Packagist, where the package is not published
 * (the Packagist metadata request returns 404). An external consumer could not
 * follow the command at all, and the three documents that carry it — the
 * README, `docs/README.md` and `docs/getting-started.md` — had no guard that
 * kept them in step with each other or with the released version.
 *
 * Until the package is published, Composer has to be pointed at the GitHub
 * repository as a VCS repository. The install command then names the
 * recommended constraint from the README's "Which version to require" section,
 * so it has to follow the release: the version is read from `CHANGELOG.md`,
 * never restated here, and a release pull request that forgets one of the
 * documents fails this test in the same way it fails the stability policy
 * guard.
 *
 * A document that goes back to the bare `composer require
 * crazy-goat/tikv-client` is the original bug, so the guard rejects it as
 * well. The check is a test rather than a Markdown linter because every other
 * documentation invariant of this repository already lives in a
 * `tests/Unit/Docs` guard that runs in the unit matrix.
 */
final class InstallInstructionsGuardTest extends TestCase
{
    /**
     * The package the documents install and `composer.json` declares.
     */
    private const PACKAGE = 'crazy-goat/tikv-client';

    /**
     * The repository the package is installed from while it is not on
     * Packagist.
     */
    private const REPOSITORY = 'https://github.com/crazy-goat/tikv-php';

    /**
     * Every document that carries the install instructions.
     *
     * @var list<string>
     */
    private const INSTALL_FILES = [
        'README.md',
        'docs/README.md',
        'docs/getting-started.md',
    ];

    public function testThePackageNameMatchesComposerJson(): void
    {
        $composer = json_decode($this->read('composer.json'), true, 512, JSON_THROW_ON_ERROR);

        self::assertIsArray($composer);
        self::assertSame(
            self::PACKAGE,
            $composer['name'] ?? null,
            'The install instructions name ' . self::PACKAGE . ', but composer.json declares a '
            . 'different package name. The package a consumer requires has to be the one this '
            . 'repository publishes.',
        );
    }

    /**
     * While the package is not published, every install instruction has to add
     * the VCS repository it can be found in — without it Composer searches
     * Packagist only and fails.
     */
    public function testEveryDocumentAddsTheVcsRepository(): void
    {
        foreach (self::INSTALL_FILES as $file) {
            $contents = $this->read($file);

            self::assertStringContainsString(
                '"type": "vcs"',
                $contents,
                sprintf(
                    '%s does not declare the package repository as a VCS repository, so the '
                    . 'install command cannot resolve %s',
                    $file,
                    self::PACKAGE,
                ),
            );
            self::assertStringContainsString(
                '"url": "' . self::REPOSITORY . '"',
                $contents,
                sprintf(
                    '%s does not point the VCS repository at %s',
                    $file,
                    self::REPOSITORY,
                ),
            );
        }
    }

    /**
     * The install command names the recommended constraint of the current
     * release, so that all three documents and the "Which version to require"
     * section install the same version.
     */
    public function testEveryDocumentRequiresTheCurrentRelease(): void
    {
        $command = 'composer require ' . self::PACKAGE . ':~' . $this->latestReleasedVersion();

        foreach (self::INSTALL_FILES as $file) {
            self::assertSame(
                1,
                substr_count($this->read($file), $command),
                sprintf(
                    '%s must contain exactly one "%s" command, so every document installs the '
                    . 'release named by the changelog and the "Which version to require" '
                    . 'section',
                    $file,
                    $command,
                ),
            );
        }
    }

    /**
     * The bare command is the original bug: it relies on Packagist, where the
     * package does not exist.
     */
    public function testNoDocumentRecommendsAnUnconstrainedInstall(): void
    {
        foreach (self::INSTALL_FILES as $file) {
            $matches = [];
            preg_match_all(
                '/composer\s+require\s+' . preg_quote(self::PACKAGE, '/') . '(?![:\\w\\/.-])/',
                $this->read($file),
                $matches,
            );

            self::assertSame(
                [],
                $matches[0],
                sprintf(
                    '%s has an unconstrained "composer require %s" command. The package is not '
                    . 'published on Packagist: require the tagged release through the VCS '
                    . 'repository instead',
                    $file,
                    self::PACKAGE,
                ),
            );
        }
    }

    /**
     * The latest released version, taken from the changelog's newest version
     * heading (the first one below `## [Unreleased]`).
     */
    private function latestReleasedVersion(): string
    {
        $matches = [];
        $found = preg_match('/^## \[v?(\d+\.\d+\.\d+)\]/m', $this->read('CHANGELOG.md'), $matches);

        self::assertSame(
            1,
            $found,
            'Unable to find the latest released version in CHANGELOG.md.',
        );

        return $matches[1];
    }

    private function read(string $relativePath): string
    {
        $path = dirname(__DIR__, 3) . '/' . $relativePath;

        self::assertFileExists($path);

        $contents = file_get_contents($path);
        self::assertNotFalse($contents);

        return $contents;
    }
}
