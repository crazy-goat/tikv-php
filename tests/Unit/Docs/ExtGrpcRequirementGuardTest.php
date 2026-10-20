<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #408: the `ext-grpc` PHP extension is a hard
 * requirement of this library, and the documentation has to say so.
 *
 * Every RPC is built on the extension (`Grpc\Call::startBatch()`,
 * `Grpc\Timeval`, `Grpc\Channel`, the `OP_*` call-operation constants), so a
 * machine without it installed the package happily and only then died on the
 * first call with "Class 'Grpc\Channel' not found" — in production, not in
 * the deployment pipeline. The package therefore declares the extension in
 * `composer.json`, which makes Composer refuse the install with a clear
 * message instead.
 *
 * Three things have to stay in step, and only this test can hold them
 * together:
 *
 *  - the constraint in `composer.json` (the enforcement),
 *  - the minimum version the documentation names (the same floor the
 *    `grpc/grpc` library constraint sets, and the first release that ships
 *    `Grpc\Call::startBatch()` and `Grpc\Timeval`),
 *  - the per-platform install instructions (Debian/Ubuntu, Alpine, macOS) a
 *    reader needs before `composer require` can succeed at all.
 *
 * The test also holds the two rules that are easy to break by accident:
 *
 *  - the quoted Composer error must be the one a consumer sees, not the
 *    root-package variant that only a contributor of this repository meets,
 *  - the Unit suite has to keep running without the extension, so exactly
 *    one install step (the unit job) skips the platform requirement, and the
 *    FAQ has to say why.
 *
 * The version is read out of `composer.json` and never restated here, so a
 * bump in the package and a bump in the docs cannot drift apart: only the
 * floor is a literal in this file, and it is a floor rather than a copy —
 * raising the requirement above it is fine, lowering it below it is not.
 */
final class ExtGrpcRequirementGuardTest extends TestCase
{
    /**
     * The first gRPC release that ships every extension API the client uses:
     * `Grpc\Call::startBatch()` (call.c) and `Grpc\Timeval` (timeval.c).
     */
    private const MINIMUM_SUPPORTED_VERSION = '1.57';

    /**
     * The files that name the requirement. A reader reaches the version from
     * any of them, so all of them have to carry it.
     *
     * @var list<string>
     */
    private const DOCUMENTED_FILES = [
        'README.md',
        'docs/README.md',
        'docs/getting-started.md',
        'docs/troubleshooting.md',
        'docs/contributing.md',
        'docs/architecture.md',
    ];

    /**
     * The files that quote the Composer error a reader meets when the
     * extension is missing.
     *
     * @var list<string>
     */
    private const ERROR_MESSAGE_FILES = [
        'README.md',
        'docs/getting-started.md',
        'docs/troubleshooting.md',
    ];

    /**
     * The files that carry the install instructions, and the markers each
     * platform needs: the distribution name and the command that installs
     * the extension there.
     *
     * @var array<string, list<string>>
     */
    private const PLATFORM_MARKERS = [
        'Debian/Ubuntu' => ['debian', 'ubuntu', 'pecl install grpc'],
        'Alpine' => ['alpine', 'apk add php'],
        'macOS' => ['macos', 'pecl install grpc'],
    ];

    /**
     * The files that install the dependencies without the extension, so the
     * Unit suite keeps running on a machine that has none.
     *
     * @var list<string>
     */
    private const EXTENSION_FREE_INSTALL_FILES = [
        '.github/workflows/ci.yml',
        'bin/worktree-setup.sh',
        'scripts/test.sh',
    ];

    /**
     * The Composer flag that skips the platform requirement for one install.
     */
    private const IGNORE_PLATFORM_REQ = '--ignore-platform-req=ext-grpc';

    public function testComposerRequiresTheGrpcExtension(): void
    {
        $constraint = $this->composerConstraint('ext-grpc');

        self::assertNotSame(
            '',
            $this->minimumVersion($constraint),
            sprintf(
                'composer.json must require ext-grpc with a minimum version, got "%s"',
                $constraint,
            ),
        );
    }

    public function testRequiredVersionIsOneTheExtensionCanProvide(): void
    {
        $minimum = $this->minimumVersion($this->composerConstraint('ext-grpc'));

        self::assertTrue(
            version_compare($minimum, self::MINIMUM_SUPPORTED_VERSION, '>='),
            sprintf(
                'ext-grpc %s is older than %s, the first release with '
                . 'Grpc\Call::startBatch() and Grpc\Timeval',
                $minimum,
                self::MINIMUM_SUPPORTED_VERSION,
            ),
        );
    }

    public function testDocumentedVersionMatchesTheRequirement(): void
    {
        $minimum = $this->minimumVersion($this->composerConstraint('ext-grpc'));

        foreach (self::DOCUMENTED_FILES as $file) {
            $contents = $this->read($file);

            self::assertStringContainsString(
                'ext-grpc',
                $contents,
                sprintf('%s must name the ext-grpc extension as a requirement', $file),
            );
            self::assertStringContainsString(
                $minimum,
                $contents,
                sprintf(
                    '%s must name the required ext-grpc version (%s)',
                    $file,
                    $minimum,
                ),
            );
        }
    }

    public function testInstallInstructionsExistForEverySupportedPlatform(): void
    {
        foreach (['README.md', 'docs/getting-started.md'] as $file) {
            $contents = strtolower($this->read($file));

            foreach (self::PLATFORM_MARKERS as $platform => $markers) {
                foreach ($markers as $marker) {
                    self::assertStringContainsString(
                        $marker,
                        $contents,
                        sprintf(
                            '%s has no %s install instructions (missing "%s")',
                            $file,
                            $platform,
                            $marker,
                        ),
                    );
                }
            }
        }
    }

    /**
     * The quoted Composer error must be the one a reader of the
     * documentation actually meets. A consumer that installs the package
     * sees the dependency form ("<package> <version> requires ext-grpc … ->
     * it is missing from your system"); the root-package form ("Root
     * composer.json requires PHP extension ext-grpc …") is what a
     * contributor working on this repository sees.
     */
    public function testTheQuotedComposerErrorIsTheOneAConsumerSees(): void
    {
        foreach (self::ERROR_MESSAGE_FILES as $file) {
            $contents = $this->read($file);

            self::assertStringContainsString(
                'requires ext-grpc',
                $contents,
                sprintf('%s must quote the Composer error a consumer sees', $file),
            );
            self::assertStringContainsString(
                '-> it is missing from your system',
                $contents,
                sprintf('%s must quote the Composer error a consumer sees', $file),
            );
        }
    }

    /**
     * The `php-grpc` distribution package lives in the sury.org PHP
     * repository, not in stock Debian or Ubuntu. Recommending it without
     * naming the repository sends the reader to a package that does not
     * exist ("E: Unable to locate package php-grpc").
     */
    public function testTheDistributionPackageNamesItsRepository(): void
    {
        foreach (self::ERROR_MESSAGE_FILES as $file) {
            $contents = $this->read($file);

            if (!str_contains($contents, 'apt-get install php-grpc')) {
                continue;
            }

            self::assertStringContainsString(
                'packages.sury.org',
                $contents,
                sprintf(
                    '%s recommends "apt-get install php-grpc" but does not say '
                    . 'that the package comes from the sury.org PHP repository',
                    $file,
                ),
            );
        }
    }

    /**
     * The library requires the extension, but the Unit suite must stay
     * extension-free — so the install of that one workflow skips the
     * platform requirement. The rule lives in the FAQ and is applied by the
     * CI job and by the local install scripts.
     */
    public function testTheExtensionFreeUnitWorkflowIsKeptAndDocumented(): void
    {
        foreach (self::EXTENSION_FREE_INSTALL_FILES as $file) {
            self::assertStringContainsString(
                self::IGNORE_PLATFORM_REQ,
                $this->read($file),
                sprintf(
                    '%s must install the dependencies with %s so the Unit suite '
                    . 'runs without the extension',
                    $file,
                    self::IGNORE_PLATFORM_REQ,
                ),
            );
        }

        $workflow = $this->read('.github/workflows/ci.yml');

        self::assertSame(
            1,
            substr_count($workflow, self::IGNORE_PLATFORM_REQ),
            sprintf(
                'only the unit job may install with %s — every other job has to '
                . 'enforce the requirement',
                self::IGNORE_PLATFORM_REQ,
            ),
        );

        self::assertStringContainsString(
            self::IGNORE_PLATFORM_REQ,
            $this->read('docs/helpers/faq.md'),
            'docs/helpers/faq.md must record why the unit job installs without the extension',
        );
    }

    /**
     * The constraint a package is required with, or an empty string when the
     * package is not required at all.
     */
    private function composerConstraint(string $package): string
    {
        $composer = json_decode($this->read('composer.json'), true, 512, JSON_THROW_ON_ERROR);
        self::assertIsArray($composer);

        $require = $composer['require'] ?? null;
        self::assertIsArray($require);

        $constraint = $require[$package] ?? null;
        self::assertIsString($constraint, sprintf('composer.json must require %s', $package));

        return $constraint;
    }

    /**
     * The lowest version a constraint allows, e.g. "1.57" for ">=1.57" and
     * for "^1.57". An empty string when the constraint names no version.
     */
    private function minimumVersion(string $constraint): string
    {
        if (preg_match('/(?:>=\s*|\^\s*)(\d+\.\d+)/', $constraint, $matches) === 1) {
            return $matches[1];
        }

        return '';
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
