<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use ReflectionClass;
use ReflectionMethod;
use SplFileInfo;

/**
 * Guard for the stability / semantic-versioning policy (issue #406).
 *
 * The policy has three halves, and each half can rot silently:
 *
 *  - the README section that names the public API surface. A public class can
 *    be renamed, moved or dropped without any test noticing, and the promise
 *    in the README then describes a surface that no longer exists — the same
 *    trap issue #394 hit with "fifteen exception classes" (see
 *    docs/helpers/faq.md). So the classes named in the policy are checked
 *    against the source tree, and the policy is checked against them.
 *  - the classification. Every class under src/Client/ is either public or
 *    internal, and a class that belongs to neither is a class a consumer
 *    cannot reason about. The tree is scanned for that case.
 *  - the CHANGELOG convention: a breaking change carries a **BREAKING**
 *    prefix and a migration note. Nothing in the toolchain can tell a
 *    breaking change from a typo, so the convention is checked mechanically:
 *    every marked entry must tell the reader what to do about it.
 *
 * The current release and the recommended Composer constraint are checked
 * against the changelog's latest released version instead of a hard-coded
 * one, so the policy follows the next release: the check fails when a
 * release invalidates the version or the constraint the policy names, and the
 * release pull request updates the policy in the same commit (see
 * docs/release-workflow.md).
 *
 * The section names the version in more than one place — the sentence, the
 * JSON block, the range the constraint resolves to, the two paragraphs below
 * them and the intro of the section. A release pull request that updates the
 * first two and leaves the rest would publish a section that contradicts
 * itself ("the current release is v0.9.0" next to "it never pulls 0.9.0"),
 * and nothing else would notice it. So *every* version the section names is
 * checked against the releases the changelog knows about.
 */
class StabilityPolicyGuardTest extends TestCase
{
    /**
     * The namespace the policy writes its class names under.
     */
    private const CLIENT_NAMESPACE = 'CrazyGoat\\TiKV\\Client\\';

    /**
     * The public API surface the policy promises. Every class here must
     * exist, and the README must name it.
     *
     * The constants are resolved by the static analysis: a class that is
     * renamed or removed makes PHPStan report "class not found" before the
     * test even runs.
     */
    private const PUBLIC_API = [
        \CrazyGoat\TiKV\Client\RawKv\RawKvClient::class,
        \CrazyGoat\TiKV\Client\RawKv\CasResult::class,
        \CrazyGoat\TiKV\Client\RawKv\ChecksumResult::class,
        \CrazyGoat\TiKV\Client\RawKv\ScanIterator::class,
        \CrazyGoat\TiKV\Client\TxnKv\TxnKvClient::class,
        \CrazyGoat\TiKV\Client\TxnKv\Transaction::class,
        \CrazyGoat\TiKV\Client\TxnKv\TransactionStatus::class,
        \CrazyGoat\TiKV\Client\Exception\TiKvException::class,
        \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class,
        \CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException::class,
        \CrazyGoat\TiKV\Client\Retry\ErrorKind::class,
        \CrazyGoat\TiKV\Client\Observability\MetricsInterface::class,
        \CrazyGoat\TiKV\Client\Observability\InMemoryMetrics::class,
        \CrazyGoat\TiKV\Client\Region\ReplicaReadMode::class,
        \CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy::class,
    ];

    /**
     * The classes the policy promises outside src/Client/ — the generated
     * ones. A generated class is the type of a public property of a
     * promised class (`RegionException::$notLeader`), so a consumer writes
     * against it and the promise has to cover it. It is named in the
     * paragraph below the table of the policy, not in the table itself.
     *
     * The constant is resolved by the static analysis: a class that is
     * renamed or removed makes PHPStan report "class not found" before the
     * test even runs.
     */
    private const PUBLIC_API_OUTSIDE_CLIENT = [
        \CrazyGoat\Proto\Errorpb\NotLeader::class,
    ];

    /**
     * The transaction exceptions, which the policy lists by name.
     */
    private const TRANSACTION_EXCEPTIONS = [
        'DeadlockException',
        'LockWaitTimeoutException',
        'TransactionConflictException',
        'TxnAbortedByGcException',
        'TxnRetryableException',
        'UndeterminedCommitException',
    ];

    /**
     * The option keys the policy promises. Each of them has to be documented
     * in the configuration guide the policy links to.
     */
    private const PROMISED_OPTION_KEYS = [
        'tls',
        'timeout',
        'metrics',
        'apiVersion',
        'keyspace',
        'replicaRead',
        'batchCommands',
        'maxScanRows',
        'maxConcurrency',
        'tsoPoolSize',
    ];

    public function testEveryPublicApiClassExistsAndIsNamedInThePolicy(): void
    {
        $table = $this->policyTable();

        foreach (self::PUBLIC_API as $class) {
            self::assertTrue(
                class_exists($class) || interface_exists($class) || enum_exists($class),
                $class . ' is part of the promised public API but does not exist any more. '
                . 'A rename or a removal is a breaking change: update README.md, the policy '
                . 'in this test and CHANGELOG.md (with a **BREAKING** prefix and a migration note).',
            );

            $shortName = substr($class, (int) strrpos($class, '\\') + 1);
            self::assertStringContainsString(
                $shortName,
                $table,
                'The stability policy does not mention ' . $shortName . ' in its table of the '
                . 'public API, which it promises as part of that surface.',
            );
        }

        foreach (self::TRANSACTION_EXCEPTIONS as $exception) {
            self::assertStringContainsString(
                $exception,
                $table,
                'The stability policy does not mention the transaction exception ' . $exception . '.',
            );
        }

        foreach (self::PUBLIC_API_OUTSIDE_CLIENT as $class) {
            self::assertTrue(
                class_exists($class),
                $class . ' is part of the promised public API but does not exist any more. '
                . 'A rename or a removal is a breaking change: update README.md, the policy '
                . 'in this test and CHANGELOG.md (with a **BREAKING** prefix and a migration note).',
            );

            $shortName = substr($class, (int) strrpos($class, '\\') + 1);
            self::assertStringContainsString(
                $shortName,
                $this->policySection(),
                'The stability policy does not mention ' . $shortName . ', which it promises as '
                . 'part of the public API surface.',
            );
        }
    }

    /**
     * Every class of src/Client/ has to be classified: public (in the table
     * of the policy) or internal (in the list below it). A class that is
     * neither is a class a consumer cannot reason about — it is not covered
     * by the promise, but it also is not announced as internal.
     */
    public function testEveryClassOfTheClientTreeIsClassified(): void
    {
        // The policy writes a whole namespace as a wildcard ("Exception\*"),
        // so the wildcard is normalised before the tokens are compared.
        $policy = str_replace('\\*', '\\', $this->policySection());

        foreach ($this->clientClasses() as $class) {
            $relative = $this->relativeName($class);
            $parts = explode('\\', $relative);
            $namespace = array_slice($parts, 0, -1);

            $classified = str_contains($policy, '`' . $relative . '`');

            for ($depth = count($namespace); $depth > 0 && !$classified; $depth--) {
                $prefix = implode('\\', array_slice($namespace, 0, $depth)) . '\\';
                $classified = str_contains($policy, '`' . $prefix . '`');
            }

            self::assertTrue(
                $classified,
                $class . ' is neither part of the promised public API nor listed as internal in '
                . 'the stability policy. Every class of src/Client/ has to be classified: add it '
                . 'to the public API table or to the internal list.',
            );
        }
    }

    /**
     * The entry points promise every public method. The TxnKV row enumerates
     * them, so the enumeration has to be complete — a method missing from the
     * row would silently fall out of the promise.
     */
    public function testThePolicyPromisesEveryPublicMethodOfTheEntryPoints(): void
    {
        foreach ([RawKvClient::class, TxnKvClient::class, Transaction::class] as $class) {
            $relative = $this->relativeName($class);
            $row = $this->policyRow('`' . $relative . '`');

            self::assertStringContainsString(
                'every public method',
                $row,
                'The stability policy does not promise every public method of ' . $relative . '.',
            );
        }

        $row = $this->policyRow('`TxnKv\TxnKvClient`');

        foreach ($this->publicMethodsOf(TxnKvClient::class) as $method) {
            self::assertStringContainsString(
                $method . '()',
                $row,
                'The stability policy does not name ' . $method . '() of TxnKv\TxnKvClient, '
                . 'although the row enumerates the methods of that class.',
            );
        }
    }

    /**
     * The policy names the release the changelog says is current, and the
     * constraint for it. The version is read from the changelog, so the
     * policy follows the next release: when a release is cut, this test
     * fails until the policy is updated in the same pull request.
     *
     * The range the recommended constraint resolves to is checked as well:
     * it names the current release and the release that follows it, so a
     * section that mixes the two cannot pass.
     */
    public function testThePolicyDocumentsTheCurrentRelease(): void
    {
        $policy = $this->policySection();
        $version = $this->latestReleasedVersion();
        $next = $this->nextMinorVersion($version);

        self::assertStringContainsString(
            'The current release is **v' . $version . '**',
            $policy,
            'The stability policy does not name the current release (v' . $version . '). The '
            . 'release pull request has to update the "Which version to require" section of '
            . 'README.md in the same commit — see docs/release-workflow.md.',
        );

        self::assertStringContainsString(
            '~' . $version,
            $policy,
            'The stability policy does not recommend a Composer constraint for the current '
            . 'release (~' . $version . '). Update the "Which version to require" section of '
            . 'README.md in the release pull request.',
        );

        self::assertStringContainsString(
            '>=' . $version . ' <' . $next,
            $policy,
            'The stability policy does not state the range the recommended constraint resolves '
            . 'to (>=' . $version . ' <' . $next . '). The range names the current release and '
            . 'the release that follows it, so it has to follow the release as well.',
        );

        self::assertStringContainsString(
            'never pulls `' . $next . '`',
            $policy,
            'The stability policy does not say that the recommended constraint never pulls the '
            . 'next minor (' . $next . '). The release that follows the current one is the '
            . 'release that may break the code, so the policy has to name it.',
        );

        self::assertStringContainsString(
            'includes `' . $next . '`',
            $policy,
            'The stability policy does not name the next minor (' . $next . ') in the list of the '
            . 'constraints that admit it. The list has to follow the release as well.',
        );
    }

    /**
     * Every version the policy names has to be a version of this library
     * the policy is about: the current release, the release that follows
     * it, or the minor line of the current release (the two-segment form
     * a constraint uses, "~0.8").
     *
     * The prose of the section names the version in more than the two
     * places the check above knows about: the range the constraint
     * resolves to, the two paragraphs below it and the intro of the
     * section. A release pull request that updated the sentence and the
     * JSON block and left the rest would publish a section that
     * contradicts itself — which is what this test prevents.
     *
     * A version whose major is not the major of the library is not a
     * version of the library — the SemVer boundary ("1.0.0"), the
     * version of the SemVer specification, the PHP version — and is
     * skipped.
     */
    public function testThePolicyNamesOnlyTheCurrentReleaseAndTheNextMinor(): void
    {
        $version = $this->latestReleasedVersion();
        $allowed = [
            $version,
            $this->nextMinorVersion($version),
            $this->minorLine($version),
        ];
        $major = $this->majorOf($version);
        $firstLine = $this->policyStartLine();

        foreach ($this->versionLiterals($this->policySection()) as $literal) {
            if ($this->majorOf($literal['literal']) !== $major) {
                continue;
            }

            self::assertContains(
                $literal['literal'],
                $allowed,
                'README.md:' . ($firstLine + $literal['line'] - 1) . ' names the version '
                . $literal['literal'] . ', which is neither the current release (' . $version
                . ') nor the release that follows it (' . $allowed[1] . '). Every version of the '
                . '"Stability and versioning" section has to follow the releases: the release '
                . 'pull request updates all of them (see docs/release-workflow.md).',
            );
        }
    }

    /**
     * The whole README, not only the policy section: a release the README
     * names outside the section (in the installation instructions, for
     * example) goes stale with the same release and has to fail the guard
     * too.
     *
     * Only a three-part version is checked here. Outside the policy a
     * two-segment number is an example value (a timeout, a port) and not a
     * release; the two-segment forms of the policy itself ("~0.8") are
     * checked by the test above.
     */
    public function testTheReadmeNamesNoReleaseButTheCurrentOneAndTheNextMinor(): void
    {
        $version = $this->latestReleasedVersion();
        $allowed = [$version, $this->nextMinorVersion($version)];
        $major = $this->majorOf($version);

        foreach ($this->versionLiterals($this->readme()) as $literal) {
            if ($literal['parts'] !== 3 || $this->majorOf($literal['literal']) !== $major) {
                continue;
            }

            // A release that is older than the current one is a historical
            // reference ("since v0.4.0") and stays valid forever; only a
            // release that is newer than the next minor is a mistake — the
            // README must not promise a release that does not exist yet.
            if (version_compare($literal['literal'], $allowed[1], '<=')) {
                continue;
            }

            self::assertContains(
                $literal['literal'],
                $allowed,
                'README.md:' . $literal['line'] . ' names the release ' . $literal['literal']
                . ', which is neither the current release (' . $version . ') nor the release '
                . 'that follows it (' . $allowed[1] . '). A release the README names has to '
                . 'follow the releases: the release pull request updates it in the same commit '
                . '(see docs/release-workflow.md).',
            );
        }
    }

    /**
     * The guard reads the changelog, so a changelog-only pull request has
     * to run the unit tests. The `changes` job of the CI workflow skips
     * them when every changed file matches its `docs-regex` — so the
     * changelog must not be in that regex.
     *
     * The rule is written down in docs/helpers/faq.md; this test keeps the
     * workflow file and the knowledge base from drifting apart.
     */
    public function testTheChangelogIsNotClassifiedAsDocumentationOnly(): void
    {
        $workflow = $this->readFile(dirname(__DIR__, 3) . '/.github/workflows/ci.yml');

        $matches = [];
        preg_match('/docs-regex:\s*[\'"]([^\'"]+)[\'"]/', $workflow, $matches);
        $regex = $matches[1] ?? '';

        self::assertNotSame(
            '',
            $regex,
            'The CI workflow has no docs-regex for the changes job, so the guard cannot tell '
            . 'which files the job classifies as documentation-only.',
        );

        self::assertSame(
            0,
            preg_match('#' . str_replace('#', '\\#', $regex) . '#', 'CHANGELOG.md'),
            'CHANGELOG.md is classified as documentation-only by the docs-regex of the changes '
            . 'job, so a changelog-only pull request skips the unit tests — and with them the '
            . 'guard that reads the changelog. See docs/helpers/faq.md.',
        );
    }

    /**
     * The promised option keys have to be documented in the configuration
     * guide the policy links to — otherwise the promise points at a section
     * that does not list them.
     */
    public function testEveryPromisedOptionKeyIsDocumentedInTheConfigurationGuide(): void
    {
        $policy = $this->policySection();
        $configuration = $this->readFile(dirname(__DIR__, 3) . '/docs/configuration.md');

        self::assertStringContainsString(
            '](docs/configuration.md)',
            $policy,
            'The stability policy has to link to the whole configuration guide, where the '
            . 'promised options[] keys are documented — a link to a single section breaks the '
            . 'promise as soon as a key is documented elsewhere.',
        );

        foreach (self::PROMISED_OPTION_KEYS as $key) {
            self::assertStringContainsString(
                '`' . $key . '`',
                $policy,
                'The stability policy does not name the option key ' . $key . '.',
            );
            self::assertStringContainsString(
                $key,
                $configuration,
                'The stability policy promises the option key ' . $key . ', but '
                . 'docs/configuration.md does not document it.',
            );
        }
    }

    public function testEveryBreakingChangelogEntryCarriesAMigrationNote(): void
    {
        $entries = $this->breakingChangelogEntries();

        self::assertNotSame(
            [],
            $entries,
            'No changelog entry is marked as breaking. The convention is that a breaking '
            . 'change carries a **BREAKING** prefix.',
        );

        foreach ($entries as $line => $entry) {
            self::assertStringContainsString(
                'Migration:',
                $entry,
                'CHANGELOG.md:' . $line . ' is marked as breaking but has no migration note.',
            );
        }
    }

    public function testTheTwoBreakingChangesShippedInV040AreMarked(): void
    {
        $breaking = implode("\n", $this->breakingChangelogEntries());

        self::assertStringContainsString(
            'RawKv\Dto\RegionInfo',
            $breaking,
            'The move of the shared region DTOs from the RawKv to the Region namespace is a '
            . 'breaking change (the old class aliases are removed) and must be marked as one.',
        );
        self::assertStringContainsString(
            'setAtomicForCAS(true)',
            $breaking,
            'The setAtomicForCAS(true) prerequisite for CAS is a breaking change and must be '
            . 'marked as one.',
        );
    }

    /**
     * The stability section of the README.
     */
    private function policySection(): string
    {
        $readme = $this->readme();
        $rest = substr($readme, $this->policyStart() + 1);
        $end = strpos($rest, "\n## ");

        return $end === false ? $rest : substr($rest, 0, $end);
    }

    /**
     * The offset the stability section starts at.
     */
    private function policyStart(): int
    {
        $start = strpos($this->readme(), '## Stability and versioning');

        self::assertNotFalse(
            $start,
            'README.md has no "## Stability and versioning" section, so the project publishes '
            . 'no stability policy.',
        );

        return $start;
    }

    /**
     * The line the stability section starts on, so that a message can
     * point at a line of README.md and not at a line of the section.
     */
    private function policyStartLine(): int
    {
        return substr_count(substr($this->readme(), 0, $this->policyStart()), "\n") + 1;
    }

    /**
     * The rows of the policy table — the public API surface.
     */
    private function policyTable(): string
    {
        $rows = [];

        foreach (explode("\n", $this->policySection()) as $line) {
            if (str_starts_with(trim($line), '|')) {
                $rows[] = $line;
            }
        }

        return implode("\n", $rows);
    }

    /**
     * The row of the policy table that mentions the given token.
     */
    private function policyRow(string $token): string
    {
        foreach (explode("\n", $this->policyTable()) as $line) {
            if (str_contains($line, $token)) {
                return $line;
            }
        }

        self::fail('The stability policy has no table row that mentions ' . $token . '.');
    }

    private function readme(): string
    {
        return $this->readFile(dirname(__DIR__, 3) . '/README.md');
    }

    private function changelog(): string
    {
        return $this->readFile(dirname(__DIR__, 3) . '/CHANGELOG.md');
    }

    private function readFile(string $path): string
    {
        $contents = file_get_contents($path);
        self::assertNotFalse($contents, 'Unable to read ' . $path);

        return $contents;
    }

    /**
     * The latest released version, taken from the changelog's newest version
     * heading (the first one below `## [Unreleased]`).
     */
    private function latestReleasedVersion(): string
    {
        $matches = [];
        $found = preg_match('/^## \[v?(\d+\.\d+\.\d+)\]/m', $this->changelog(), $matches);

        self::assertSame(
            1,
            $found,
            'Unable to find the latest released version in CHANGELOG.md.',
        );

        return $matches[1];
    }

    /**
     * The release that follows the given one: the next minor of the same
     * major line ("0.9.0" after "0.8.0"). It is the release that may break
     * the code, so the policy has to name it as well.
     */
    private function nextMinorVersion(string $version): string
    {
        $parts = $this->versionParts($version);

        return $parts['major'] . '.' . ((int) $parts['minor'] + 1) . '.0';
    }

    /**
     * The minor line of the given version ("0.8" of "0.8.0") — the form a
     * two-segment constraint uses ("~0.8").
     */
    private function minorLine(string $version): string
    {
        $parts = $this->versionParts($version);

        return $parts['major'] . '.' . $parts['minor'];
    }

    /**
     * The major version of a version ("0" of "0.8.0" and of "0.8").
     */
    private function majorOf(string $version): string
    {
        $parts = explode('.', $version);

        return $parts[0];
    }

    /**
     * The parts of a version.
     *
     * @return array{major: string, minor: string, patch: string}
     */
    private function versionParts(string $version): array
    {
        $matches = [];
        $found = preg_match('/^(\d+)\.(\d+)\.(\d+)$/', $version, $matches);

        self::assertSame(
            1,
            $found,
            'The version ' . $version . ' is not a major.minor.patch version.',
        );

        return [
            'major' => $matches[1],
            'minor' => $matches[2],
            'patch' => $matches[3],
        ];
    }

    /**
     * Every version literal of a text.
     *
     * A literal is a dotted number — "0.8.0" (a release) or "0.8" (the
     * minor line of a release, the way a two-segment constraint writes
     * it). The number of an IP address is not a version, so a literal that
     * starts inside one is skipped.
     *
     * @return list<array{literal: string, parts: int, line: int}>
     */
    private function versionLiterals(string $text): array
    {
        $literals = [];

        foreach (explode("\n", $text) as $index => $line) {
            $matches = [];
            // The lookahead skips an IPv4 address ("127.0.0.1" must not be
            // read as the version "127.0.0"), the lookbehind skips the
            // second half of one.
            preg_match_all('/(?<![\w.])(?:v)?(\d+\.\d+(?:\.\d+)?)(?!\.\d)/', $line, $matches);

            foreach ($matches[1] as $literal) {
                $literals[] = [
                    'literal' => $literal,
                    'parts' => substr_count($literal, '.') + 1,
                    'line' => $index + 1,
                ];
            }
        }

        return $literals;
    }

    /**
     * Every changelog entry marked as breaking, keyed by the line it starts
     * on. An entry spans the bullet and its continuation lines.
     *
     * Only the version sections are scanned: the preamble of the file
     * describes the convention itself, and a bullet there explains the
     * marker instead of using it.
     *
     * The marker is a **prefix** of the bullet, not a word anywhere in it: the
     * convention is a prefix, and a prose mention of it (an entry that
     * explains the convention) must not be read as a breaking change.
     *
     * @return array<int, string>
     */
    private function breakingChangelogEntries(): array
    {
        $lines = explode("\n", $this->changelog());
        $entries = [];
        $current = null;
        $body = [];
        $inSection = false;

        foreach ($lines as $index => $line) {
            if (str_starts_with($line, '## [')) {
                $inSection = true;
            }

            if (!$inSection) {
                continue;
            }

            $isBullet = preg_match('/^\s*[-*]\s/', $line) === 1;

            if ($isBullet) {
                if ($current !== null) {
                    $entries[$current] = implode("\n", $body);
                }

                $current = null;
                $body = [];

                if (preg_match('/^\s*[-*]\s+\*\*BREAKING\*\*/', $line) === 1) {
                    $current = $index + 1;
                    $body[] = $line;
                }

                continue;
            }

            if ($current !== null) {
                $body[] = $line;
            }
        }

        if ($current !== null) {
            $entries[$current] = implode("\n", $body);
        }

        return $entries;
    }

    /**
     * Every class, interface and enum under src/Client/, by fully qualified
     * name.
     *
     * @return list<string>
     */
    private function clientClasses(): array
    {
        $root = dirname(__DIR__, 3) . '/src/Client';
        $classes = [];

        $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root));

        foreach ($files as $file) {
            if (!$file instanceof SplFileInfo || !$file->isFile() || $file->getExtension() !== 'php') {
                continue;
            }

            $contents = $this->readFile($file->getPathname());
            $namespace = $this->firstMatch('/^namespace\s+([^;]+);/m', $contents, $file->getPathname());
            $type = $this->firstMatch(
                '/^(?:(?:final|readonly|abstract)\s+)*(?:class|interface|enum|trait)\s+(\w+)/m',
                $contents,
                $file->getPathname(),
            );

            $classes[] = $namespace . '\\' . $type;
        }

        sort($classes);

        return $classes;
    }

    /**
     * The first capture group of the first match of a pattern.
     */
    private function firstMatch(string $pattern, string $contents, string $path): string
    {
        $matches = [];
        $found = preg_match($pattern, $contents, $matches);

        self::assertSame(
            1,
            $found,
            $path . ' does not match ' . $pattern . ' — the guard test cannot classify it.',
        );

        return $matches[1] ?? '';
    }

    /**
     * The name of a class relative to the client namespace, the way the
     * policy writes it ("RawKv\RawKvClient").
     */
    private function relativeName(string $class): string
    {
        if (!str_starts_with($class, self::CLIENT_NAMESPACE)) {
            return $class;
        }

        return substr($class, strlen(self::CLIENT_NAMESPACE));
    }

    /**
     * The public methods of a class, without the magic ones.
     *
     * @param class-string $class
     *
     * @return list<string>
     */
    private function publicMethodsOf(string $class): array
    {
        $methods = [];

        foreach ((new ReflectionClass($class))->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
            if (str_starts_with($method->getName(), '__')) {
                continue;
            }

            $methods[] = $method->getName();
        }

        sort($methods);

        return $methods;
    }
}
