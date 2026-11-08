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
        \CrazyGoat\TiKV\Client\RawKv\KeyTtl::class,
        \CrazyGoat\TiKV\Client\RawKv\ScanIterator::class,
        \CrazyGoat\TiKV\Client\TxnKv\TxnKvClient::class,
        \CrazyGoat\TiKV\Client\TxnKv\Transaction::class,
        \CrazyGoat\TiKV\Client\TxnKv\TransactionStatus::class,
        \CrazyGoat\TiKV\Client\Exception\TiKvException::class,
        \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class,
        \CrazyGoat\TiKV\Client\Exception\NotLeaderHint::class,
        \CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException::class,
        \CrazyGoat\TiKV\Client\Retry\ErrorKind::class,
        \CrazyGoat\TiKV\Client\Observability\MetricsInterface::class,
        \CrazyGoat\TiKV\Client\Observability\InMemoryMetrics::class,
        \CrazyGoat\TiKV\Client\Region\ReplicaReadMode::class,
        \CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy::class,
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
    }

    /**
     * Every class of src/Client/ has to be classified: public (in the table
     * of the policy) or internal (in the list below it). A class that is
     * neither is a class a consumer cannot reason about — it is not covered
     * by the promise, but it also is not announced as internal.
     *
     * The classification compares the two lists as sets instead of asking
     * whether the class is named somewhere in the section: a class that a
     * release pull request adds to both the public table and the internal
     * list is ambiguous, and the reader cannot tell which promise applies.
     */
    public function testEveryClassOfTheClientTreeIsClassified(): void
    {
        $violations = $this->classificationViolations($this->policySection(), $this->clientClasses());

        self::assertSame(
            [],
            $violations,
            "The stability policy does not classify every class of src/Client/ exactly once:\n"
            . implode("\n", $violations),
        );
    }

    /**
     * The set comparison is the point of the test above, so it has to be
     * exercised where a class is named twice: the fixture is a miniature
     * policy whose public table and internal list both name the same class.
     */
    public function testTheClassificationRejectsAClassThatIsBothPublicAndInternal(): void
    {
        $policy = implode("\n", [
            '### Public API',
            '',
            '| Surface | What it covers |',
            '|---|---|',
            '| `RawKv\RawKvClient` | the RawKV entry point |',
            '',
            '### Internal',
            '',
            '- `RawKv\RawKvClient` — the only collaborator behind the entry point.',
            '',
        ]);

        $violations = $this->classificationViolations($policy, [RawKvClient::class]);

        self::assertCount(1, $violations);
        self::assertStringContainsString('public', $violations[0]);
        self::assertStringContainsString('internal', $violations[0]);
    }

    /**
     * A class that the policy names nowhere is reported as well, so the set
     * comparison does not silently accept an empty classification.
     */
    public function testTheClassificationRejectsAnUnclassifiedClass(): void
    {
        $policy = implode("\n", [
            '### Public API',
            '',
            '| Surface | What it covers |',
            '|---|---|',
            '| `RawKv\RawKvClient` | the RawKV entry point |',
            '',
            '### Internal',
            '',
            '- `Batch\` — the batch collaborators.',
            '',
        ]);

        $violations = $this->classificationViolations($policy, [TxnKvClient::class]);

        self::assertCount(1, $violations);
        self::assertStringContainsString('neither', $violations[0]);
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
     * A literal whose major is not the major of the current release is
     * skipped as a reference to another release line — the SemVer boundary
     * the intro names ("1.0.0"), the version of another product, or a
     * historical major line. It cannot be a version of the release line
     * the policy documents. A literal that is part of a URL and a
     * four-part dotted number never reach this loop: versionLiterals()
     * skips both.
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
     *
     * Every three-part version on the current major line has to be the
     * current release or the next minor. A version older than the current
     * release is stale wherever it stands outside the policy, and a version
     * newer than the next minor promises a release that does not exist yet.
     * A version on another major line belongs to a previous line or to
     * another product (grpc) and is skipped; a literal that is part of a
     * URL and a four-part dotted number are not versions at all and are
     * filtered out by versionLiterals().
     */
    public function testTheReadmeNamesNoReleaseButTheCurrentOneAndTheNextMinor(): void
    {
        $version = $this->latestReleasedVersion();
        $violations = $this->readmeReleaseViolations($this->readme(), $version);

        self::assertSame(
            [],
            $violations,
            "README.md names a release that is neither the current one nor the next minor:\n"
            . implode("\n", $violations),
        );
    }

    /**
     * A stale release outside the policy is the case the earlier version of
     * the guard let through, so the comparison has to be exercised without
     * a stale README in the checkout.
     */
    public function testTheReadmeWideCheckRejectsAStaleRelease(): void
    {
        $violations = $this->readmeReleaseViolations(
            "Project status: the current release is v0.9.0.\n",
            '0.14.0',
        );

        self::assertCount(1, $violations);
        self::assertStringContainsString('0.9.0', $violations[0]);

        self::assertSame(
            [],
            $this->readmeReleaseViolations(
                "Project status: the current release is v0.14.0.\n",
                '0.14.0',
            ),
        );
    }

    /**
     * The version scan skips what is not a version of the library: the
     * version in a URL and an IPv4 address. Both are normal README content
     * and neither may fail the guard on the library's own release line.
     */
    public function testTheVersionScanSkipsAUrlAndAnIpAddress(): void
    {
        self::assertSame(
            [],
            $this->versionLiterals('See https://example.com/releases/v2.0.0 for details.'),
            'The version of a URL was read as a release of the library.',
        );
        self::assertSame(
            [],
            $this->versionLiterals('The SemVer specification is https://semver.org/spec/v2.0.0.html.'),
            'The version of the SemVer URL was read as a release of the library.',
        );
        self::assertSame(
            [],
            $this->versionLiterals('Connect to 127.0.0.1:2379 or bind 0.0.0.0.'),
            'An IPv4 address was read as a version.',
        );
        self::assertSame(
            [['literal' => '0.14.0', 'parts' => 3, 'line' => 1]],
            $this->versionLiterals('The current release is v0.14.0.'),
            'A release next to a URL or an address is still read.',
        );
        self::assertSame(
            [['literal' => '0.14.0', 'parts' => 3, 'line' => 1]],
            $this->versionLiterals('See https://example.com/v2.0.0 and require v0.14.0.'),
            'The URL hid the release on the same line.',
        );
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
            . 'change carries a **BREAKING** or **Breaking change:** prefix.',
        );

        $withoutNote = $this->breakingEntriesWithoutMigration($this->changelog());

        foreach ($withoutNote as $line => $entry) {
            self::fail(
                'CHANGELOG.md:' . $line . ' is marked as breaking but has no migration note: '
                . $entry,
            );
        }
    }

    /**
     * Both markers of the convention are checked, not only the all-caps one:
     * a `**Breaking change:**` entry breaks an internal extension point and
     * carries a migration note like a `**BREAKING**` entry does.
     */
    public function testABreakingChangeEntryWithoutAMigrationNoteIsReported(): void
    {
        $changelog = "## [Unreleased]\n\n### Changed\n\n"
            . "- **Breaking change:** the internal hook changed its signature.\n";

        self::assertSame(
            [5],
            array_keys($this->breakingEntriesWithoutMigration($changelog)),
            'A "**Breaking change:**" entry without a migration note was not reported.',
        );

        self::assertSame(
            [],
            $this->breakingEntriesWithoutMigration(
                $changelog . "  **Migration:** call the new hook instead.\n",
            ),
            'A "**Breaking change:**" entry with a migration note was reported.',
        );
    }

    /**
     * The pre-1.0 wording is the part of the policy intro that has to change
     * at the 1.0.0 release. The guard follows the changelog, so the release
     * pull request that adds the first 1.x section fails until the intro
     * describes the released major line instead.
     */
    public function testThePreOnePointZeroWordingFailsOnceAVersionOneIsReleased(): void
    {
        $violation = $this->preOnePointZeroWordingViolation($this->readme(), $this->changelog());

        self::assertNull(
            $violation,
            (string) $violation,
        );

        $changelog = "## [Unreleased]\n\n### Added\n\n- Something.\n\n## [v1.0.0] - 2027-01-01\n";

        self::assertNotNull(
            $this->preOnePointZeroWordingViolation(
                "The library is **pre-1.0**. It follows Semantic Versioning.",
                $changelog,
            ),
            'The "pre-1.0" wording passed after the changelog named a 1.x release.',
        );

        self::assertNull(
            $this->preOnePointZeroWordingViolation(
                'The library is **1.0**. It follows Semantic Versioning.',
                $changelog,
            ),
            'The updated intro was reported after the changelog named a 1.x release.',
        );
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
        return $this->policyTableOf($this->policySection());
    }

    /**
     * The rows of the table of a policy section.
     */
    private function policyTableOf(string $section): string
    {
        $rows = [];

        foreach (explode("\n", $section) as $line) {
            if (str_starts_with(trim($line), '|')) {
                $rows[] = $line;
            }
        }

        return implode("\n", $rows);
    }

    /**
     * The class tokens of the two classification lists of a policy section,
     * as names relative to the client namespace: the public API table and
     * the internal list below it.
     *
     * Only the first cell of a table row counts: the second cell explains
     * the surface and mentions classes in prose ("`TiKvException` and every
     * subclass"), which is not a classification of its own.
     *
     * An internal bullet explains its item after an em dash, so only the
     * part before the dash counts. That is what makes the documented
     * exceptions work: "`Region\` — except `Region\ReplicaReadMode` ..."
     * lists the namespace as internal without listing the two public
     * classes as internal in the same breath.
     *
     * @return array{public: list<string>, internal: list<string>}
     */
    private function policyClassificationTokens(string $section): array
    {
        $public = [];

        foreach (explode("\n", $this->policyTableOf($section)) as $line) {
            $cells = explode('|', trim($line));
            $public = array_merge($public, $this->classTokens($cells[1] ?? ''));
        }

        $internal = [];

        foreach ($this->internalListItems($section) as $item) {
            $internal = array_merge($internal, $this->classTokens($item));
        }

        return [
            'public' => array_values(array_unique($public)),
            'internal' => array_values(array_unique($internal)),
        ];
    }

    /**
     * The items of the internal list of a policy section: every bullet, up
     * to the em dash that introduces its explanation.
     *
     * @return list<string>
     */
    private function internalListItems(string $section): array
    {
        $start = strpos($section, '### Internal');

        if ($start === false) {
            self::fail(
                'The stability policy has no "### Internal" list, so the guard cannot classify '
                . 'the internal half of src/Client/.',
            );
        }

        $items = [];
        $current = null;

        foreach (explode("\n", substr($section, $start)) as $line) {
            $matches = [];

            if (preg_match('/^\s*[-*]\s+(.*)$/', $line, $matches) === 1) {
                if ($current !== null) {
                    $items[] = $current;
                }

                $current = $matches[1];

                continue;
            }

            if (trim($line) === '') {
                if ($current !== null) {
                    $items[] = $current;
                    $current = null;
                }

                continue;
            }

            if ($current !== null && !str_starts_with($line, '#')) {
                $current .= ' ' . trim($line);
            }
        }

        if ($current !== null) {
            $items[] = $current;
        }

        return array_map(
            static fn (string $item): string => explode(' — ', $item, 2)[0],
            $items,
        );
    }

    /**
     * The backticked class and namespace tokens of a text. A trailing
     * backslash is the namespace wildcard the policy writes ("Batch\"), and
     * the "\*" form of the same wildcard is normalised to it.
     *
     * A token without a namespace separator, a method name ("create()"), a
     * path ("src/Proto/") or a property ("RegionException::$errorKind") is
     * not a class token and is ignored.
     *
     * @return list<string>
     */
    private function classTokens(string $text): array
    {
        $matches = [];
        preg_match_all('/`([^`]+)`/', $text, $matches);

        $tokens = [];

        foreach ($matches[1] as $token) {
            $token = str_replace('\\*', '\\', $token);

            if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*(?:\\\\[A-Za-z0-9_]*)+\\\\?$/', $token) !== 1) {
                continue;
            }

            $tokens[] = $token;
        }

        return $tokens;
    }

    /**
     * Every way a class of the client tree is not classified exactly once.
     *
     * An exact entry is more specific than a namespace wildcard, so a class
     * the public table names explicitly stays public although the internal
     * list covers its namespace ("Region\"). Two exact entries, or two
     * wildcards, are ambiguous and reported.
     *
     * @param list<string> $classes fully qualified class names
     *
     * @return list<string>
     */
    private function classificationViolations(string $section, array $classes): array
    {
        $tokens = $this->policyClassificationTokens($section);
        $violations = [];

        foreach ($classes as $class) {
            $relative = $this->relativeName($class);
            $publicExact = in_array($relative, $tokens['public'], true);
            $internalExact = in_array($relative, $tokens['internal'], true);
            $public = $publicExact || $this->matchesAnyToken($relative, $tokens['public']);
            $internal = $internalExact || $this->matchesAnyToken($relative, $tokens['internal']);

            if ($publicExact && $internalExact) {
                $violations[] = $class . ' is named in the public API table and in the internal list '
                    . 'of the stability policy. A class is either public or internal: remove it '
                    . 'from one of the two lists.';

                continue;
            }

            if ($public && $internal) {
                // An exact entry is the documented exception to a wildcard
                // of the other list, so the two are not ambiguous.
                if ($publicExact || $internalExact) {
                    continue;
                }

                $violations[] = $class . ' matches a wildcard of the public API table and a '
                    . 'wildcard of the internal list of the stability policy, so its status is '
                    . 'ambiguous. Name it explicitly in the list that applies to it.';

                continue;
            }

            if (!$public && !$internal) {
                $violations[] = $class . ' is neither part of the promised public API nor listed as '
                    . 'internal in the stability policy. Every class of src/Client/ has to be '
                    . 'classified: add it to the public API table or to the internal list.';
            }
        }

        return $violations;
    }

    /**
     * Whether a relative class name matches an exact token or a namespace
     * wildcard of a token list.
     *
     * @param list<string> $tokens
     */
    private function matchesAnyToken(string $relative, array $tokens): bool
    {
        foreach ($tokens as $token) {
            if ($token === $relative) {
                return true;
            }

            if (str_ends_with($token, '\\') && str_starts_with($relative, $token)) {
                return true;
            }
        }

        return false;
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
     * The violation of the pre-1.0 wording, or null when the README and the
     * changelog agree.
     *
     * The intro of the policy says "The library is **pre-1.0**" and explains
     * the minor-bump rule "before `1.0.0`". The wording has to go when the
     * first 1.x release is cut, because the section then describes a
     * released major line — the release pull request updates it in the same
     * commit. The guard follows the changelog instead of a hard-coded
     * version, so it fails at the release cut and nowhere else.
     */
    private function preOnePointZeroWordingViolation(string $readme, string $changelog): ?string
    {
        if (preg_match('/^## \[v?([1-9]\d*)\.\d+\.\d+\]/m', $changelog) !== 1) {
            return null;
        }

        if (!str_contains($readme, 'pre-1.0')) {
            return null;
        }

        return 'README.md still calls the library "pre-1.0", but CHANGELOG.md already names a '
            . '1.x release. The intro of the stability policy has to describe the released major '
            . 'line instead: drop the "pre-1.0" wording and the "before `1.0.0`" rule in the '
            . 'release pull request (see docs/release-workflow.md).';
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
     * Every three-part release of a text that is neither the current
     * release nor the next minor, with the line it appears on.
     *
     * A version on another major line is skipped: it belongs to a previous
     * major line or to another product, not to the release line this guard
     * follows.
     *
     * @return list<string>
     */
    private function readmeReleaseViolations(string $readme, string $version): array
    {
        $allowed = [$version, $this->nextMinorVersion($version)];
        $major = $this->majorOf($version);
        $violations = [];

        foreach ($this->versionLiterals($readme) as $literal) {
            if ($literal['parts'] !== 3 || $this->majorOf($literal['literal']) !== $major) {
                continue;
            }

            if (in_array($literal['literal'], $allowed, true)) {
                continue;
            }

            $violations[] = 'README.md:' . $literal['line'] . ' names the release '
                . $literal['literal'] . ', which is neither the current release (' . $version
                . ') nor the release that follows it (' . $allowed[1] . '). A release the README '
                . 'names has to follow the releases: the release pull request updates it in the '
                . 'same commit (see docs/release-workflow.md).';
        }

        return $violations;
    }

    /**
     * Every version literal of a text.
     *
     * A literal is a two- or three-part dotted number — "0.8.0" (a release)
     * or "0.8" (the minor line of a release, the way a two-segment
     * constraint writes it).
     *
     * A literal that is part of a URL is skipped: the version in a URL
     * belongs to the URL. The SemVer specification link is
     * "https://semver.org/spec/v2.0.0.html", and its "2.0.0" is not a
     * release of this library — it must not start failing the guard when
     * the library reaches 2.0.0 itself, nor when it reaches 2.x.
     *
     * A four-part dotted number is an IPv4 address — "127.0.0.1",
     * "0.0.0.0" — and not a version, so it is skipped as well. The pattern
     * captures the whole dotted run and the part count rejects it, instead
     * of relying on the major version of the current release.
     *
     * @return list<array{literal: string, parts: int, line: int}>
     */
    private function versionLiterals(string $text): array
    {
        $literals = [];

        foreach (explode("\n", $text) as $index => $line) {
            $urls = $this->urlSpans($line);
            $matches = [];
            // The lookbehind skips the second half of a dotted run, and the
            // lookahead keeps a run that is followed by a word char or by
            // another dotted segment out of the capture, so a longer run is
            // captured whole and rejected by its part count. A sentence
            // period after the version does not start another segment.
            preg_match_all(
                '/(?<![\w.])(?:v)?(\d+(?:\.\d+)+)(?!\w|\.\d)/',
                $line,
                $matches,
                PREG_OFFSET_CAPTURE,
            );

            foreach ($matches[1] as $match) {
                $literal = $match[0];
                $parts = substr_count($literal, '.') + 1;

                if ($parts < 2 || $parts > 3) {
                    continue;
                }

                if ($this->isInsideASpan($match[1], $urls)) {
                    continue;
                }

                $literals[] = [
                    'literal' => $literal,
                    'parts' => $parts,
                    'line' => $index + 1,
                ];
            }
        }

        return $literals;
    }

    /**
     * The byte spans of the URLs of a line.
     *
     * @return list<array{start: int, end: int}>
     */
    private function urlSpans(string $line): array
    {
        $matches = [];
        preg_match_all('~\bhttps?://[^\s<>()\[\]]+~', $line, $matches, PREG_OFFSET_CAPTURE);

        $spans = [];

        foreach ($matches[0] as $match) {
            $spans[] = [
                'start' => $match[1],
                'end' => $match[1] + strlen($match[0]),
            ];
        }

        return $spans;
    }

    /**
     * Whether a byte offset falls inside one of the spans.
     *
     * @param list<array{start: int, end: int}> $spans
     */
    private function isInsideASpan(int $offset, array $spans): bool
    {
        foreach ($spans as $span) {
            if ($offset >= $span['start'] && $offset < $span['end']) {
                return true;
            }
        }

        return false;
    }

    /**
     * Every changelog entry marked as breaking, keyed by the line it starts
     * on. An entry spans the bullet and its continuation lines.
     *
     * Both markers of the convention count: **BREAKING** for a break of the
     * public API surface, **Breaking change:** for a break of a class
     * outside it. Only the version sections are scanned: the preamble of the
     * file describes the convention itself, and a bullet there explains the
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
        return $this->breakingEntriesIn($this->changelog());
    }

    /**
     * The breaking entries of a changelog text that carry no migration note,
     * keyed by the line they start on.
     *
     * @return array<int, string>
     */
    private function breakingEntriesWithoutMigration(string $changelog): array
    {
        return array_filter(
            $this->breakingEntriesIn($changelog),
            static fn (string $entry): bool => !str_contains($entry, 'Migration:'),
        );
    }

    /**
     * Every changelog entry of a text marked as breaking.
     *
     * @return array<int, string>
     */
    private function breakingEntriesIn(string $changelog): array
    {
        $lines = explode("\n", $changelog);
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

                if (preg_match('/^\s*[-*]\s+\*\*(?:BREAKING|Breaking change:)\*\*/', $line) === 1) {
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
