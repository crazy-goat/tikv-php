<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #403 [DOC-37]: the timeout map in
 * docs/configuration.md has to list exactly the keys TimeoutConfig accepts.
 *
 * ConnectionFactory::buildTimeoutConfig() derives its recognised keys from
 * TimeoutConfig's constructor (issue #272, GRPC-13), and
 * ConnectionFactoryTest::testEveryTimeoutConfigFieldIsConfigurableByItsOwnName()
 * proves that every field reaches the DTO. This guard closes the remaining
 * side of the triangle: a field added to TimeoutConfig, or a default changed
 * there, fails the suite until the documented map is updated.
 */
class TimeoutOptionKeysDocumentationGuardTest extends TestCase
{
    private const DOCUMENTED_FILE = 'docs/configuration.md';

    public function testTheDocumentedTimeoutKeysExactlyMatchTheTimeoutConfigFields(): void
    {
        $documented = $this->documentedTimeoutOptions();

        $fields = $this->timeoutConfigFields();
        self::assertNotEmpty($fields, 'TimeoutConfig must have timeout fields');

        $documentedKeys = array_keys($documented);
        sort($documentedKeys);
        sort($fields);

        self::assertSame(
            $fields,
            $documentedKeys,
            'docs/configuration.md must document exactly the timeout keys TimeoutConfig accepts: '
            . 'a missing key is an option the reader cannot find, and an extra key is an option '
            . 'the client rejects.',
        );
    }

    public function testTheDocumentedTimeoutDefaultsMatchTheTimeoutConfigDefaults(): void
    {
        $documented = $this->documentedTimeoutOptions();
        /** @var array<string, int> $defaults */
        $defaults = get_object_vars(new TimeoutConfig());

        foreach ($documented as $key => $value) {
            self::assertSame(
                $defaults[$key],
                $value,
                "docs/configuration.md documents {$key} with the wrong default",
            );
        }
    }

    public function testChecksumAndIngestTimeoutsAreDocumented(): void
    {
        $documented = $this->documentedTimeoutOptions();

        self::assertArrayHasKey(
            'checksumTimeoutMs',
            $documented,
            'checksumTimeoutMs sizes checksum(), which reads a whole key range and needs a '
            . 'documented way to raise its deadline.',
        );
        self::assertArrayHasKey(
            'ingestTimeoutMs',
            $documented,
            "ingestTimeoutMs is read from options['timeout'] since issue #272; it is not a "
            . 'fixed deadline and must stay documented.',
        );
    }

    /**
     * The timeout keys and their documented defaults, in the order they
     * appear in the "Per-Operation Timeouts" section.
     *
     * @return array<string, int>
     */
    private function documentedTimeoutOptions(): array
    {
        $path = dirname(__DIR__, 3) . '/' . self::DOCUMENTED_FILE;
        self::assertFileExists($path);

        $contents = file_get_contents($path);
        self::assertNotFalse($contents, 'Unable to read ' . self::DOCUMENTED_FILE);

        $start = strpos($contents, '### Per-Operation Timeouts');
        $end = strpos($contents, '### Default Timeouts');
        self::assertNotFalse($start, 'docs/configuration.md has no "Per-Operation Timeouts" section');
        self::assertNotFalse($end, 'docs/configuration.md has no "Default Timeouts" section');
        self::assertGreaterThan($start, $end);

        $section = substr($contents, $start, $end - $start);
        self::assertStringContainsString("'timeout' => [", $section);

        $documented = [];
        foreach (explode("\n", $section) as $line) {
            if (preg_match("/'([A-Za-z][A-Za-z0-9]*)'\\s*=>\\s*(-?\\d+)/", $line, $matches) !== 1) {
                continue;
            }

            $documented[$matches[1]] = (int) $matches[2];
        }

        self::assertNotEmpty($documented, 'The timeout map in docs/configuration.md is empty');

        return $documented;
    }

    /**
     * @return list<string>
     */
    private function timeoutConfigFields(): array
    {
        $parameters = (new \ReflectionMethod(TimeoutConfig::class, '__construct'))->getParameters();

        $fields = [];
        foreach ($parameters as $parameter) {
            $fields[] = $parameter->getName();
        }

        return $fields;
    }
}
