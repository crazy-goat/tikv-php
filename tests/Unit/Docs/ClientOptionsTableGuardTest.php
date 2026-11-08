<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #720: the "Configuration Options" table of
 * README.md has to name every option the create() entry points read.
 *
 * The table is hand-maintained and the code is not, so the two drifted:
 * `maxChannels`, `idleTtlMs` and `grpcClient` were accepted long before the
 * table mentioned them, and `maxIngestSize` was missing when this guard was
 * added. A reader who trusts the table never learns that the option exists.
 *
 * The expected set is derived from the source instead of restated here:
 *
 *  - the body of `create()` on `RawKvClient` and `TxnKvClient` supplies the
 *    `self::OPT_*` constants it reads, and the private `resolve*()` helpers
 *    those bodies call are followed transitively — `maxIngestSize` lives in
 *    such a helper;
 *  - `ConnectionFactory::create()` reads its transport, TLS, timeout, metrics
 *    and PD options through literal `$options['…']` accesses in its own
 *    private helpers (including the key arguments of
 *    `resolvePositiveIntOption()`), so its whole implementation is followed;
 *  - `ResourceControl::fromOptions()` is recognised as the source of
 *    `resourceGroup` and `resourceGroupTag`.
 *
 * Only the create() entry points are covered: the `TxnKvClient::begin()`
 * options are per-transaction settings with their own paragraph below the
 * table, not client options.
 *
 * The second test walks the other direction: a row for an option that no
 * create() path reads any more is just as wrong as a missing row, because it
 * keeps promising a setting the client ignores.
 */
final class ClientOptionsTableGuardTest extends TestCase
{
    /** Heading of the options table. */
    private const OPTIONS_HEADING = '### Configuration Options';

    public function testEveryCreateOptionAppearsInTheReadmeTable(): void
    {
        $documented = $this->documentedOptionKeys();

        $missing = array_values(array_diff($this->createOptionKeys(), $documented));

        self::assertSame(
            [],
            $missing,
            "README.md does not document every client option read by create(). Add the "
            . "missing keys to the \"Configuration Options\" table:\n" . implode("\n", $missing),
        );
    }

    public function testTheReadmeTableNamesOnlyOptionsCreateStillReads(): void
    {
        $known = $this->createOptionKeys();

        $stale = array_values(array_diff($this->documentedOptionKeys(), $known));

        self::assertSame(
            [],
            $stale,
            "README.md documents client options that no create() path reads any more. "
            . "Remove the stale rows from the \"Configuration Options\" table:\n"
            . implode("\n", $stale),
        );
    }

    /**
     * Every top-level option key the three create() entry points read.
     *
     * @return list<string>
     */
    private function createOptionKeys(): array
    {
        $keys = [];

        foreach ([RawKvClient::class, TxnKvClient::class] as $class) {
            foreach ($this->clientCreateOptionKeys($class) as $key) {
                $keys[$key] = true;
            }
        }

        foreach ($this->connectionFactoryOptionKeys() as $key) {
            $keys[$key] = true;
        }

        $keys = array_keys($keys);
        sort($keys);

        self::assertNotEmpty($keys, 'No client option was derived from the source — the guard would be vacuous.');

        return $keys;
    }

    /**
     * The option keys the given client's `create()` method reads directly or
     * through a private `self::resolve*()` helper.
     *
     * @param class-string $class
     * @return list<string>
     */
    private function clientCreateOptionKeys(string $class): array
    {
        $source = $this->sourceFor($class);
        $constants = $this->optionConstants($class);
        $bodies = $this->methodBodies($source);

        $keys = [];
        $queue = ['create'];
        $visited = [];

        while ($queue !== []) {
            $method = array_shift($queue);
            if (isset($visited[$method]) || !isset($bodies[$method])) {
                continue;
            }
            $visited[$method] = true;
            $body = $bodies[$method];

            foreach ($this->captured('/self::(OPT_[A-Z0-9_]+)/', $body) as $constant) {
                if (isset($constants[$constant])) {
                    $keys[$constants[$constant]] = true;
                }
            }

            foreach ($this->captured('/\$options\[\'([^\']+)\'\]/', $body) as $key) {
                $keys[$key] = true;
            }

            if (str_contains($body, 'ResourceControl::fromOptions')) {
                $keys[ResourceControl::OPTION_RESOURCE_GROUP] = true;
                $keys[ResourceControl::OPTION_RESOURCE_GROUP_TAG] = true;
            }

            foreach ($this->captured('/self::([a-zA-Z_][a-zA-Z0-9_]*)\(/', $body) as $called) {
                if (isset($bodies[$called])) {
                    $queue[] = $called;
                }
            }
        }

        $keys = array_keys($keys);
        sort($keys);

        return $keys;
    }

    /**
     * The top-level option keys `ConnectionFactory` reads.
     *
     * The factory has no `OPT_*` constants for these: it either reads
     * `$options['…']` directly or passes the key to
     * `resolvePositiveIntOption()`. Its private helpers are all reached from
     * `create()`, so the whole class is scanned rather than following the
     * calls method by method.
     *
     * @return list<string>
     */
    private function connectionFactoryOptionKeys(): array
    {
        $factorySource = $this->repoRoot() . '/src/Client/Connection/ConnectionFactory.php';
        $source = implode("\n", $this->methodBodies($this->readFile($factorySource)));

        $keys = [];

        foreach ($this->captured('/\$options\[\'([^\']+)\'\]/', $source) as $key) {
            $keys[$key] = true;
        }

        foreach ($this->captured('/array_key_exists\(\s*\'([^\']+)\'\s*,\s*\$options\s*\)/', $source) as $key) {
            $keys[$key] = true;
        }

        foreach ($this->captured('/resolvePositiveIntOption\(\s*\$options\s*,\s*\'([^\']+)\'/', $source) as $key) {
            $keys[$key] = true;
        }

        $keys = array_keys($keys);
        sort($keys);

        return $keys;
    }

    /**
     * The string values of the `OPT_*` constants of a client class.
     *
     * @param class-string $class
     * @return array<string, string>
     */
    private function optionConstants(string $class): array
    {
        $constants = [];

        foreach ((new \ReflectionClass($class))->getConstants() as $name => $value) {
            if (str_starts_with($name, 'OPT_') && is_string($value)) {
                $constants[$name] = $value;
            }
        }

        return $constants;
    }

    /**
     * Every method body of a PHP source file, keyed by method name.
     *
     * The bodies are found with the tokenizer rather than a regular
     * expression, so a brace inside a string, comment or interpolated
     * expression cannot end the body early. `token_get_all()` spells
     * `{`/`}` as plain string tokens only where they are actual block
     * braces; `T_CURLY_OPEN` and `T_DOLLAR_OPEN_CURLY_BRACES` are arrays and
     * are left alone.
     *
     * @return array<string, string>
     */
    private function methodBodies(string $source): array
    {
        $tokens = token_get_all($source);
        $bodies = [];
        $count = count($tokens);

        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || $token[0] !== T_FUNCTION) {
                continue;
            }

            $j = $i + 1;
            while ($j < $count) {
                $candidate = $tokens[$j];
                if (is_array($candidate) && $candidate[0] === T_STRING) {
                    break;
                }
                if ($candidate === '{' || $candidate === ';') {
                    break;
                }
                $j++;
            }

            if ($j >= $count || !is_array($tokens[$j]) || $tokens[$j][0] !== T_STRING) {
                continue;
            }

            $name = $tokens[$j][1];

            while ($j < $count && $tokens[$j] !== '{') {
                $j++;
            }
            if ($j >= $count) {
                continue;
            }

            $depth = 0;
            $body = '';
            for (; $j < $count; $j++) {
                $current = $tokens[$j];
                $body .= is_array($current) ? $current[1] : $current;

                if ($current === '{') {
                    $depth++;
                } elseif ($current === '}') {
                    $depth--;
                    if ($depth === 0) {
                        break;
                    }
                }
            }

            $bodies[$name] = $body;
        }

        return $bodies;
    }

    /**
     * The first capture group of every match of a pattern.
     *
     * @return list<string>
     */
    private function captured(string $pattern, string $subject): array
    {
        $matches = [];
        $found = preg_match_all($pattern, $subject, $matches);
        self::assertIsInt($found);

        return $matches[1];
    }

    /**
     * The option keys named in the first cell of every row of the README
     * table.
     *
     * Only a table row counts: a key mentioned in the prose around the table
     * does not put an option in the list a reader scans.
     *
     * @return list<string>
     */
    private function documentedOptionKeys(): array
    {
        $section = $this->section(
            $this->readFile($this->repoRoot() . '/README.md'),
            self::OPTIONS_HEADING,
        );

        $keys = [];
        foreach (explode("\n", $section) as $line) {
            $line = trim($line);
            if (!str_starts_with($line, '|')) {
                continue;
            }

            $cells = explode('|', $line);
            $optionCell = trim($cells[1] ?? '');
            foreach ($this->captured('/`([^`]+)`/', $optionCell) as $key) {
                $keys[$key] = true;
            }
        }

        $keys = array_keys($keys);
        sort($keys);

        self::assertNotEmpty($keys, 'No option row was found in the README table — the guard would be vacuous.');

        return $keys;
    }

    /**
     * The text of a markdown section, from its heading up to the next
     * heading of the same or a shallower level.
     *
     * The next-heading search is fence-aware: a `# Example` line inside a
     * fenced code block is a sample, not a heading, so it cannot end the
     * section early and hide the rows below it.
     */
    private function section(string $contents, string $heading): string
    {
        $lines = explode("\n", $contents);
        $start = null;

        foreach ($lines as $index => $line) {
            if (trim($line) === $heading) {
                $start = $index;
                break;
            }
        }

        self::assertNotNull($start, sprintf('No "%s" section found.', $heading));

        $level = 1;
        if (preg_match('/^#+/', $heading, $matches) === 1) {
            $level = strlen($matches[0]);
        }

        $lineCount = count($lines);
        $end = $lineCount;
        $fenceCharacter = null;
        $fenceLength = 0;

        for ($index = $start + 1; $index < $lineCount; $index++) {
            $trimmed = trim($lines[$index]);

            if ($fenceCharacter !== null) {
                if (
                    preg_match('/^(`{3,}|~{3,})$/', $trimmed, $matches) === 1
                    && $matches[1][0] === $fenceCharacter
                    && strlen($matches[1]) >= $fenceLength
                ) {
                    $fenceCharacter = null;
                }

                continue;
            }

            if (preg_match('/^(`{3,}|~{3,})/', $trimmed, $matches) === 1) {
                $fenceCharacter = $matches[1][0];
                $fenceLength = strlen($matches[1]);
                continue;
            }

            if (preg_match('/^#{1,' . $level . '}\s/', $lines[$index]) === 1) {
                $end = $index;
                break;
            }
        }

        return implode("\n", array_slice($lines, $start + 1, $end - $start - 1));
    }

    /**
     * @param class-string $class
     */
    private function sourceFor(string $class): string
    {
        $file = match ($class) {
            RawKvClient::class => 'src/Client/RawKv/RawKvClient.php',
            TxnKvClient::class => 'src/Client/TxnKv/TxnKvClient.php',
            default => self::fail(sprintf('No source file is mapped for %s.', $class)),
        };

        return $this->readFile($this->repoRoot() . '/' . $file);
    }

    private function readFile(string $path): string
    {
        $contents = file_get_contents($path);
        self::assertNotFalse($contents, sprintf('Unable to read %s', $path));

        return $contents;
    }

    private function repoRoot(): string
    {
        return dirname(__DIR__, 3);
    }
}
