<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Security;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #269 (GRPC-10): raw user keys must never be
 * interpolated into an exception message.
 *
 * The issue asked for a static-analysis rule (PHPCS/PHPStan) so the sites
 * cannot regress. Writing a custom PHPStan/PHPCS extension for this is out of
 * proportion, so this test is the pragmatic stand-in: it tokenises every
 * src/Client file, finds `sprintf()` calls whose format string mentions a
 * key and fails unless the call also contains a
 * `KeyRedactor::redact()` invocation.
 *
 * Scanning the sprintf argument list (rather than a regex over the raw text)
 * makes the check robust against the message's quoting and against the raw
 * key being passed to a *different* constructor argument (e.g.
 * `RetryBudgetExhaustedException(..., rawKey: $key)`, which is intentional and
 * must not be flagged).
 */
class ExceptionMessageRedactionGuardTest extends TestCase
{
    /**
     * Message fragments that interpolate key *material* (as opposed to the
     * word "key" in a generic message such as "Batch keys must be strings").
     *
     * @var list<string>
     */
    private const KEY_MESSAGE_MARKERS = ['for key', 'locked key'];

    public function testNoRawKeyInterpolationInExceptionMessages(): void
    {
        $violations = [];

        foreach ($this->phpFilesUnder(__DIR__ . '/../../../src/Client') as $file) {
            $contents = file_get_contents($file);
            self::assertNotFalse($contents, "Unable to read {$file}");

            $missing = $this->sprintfKeyMessagesMissingRedaction($contents);
            if ($missing > 0) {
                $violations[] = sprintf('%s: %d unredacted key message(s)', $file, $missing);
            }
        }

        self::assertSame(
            [],
            $violations,
            "Raw key interpolated into an exception message:\n" . implode("\n", $violations),
        );
    }

    /**
     * Number of `sprintf()` calls in the source that carry a key-bearing
     * message but no `KeyRedactor::redact()` in their argument list.
     */
    private function sprintfKeyMessagesMissingRedaction(string $code): int
    {
        $tokens = token_get_all($code);
        $count = count($tokens);
        $missing = 0;

        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (
                !is_array($token)
                || $token[0] !== T_STRING
                || strtolower($token[1]) !== 'sprintf'
            ) {
                continue;
            }

            // Move past optional whitespace/comments to the opening paren.
            $j = $i + 1;
            while (
                $j < $count
                && is_array($tokens[$j])
                && ($tokens[$j][0] === T_WHITESPACE || $tokens[$j][0] === T_COMMENT)
            ) {
                $j++;
            }
            if ($j >= $count || $tokens[$j] !== '(') {
                continue;
            }

            $hasKeyMessage = false;
            $hasRedaction = false;
            $depth = 0;

            for ($k = $j; $k < $count; $k++) {
                $current = $tokens[$k];

                if ($current === '(') {
                    $depth++;
                    continue;
                }
                if ($current === ')') {
                    $depth--;
                    if ($depth === 0) {
                        break;
                    }
                    continue;
                }
                if (!is_array($current)) {
                    continue;
                }

                if ($current[0] === T_CONSTANT_ENCAPSED_STRING) {
                    foreach (self::KEY_MESSAGE_MARKERS as $marker) {
                        if (str_contains($current[1], $marker)) {
                            $hasKeyMessage = true;
                            break;
                        }
                    }
                }
                if ($current[0] === T_STRING && $current[1] === 'KeyRedactor') {
                    $hasRedaction = true;
                }
            }

            if ($hasKeyMessage && !$hasRedaction) {
                $missing++;
            }
        }

        return $missing;
    }

    /**
     * @return list<string>
     */
    private function phpFilesUnder(string $dir): array
    {
        $files = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
        );

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $files[] = $entry->getPathname();
            }
        }

        sort($files);

        return $files;
    }
}
