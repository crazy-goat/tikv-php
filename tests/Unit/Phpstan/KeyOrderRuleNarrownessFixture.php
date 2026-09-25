<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Phpstan;

/**
 * Negative fixture for the custom PHPStan rule
 * `CrazyGoat\TiKV\Phpstan\KeyOrderComparisonRule` (issue #186) — the shapes
 * it must stay quiet on, because they are not about TiKV keys.
 *
 * Unlike `KeyOrderRuleFixture`, nothing whitelists this file: a finding here
 * is a plain PHPStan error, so widening the rule past a non-key string
 * comparison or past the plain-`string` type check fails `composer phpstan`
 * directly. The `ignoreErrors` entry for the identifier is scoped to the
 * positive fixture only — keep it that way.
 */
final class KeyOrderRuleNarrownessFixture
{
    /**
     * Names that do not read like a key, against string literals: labels,
     * messages, deadlines and log levels are ordinary strings and must stay
     * comparable with PHP's own operators.
     */
    public static function comparesNonKeyStrings(string $message, string $deadline, string $level): bool
    {
        return $message < 'z' && $deadline >= 'z' && $level <= 'z';
    }

    /**
     * A key against an `int` is a count, an offset or a limit, not a key: the
     * rule's plain-`string` type check must reject the comparison (this is
     * the shape of the call-index comparison in
     * `BatchPartialFailureException::getFirstRegionError()`).
     */
    public static function comparesKeyToInt(string $key, int $limit): bool
    {
        return $key < $limit;
    }
}
