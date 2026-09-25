<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Phpstan;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

/**
 * Positive fixture for the custom PHPStan rule
 * `CrazyGoat\TiKV\Phpstan\KeyOrderComparisonRule` (issue #186) — the shapes
 * it MUST report.
 *
 * It is the whole test, and it is a self-test: `composer phpstan` analyses
 * this file, and the `ignoreErrors` entry in `phpstan.neon` that whitelists
 * the identifier is scoped to this path with `reportUnmatched: true`. A rule
 * that stopped reporting these shapes therefore fails the analysis with
 * `ignore.unmatched` instead of silently passing. The negative half lives in
 * `KeyOrderRuleNarrownessFixture`, which is *not* whitelisted.
 *
 * It cannot be a PHPUnit test: the rule implements `PHPStan\Rules\Rule`, and
 * PHPStan's classes are only autoloadable from phpstan.phar, which needs
 * ext-phar. CI's `unit-tests` job runs `php -n` (no extensions), so there is
 * no phar there. Hence a fixture, not a `TestCase`.
 *
 * Deliberately NOT named `*Test.php`, so the `Unit` suite (suffix `Test.php`)
 * does not collect it. Every method below must keep comparing key-like
 * values with a relational operator — replace one with `KeyOrder::*` and this
 * file stops testing the rule.
 */
final class KeyOrderRuleFixture
{
    /**
     * Two key-like variables.
     */
    public static function comparesTwoKeyVariables(string $startKey, string $endKey): bool
    {
        return $startKey < $endKey;
    }

    /**
     * A key against a property named like a key.
     */
    public static function comparesPropertyToVariable(RegionInfo $region, string $limitKey): bool
    {
        return $region->endKey >= $limitKey;
    }

    /**
     * A key against a string literal — the shape `isKeyLikeOperand()` has to
     * accept as key-like for `$key < 'z'` to be reported at all.
     */
    public static function comparesKeyToLiteral(string $key): bool
    {
        return $key < 'z';
    }

    /**
     * Two literals, i.e. the pre-#186 bug written out literally: PHP reads
     * '20' and '100' as the numbers 20 and 100.
     */
    public static function comparesTwoLiterals(): bool
    {
        return '20' < '100';
    }
}
