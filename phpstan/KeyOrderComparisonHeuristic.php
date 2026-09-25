<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Phpstan;

use PhpParser\Node;
use PhpParser\Node\Expr;
use PhpParser\Node\Expr\ConstFetch;
use PhpParser\Node\Expr\PropertyFetch;
use PhpParser\Node\Expr\Variable;
use PhpParser\Node\Scalar\String_;

/**
 * The decision logic behind {@see KeyOrderComparisonRule}: which operands read
 * like a TiKV key, and which `KeyOrder` helper expresses a given operator. It
 * is split out so the rule itself only builds the message, and so this file
 * stays free of PHPStan types — it depends on php-parser alone.
 *
 * The *rule* cannot be unit tested: it implements `PHPStan\Rules\Rule`, and
 * PHPStan's classes are only autoloadable from phpstan.phar, which needs
 * ext-phar. CI's `unit-tests` job runs `php -n` (no extensions — see
 * docs/helpers/faq.md), so there is no phar and `PHPStan\Rules\Rule` is not
 * autoloadable there. The rule is therefore covered by the `composer phpstan`
 * run itself: `tests/Unit/Phpstan/KeyOrderRuleFixture.php` holds the comparison
 * shapes it must flag, and the `ignoreErrors` entry that whitelists that
 * fixture has `reportUnmatched: true`, so a rule that stopped firing fails the
 * analysis with `ignore.unmatched`; `KeyOrderRuleNarrownessFixture.php` holds
 * the shapes it must stay quiet on and is deliberately not whitelisted.
 */
final class KeyOrderComparisonHeuristic
{
    /**
     * PHPStan error identifier reported by the rule. Stable: CI baselines and
     * `ignoreErrors` entries are keyed on it.
     */
    public const IDENTIFIER = 'tikv.keyOrder.relationOnStrings';

    /**
     * Fragment of a variable or property name that marks its value as a
     * TiKV key. Deliberately a name heuristic: PHPStan has no way to know
     * that a given `string` flows from a key, and the alternative (flagging
     * every string-to-string comparison) buries the real findings.
     */
    private const KEY_LIKE_NAME = '/(key|prefix|start|end|begin|range|bound|cursor|seek)/i';

    /**
     * Whether a variable or property name reads like a TiKV key.
     */
    private static function isKeyLikeName(string $name): bool
    {
        return preg_match(self::KEY_LIKE_NAME, $name) === 1;
    }

    /**
     * Whether the expression is a variable, a property or a string literal
     * that reads like a TiKV key.
     *
     * A string literal counts as key-like, because a key is very often
     * compared against one — `$key < 'z'`, `$key < 'minKey'` — and a literal
     * on one side is enough to make the comparison about keys. The rule then
     * requires *both* operands to be key-like and both to be plain `string`:
     * with `||` (either side key-like) the literal would make every
     * `$message < 'z'` in the tree a finding.
     *
     * Blind spots: a function-call result and an array element carry no name to
     * narrow on, so `$key < self::limitKey()` or `$key < $bounds[0]` is not
     * reported. Both are rare in this tree, and closing them means flagging
     * every call site, not narrowing further on names.
     */
    public static function isKeyLikeOperand(Expr $expr): bool
    {
        if ($expr instanceof Variable) {
            return is_string($expr->name) && self::isKeyLikeName($expr->name);
        }

        if ($expr instanceof PropertyFetch) {
            return $expr->name instanceof Node\Identifier
                && self::isKeyLikeName($expr->name->name);
        }
        // php-parser models a string literal as Scalar\String_, not as a
        // ConstFetch; ConstFetch covers string *constants* (PHP_EOL, …),
        // which PHPStan also types as string.
        return $expr instanceof String_ || $expr instanceof ConstFetch;
    }

    /**
     * The `KeyOrder` method expressing the same ordering as the operator.
     *
     * @param string $operator one of `<`, `<=`, `>`, `>=`
     */
    public static function helperMethodFor(string $operator): string
    {
        return match ($operator) {
            '<' => 'lt',
            '<=' => 'lte',
            '>' => 'gt',
            default => 'gte',
        };
    }
}
