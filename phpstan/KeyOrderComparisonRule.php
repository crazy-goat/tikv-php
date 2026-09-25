<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Phpstan;

use PhpParser\Node;
use PhpParser\Node\Expr\BinaryOp\Greater;
use PhpParser\Node\Expr\BinaryOp\GreaterOrEqual;
use PhpParser\Node\Expr\BinaryOp\Smaller;
use PhpParser\Node\Expr\BinaryOp\SmallerOrEqual;
use PHPStan\Analyser\Scope;
use PHPStan\Rules\Rule;
use PHPStan\Rules\RuleErrorBuilder;

/**
 * Forbids PHP's relational operators (`<`, `<=`, `>`, `>=`) between two
 * key-like strings, so TiKV keys can never be ordered numerically by
 * accident (issue #186).
 *
 * PHP 8 compares two strings bytewise *except* when both operands are
 * "numeric strings", in which case it compares them numerically. TiKV always
 * compares keys bytewise, so `"20" > "100"` is the wrong answer and
 * `"007" == "7"` is not an identity. All key ordering must therefore go
 * through {@see \CrazyGoat\TiKV\Client\Util\KeyOrder}.
 *
 * The rule is deliberately narrow so that ordinary (non-key) string
 * comparisons elsewhere in the tree stay legal:
 *
 * - both operand types must be plain `string` (an `int|string` such as an
 *   array key or a call index is not a key — that is the shape of
 *   `BatchPartialFailureException::getFirstRegionError()`, which picks the
 *   minimum *call index* and must keep using `<`);
 * - both operands must be key-like, meaning a name reading like a key
 *   (`$startKey`, `$region->endKey`, `$prefix`, …) or a string literal, so
 *   comparisons of two label/message/host strings are not touched. Requiring
 *   *both* sides is what keeps the rule quiet on non-key string comparisons;
 *   the residual blind spots (function results and array elements, which have
 *   no name to narrow on) are listed on
 *   {@see KeyOrderComparisonHeuristic::isKeyLikeOperand()}.
 *
 * The decision logic lives in {@see KeyOrderComparisonHeuristic}. Registered
 * once in `phpstan.neon` (a `services:` entry carrying the `phpstan.rules.rule`
 * tag) — do NOT also list the class under `rules:`, which registers a second
 * instance and reports every finding twice.
 *
 * The rule is tested by `composer phpstan` itself, in both directions: the
 * fixture in `tests/Unit/Phpstan/KeyOrderRuleFixture.php` holds the
 * comparison shapes it must flag and is whitelisted by an `ignoreErrors`
 * entry with `reportUnmatched: true`, so the run fails with `ignore.unmatched`
 * if the rule ever stops firing, while
 * `tests/Unit/Phpstan/KeyOrderRuleNarrownessFixture.php` holds the shapes it
 * must stay quiet on and is not whitelisted at all, so widening the rule
 * fails the run too. Nothing can instantiate this class in the `Unit` suite —
 * see {@see KeyOrderComparisonHeuristic}.
 *
 * Debugging: PHPStan's result cache is not keyed on custom-rule source, so
 * after editing this file run `vendor/bin/phpstan clear-result-cache`, or the
 * next run may serve stale results and the rule looks like it does not fire.
 *
 * @implements Rule<Node\Expr\BinaryOp>
 */
final class KeyOrderComparisonRule implements Rule
{
    private const OPERATORS = [
        Greater::class => '>',
        GreaterOrEqual::class => '>=',
        Smaller::class => '<',
        SmallerOrEqual::class => '<=',
    ];

    public function getNodeType(): string
    {
        // PHPStan's own comparison rules subscribe to BinaryOp and filter by
        // instanceof, so that is what this rule does too — subscribing to
        // Node\Expr would run it on every expression in every analysed file.
        return Node\Expr\BinaryOp::class;
    }

    public function processNode(Node $node, Scope $scope): array
    {
        if (
            !$node instanceof Greater
            && !$node instanceof GreaterOrEqual
            && !$node instanceof Smaller
            && !$node instanceof SmallerOrEqual
        ) {
            return [];
        }

        if (
            !$scope->getType($node->left)->isString()->yes()
            || !$scope->getType($node->right)->isString()->yes()
        ) {
            return [];
        }

        if (
            !KeyOrderComparisonHeuristic::isKeyLikeOperand($node->left)
            || !KeyOrderComparisonHeuristic::isKeyLikeOperand($node->right)
        ) {
            return [];
        }

        $operator = self::OPERATORS[$node::class] ?? '<';

        return [
            RuleErrorBuilder::message(sprintf(
                'Relational operator "%s" between two key-like strings is a numeric comparison '
                . 'when both are numeric strings (PHP), while TiKV orders keys bytewise '
                . '("20" > "100" is false in PHP, true in TiKV) — use '
                . 'CrazyGoat\TiKV\Client\Util\KeyOrder::%s() instead (issue #186).',
                $operator,
                KeyOrderComparisonHeuristic::helperMethodFor($operator),
            ))
                ->identifier(KeyOrderComparisonHeuristic::IDENTIFIER)
                ->line($node->getStartLine())
                ->build(),
        ];
    }
}
