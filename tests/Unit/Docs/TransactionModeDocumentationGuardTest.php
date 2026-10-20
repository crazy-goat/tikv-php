<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #439 (DIV-03): the mode of a transaction
 * started with `TxnKvClient::begin()` must be documented, in the README and
 * in the method's docblock.
 *
 * The default is **pessimistic**, which is the opposite of the TiKV
 * documentation and of client-go and client-rust, where optimistic is the
 * default. The default is kept (changing it is a breaking change that
 * belongs in a major release), so the documentation is the only thing that
 * tells a porter what `begin()` really does — and the README is the first
 * place a reader looks. `AtomicModeDocumentationGuardTest` guards a
 * documentation invariant the same way for the atomic-mode prerequisite.
 *
 * The checks are deliberately about the substance and not about one exact
 * wording: the README has to say that the pessimistic mode is the default and
 * it has to name both explicit methods, and the `begin()` docblock has to
 * mention the mode and the default. A reworded sentence that still says it
 * passes; a sentence that drops it does not.
 */
class TransactionModeDocumentationGuardTest extends TestCase
{
    public function testReadmeStatesTheDefaultTransactionMode(): void
    {
        $readme = $this->readme();

        self::assertMatchesRegularExpression(
            '/pessimistic`?\s+defaults?\s+to\s+\**true/i',
            $readme,
            'README.md must state that begin() starts a pessimistic transaction '
            . 'by default (issue #439). The default differs from the TiKV '
            . 'documentation and from client-go / client-rust, so the README '
            . 'is the only place a reader learns what begin() does.',
        );
    }

    public function testReadmeDocumentsTheExplicitModeMethods(): void
    {
        $readme = $this->readme();

        self::assertStringContainsString(
            'beginOptimistic()',
            $readme,
            'README.md must document beginOptimistic() (issue #439): it is the '
            . 'explicit way to ask for the optimistic mode.',
        );
        self::assertStringContainsString(
            'beginPessimistic()',
            $readme,
            'README.md must document beginPessimistic() (issue #439): it is the '
            . 'explicit way to ask for the pessimistic mode.',
        );
    }

    public function testBeginDocblockStatesTheDefaultTransactionMode(): void
    {
        $docblock = $this->beginDocblock();

        self::assertMatchesRegularExpression(
            '/defaults?\s+(to|is)\s+\**pessimistic/i',
            $docblock,
            'The docblock of TxnKvClient::begin() must state that the '
            . 'pessimistic mode is the default (issue #439).',
        );
    }

    private function readme(): string
    {
        $file = dirname(__DIR__, 3) . '/README.md';
        $contents = file_get_contents($file);
        self::assertNotFalse($contents, "Unable to read {$file}");

        return $contents;
    }

    /**
     * The docblock that immediately precedes `public function begin()`.
     *
     * Read out of the source rather than restated here, so a docblock that
     * lost the statement fails this test instead of being papered over by a
     * copy in the test file.
     */
    private function beginDocblock(): string
    {
        $file = dirname(__DIR__, 3) . '/src/Client/TxnKv/TxnKvClient.php';
        $source = file_get_contents($file);
        self::assertNotFalse($source, "Unable to read {$file}");

        $method = strpos($source, 'public function begin(');
        self::assertNotFalse($method, "Unable to find begin() in {$file}");

        $start = strrpos(substr($source, 0, $method), '/**');
        self::assertNotFalse($start, "Unable to find the docblock of begin() in {$file}");

        $end = strpos($source, '*/', $start);
        self::assertNotFalse($end, "Unable to find the end of the docblock of begin() in {$file}");

        return substr($source, $start, $end - $start + 2);
    }
}
