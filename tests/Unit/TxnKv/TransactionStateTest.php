<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class TransactionStateTest extends TestCase
{
    public function testNewStateIsActive(): void
    {
        $state = new TransactionState();

        $this->assertTrue($state->isActive());
        $this->assertSame(TransactionStatus::Active, $state->getStatus());
    }

    public function testActiveStatusWithClosedFlagIsNotActive(): void
    {
        $state = new TransactionState();
        $state->close();

        // The closed flag alone ends the lifecycle; the status is a separate
        // fact that close() does not rewrite (issue #342).
        $this->assertSame(TransactionStatus::Active, $state->getStatus());
        $this->assertFalse($state->isActive());
    }

    /**
     * @return iterable<string, array{TransactionStatus}>
     */
    public static function terminalStatusProvider(): iterable
    {
        yield 'committed' => [TransactionStatus::Committed];
        yield 'rolled back' => [TransactionStatus::RolledBack];
        yield 'undetermined' => [TransactionStatus::Undetermined];
    }

    #[DataProvider('terminalStatusProvider')]
    public function testTerminalStatusIsNotActive(TransactionStatus $status): void
    {
        $state = new TransactionState();
        $state->setStatus($status);

        $this->assertFalse($state->isActive());
    }

    public function testEnsureActiveAcceptsAnActiveState(): void
    {
        $state = new TransactionState();

        $state->ensureActive();

        $this->addToAssertionCount(1);
    }

    public function testEnsureActiveRejectsAClosedState(): void
    {
        $state = new TransactionState();
        $state->close();

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $state->ensureActive();
    }

    #[DataProvider('terminalStatusProvider')]
    public function testEnsureActiveRejectsATerminalState(TransactionStatus $status): void
    {
        $state = new TransactionState();
        $state->setStatus($status);

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Transaction is not active');
        $state->ensureActive();
    }

    public function testIsActiveMatchesEnsureActiveForEveryState(): void
    {
        $states = [
            'new' => new TransactionState(),
            'closed' => (static function (): TransactionState {
                $state = new TransactionState();
                $state->close();

                return $state;
            })(),
        ];
        foreach (TransactionStatus::cases() as $status) {
            $state = new TransactionState();
            $state->setStatus($status);
            $states[$status->name] = $state;
        }

        foreach ($states as $name => $state) {
            $active = true;
            try {
                $state->ensureActive();
            } catch (InvalidStateException) {
                $active = false;
            }

            $this->assertSame(
                $active,
                $state->isActive(),
                sprintf('isActive() agrees with ensureActive() for the "%s" state', $name),
            );
        }
    }
}
