<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * A write would push the transaction past one of its configured limits
 * (issue #430).
 *
 * Raised by {@see \CrazyGoat\TiKV\Client\TxnKv\TransactionState::setWrite()}
 * (through `Transaction::set()`/`delete()`) before the write is buffered and
 * before any RPC is issued. The rejected write is the only one that was not
 * applied, so the transaction stays usable: the caller can commit the smaller
 * write set, roll back, or start a new transaction with larger limits.
 */
final class TransactionTooLargeException extends TiKvException
{
    public const LIMIT_ENTRY_COUNT = 'maxTxnEntryCount';
    public const LIMIT_ENTRY_SIZE = 'maxTxnEntrySize';
    public const LIMIT_TOTAL_SIZE = 'maxTxnTotalSize';

    private function __construct(
        string $message,
        private readonly string $limitName,
        private readonly int $limit,
        private readonly int $actual,
    ) {
        parent::__construct($message);
    }

    public static function entryCount(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'Transaction write set would hold %d entries, exceeding %s of %d',
                $actual,
                self::LIMIT_ENTRY_COUNT,
                $limit,
            ),
            self::LIMIT_ENTRY_COUNT,
            $limit,
            $actual,
        );
    }

    public static function entrySize(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'Transaction write set entry would be %d bytes, exceeding %s of %d bytes',
                $actual,
                self::LIMIT_ENTRY_SIZE,
                $limit,
            ),
            self::LIMIT_ENTRY_SIZE,
            $limit,
            $actual,
        );
    }

    public static function totalSize(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'Transaction write set would be %d bytes, exceeding %s of %d bytes',
                $actual,
                self::LIMIT_TOTAL_SIZE,
                $limit,
            ),
            self::LIMIT_TOTAL_SIZE,
            $limit,
            $actual,
        );
    }

    /** The `begin()` option name of the limit that was exceeded. */
    public function getLimitName(): string
    {
        return $this->limitName;
    }

    /** The configured limit, in entries or bytes. */
    public function getLimit(): int
    {
        return $this->limit;
    }

    /** The value the write would have produced, in entries or bytes. */
    public function getActual(): int
    {
        return $this->actual;
    }
}
