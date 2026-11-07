<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * A write or read would push the transaction past one of its configured
 * limits (issues #430 and #297).
 *
 * Raised by {@see \CrazyGoat\TiKV\Client\TxnKv\TransactionState::setWrite()}
 * (through `Transaction::set()`/`delete()`) before the write is buffered and
 * before any RPC is issued, and by
 * {@see \CrazyGoat\TiKV\Client\TxnKv\TransactionState::setReadValue()} before
 * a fetched value is retained in the read set. The rejected write is the only
 * one that was not applied, so the transaction stays usable: the caller can
 * commit the smaller write set, roll back, read less, or start a new
 * transaction with larger limits.
 */
final class TransactionTooLargeException extends TiKvException
{
    public const LIMIT_ENTRY_COUNT = 'maxTxnEntryCount';
    public const LIMIT_ENTRY_SIZE = 'maxTxnEntrySize';
    public const LIMIT_TOTAL_SIZE = 'maxTxnTotalSize';
    public const LIMIT_READ_SET_COUNT = 'maxReadSetEntries';
    public const LIMIT_READ_SET_TOTAL_SIZE = 'maxReadSetTotalSize';

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

    public static function readSetCount(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'Transaction read set would hold %d entries, exceeding %s of %d',
                $actual,
                self::LIMIT_READ_SET_COUNT,
                $limit,
            ),
            self::LIMIT_READ_SET_COUNT,
            $limit,
            $actual,
        );
    }

    public static function readSetTotalSize(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'Transaction read set would be %d bytes, exceeding %s of %d bytes',
                $actual,
                self::LIMIT_READ_SET_TOTAL_SIZE,
                $limit,
            ),
            self::LIMIT_READ_SET_TOTAL_SIZE,
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
