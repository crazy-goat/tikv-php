<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * Thrown when a scan hits a client-side row cap instead of silently
 * truncating its result (issue #440).
 *
 * Two caps exist, and both fail loudly:
 *
 *  - an unbounded RawKV scan (`limit: 0`) collects every row of the range in
 *    one PHP array; the `options['maxScanRows']` guard (default
 *    {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::DEFAULT_MAX_SCAN_ROWS})
 *    bounds that buffer;
 *  - an unbounded transactional scan (`Transaction::scan()` with `limit: 0`)
 *    is bounded by the per-transaction scan cap
 *    ({@see \CrazyGoat\TiKV\Client\TxnKv\Transaction::MAX_SCAN_LIMIT}).
 *
 * In both cases the exception is thrown instead of silently truncating the
 * result. The remedy depends on the path: the RawKV path can raise
 * `options['maxScanRows']` or switch to the constant-memory
 * `scanIterator()` / `scanPrefixIterator()` API; the transactional path has no
 * option (its cap is `Transaction::MAX_SCAN_LIMIT`) and has to page the
 * range explicitly. Note that the iterators are a **raw** read: they do not
 * see the transaction's own writes.
 */
final class ScanLimitExceededException extends TiKvException
{
    /**
     * @param int      $maxRows     the cap the scan exceeded
     * @param int      $scannedRows the rows the scan had collected when the
     *                              cap was hit
     * @param string|null $message  overrides the default wording; used by
     *                              {@see self::forTransactionScan()}, whose
     *                              cap is not the `maxScanRows` option
     */
    public function __construct(
        private readonly int $maxRows,
        private readonly int $scannedRows,
        ?string $message = null,
    ) {
        parent::__construct($message ?? sprintf(
            'Unbounded scan collected %d rows, exceeding the configured maximum of %d '
            . '(options[\'maxScanRows\']); use scanIterator()/scanPrefixIterator() for '
            . 'large ranges or raise the option',
            $scannedRows,
            $maxRows,
        ));
    }

    /**
     * A transactional scan collected more rows than the per-transaction scan
     * cap allows.
     *
     * The RawKV wording does not fit here: the transactional cap is a
     * constant of the transaction, not the `options['maxScanRows']` client
     * option, and a transaction has no scan iterator of its own — the
     * unbounded alternative is the RawKV iterator, which is not a snapshot
     * read.
     */
    public static function forTransactionScan(int $maxRows, int $scannedRows): self
    {
        return new self($maxRows, $scannedRows, sprintf(
            'Transactional scan collected %d rows, exceeding the maximum of %d rows; '
            . 'the range holds more keys — page the range explicitly or use '
            . 'RawKvClient::scanIterator() for an unbounded raw scan',
            $scannedRows,
            $maxRows,
        ));
    }

    /**
     * The configured maximum number of rows an unbounded scan may collect.
     */
    public function getMaxRows(): int
    {
        return $this->maxRows;
    }

    /**
     * The number of rows the scan had collected when the guard fired.
     */
    public function getScannedRows(): int
    {
        return $this->scannedRows;
    }
}
