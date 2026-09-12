<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * Thrown when an unbounded scan (limit 0) collects more rows than the
 * configured client-side maximum.
 *
 * A scan with `limit: 0` pages internally so the documented "whole range"
 * contract holds, buffering every returned row in a PHP array. The
 * `options['maxScanRows']` guard (default
 * {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::DEFAULT_MAX_SCAN_ROWS})
 * bounds that buffer. This exception is thrown instead of silently
 * truncating the result, so a caller that genuinely needs a larger range
 * can switch to the constant-memory `scanIterator()` /
 * `scanPrefixIterator()` API or raise the option.
 */
final class ScanLimitExceededException extends TiKvException
{
    public function __construct(
        private readonly int $maxRows,
        private readonly int $scannedRows,
    ) {
        parent::__construct(sprintf(
            'Unbounded scan collected %d rows, exceeding the configured maximum of %d '
            . '(options[\'maxScanRows\']); use scanIterator()/scanPrefixIterator() for '
            . 'large ranges or raise the option',
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
