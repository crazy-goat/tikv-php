<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * An SST ingest payload would exceed the configured `maxIngestSize`
 * (issue #297).
 *
 * Raised by {@see \CrazyGoat\TiKV\Client\RawKv\SstIngestor::ingest()} before
 * the payload is converted to protobuf messages, before the cluster is
 * switched into import mode and before any RPC is issued. Nothing has been
 * written when this exception escapes, so the caller can split the payload
 * and retry with a smaller batch.
 */
final class IngestTooLargeException extends TiKvException
{
    public const LIMIT_NAME = 'maxIngestSize';

    private function __construct(
        string $message,
        private readonly int $limit,
        private readonly int $actual,
    ) {
        parent::__construct($message);
    }

    public static function payloadSize(int $actual, int $limit): self
    {
        return new self(
            sprintf(
                'SST ingest payload would be %d bytes, exceeding %s of %d bytes',
                $actual,
                self::LIMIT_NAME,
                $limit,
            ),
            $limit,
            $actual,
        );
    }

    /** The `create()` option name of the limit that was exceeded. */
    public function getLimitName(): string
    {
        return self::LIMIT_NAME;
    }

    /** The configured limit, in bytes. */
    public function getLimit(): int
    {
        return $this->limit;
    }

    /** The payload size that would have been sent, in bytes. */
    public function getActual(): int
    {
        return $this->actual;
    }
}
