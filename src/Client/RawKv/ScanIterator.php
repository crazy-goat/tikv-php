<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use Iterator;

/**
 * Lazy auto-paginating scan iterator.
 *
 * Fetches results from TiKV in batches on demand, keeping only
 * one batch (page) in memory at a time. Essential for scanning
 * large datasets without exhausting PHP memory.
 *
 * Usage:
 *   foreach ($client->scanIterator('start', 'end') as $key => $value) {
 *       // process one key-value at a time
 *   }
 *
 * @implements Iterator<string, ?string>
 */
final class ScanIterator implements Iterator
{
    /** @var list<array{key: string, value: ?string}> */
    private array $buffer = [];

    private int $bufferIndex = 0;

    private string $currentStartKey;

    private bool $exhausted = false;

    private \Closure $scanFn;

    /**
     * @param callable(string, string, int, bool): list<array{key: string, value: ?string}> $scanFn
     */
    public function __construct(
        callable $scanFn,
        private readonly string $startKey,
        private readonly string $endKey,
        private readonly int $batchSize = 256,
        private readonly bool $keyOnly = false,
    ) {
        $this->scanFn = \Closure::fromCallable($scanFn);

        if ($batchSize <= 0) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException('batchSize must be greater than 0');
        }

        if ($batchSize > RawKvClient::MAX_SCAN_LIMIT) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(sprintf(
                'batchSize (%d) exceeds maximum allowed scan limit of %d',
                $batchSize,
                RawKvClient::MAX_SCAN_LIMIT,
            ));
        }

        $this->currentStartKey = $startKey;
    }

    public function current(): ?string
    {
        return $this->buffer[$this->bufferIndex]['value'];
    }

    public function key(): string
    {
        return $this->buffer[$this->bufferIndex]['key'];
    }

    public function next(): void
    {
        $this->bufferIndex++;

        if ($this->bufferIndex >= count($this->buffer) && !$this->exhausted) {
            $this->fetchNextBatch();
        }
    }

    public function valid(): bool
    {
        if ($this->buffer !== [] && $this->bufferIndex < count($this->buffer)) {
            return true;
        }

        if ($this->exhausted) {
            return false;
        }

        $this->fetchNextBatch();

        return $this->bufferIndex < count($this->buffer);
    }

    public function rewind(): void
    {
        $this->currentStartKey = $this->startKey;
        $this->buffer = [];
        $this->bufferIndex = 0;
        $this->exhausted = false;
    }

    private function fetchNextBatch(): void
    {
        if ($this->exhausted) {
            $this->buffer = [];
            return;
        }

        if ($this->endKey !== '' && $this->currentStartKey >= $this->endKey) {
            $this->buffer = [];
            $this->exhausted = true;
            return;
        }

        $results = ($this->scanFn)(
            $this->currentStartKey,
            $this->endKey,
            $this->batchSize,
            $this->keyOnly,
        );

        if ($results === []) {
            $this->buffer = [];
            $this->exhausted = true;
            return;
        }

        $lastKey = $results[count($results) - 1]['key'];
        $this->currentStartKey = $lastKey . "\x00";

        if (count($results) < $this->batchSize) {
            $this->exhausted = true;
        }

        $this->buffer = $results;
        $this->bufferIndex = 0;
    }
}
