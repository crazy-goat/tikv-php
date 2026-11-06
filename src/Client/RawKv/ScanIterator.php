<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
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
 * The factory methods (`RawKvClient::scanIterator()` and
 * `RawKvClient::scanPrefixIterator()`) only validate the arguments and return
 * this object; the scan RPCs run while it is iterated. The failures therefore
 * surface from {@see self::next()} and {@see self::valid()}: a
 * ClientClosedException when the owning client was closed after the iterator
 * was created, a RegionException / GrpcException from the RPC, a
 * KeyErrorException for a per-pair key error, a StoreNotFoundException when
 * PD no longer knows the store backing the region, a
 * RetryBudgetExhaustedException when the page's retry loop exhausts its
 * attempt cap or wall-clock deadline, and the base TiKvException when the
 * server returns a page that does not advance the cursor.
 *
 * @implements Iterator<string, ?string>
 */
final class ScanIterator implements Iterator
{
    /** @var array<array{key: string, value: ?string}> */
    private array $buffer = [];

    private int $bufferIndex = 0;

    private string $currentStartKey;

    private bool $exhausted = false;

    private readonly \Closure $scanFn;

    /** @var (\Closure(): void)|null */
    private readonly ?\Closure $guard;

    /**
     * @param callable(string, string, int, bool, string): array<array{key: string, value: ?string}> $scanFn
     * @param (callable(): void)|null $guard Invoked before every page fetch. RawKvClient
     *        passes its ensureOpen() check so an iterator created by a client that has
     *        since been closed fails with ClientClosedException instead of issuing an
     *        RPC on a closed channel.
     *
     * @throws \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException When
     *         batchSize is below 1 or above RawKvClient::MAX_SCAN_LIMIT
     */
    public function __construct(
        callable $scanFn,
        private readonly string $startKey,
        private readonly string $endKey,
        private readonly int $batchSize = 1024,
        private readonly bool $keyOnly = false,
        private readonly string $columnFamily = '',
        ?callable $guard = null,
    ) {
        $this->scanFn = \Closure::fromCallable($scanFn);
        $this->guard = $guard === null ? null : \Closure::fromCallable($guard);

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

    /**
     * Advance to the next row, fetching the next page when the buffer is
     * exhausted.
     *
     * @throws ClientClosedException When the owning client was closed
     * @throws RegionException On a region error
     * @throws GrpcException On a gRPC transport error
     * @throws KeyErrorException On a per-pair key error
     * @throws StoreNotFoundException When PD no longer knows the store backing
     *         the region
     * @throws RetryBudgetExhaustedException When the page's retry attempt cap
     *         or wall-clock deadline is exhausted
     * @throws TiKvException When the server returns a page that does not
     *         advance the scan cursor
     */
    public function next(): void
    {
        $this->bufferIndex++;

        if ($this->bufferIndex >= count($this->buffer) && !$this->exhausted) {
            $this->fetchNextBatch();
        }
    }

    /**
     * Whether the current position holds a row, fetching the next page when
     * the buffer is exhausted.
     *
     * @throws ClientClosedException When the owning client was closed
     * @throws RegionException On a region error
     * @throws GrpcException On a gRPC transport error
     * @throws KeyErrorException On a per-pair key error
     * @throws StoreNotFoundException When PD no longer knows the store backing
     *         the region
     * @throws RetryBudgetExhaustedException When the page's retry attempt cap
     *         or wall-clock deadline is exhausted
     * @throws TiKvException When the server returns a page that does not
     *         advance the scan cursor
     */
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

        if ($this->endKey !== '' && KeyOrder::gte($this->currentStartKey, $this->endKey)) {
            $this->buffer = [];
            $this->exhausted = true;
            return;
        }

        // Fail closed before the RPC: a client closed after this iterator was
        // created must not keep scanning on closed channels (issue #344).
        if ($this->guard instanceof \Closure) {
            ($this->guard)();
        }

        $results = ($this->scanFn)(
            $this->currentStartKey,
            $this->endKey,
            $this->batchSize,
            $this->keyOnly,
            $this->columnFamily,
        );

        if ($results === []) {
            $this->buffer = [];
            $this->exhausted = true;
            return;
        }

        $lastKey = $results[count($results) - 1]['key'];
        $nextStartKey = KeyOrder::successor($lastKey);

        // A well-behaved server returns strictly increasing keys, all >= the
        // requested start key. Without these checks a server that keeps
        // answering a full page ending on the same key never ends any
        // forward scan, bounded or not: the cursor stays below endKey. Only
        // a one-row page equal to the cursor passes (it advances by one
        // byte). Keys are not put in the message.
        if (!$this->pageProgresses($results)) {
            $this->buffer = [];
            $this->exhausted = true;

            throw new TiKvException('Scan cursor did not advance; server returned non-monotonic keys');
        }

        $this->currentStartKey = $nextStartKey;

        if (count($results) < $this->batchSize) {
            $this->exhausted = true;
        }

        $this->buffer = $results;
        $this->bufferIndex = 0;
    }

    /**
     * @param non-empty-array<array{key: string, value: ?string}> $results
     */
    private function pageProgresses(array $results): bool
    {
        $previous = null;
        foreach ($results as $row) {
            $key = $row['key'];
            if (
                $previous === null
                    ? KeyOrder::lt($key, $this->currentStartKey)
                    : KeyOrder::lte($key, $previous)
            ) {
                return false;
            }
            $previous = $key;
        }

        return true;
    }
}
