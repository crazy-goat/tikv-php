<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;

final class RawKvSplitter
{
    /**
     * @template T
     * @param T[] $items
     * @param callable(T): int $sizeFn
     * @return T[][]
     */
    public static function splitIntoBatches(array $items, int $maxCount, int $maxBytes, callable $sizeFn): array
    {
        $batches = [];
        $current = [];
        $currentCount = 0;
        $currentBytes = 0;

        foreach ($items as $item) {
            $size = $sizeFn($item);

            if ($currentCount > 0 && ($currentCount >= $maxCount || $currentBytes + $size > $maxBytes)) {
                $batches[] = $current;
                $current = [];
                $currentCount = 0;
                $currentBytes = 0;
            }

            $current[] = $item;
            $currentCount++;
            $currentBytes += $size;
        }

        if ($currentCount > 0) {
            $batches[] = $current;
        }

        return $batches;
    }

    /**
     * @param KvPair[] $pairs
     * @param int[] $ttls
     * @return array<array{pairs: KvPair[], ttls: int[]}>
     */
    public static function splitPairsIntoBatches(array $pairs, int $maxCount, int $maxBytes, array $ttls = []): array
    {
        // Normalize keys to sequential 0-indexed arrays so TTLs always align
        // with pairs regardless of incoming key shape.
        $pairs = array_values($pairs);
        $hasTtls = $ttls !== [];
        if ($hasTtls) {
            $ttls = array_values($ttls);
        }

        $batches = [];
        $currentPairs = [];
        $currentTtls = [];
        $currentCount = 0;
        $currentBytes = 0;

        foreach ($pairs as $i => $pair) {
            $size = strlen($pair->getKey()) + strlen($pair->getValue());

            if ($currentCount > 0 && ($currentCount >= $maxCount || $currentBytes + $size > $maxBytes)) {
                $batches[] = ['pairs' => $currentPairs, 'ttls' => $currentTtls];
                $currentPairs = [];
                $currentTtls = [];
                $currentCount = 0;
                $currentBytes = 0;
            }

            $currentPairs[] = $pair;
            if ($hasTtls) {
                $currentTtls[] = $ttls[$i];
            }
            $currentCount++;
            $currentBytes += $size;
        }

        if ($currentCount > 0) {
            $batches[] = ['pairs' => $currentPairs, 'ttls' => $currentTtls];
        }

        return $batches;
    }

    public static function calculatePrefixEndKey(string $prefix): string
    {
        if ($prefix === '') {
            return '';
        }

        $lastByte = ord($prefix[strlen($prefix) - 1]);

        if ($lastByte === 255) {
            $trimmed = rtrim($prefix, "\xff");
            if ($trimmed === '') {
                return '';
            }
            $lastByte = ord($trimmed[strlen($trimmed) - 1]);
            if ($lastByte === 255) {
                return '';
            }
            return substr($trimmed, 0, -1) . chr($lastByte + 1);
        }

        return substr($prefix, 0, -1) . chr($lastByte + 1);
    }
}
