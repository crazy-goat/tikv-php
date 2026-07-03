<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Cache;

use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RegionCache implements RegionCacheInterface
{
    /** @var RegionEntry[] */
    private array $entries = [];

    /** @var array<int, int> regionId => index */
    private array $idToIndex = [];

    public function __construct(
        private readonly int $ttlSeconds = 600,
        private readonly int $jitterSeconds = 60,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function getByKey(string $key): ?RegionInfo
    {
        $index = $this->binarySearch($key);
        if ($index === null) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        $entry = $this->entries[$index];

        if ($this->isExpired($entry)) {
            $this->removeByIndex($index);
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        if ($entry->region->endKey !== '' && $key >= $entry->region->endKey) {
            $this->logger->debug('Region cache miss', ['key' => KeyRedactor::redact($key)]);
            return null;
        }

        $this->logger->debug('Region cache hit', ['key' => KeyRedactor::redact($key), 'regionId' => $entry->region->regionId]);

        return $this->resolveRegionInfo($entry);
    }

    public function put(RegionInfo $region): void
    {
        $this->removeById($region->regionId);

        $position = $this->findInsertPosition($region->startKey);
        $entry = new RegionEntry($region, $this->now() + $this->ttlSeconds + $this->jitter());
        array_splice($this->entries, $position, 0, [$entry]);
        $this->rebuildIdToIndex();

        $this->logger->debug('Region cached', [
            'regionId' => $region->regionId,
            'startKey' => KeyRedactor::redact($region->startKey),
            'endKey' => KeyRedactor::redact($region->endKey),
            'ttl' => $entry->expiresAt - $this->now(),
        ]);
    }

    public function invalidate(int $regionId): void
    {
        $this->logger->info('Region invalidated', ['regionId' => $regionId]);
        $this->removeById($regionId);
    }

    public function switchLeader(int $regionId, int $leaderStoreId): bool
    {
        $index = $this->idToIndex[$regionId] ?? null;
        if ($index === null) {
            return false;
        }

        $entry = $this->entries[$index];
        $result = $entry->switchLeader($leaderStoreId);
        if ($result) {
            $this->logger->info('Region leader switched', [
                'regionId' => $regionId,
                'newLeaderStoreId' => $leaderStoreId,
            ]);
        }
        return $result;
    }

    public function clear(): void
    {
        $this->entries = [];
        $this->idToIndex = [];
    }

    protected function now(): int
    {
        return time();
    }

    private function binarySearch(string $key): ?int
    {
        $left = 0;
        $right = count($this->entries) - 1;
        $result = null;

        while ($left <= $right) {
            $mid = (int) (($left + $right) / 2);
            $entry = $this->entries[$mid];

            if ($entry->region->startKey <= $key) {
                $result = $mid;
                $left = $mid + 1;
            } else {
                $right = $mid - 1;
            }
        }

        return $result;
    }

    private function findInsertPosition(string $startKey): int
    {
        $left = 0;
        $right = count($this->entries);

        while ($left < $right) {
            $mid = (int) (($left + $right) / 2);
            if ($this->entries[$mid]->region->startKey < $startKey) {
                $left = $mid + 1;
            } else {
                $right = $mid;
            }
        }

        return $left;
    }

    private function removeById(int $regionId): void
    {
        $index = $this->idToIndex[$regionId] ?? null;
        if ($index !== null) {
            $this->removeByIndex($index);
        }
    }

    private function removeByIndex(int $index): void
    {
        if (isset($this->entries[$index])) {
            array_splice($this->entries, $index, 1);
            $this->rebuildIdToIndex();
        }
    }

    private function rebuildIdToIndex(): void
    {
        $this->idToIndex = [];
        foreach ($this->entries as $index => $entry) {
            $this->idToIndex[$entry->region->regionId] = $index;
        }
    }

    private function isExpired(RegionEntry $entry): bool
    {
        return $this->now() >= $entry->expiresAt;
    }

    private function jitter(): int
    {
        if ($this->jitterSeconds <= 0) {
            return 0;
        }

        return random_int(0, $this->jitterSeconds);
    }

    private function resolveRegionInfo(RegionEntry $entry): RegionInfo
    {
        if ($entry->getLeaderStoreId() === $entry->region->leaderStoreId
            && $entry->getLeaderPeerId() === $entry->region->leaderPeerId) {
            return $entry->region;
        }

        return new RegionInfo(
            regionId: $entry->region->regionId,
            leaderPeerId: $entry->getLeaderPeerId(),
            leaderStoreId: $entry->getLeaderStoreId(),
            epochConfVer: $entry->region->epochConfVer,
            epochVersion: $entry->region->epochVersion,
            startKey: $entry->region->startKey,
            endKey: $entry->region->endKey,
            peers: $entry->region->peers,
        );
    }
}
