<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

/**
 * Resolves and caches API V2 keyspace names to their PD-assigned IDs.
 */
final class KeyspaceResolver
{
    public const DEFAULT_NAME = 'DEFAULT';

    /** @var array<string, int> */
    private array $cache = [];

    public function __construct(private readonly PdClientInterface $pdClient)
    {
    }

    public function resolve(string $name): int
    {
        $name = $name === '' ? self::DEFAULT_NAME : $name;
        if (isset($this->cache[$name])) {
            return $this->cache[$name];
        }

        $id = $this->pdClient->getKeyspaceId($name);
        if ($id < 0 || $id > 0xFFFFFF) {
            throw new InvalidArgumentException(sprintf(
                'PD returned an out-of-range keyspace ID for "%s": %d',
                $name,
                $id,
            ));
        }

        return $this->cache[$name] = $id;
    }
}
