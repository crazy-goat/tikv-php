<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\TiKV\Client\Util\KeyRedactor;

/**
 * A V2 keyspace codec received a key belonging to another keyspace or a
 * malformed key prefix.
 */
final class KeyOutOfBoundsException extends TiKvException
{
    public static function forKey(string $key): self
    {
        return new self('API V2 key is outside the configured keyspace: ' . KeyRedactor::redact($key));
    }
}
