<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

final class InvalidStateException extends TiKvException
{
    public function __construct(string $message)
    {
        parent::__construct($message);
    }
}
