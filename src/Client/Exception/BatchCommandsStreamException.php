<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * Thrown when the BatchCommands stream layer fails: the peer half-closed the
 * stream before every request_id was answered, a wire message is malformed
 * (response without request_id), or the drain deadline expired. Callers
 * treat it as a per-round-trip failure and fall back to the unary path.
 */
final class BatchCommandsStreamException extends TiKvException
{
}
