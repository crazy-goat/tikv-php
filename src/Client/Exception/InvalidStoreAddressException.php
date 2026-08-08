<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * Thrown when PD returns a store address that fails validation
 * (malformed, i.e. not a bare host:port, or outside the configured
 * allowed set). Distinct from StoreNotFoundException so callers can
 * tell "store missing" apart from "store address rejected".
 */
final class InvalidStoreAddressException extends TiKvException
{
}
