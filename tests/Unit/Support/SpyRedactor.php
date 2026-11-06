<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

/**
 * A redactor that counts how often it was asked to redact a key.
 *
 * `KeyRedactor::setRedactor()` installs a process-global redactor, so the
 * tests that use this one restore the static state in `setUp()`/`tearDown()`
 * through `KeyRedactorState`.
 */
final class SpyRedactor
{
    public int $calls = 0;

    public function __invoke(string $key): string
    {
        $this->calls++;

        return 'redacted(' . $key . ')';
    }
}
