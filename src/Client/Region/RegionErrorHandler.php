<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\TiKV\Client\Exception\RegionException;

final class RegionErrorHandler
{
    public static function check(object $response): void
    {
        if (!method_exists($response, 'getRegionError')) {
            return;
        }

        $regionError = $response->getRegionError();
        if ($regionError === null) {
            return;
        }

        throw RegionException::fromRegionError($regionError);
    }
}
