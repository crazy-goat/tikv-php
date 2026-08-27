<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;
use Rector\Set\ValueObject\LevelSetList;
use Rector\Set\ValueObject\SetList;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/src/Client',
        __DIR__ . '/tests',
    ])
    ->withPhpVersion(\Rector\ValueObject\PhpVersion::PHP_82)
    ->withSets([
        LevelSetList::UP_TO_PHP_82,
        SetList::DEAD_CODE,
        SetList::CODE_QUALITY,
        SetList::TYPE_DECLARATION,
        SetList::PRIVATIZATION,
        SetList::EARLY_RETURN,
    ])
    // Rector 2.6 has no safe transform for these two test classes: every
    // rule chain it proposes for their setUp()-initialised properties breaks
    // them — ReadOnlyPropertyRector + RemoveUnusedPrivateMethodRector leaves
    // the props uninitialised ("must not be accessed before initialization"),
    // and PrivatizeFinalClassMethodRector makes setUp() private, which
    // PHPUnit rejects with a fatal "must be protected" error. Skip wholesale
    // rather than adopt a broken transform (see issue #485).
    ->withSkip([
        __DIR__ . '/tests/Unit/RawKv/RegionResolverBinaryKeyTest.php',
        __DIR__ . '/tests/Unit/Region/RegionRangeClipperTest.php',
    ]);
