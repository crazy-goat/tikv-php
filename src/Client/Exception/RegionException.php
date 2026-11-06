<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;

final class RegionException extends TiKvException
{
    /**
     * @param ErrorKind|null $errorKind The typed error variant detected from
     *     the proto Error oneof, or null when the error could not be mapped.
     *     When non-null the classifier can determine the correct BackoffType
     *     without inspecting the message text.
     * @param EpochNotMatch|null $epochNotMatch The payload of an
     *     EpochNotMatch error. TiKV puts the regions that now cover the
     *     stale range in `current_regions`, so the retry can refresh the
     *     region cache without a PD round trip (issue #246).
     * @param RegionInfo|null $cachedRegion The region entry that produced
     *     the error, captured by RegionErrorHandler::check() before it
     *     invalidates the entry. EpochNotMatch carries no leader, so the
     *     retry needs the stale entry's leader store to seed the new
     *     regions. Null when the caller did not pass a cache.
     */
    public function __construct(
        string $operation,
        string $message,
        public readonly ?NotLeader $notLeader = null,
        public readonly ?ErrorKind $errorKind = null,
        public readonly ?EpochNotMatch $epochNotMatch = null,
        public readonly ?RegionInfo $cachedRegion = null,
    ) {
        parent::__construct("{$operation} failed: {$message}");
    }

    /**
     * Factory method that builds a RegionException from a protobuf Error
     * message, automatically detecting the error kind from the oneof fields.
     *
     * $cachedRegion is the region entry that produced the error, when the
     * caller has one; it is only used by the EpochNotMatch cache-refresh
     * path (see the constructor docblock).
     */
    public static function fromRegionError(Error $error, ?RegionInfo $cachedRegion = null): self
    {
        $kind = self::detectErrorKind($error);

        return new self(
            operation: 'RegionError',
            message: $error->getMessage(),
            notLeader: $error->getNotLeader(),
            errorKind: $kind,
            epochNotMatch: $error->getEpochNotMatch(),
            cachedRegion: $cachedRegion,
        );
    }

    /**
     * Detect which oneof field is set on the Error message and return the
     * corresponding ErrorKind.
     */
    private static function detectErrorKind(Error $error): ?ErrorKind
    {
        foreach (ErrorKind::cases() as $kind) {
            // Convert snake_case field name to PascalCase method name.
            // e.g. 'region_not_found' → 'hasRegionNotFound'
            $method = 'has' . str_replace(
                '_',
                '',
                ucwords($kind->value, '_'),
            );

            if ($error->$method()) {
                return $kind;
            }
        }

        return null;
    }
}
