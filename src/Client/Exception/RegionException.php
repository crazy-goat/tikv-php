<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\StoreNotMatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Util\ServerText;

final class RegionException extends TiKvException
{
    /**
     * @param NotLeaderHint|null $notLeader The leader hint of a NotLeader
     *     error, in the library's own types (issue #734). The generated
     *     `Errorpb\NotLeader` message never leaves this class: it is
     *     regenerated with every protobuf bump, so promising it would hand
     *     consumers a type the library may change without a version break.
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
     * @param StoreNotMatch|null $storeNotMatch The payload of a
     *     StoreNotMatch error. It names the store ID the request was sent to,
     *     which is the store-cache entry the retry must drop (issue #247).
     * @param bool $fromErrorpb Whether this exception was built from a real
     *     `errorpb.Error` by {@see self::fromRegionError()}. Only such an
     *     exception is an unrecognised REGION error when its kind is null and
     *     gets the bounded retry (issue #444); a synthetic RegionException
     *     wrapping a response-level error string or KeyError stays fatal.
     */
    public function __construct(
        string $operation,
        string $message,
        public readonly ?NotLeaderHint $notLeader = null,
        public readonly ?ErrorKind $errorKind = null,
        public readonly ?EpochNotMatch $epochNotMatch = null,
        public readonly ?RegionInfo $cachedRegion = null,
        public readonly ?StoreNotMatch $storeNotMatch = null,
        public readonly bool $fromErrorpb = false,
    ) {
        // $message is server-supplied at the fromRegionError() and
        // RegionErrorHandler call sites; sanitise it before it reaches a log
        // sink (SEC-11, issue #314).
        parent::__construct(sprintf('%s failed: %s', $operation, ServerText::sanitise($message)));
    }

    /**
     * Factory method that builds a RegionException from a protobuf Error
     * message, automatically detecting the error kind from the oneof fields.
     *
     * $cachedRegion is the region entry that produced the error, when the
     * caller has one; it is used by the EpochNotMatch cache-refresh path and
     * as the StoreNotMatch fallback when the payload does not name the
     * requested store (see the constructor docblock).
     */
    public static function fromRegionError(Error $error, ?RegionInfo $cachedRegion = null): self
    {
        $kind = self::detectErrorKind($error);
        $notLeader = $error->getNotLeader();
        $leader = $notLeader?->getLeader();

        return new self(
            operation: 'RegionError',
            message: $error->getMessage(),
            notLeader: $notLeader === null ? null : new NotLeaderHint(
                regionId: (int) $notLeader->getRegionId(),
                leaderPeerId: $leader === null ? null : (int) $leader->getId(),
                leaderStoreId: $leader === null ? null : (int) $leader->getStoreId(),
            ),
            errorKind: $kind,
            epochNotMatch: $error->getEpochNotMatch(),
            cachedRegion: $cachedRegion,
            storeNotMatch: $error->getStoreNotMatch(),
            fromErrorpb: true,
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
