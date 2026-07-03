<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;

final class RegionException extends TiKvException
{
    /**
     * @param ErrorKind|null $errorKind The typed error variant detected from
     *     the proto Error oneof, or null when the error could not be mapped.
     *     When non-null the classifier can determine the correct BackoffType
     *     without inspecting the message text.
     */
    public function __construct(
        string $operation,
        string $message,
        public readonly ?NotLeader $notLeader = null,
        public readonly ?ErrorKind $errorKind = null,
    ) {
        parent::__construct("{$operation} failed: {$message}");
    }

    /**
     * Factory method that builds a RegionException from a protobuf Error
     * message, automatically detecting the error kind from the oneof fields.
     */
    public static function fromRegionError(Error $error): self
    {
        $kind = self::detectErrorKind($error);

        return new self(
            operation: 'RegionError',
            message: $error->getMessage(),
            notLeader: $error->getNotLeader(),
            errorKind: $kind,
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
