<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use Google\Protobuf\Internal\Message;

final class GrpcResponseParser
{
    /**
     * Maximum allowed protobuf message size in bytes.
     * Messages larger than this will be rejected before decoding.
     * Set to 0 or negative to disable the limit.
     */
    private static int $maxMessageSize = 0;

    /**
     * Set the maximum allowed protobuf message size in bytes.
     * Messages exceeding this limit will throw an InvalidArgumentException.
     * Set to 0 or negative to disable the limit (default).
     */
    public static function setMaxMessageSize(int $bytes): void
    {
        self::$maxMessageSize = $bytes;
    }

    public static function getMaxMessageSize(): int
    {
        return self::$maxMessageSize;
    }

    /**
     * @return array{code: int, details: string}
     */
    public static function extractStatus(mixed $event): array
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $status = $eventArray['status'] ?? null;
        if (is_object($status)) {
            $status = (array) $status;
        }

        /** @var array<string, mixed> $statusArray */
        $statusArray = is_array($status) ? $status : [];

        $code = $statusArray['code'] ?? 0;
        $details = $statusArray['details'] ?? '';

        return [
            'code' => is_int($code) ? $code : (is_string($code) ? (int) $code : 0),
            'details' => is_string($details) ? $details : (is_scalar($details) ? (string) $details : ''),
        ];
    }

    /**
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     *
     * @throws \InvalidArgumentException if the message exceeds the configured max size
     */
    public static function deserialize(mixed $event, string $responseClass): Message
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $message = $eventArray['message'] ?? null;

        if ($message !== null && $message !== '' && is_string($message)) {
            $messageLen = strlen($message);
            if (self::$maxMessageSize > 0 && $messageLen > self::$maxMessageSize) {
                throw new \InvalidArgumentException(sprintf(
                    'Protobuf message size %d bytes exceeds maximum allowed %d bytes',
                    $messageLen,
                    self::$maxMessageSize,
                ));
            }

            /** @var T $response */
            $response = new $responseClass();
            $response->mergeFromString($message);

            return $response;
        }

        /** @var T $response */
        $response = new $responseClass();

        return $response;
    }
}
