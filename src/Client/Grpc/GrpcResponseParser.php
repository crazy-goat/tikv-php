<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
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
     *
     * @throws GrpcException when the event carries no usable status. An absent
     *                       status must never read as STATUS_OK: the call did not
     *                       demonstrably succeed.
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

        if (!is_array($status) || !array_key_exists('code', $status)) {
            throw new GrpcException(
                'gRPC completion event carried no status; the call did not complete normally',
                GrpcStatusCode::Internal->value,
            );
        }

        /** @var array<string, mixed> $status */
        $code = $status['code'];
        if (!is_int($code)) {
            throw new GrpcException(
                sprintf('gRPC status code has unexpected type %s', get_debug_type($code)),
                GrpcStatusCode::Internal->value,
            );
        }

        $details = $status['details'] ?? '';

        return [
            'code' => $code,
            'details' => is_string($details) ? $details : (is_scalar($details) ? (string) $details : ''),
        ];
    }

    /**
     * @template T of Message
     * @param class-string<T> $responseClass
     * @param bool $requireMessage When true, an event without a message (null/absent)
     *                             is an error. A zero-length message is always valid:
     *                             it is the encoding of an all-defaults response.
     * @return T
     *
     * @throws \InvalidArgumentException if the message exceeds the configured max size
     * @throws GrpcException when a message is required but the event carries none
     */
    public static function deserialize(mixed $event, string $responseClass, bool $requireMessage = true): Message
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $message = $eventArray['message'] ?? null;

        if (!is_string($message)) {
            if ($requireMessage) {
                throw new GrpcException(
                    sprintf('gRPC call returned status OK but no response body for %s', $responseClass),
                    GrpcStatusCode::Internal->value,
                );
            }

            /** @var T $response */
            $response = new $responseClass();

            return $response;
        }

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
}
