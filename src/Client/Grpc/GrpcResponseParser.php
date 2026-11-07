<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use Google\Protobuf\Internal\Message;

final class GrpcResponseParser
{
    /**
     * Post-allocation size assertion for a decoded response, in bytes.
     *
     * This is **not** the control that bounds response memory. The effective
     * receive limit is the `grpc.max_receive_message_length` channel argument
     * set by {@see GrpcClient::channelArgs()} (configurable through
     * `options['grpc']['maxReceiveMessageBytes']`, default 64 MB): grpc-core
     * rejects an oversized frame before its payload is materialised as a PHP
     * string. The check below runs only after that string exists, so it cannot
     * prevent the allocation it appears to guard against (SEC-16, issue #319).
     *
     * It is kept as defence in depth: a caller can set it tighter than the
     * transport limit to skip the second, larger allocation performed by
     * protobuf decoding. Set to 0 or negative to disable it (default).
     */
    private static int $maxMessageSize = 0;

    /**
     * Set the post-allocation response-size assertion in bytes.
     *
     * The value is a defence-in-depth bound checked after the gRPC event has
     * already delivered the message as a PHP string; it does not prevent that
     * allocation. Use `options['grpc']['maxReceiveMessageBytes']` to bound
     * the payload in grpc-core instead (SEC-16, issue #319).
     *
     * Messages exceeding this limit throw an InvalidArgumentException. Set to
     * 0 or negative to disable the assertion (default).
     */
    public static function setMaxMessageSize(int $bytes): void
    {
        self::$maxMessageSize = $bytes;
    }

    /**
     * Current post-allocation response-size assertion in bytes (0 = disabled).
     */
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
     * @throws \InvalidArgumentException if the message exceeds the configured
     *                                   post-allocation assertion. This cannot
     *                                   prevent the gRPC event from allocating
     *                                   the payload string; the transport-level
     *                                   bound is `grpc.max_receive_message_length`
     *                                   (SEC-16, issue #319).
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

        // Defence in depth, not the transport bound: the string is already
        // allocated here. grpc-core enforces grpc.max_receive_message_length
        // before this method is reached (SEC-16, issue #319).
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
