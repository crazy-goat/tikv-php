<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use Google\Protobuf\Internal\Message;

final class GrpcResponseParser
{
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
     */
    public static function deserialize(mixed $event, string $responseClass): Message
    {
        if (is_object($event)) {
            $event = (array) $event;
        }

        /** @var array<string, mixed> $eventArray */
        $eventArray = $event;
        $message = $eventArray['message'] ?? null;

        /** @var T $response */
        $response = new $responseClass();

        if ($message !== null && $message !== '' && is_string($message)) {
            $response->mergeFromString($message);
        }

        return $response;
    }
}
