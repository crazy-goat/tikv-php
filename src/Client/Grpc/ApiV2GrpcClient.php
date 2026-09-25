<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use Google\Protobuf\Internal\Message;
use Grpc\Channel;

/**
 * Adds API V2 key/context translation around the shared gRPC transport.
 *
 * The application-facing clients deliberately pass user keys to their
 * components. Keeping the translation at the transport boundary means every
 * unary, streaming, and fan-out TiKV request follows the same codec without
 * duplicating key handling in dozens of request builders.
 */
final readonly class ApiV2GrpcClient implements GrpcClientInterface
{
    private ApiV2MessageTransformer $transformer;

    public function __construct(
        private GrpcClientInterface $inner,
        CodecInterface $codec,
        Mode $mode,
    ) {
        $this->transformer = new ApiV2MessageTransformer(
            $codec instanceof CodecV2 ? $codec : null,
            $mode,
        );
    }

    public function isApiV2Enabled(): bool
    {
        return $this->transformer->isEnabled();
    }

    public function messageTransformer(): ApiV2MessageTransformer
    {
        return $this->transformer;
    }

    public function transformRequest(Message $request, string $service): Message
    {
        return $this->transformer->transformRequest($request, $service);
    }

    public function transformResponse(Message $response, string $service): Message
    {
        return $this->transformer->transformResponse($response, $service);
    }

    /**
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        $response = $this->inner->call(
            $address,
            $service,
            $method,
            $this->transformer->transformRequest($request, $service),
            $responseClass,
            $timeoutMs,
        );

        $transformed = $this->transformer->transformResponse($response, $service);
        /** @var T $transformed */
        return $transformed;
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        $innerFuture = $this->inner->callAsync(
            $address,
            $service,
            $method,
            $this->transformer->transformRequest($request, $service),
            $responseClass,
            $timeoutMs,
        );

        return GrpcFuture::fromWaiter(
            fn (): Message => $this->transformer->transformResponse($innerFuture->wait(), $service),
            static function () use ($innerFuture): void {
                $innerFuture->cancel();
            },
        );
    }

    /**
     * @template T of Message
     * @param class-string<T> $responseClass
     * @return T
     */
    public function callStreaming(
        string $address,
        string $service,
        string $method,
        array $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        $transformedRequests = [];
        foreach ($requests as $request) {
            $transformedRequests[] = $this->transformer->transformRequest($request, $service);
        }

        $response = $this->inner->callStreaming(
            $address,
            $service,
            $method,
            $transformedRequests,
            $responseClass,
            $timeoutMs,
        );

        $transformed = $this->transformer->transformResponse($response, $service);
        /** @var T $transformed */
        return $transformed;
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function closeChannel(string $address): void
    {
        $this->inner->closeChannel($address);
    }

    public function getChannel(string $address): Channel
    {
        return $this->inner->getChannel($address);
    }
}
