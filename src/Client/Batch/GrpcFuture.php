<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcResponseParser;
use Google\Protobuf\Internal\Message;
use Grpc\Call;

final class GrpcFuture
{
    private bool $completed = false;
    private ?Message $result = null;
    private ?GrpcException $error = null;

    /** @param class-string<Message> $responseClass */
    public function __construct(
        private readonly Call $call,
        /** @var class-string<Message> */
        private readonly string $responseClass,
    ) {
    }

    public function wait(): Message
    {
        if ($this->completed) {
            if ($this->error instanceof \CrazyGoat\TiKV\Client\Exception\GrpcException) {
                throw $this->error;
            }
            if (!$this->result instanceof \Google\Protobuf\Internal\Message) {
                throw new GrpcException('Unexpected null result', \Grpc\STATUS_INTERNAL);
            }
            return $this->result;
        }

        $event = $this->call->startBatch([
            \Grpc\OP_RECV_INITIAL_METADATA => true,
            \Grpc\OP_RECV_MESSAGE => true,
            \Grpc\OP_RECV_STATUS_ON_CLIENT => true,
        ]);

        $status = GrpcResponseParser::extractStatus($event);

        if ($status['code'] !== \Grpc\STATUS_OK) {
            $this->error = new GrpcException($status['details'], $status['code']);
            $this->completed = true;
            throw $this->error;
        }

        $this->result = GrpcResponseParser::deserialize($event, $this->responseClass);
        $this->completed = true;

        return $this->result;
    }
}
