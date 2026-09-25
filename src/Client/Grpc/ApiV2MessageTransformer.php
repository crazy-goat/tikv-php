<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;

/**
 * Translates protobuf key fields and request context for API V2.
 *
 * TiKV's V2 contract is expressed in the generated request/response messages,
 * so walking their generated accessors keeps all existing public client code on
 * user keys while covering unary, streaming, and async calls uniformly.
 */
final readonly class ApiV2MessageTransformer
{
    private const MAX_DEPTH = 16;

    /** Fields whose empty value is an unbounded range boundary. */
    private const UNBOUNDED_FIELDS = [
        'start_key',
        'end_key',
        'compacted_start_key',
        'compacted_end_key',
        'split_key',
    ];

    /** Response boundaries are region keys and therefore MCE-encoded. */
    private const REGION_BOUNDARY_FIELDS = [
        'start_key',
        'end_key',
        'compacted_start_key',
        'compacted_end_key',
    ];

    public function __construct(
        private ?CodecV2 $codec,
        private Mode $mode,
    ) {
    }

    public function isEnabled(): bool
    {
        return $this->codec instanceof CodecV2;
    }

    /**
     * @template T of Message
     * @param T $request
     * @return T
     */
    public function transformRequest(Message $request, string $service): Message
    {
        if (!$this->codec instanceof CodecV2 || !$this->shouldTransform($service)) {
            return $request;
        }

        $copy = clone $request;
        $this->walk($copy, true);

        return $copy;
    }

    /**
     * @template T of Message
     * @param T $response
     * @return T
     */
    public function transformResponse(Message $response, string $service): Message
    {
        if (!$this->codec instanceof CodecV2 || !$this->shouldTransform($service)) {
            return $response;
        }

        $copy = clone $response;
        $this->walk($copy, false);

        return $copy;
    }

    private function shouldTransform(string $service): bool
    {
        return !str_starts_with($service, 'pdpb.')
            && !str_starts_with($service, 'keyspacepb.');
    }

    private function walk(Message $message, bool $request, int $depth = 0): void
    {
        $codec = $this->codec;
        if (!$codec instanceof CodecV2 || $depth > self::MAX_DEPTH) {
            return;
        }

        if ($request) {
            $this->setContext($message);
            $this->forceDefaultColumnFamily($message);
            $this->transformRequestRange($message);
        }

        $this->walkGeneratedAccessors($message, $request, $depth);
    }

    private function transformRequestRange(Message $message): void
    {
        $codec = $this->codec;
        if (!$codec instanceof CodecV2) {
            return;
        }

        $hasKeyRange = method_exists($message, 'getStartKey')
            && method_exists($message, 'getEndKey')
            && method_exists($message, 'setStartKey')
            && method_exists($message, 'setEndKey');
        $hasPlainRange = method_exists($message, 'getStart')
            && method_exists($message, 'getEnd')
            && method_exists($message, 'setStart')
            && method_exists($message, 'setEnd');
        if (!$hasKeyRange && !$hasPlainRange) {
            return;
        }

        $startGetter = $hasKeyRange ? 'getStartKey' : 'getStart';
        $endGetter = $hasKeyRange ? 'getEndKey' : 'getEnd';
        $start = $this->invoke($message, $startGetter);
        $end = $this->invoke($message, $endGetter);
        if (!is_string($start) || !is_string($end)) {
            return;
        }

        $reverse = method_exists($message, 'getReverse')
            && (bool) $this->invoke($message, 'getReverse');
        [$encodedStart, $encodedEnd] = $codec->encodeRequestRange($start, $end, $reverse);

        $startSetter = $hasKeyRange ? 'setStartKey' : 'setStart';
        $endSetter = $hasKeyRange ? 'setEndKey' : 'setEnd';
        $this->invoke($message, $startSetter, $encodedStart);
        $this->invoke($message, $endSetter, $encodedEnd);
    }

    private function walkGeneratedAccessors(Message $message, bool $request, int $depth): void
    {
        if ($depth > self::MAX_DEPTH) {
            return;
        }

        foreach (get_class_methods($message) as $method) {
            if (preg_match('/^get([A-Z].*)$/', $method, $matches) !== 1) {
                continue;
            }

            $setter = 'set' . $matches[1];
            if (!method_exists($message, $setter)) {
                continue;
            }

            $value = $this->invoke($message, $method);
            $field = $this->snakeCase($matches[1]);
            if ($request && $this->isRequestRangeField($message, $field)) {
                continue;
            }
            if ($value instanceof RepeatedField) {
                $items = [];
                $regionEncodedKeys = !$request
                    && $field === 'keys'
                    && str_ends_with($message::class, 'BucketVersionNotMatch');
                foreach ($value as $item) {
                    if ($this->isKeyField($field) && is_string($item)) {
                        $items[] = $regionEncodedKeys
                            ? $this->decodeRegionKey($item)
                            : $this->transformKey($item, $field, $request);
                        continue;
                    }
                    if ($item instanceof Message) {
                        $item = clone $item;
                        $this->walk($item, $request, $depth + 1);
                    }
                    $items[] = $item;
                }
                $this->invoke($message, $setter, $items);
                continue;
            }

            if ($value instanceof Message) {
                $nested = clone $value;
                $this->walk($nested, $request, $depth + 1);
                $this->invoke($message, $setter, $nested);
                continue;
            }

            if ($this->isKeyField($field) && is_string($value)) {
                $this->invoke($message, $setter, $this->transformKey($value, $field, $request));
            }
        }
    }

    private function decodeRegionKey(string $key): string
    {
        if ($key === '') {
            return '';
        }
        $codec = $this->codec;
        if (!$codec instanceof CodecV2) {
            return $key;
        }

        return $codec->decodeRegionKey($key);
    }

    private function invoke(Message $message, string $method, mixed ...$arguments): mixed
    {
        /** @var callable $callback */
        $callback = [$message, $method];

        return $callback(...$arguments);
    }

    private function snakeCase(string $value): string
    {
        return strtolower((string) preg_replace('/(?<!^)[A-Z]/', '_$0', $value));
    }

    private function transformKey(string $key, string $field, bool $request): string
    {
        if (!$request && $key === '') {
            return '';
        }
        if (
            $request
            && $key === ''
            && (
                in_array($field, self::UNBOUNDED_FIELDS, true)
                || $field === 'primary_key'
            )
        ) {
            return '';
        }

        $codec = $this->codec;
        if (!$codec instanceof CodecV2) {
            return $key;
        }

        if ($request) {
            return $codec->encodeKey($key);
        }
        if ($field === 'end' && $codec->isUpperBound($key)) {
            return '';
        }

        return in_array($field, self::REGION_BOUNDARY_FIELDS, true)
            ? $codec->decodeRegionKey($key)
            : $codec->decodeKey($key);
    }

    private function isRequestRangeField(Message $message, string $field): bool
    {
        if ($field === 'start_key' || $field === 'end_key') {
            return method_exists($message, 'getStartKey')
                && method_exists($message, 'getEndKey')
                && method_exists($message, 'setStartKey')
                && method_exists($message, 'setEndKey');
        }
        if ($field === 'start' || $field === 'end') {
            return method_exists($message, 'getStart')
                && method_exists($message, 'getEnd')
                && method_exists($message, 'setStart')
                && method_exists($message, 'setEnd');
        }

        return false;
    }

    private function isKeyField(string $name): bool
    {
        return in_array($name, ['key', 'keys', 'primary', 'primary_lock', 'secondaries', 'start', 'end'], true)
            || str_ends_with($name, '_key')
            || str_ends_with($name, '_keys');
    }

    private function setContext(Message $message): void
    {
        $codec = $this->codec;
        if (!$codec instanceof CodecV2) {
            return;
        }
        if (!method_exists($message, 'getContext') || !method_exists($message, 'setContext')) {
            return;
        }

        $context = $message->getContext();
        if (!$context instanceof Message) {
            $context = new \CrazyGoat\Proto\Kvrpcpb\Context();
        } else {
            $context = clone $context;
        }

        if (
            !method_exists($context, 'setApiVersion')
            || !method_exists($context, 'setKeyspaceName')
            || !method_exists($context, 'setKeyspaceId')
        ) {
            return;
        }

        $context->setApiVersion($codec->getApiVersion());
        $context->setKeyspaceName($codec->getKeyspaceName());
        $context->setKeyspaceId($codec->getKeyspaceId());
        $message->setContext($context);
    }

    private function forceDefaultColumnFamily(Message $message): void
    {
        if ($this->mode !== Mode::Raw) {
            return;
        }
        if (method_exists($message, 'getCf') && method_exists($message, 'setCf')) {
            // API V2 treats an explicitly populated cf as deprecated. An
            // empty proto3 scalar is omitted on the wire, which selects the
            // server's required default column family.
            $message->setCf('');
        }
        if (method_exists($message, 'getCfName') && method_exists($message, 'setCfName')) {
            $message->setCfName('');
        }
    }
}
