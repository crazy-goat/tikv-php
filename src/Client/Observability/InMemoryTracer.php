<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * In-memory trace-event recorder for tests and diagnostics.
 *
 * Retains every event in order so assertions can be made on the exact
 * sequence the client emits. Like {@see InMemoryMetrics} it is not intended
 * for production: the buffer grows without bound and is not shared across
 * PHP-FPM workers.
 */
final class InMemoryTracer implements TraceEventInterface
{
    /**
     * @var list<array{category: string, event: string, fields: array<string, string|int|float|bool|null>}>
     */
    private array $events = [];

    public function traceEvent(string $category, string $event, array $fields = []): void
    {
        $this->events[] = [
            'category' => $category,
            'event' => $event,
            'fields' => $fields,
        ];
    }

    /**
     * @return list<array{category: string, event: string, fields: array<string, string|int|float|bool|null>}>
     */
    public function getEvents(): array
    {
        return $this->events;
    }

    /**
     * Event names in emission order, optionally restricted to one category.
     *
     * @return list<string>
     */
    public function eventNames(?string $category = null): array
    {
        $names = [];
        foreach ($this->events as $event) {
            if ($category === null || $event['category'] === $category) {
                $names[] = $event['event'];
            }
        }

        return $names;
    }

    public function reset(): void
    {
        $this->events = [];
    }
}
