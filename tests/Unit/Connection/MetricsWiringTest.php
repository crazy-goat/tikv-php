<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Guard for the #320 (SEC-17) wiring: the backend a caller passes through
 * `options['metrics']` must reach the client and the transport the factory
 * builds, so the counters the operator configured are actually incremented.
 *
 * The production fix shipped with #368. The earlier tests only pinned that
 * `getMetrics()` returns the injected instance — which stays true even when
 * the client-side collaborators and the transport keep a `NoOpMetrics`.
 * These tests drive one operation through each factory and assert the events
 * themselves, so dropping `metrics: $bundle->metrics` from either `create()`
 * fails here.
 *
 * `allowInsecure: false` makes the first PD call fail closed in
 * `getChannel()` right after `rpcStarted()`, before any channel is created.
 * The whole path therefore runs in the extension-free Unit suite.
 */
final class MetricsWiringTest extends TestCase
{
    public function testRawKvFactoryMetricsReceiveRegionCacheAndRpcEvents(): void
    {
        $metrics = new InMemoryMetrics();

        $client = RawKvClient::create(['127.0.0.1:2379'], options: [
            'allowInsecure' => false,
            'metrics' => $metrics,
        ]);

        try {
            $client->get('key');
            self::fail('Expected the plaintext channel to be refused');
        } catch (InvalidStateException) {
            // The refused channel is the injection point for the PD RPC.
        } finally {
            $client->close();
        }

        self::assertSame($metrics, $client->getMetrics());

        // Client-side counter: the region resolver was built with the backend.
        self::assertSame(1, $metrics->getCacheMisses('region_resolution'));

        // Transport counter: the factory-built GrpcClient carries it too.
        self::assertSame(1, $metrics->getRpcStarted('pdpb.PD/GetRegion'));
        self::assertSame(1, $metrics->getRpcFailed('pdpb.PD/GetRegion'));
        self::assertSame(0, $metrics->getRpcSucceeded('pdpb.PD/GetRegion'));
    }

    public function testTxnKvFactoryMetricsReceiveRpcEvents(): void
    {
        $metrics = new InMemoryMetrics();

        $client = TxnKvClient::create(['127.0.0.1:2379'], options: [
            'allowInsecure' => false,
            'metrics' => $metrics,
        ]);

        try {
            $client->begin();
            self::fail('Expected the plaintext channel to be refused');
        } catch (InvalidStateException) {
            // begin() fetches its start timestamp from PD, so the refused
            // TSO channel is the injection point.
        } finally {
            $client->close();
        }

        self::assertSame($metrics, $client->getMetrics());
        self::assertSame(1, $metrics->getRpcStarted('pdpb.PD/Tso'));
        self::assertSame(1, $metrics->getRpcFailed('pdpb.PD/Tso'));
        self::assertSame(0, $metrics->getRpcSucceeded('pdpb.PD/Tso'));
    }
}
