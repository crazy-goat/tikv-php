<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Cache;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use CrazyGoat\TiKV\Tests\Unit\Support\BrokenLevelLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\DebugRecordingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\DiscardingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\IntLevelLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\LevelFilteredLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\LogRecordingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\SpyRedactor;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #300 (PERF-13): the region cache built the log context — and with
 * it the `KeyRedactor::redact()` string — before every debug record, even
 * when the logger could not emit it. The redaction is now computed only
 * when the cache emits debug records at all, which is decided once at
 * construction from the logger itself: a logger that discards every
 * record (the `NullLogger` and the subclasses that keep psr/log's own
 * methods) pays nothing, and a logger that filters the debug level out (a
 * Monolog logger at `INFO`) is asked through its level API.
 *
 * A logger that may record — a `NullLogger` subclass that overrides
 * `log()` or `debug()`, a plain PSR-3 logger, a logger whose level API
 * cannot be read — keeps the records it used to get: the decision never
 * costs a record.
 */
class RegionCacheDebugLoggingTest extends TestCase
{
    protected function tearDown(): void
    {
        // The redactor is process-global: restore the default one so the
        // rest of the suite sees the standard redaction.
        KeyRedactor::setRedactor(null);
    }

    public function testGetByKeyDoesNotRedactTheKeyWhenDebugLoggingIsOff(): void
    {
        $spy = $this->installSpyRedactor();
        $cache = new RegionCache(logger: new NullLogger());
        $cache->put($this->region(42, 'k1', 'k2'));

        $spy->calls = 0;
        $hit = $cache->getByKey('k1x');

        self::assertNotNull($hit);
        self::assertSame(0, $spy->calls, 'A disabled debug logger must not pay for the redaction');
    }

    public function testGetByKeyMissDoesNotRedactTheKeyWhenDebugLoggingIsOff(): void
    {
        $spy = $this->installSpyRedactor();
        $cache = new RegionCache(logger: new NullLogger());

        self::assertNull($cache->getByKey('missing'));
        self::assertSame(0, $spy->calls, 'A disabled debug logger must not pay for the redaction');
    }

    public function testPutDoesNotRedactTheKeysWhenDebugLoggingIsOff(): void
    {
        $spy = $this->installSpyRedactor();
        $cache = new RegionCache(logger: new NullLogger());

        $cache->put($this->region(42, 'k1', 'k2'));

        self::assertSame(0, $spy->calls, 'A disabled debug logger must not pay for the redaction');
    }

    public function testNullLoggerSubclassThatKeepsPsrLogImplementationPaysNothing(): void
    {
        $spy = $this->installSpyRedactor();
        $cache = new RegionCache(logger: new DiscardingNullLogger());
        $cache->put($this->region(42, 'k1', 'k2'));

        $spy->calls = 0;
        $cache->getByKey('k1x');

        self::assertSame(0, $spy->calls, 'A logger that keeps psr/log\'s discarding methods pays nothing');
    }

    public function testNullLoggerSubclassThatOverridesLogStillReceivesTheRecords(): void
    {
        $logger = new LogRecordingNullLogger();
        $cache = new RegionCache(logger: $logger);
        $cache->put($this->region(42, 'k1', 'k2'));

        $cache->getByKey('k1x');
        $cache->getById(42);
        $cache->getByKey('missing');

        self::assertGreaterThan(0, $logger->records, 'A NullLogger subclass that records must not be silenced');
    }

    public function testNullLoggerSubclassThatOverridesDebugStillReceivesTheRecords(): void
    {
        $logger = new DebugRecordingNullLogger();
        $cache = new RegionCache(logger: $logger);
        $cache->put($this->region(42, 'k1', 'k2'));

        $cache->getByKey('k1x');
        $cache->getById(42);
        $cache->getByKey('missing');

        self::assertGreaterThan(0, $logger->records, 'A subclass that overrides debug() must not be silenced');
    }

    public function testLevelFilteredLoggerPaysNoRedaction(): void
    {
        $spy = $this->installSpyRedactor();
        $cache = new RegionCache(logger: new LevelFilteredLogger());
        $cache->put($this->region(42, 'k1', 'k2'));

        $spy->calls = 0;
        $cache->getByKey('k1x');
        $cache->getByKey('missing');

        self::assertSame(0, $spy->calls, 'A logger that filters the debug level out pays nothing');
    }

    public function testLevelFilteredLoggerReceivesNoRecord(): void
    {
        $logger = new LevelFilteredLogger();
        $cache = new RegionCache(logger: $logger);
        $cache->put($this->region(42, 'k1', 'k2'));

        $cache->getByKey('k1x');
        $cache->getById(42);
        $cache->getByKey('missing');

        self::assertSame(0, $logger->records, 'A logger that filters the debug level out is not called');
    }

    public function testMonologLoggerAboveTheDebugLevelPaysNoRedaction(): void
    {
        $spy = $this->installSpyRedactor();
        $handler = new TestHandler(Level::Info);
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));
        $cache->put($this->region(42, 'k1', 'k2'));

        $spy->calls = 0;
        $cache->getByKey('k1x');

        self::assertSame(0, $spy->calls, 'A Monolog logger at INFO must not pay for the redaction');
        self::assertSame([], $handler->getRecords(), 'A Monolog logger at INFO records no debug line');
    }

    public function testMonologLoggerWithTheIntLevelSignaturePaysNoRedaction(): void
    {
        $spy = $this->installSpyRedactor();
        $logger = new IntLevelLogger();
        $cache = new RegionCache(logger: $logger);
        $cache->put($this->region(42, 'k1', 'k2'));

        $spy->calls = 0;
        $cache->getByKey('k1x');

        self::assertSame(0, $spy->calls, 'A logger with Monolog 2\'s isHandling(int) pays nothing');
        self::assertSame(0, $logger->records);
    }

    public function testBrokenLevelApiDoesNotBreakTheClient(): void
    {
        $logger = new BrokenLevelLogger();
        $cache = new RegionCache(logger: $logger);

        $cache->put($this->region(42, 'k1', 'k2'));
        $cache->getByKey('k1x');

        self::assertGreaterThan(0, $logger->records, 'A logger that cannot be asked still receives its records');
    }

    public function testTheDecisionIsTakenOnceAtConstruction(): void
    {
        // The answer is frozen when the cache is built: a logger that is
        // reconfigured afterwards keeps the answer it was given, in both
        // directions. Configure the logger before the client is built.
        $handler = new TestHandler(Level::Info);
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));
        $cache->put($this->region(42, 'k1', 'k2'));

        $handler->setLevel(Level::Debug);
        $cache->getByKey('k1x');

        self::assertSame([], $handler->getRecords(), 'A logger reconfigured afterwards is not asked again');
    }

    public function testGetByKeyEmitsTheFullContextWhenDebugLoggingIsOn(): void
    {
        $handler = new TestHandler();
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));
        $cache->put($this->region(42, 'k1', 'k2'));

        $cache->getByKey('k1x');

        $record = $this->debugRecord($handler, 'Region cache hit');
        self::assertSame(KeyRedactor::redact('k1x'), $record->context['key']);
        self::assertSame(42, $record->context['regionId']);
    }

    public function testGetByKeyMissEmitsTheFullContextWhenDebugLoggingIsOn(): void
    {
        $handler = new TestHandler();
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));

        $cache->getByKey('missing');

        $record = $this->debugRecord($handler, 'Region cache miss');
        self::assertSame(KeyRedactor::redact('missing'), $record->context['key']);
    }

    public function testGetByIdEmitsTheFullContextWhenDebugLoggingIsOn(): void
    {
        $handler = new TestHandler();
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));
        $cache->put($this->region(42, 'k1', 'k2'));

        $cache->getById(42);

        $record = $this->debugRecord($handler, 'Region cache hit by id');
        self::assertSame(42, $record->context['regionId']);
    }

    public function testPutEmitsTheFullContextWhenDebugLoggingIsOn(): void
    {
        $handler = new TestHandler();
        $cache = new RegionCache(logger: new Logger('tikv-test', [$handler]));

        $cache->put($this->region(42, 'k1', 'k2'));

        $record = $this->debugRecord($handler, 'Region cached');
        self::assertSame(42, $record->context['regionId']);
        self::assertSame(KeyRedactor::redact('k1'), $record->context['startKey']);
        self::assertSame(KeyRedactor::redact('k2'), $record->context['endKey']);
        self::assertIsInt($record->context['ttl']);
    }

    private function installSpyRedactor(): SpyRedactor
    {
        $spy = new SpyRedactor();
        KeyRedactor::setRedactor($spy);

        return $spy;
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function debugRecord(TestHandler $handler, string $message): LogRecord
    {
        $records = array_values(array_filter(
            $handler->getRecords(),
            static fn (LogRecord $record): bool => $record->message === $message,
        ));

        self::assertCount(1, $records, sprintf('Expected exactly one "%s" debug record', $message));

        return $records[0];
    }
}
