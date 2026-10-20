<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use CrazyGoat\TiKV\Client\Observability\DebugLogging;
use CrazyGoat\TiKV\Tests\Unit\Support\BrokenLevelLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\DebugRecordingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\DiscardingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\IntLevelLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\LevelFilteredLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\LogRecordingNullLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\PlainLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\VagueLevelLogger;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #300 (PERF-13): the client asks the logger once whether it can
 * emit a debug record at all, so a logger that discards them never pays
 * for a log context.
 */
class DebugLoggingTest extends TestCase
{
    public function testNullLoggerDiscardsEverything(): void
    {
        self::assertFalse(DebugLogging::handlesDebug(new NullLogger()));
    }

    public function testNullLoggerSubclassThatKeepsPsrLogImplementationDiscardsEverything(): void
    {
        self::assertFalse(DebugLogging::handlesDebug(new DiscardingNullLogger()));
    }

    public function testNullLoggerSubclassThatOverridesLogHandlesDebug(): void
    {
        self::assertTrue(DebugLogging::handlesDebug(new LogRecordingNullLogger()));
    }

    public function testNullLoggerSubclassThatOverridesDebugHandlesDebug(): void
    {
        self::assertTrue(DebugLogging::handlesDebug(new DebugRecordingNullLogger()));
    }

    public function testPlainPsrLoggerHandlesDebug(): void
    {
        self::assertTrue(DebugLogging::handlesDebug(new PlainLogger()));
    }

    public function testLevelFilteredLoggerDoesNotHandleDebug(): void
    {
        self::assertFalse(DebugLogging::handlesDebug(new LevelFilteredLogger()));
    }

    public function testLoggerWithTheIntLevelSignatureDoesNotHandleDebug(): void
    {
        self::assertFalse(DebugLogging::handlesDebug(new IntLevelLogger()));
    }

    public function testVagueLevelApiFallsBackToTheClass(): void
    {
        self::assertTrue(DebugLogging::handlesDebug(new VagueLevelLogger()));
    }

    public function testBrokenLevelApiFallsBackToTheClass(): void
    {
        self::assertTrue(DebugLogging::handlesDebug(new BrokenLevelLogger()));
    }

    public function testMonologLoggerAtDebugHandlesDebug(): void
    {
        $logger = new Logger('tikv-test', [new TestHandler(Level::Debug)]);

        self::assertTrue(DebugLogging::handlesDebug($logger));
    }

    public function testMonologLoggerAtInfoDoesNotHandleDebug(): void
    {
        $logger = new Logger('tikv-test', [new TestHandler(Level::Info)]);

        self::assertFalse(DebugLogging::handlesDebug($logger));
    }
}
