<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

trait GrpcExtensionGate
{
    /**
     * Whether this class already emitted the "transport tests skipped"
     * warning. One warning per class is enough to name it in the run summary;
     * one per test would drown the output of a large transport class.
     */
    private static bool $grpcSkipWarningEmitted = false;

    private function requireGrpcExtension(): void
    {
        if (class_exists(\Grpc\Timeval::class)) {
            return;
        }

        if (getenv('REQUIRE_GRPC_EXTENSION') === '1') {
            self::fail(
                'The gRPC extension is required in this environment but is not loaded. '
                . 'Install ext-grpc >= 1.57 with "pecl install grpc" (or the platform package) '
                . 'and rerun the suite.',
            );
        }

        $message = sprintf(
            'The transport tests in %s did not run because the gRPC extension is not loaded. '
            . 'Install ext-grpc >= 1.57 with "pecl install grpc" and rerun "composer test:grpc".',
            static::class,
        );

        if (!self::$grpcSkipWarningEmitted) {
            self::$grpcSkipWarningEmitted = true;

            // A warning reaches PHPUnit's end-of-run summary, which
            // `displayDetailsOnTestsThatTriggerWarnings` prints even when
            // TestDox output is enabled. A plain skip only shows an "S".
            trigger_error($message, E_USER_WARNING);
        }

        $this->markTestSkipped($message);
    }
}
