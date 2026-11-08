<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

/**
 * The operation types slow logging can measure (issue #701).
 *
 * This enum is the single source of truth for the operation-type vocabulary:
 * {@see SlowLogConfig::getThreshold()} matches over its cases with no
 * `default` arm, so adding a case fails PHPStan until it gets a threshold,
 * and a call site cannot pass a string the configuration does not know.
 *
 * The backed values are the `operation` context field of the emitted
 * warning and are part of the documented output, so they must not be
 * renamed without updating the documentation.
 */
enum SlowLogOperation: string
{
    case Read = 'read';
    case Write = 'write';
    case BatchRead = 'batch_read';
    case BatchWrite = 'batch_write';
    case Scan = 'scan';
    case DeleteRange = 'delete_range';
    case Checksum = 'checksum';
}
