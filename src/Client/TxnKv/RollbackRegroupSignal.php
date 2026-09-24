<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

/**
 * Internal control-flow signal thrown by the rollback retry closures in
 * TwoPhaseCommitter when the re-resolved region no longer covers the whole
 * key group (a region split happened since the initial grouping, issue #505).
 *
 * It deliberately extends \RuntimeException — NOT TiKvException — so that
 * RetryExecutor::execute() does not catch and retry it; it must escape to
 * the caller, which re-groups the remaining keys against the fresh region
 * layout (capped, mirroring PESSIMISTIC_LOCK_MAX_REGROUPS from #503).
 *
 * @internal
 */
final class RollbackRegroupSignal extends \RuntimeException
{
    /**
     * @param string $groupFirstKey First key of the group that raised the
     *                              signal. With the #291 parallel fan-out
     *                              the catch site no longer knows which
     *                              group signalled from loop state, so the
     *                              group is identified by its first key
     *                              (group first keys are distinct — the
     *                              groups partition the key list).
     */
    public function __construct(
        string $message,
        public readonly string $groupFirstKey = '',
    ) {
        parent::__construct($message);
    }
}
