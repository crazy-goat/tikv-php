<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

/**
 * ErrorKind enumerates all possible error variants that can be carried by
 * Errorpb\Error (region-level errors).
 *
 * Each case corresponds to a oneof field in the protobuf message
 * errorpb.Error.  Instances are detected during RegionException construction
 * and used by ErrorClassifier to determine the correct BackoffType without
 * resorting to message-string matching.
 */
enum ErrorKind: string
{
    case NotLeader = 'not_leader';
    case RegionNotFound = 'region_not_found';
    case KeyNotInRegion = 'key_not_in_region';
    case EpochNotMatch = 'epoch_not_match';
    case ServerIsBusy = 'server_is_busy';
    case StaleCommand = 'stale_command';
    case StoreNotMatch = 'store_not_match';
    case RaftEntryTooLarge = 'raft_entry_too_large';
    case MaxTimestampNotSynced = 'max_timestamp_not_synced';
    case ReadIndexNotReady = 'read_index_not_ready';
    case ProposalInMergingMode = 'proposal_in_merging_mode';
    case DataIsNotReady = 'data_is_not_ready';
    case RegionNotInitialized = 'region_not_initialized';
    case DiskFull = 'disk_full';
    case RecoveryInProgress = 'recovery_in_progress';
    case FlashbackInProgress = 'flashback_in_progress';
    case FlashbackNotPrepared = 'flashback_not_prepared';
    case IsWitness = 'is_witness';
    case MismatchPeerId = 'mismatch_peer_id';
    case BucketVersionNotMatch = 'bucket_version_not_match';
    case UndeterminedResult = 'undetermined_result';
}
