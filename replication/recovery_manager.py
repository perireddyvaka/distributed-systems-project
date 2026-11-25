"""Incremental Replica Recovery Manager coordinating sequence-range catch-up."""

import logging
from dataclasses import dataclass
from typing import Optional, Tuple
from services.replica_client import ReplicaClient
from services.shard_client import ShardClient
from shared.enums import ErrorCode, OperationType, ReplicaStatus
from shared.exceptions import HistoryUnavailableError
from shared.models import UpdateEvent

logger = logging.getLogger(__name__)


@dataclass
class RecoveryResult:
    """Result of an incremental recovery attempt."""
    success: bool
    shard_id: int
    replayed_events: int
    initial_checkpoint: int
    final_checkpoint: int
    primary_latest_sequence: int
    status: ReplicaStatus
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class RecoveryManager:
    """Coordinates incremental recovery by replaying only missing sequence events from Primary to Replica."""

    def __init__(
        self,
        shard_id: int,
        primary_client: ShardClient,
        replica_client: ReplicaClient,
    ):
        self.shard_id = shard_id
        self.primary_client = primary_client
        self.replica_client = replica_client

    def calculate_missing_range(self) -> Tuple[int, int, int]:
        """Calculate the missing sequence range [start_seq, end_seq] and event count.
        
        Returns:
            Tuple of:
            - start_seq: int (last_applied_sequence + 1)
            - end_seq: int (primary_latest_sequence)
            - count: int (number of missed events)
            
        Raises:
            HistoryUnavailableError: If replica checkpoint is older than the primary's oldest available log entry.
        """
        # 1. Retrieve replica checkpoint
        checkpoint_resp = self.replica_client.get_checkpoint(self.shard_id)
        replica_checkpoint = checkpoint_resp.last_applied_sequence

        # 2. Retrieve primary latest sequence and earliest available log sequence
        seq_resp = self.primary_client.get_latest_sequence()
        primary_latest = seq_resp.latest_sequence
        primary_oldest = seq_resp.oldest_sequence

        if replica_checkpoint >= primary_latest:
            return replica_checkpoint + 1, primary_latest, 0

        # 3. Check for history availability (log continuity invariant)
        if primary_oldest > 0 and replica_checkpoint < primary_oldest - 1:
            raise HistoryUnavailableError(
                shard_id=self.shard_id,
                requested_seq=replica_checkpoint + 1,
                oldest_available_seq=primary_oldest,
            )

        start_seq = replica_checkpoint + 1
        count = primary_latest - replica_checkpoint
        return start_seq, primary_latest, count

    def recover(
        self,
        batch_size: int = 100,
        max_events_to_replay: Optional[int] = None,
    ) -> RecoveryResult:
        """Execute incremental recovery replay.
        
        Args:
            batch_size: Maximum events to fetch per batch from primary.
            max_events_to_replay: Optional limit on replayed events (useful to simulate interruption).
            
        Returns:
            RecoveryResult reporting status, counts, and final checkpoints.
        """
        checkpoint_resp = self.replica_client.get_checkpoint(self.shard_id)
        initial_checkpoint = checkpoint_resp.last_applied_sequence

        # Mark replica status as RECOVERING
        self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.RECOVERING)

        try:
            start_seq, primary_latest, missing_count = self.calculate_missing_range()
        except HistoryUnavailableError as e:
            self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.HISTORY_UNAVAILABLE)
            return RecoveryResult(
                success=False,
                shard_id=self.shard_id,
                replayed_events=0,
                initial_checkpoint=initial_checkpoint,
                final_checkpoint=initial_checkpoint,
                primary_latest_sequence=0,
                status=ReplicaStatus.HISTORY_UNAVAILABLE,
                error_code=ErrorCode.HISTORY_UNAVAILABLE.value,
                error_message=str(e),
            )
        except Exception as e:
            self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.FAILED)
            return RecoveryResult(
                success=False,
                shard_id=self.shard_id,
                replayed_events=0,
                initial_checkpoint=initial_checkpoint,
                final_checkpoint=initial_checkpoint,
                primary_latest_sequence=0,
                status=ReplicaStatus.FAILED,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

        if missing_count == 0:
            self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.SYNCHRONIZED)
            return RecoveryResult(
                success=True,
                shard_id=self.shard_id,
                replayed_events=0,
                initial_checkpoint=initial_checkpoint,
                final_checkpoint=initial_checkpoint,
                primary_latest_sequence=primary_latest,
                status=ReplicaStatus.SYNCHRONIZED,
            )

        replayed_count = 0
        current_seq = initial_checkpoint

        while current_seq < primary_latest:
            if max_events_to_replay is not None and replayed_count >= max_events_to_replay:
                # Interrupted midway
                logger.info(
                    f"Recovery interrupted on shard {self.shard_id} at sequence {current_seq} after {replayed_count} events."
                )
                break

            limit = batch_size
            if max_events_to_replay is not None:
                limit = min(limit, max_events_to_replay - replayed_count)

            # Fetch batch of missing updates from primary
            updates_resp = self.primary_client.get_updates_after(
                since_sequence=current_seq,
                max_events=limit,
            )

            if not updates_resp.events:
                # No more events returned by primary
                break

            # Convert to internal UpdateEvent models
            batch_events = []
            for pe in updates_resp.events:
                batch_events.append(
                    UpdateEvent(
                        shard_id=pe.shard_id,
                        sequence_number=pe.sequence_number,
                        doc_id=pe.doc_id,
                        operation=OperationType(pe.operation),
                        version=pe.version,
                        payload=pe.payload,
                        committed_at=pe.committed_at,
                    )
                )

            # Replay batch on replica
            batch_resp = self.replica_client.apply_batch(batch_events)
            if not batch_resp.success:
                self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.FAILED)
                return RecoveryResult(
                    success=False,
                    shard_id=self.shard_id,
                    replayed_events=replayed_count,
                    initial_checkpoint=initial_checkpoint,
                    final_checkpoint=batch_resp.last_applied_sequence,
                    primary_latest_sequence=primary_latest,
                    status=ReplicaStatus.FAILED,
                    error_code=batch_resp.error_code,
                    error_message=batch_resp.error_message,
                )

            replayed_count += batch_resp.events_applied
            current_seq = batch_resp.last_applied_sequence

        # Verify final synchronization status
        final_checkpoint_resp = self.replica_client.get_checkpoint(self.shard_id)
        final_checkpoint = final_checkpoint_resp.last_applied_sequence

        if final_checkpoint >= primary_latest:
            self.replica_client.set_replica_status(self.shard_id, ReplicaStatus.SYNCHRONIZED)
            final_status = ReplicaStatus.SYNCHRONIZED
        else:
            final_status = ReplicaStatus.RECOVERING

        return RecoveryResult(
            success=(final_status == ReplicaStatus.SYNCHRONIZED),
            shard_id=self.shard_id,
            replayed_events=replayed_count,
            initial_checkpoint=initial_checkpoint,
            final_checkpoint=final_checkpoint,
            primary_latest_sequence=primary_latest,
            status=final_status,
        )
