"""Replication Manager coordinating primary-to-replica streaming and lag tracking."""

import logging
from typing import Optional
from services.replica_client import ReplicaClient
from shared.enums import ReplicaStatus
from shared.models import UpdateEvent

logger = logging.getLogger(__name__)


class ReplicationManager:
    """Manages active streaming replication and lag tracking from a Primary shard to its Replica."""

    def __init__(self, shard_id: int, replica_address: Optional[str] = None):
        self.shard_id = shard_id
        self.replica_address = replica_address
        self._replica_client: Optional[ReplicaClient] = (
            ReplicaClient(replica_address) if replica_address else None
        )
        self._status = ReplicaStatus.DISCONNECTED

    def set_replica_address(self, replica_address: str) -> None:
        """Update or change replica target endpoint."""
        if self._replica_client:
            self._replica_client.close()
        self.replica_address = replica_address
        self._replica_client = ReplicaClient(replica_address)

    def replicate_event(self, event: UpdateEvent) -> bool:
        """Stream an update event to the replica.
        
        If the replica is unreachable or disconnected, the primary continues
        operating uninterrupted and retains the event in update_log for recovery.
        """
        if not self._replica_client:
            return False

        try:
            resp = self._replica_client.apply_update(event)
            if resp.success:
                self._status = ReplicaStatus.SYNCHRONIZED
                return True
            else:
                logger.warning(
                    f"Replica for shard {self.shard_id} rejected event {event.sequence_number}: {resp.error_code} - {resp.error_message}"
                )
                return False
        except Exception as e:
            logger.info(
                f"Replica for shard {self.shard_id} at {self.replica_address} is currently unavailable: {e}"
            )
            self._status = ReplicaStatus.DISCONNECTED
            return False

    def get_replication_lag(self, primary_latest_sequence: int) -> int:
        """Compute current replication lag = primary_latest_seq - replica_applied_seq."""
        if not self._replica_client:
            return primary_latest_sequence

        try:
            resp = self._replica_client.get_checkpoint(self.shard_id)
            applied_seq = resp.last_applied_sequence
            self._status = ReplicaStatus(resp.status)
            return max(0, primary_latest_sequence - applied_seq)
        except Exception:
            self._status = ReplicaStatus.DISCONNECTED
            return primary_latest_sequence

    @property
    def status(self) -> ReplicaStatus:
        """Get the current replication status."""
        return self._status

    def close(self) -> None:
        """Close connection to replica."""
        if self._replica_client:
            self._replica_client.close()
            self._replica_client = None
