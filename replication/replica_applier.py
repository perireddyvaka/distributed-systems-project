"""Replica event applier enforcing sequence ordering, deduplication, and version protection."""

from typing import Optional, Tuple
from indexing.document_manager import DocumentManager, TombstoneConflictError
from shared.enums import OperationType, ReplicaStatus
from shared.exceptions import SequenceGapError, StaleVersionError
from shared.models import UpdateEvent
from storage.shard_store import ShardStore


class ReplicaApplier:
    """Validates and atomically applies replicated events to a replica shard."""

    def __init__(
        self,
        shard_id: int,
        replica_id: str,
        store: ShardStore,
        doc_manager: Optional[DocumentManager] = None,
    ):
        self.shard_id = shard_id
        self.replica_id = replica_id
        self.store = store
        self.doc_manager = doc_manager if doc_manager is not None else store.reconstruct_in_memory_state()

    def get_last_applied_sequence(self) -> int:
        """Get the last successfully applied checkpoint sequence."""
        _, last_applied, _ = self.store.replica_repo.get_checkpoint(self.shard_id)
        return last_applied

    def apply_event(self, event: UpdateEvent) -> Tuple[bool, bool, str]:
        """Validate and apply an incoming UpdateEvent.
        
        Returns:
            Tuple of:
            - is_success: bool
            - is_duplicate: bool
            - error_message: str
            
        Raises:
            SequenceGapError: If event.sequence_number > last_applied + 1.
            StaleVersionError: If event.version <= existing document version.
            TombstoneConflictError: If event attempts to resurrect a tombstoned document with version <= tombstone.
        """
        _, last_applied, _ = self.store.replica_repo.get_checkpoint(self.shard_id)

        # 1. Duplicate detection: seq <= last_applied
        if event.sequence_number <= last_applied:
            return True, True, "Duplicate event acknowledged without applying"

        # 2. Sequence gap detection: seq > last_applied + 1
        if event.sequence_number > last_applied + 1:
            raise SequenceGapError(
                shard_id=self.shard_id,
                expected_seq=last_applied + 1,
                received_seq=event.sequence_number,
            )

        # 3. Document version and tombstone validation
        existing = self.doc_manager.get_document(event.doc_id, include_deleted=True)
        if existing is not None:
            if existing.is_deleted:
                # Existing record is a tombstone
                if event.operation != OperationType.DELETE and event.version <= existing.version:
                    raise TombstoneConflictError(
                        doc_id=event.doc_id,
                        tombstone_version=existing.version,
                        incoming_version=event.version,
                    )
            else:
                # Active document exists
                if event.version <= existing.version:
                    raise StaleVersionError(
                        doc_id=event.doc_id,
                        current_version=existing.version,
                        incoming_version=event.version,
                    )

        # 4. Atomic commit to SQLite (data + postings + checkpoint) and in-memory index
        self.store.execute_replica_apply(event, self.replica_id, self.doc_manager)
        return True, False, ""
