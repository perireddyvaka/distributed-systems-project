"""Replica gRPC service implementation for update replication and checkpoint reporting."""

import grpc
from concurrent import futures
from typing import Optional
from proto.generated import replication_pb2, replication_pb2_grpc, shard_pb2
from indexing.document_manager import TombstoneConflictError
from replication.replica_applier import ReplicaApplier
from shared.enums import ErrorCode, OperationType, ReplicaStatus
from shared.exceptions import SequenceGapError, StaleVersionError
from shared.models import UpdateEvent
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


class ReplicaServicer(replication_pb2_grpc.ReplicaServiceServicer):
    """Implementation of ReplicaService gRPC contract."""

    def __init__(
        self,
        shard_id: int,
        replica_id: str,
        store: ShardStore,
        applier: Optional[ReplicaApplier] = None,
    ):
        self.shard_id = shard_id
        self.replica_id = replica_id
        self.store = store
        self.applier = (
            applier
            if applier is not None
            else ReplicaApplier(shard_id=shard_id, replica_id=replica_id, store=store)
        )

    def ApplyUpdate(self, request, context):
        """Validate and apply a single replicated update event."""
        event = UpdateEvent(
            shard_id=request.shard_id,
            sequence_number=request.sequence_number,
            doc_id=request.doc_id,
            operation=OperationType(request.operation),
            version=request.version,
            payload=request.payload,
            committed_at=request.committed_at,
        )

        try:
            success, is_dup, msg = self.applier.apply_event(event)
            last_applied = self.applier.get_last_applied_sequence()
            return replication_pb2.ApplyUpdateResponse(
                success=success,
                shard_id=self.shard_id,
                applied_sequence=last_applied,
                is_duplicate=is_dup,
                error_code=ErrorCode.OK.value,
                error_message=msg,
            )
        except SequenceGapError as e:
            last_applied = self.applier.get_last_applied_sequence()
            return replication_pb2.ApplyUpdateResponse(
                success=False,
                shard_id=self.shard_id,
                applied_sequence=last_applied,
                is_duplicate=False,
                error_code=ErrorCode.SEQUENCE_GAP.value,
                error_message=str(e),
            )
        except StaleVersionError as e:
            last_applied = self.applier.get_last_applied_sequence()
            return replication_pb2.ApplyUpdateResponse(
                success=False,
                shard_id=self.shard_id,
                applied_sequence=last_applied,
                is_duplicate=False,
                error_code=ErrorCode.STALE_VERSION.value,
                error_message=str(e),
            )
        except TombstoneConflictError as e:
            last_applied = self.applier.get_last_applied_sequence()
            return replication_pb2.ApplyUpdateResponse(
                success=False,
                shard_id=self.shard_id,
                applied_sequence=last_applied,
                is_duplicate=False,
                error_code=ErrorCode.TOMBSTONE_CONFLICT.value,
                error_message=str(e),
            )
        except Exception as e:
            last_applied = self.applier.get_last_applied_sequence()
            return replication_pb2.ApplyUpdateResponse(
                success=False,
                shard_id=self.shard_id,
                applied_sequence=last_applied,
                is_duplicate=False,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def ApplyBatchUpdates(self, request, context):
        """Apply a sequence of update events in order."""
        applied_count = 0
        last_applied = self.applier.get_last_applied_sequence()

        for proto_event in request.events:
            event = UpdateEvent(
                shard_id=proto_event.shard_id,
                sequence_number=proto_event.sequence_number,
                doc_id=proto_event.doc_id,
                operation=OperationType(proto_event.operation),
                version=proto_event.version,
                payload=proto_event.payload,
                committed_at=proto_event.committed_at,
            )
            try:
                success, is_dup, _ = self.applier.apply_event(event)
                if success and not is_dup:
                    applied_count += 1
                last_applied = self.applier.get_last_applied_sequence()
            except Exception as e:
                return replication_pb2.ApplyBatchResponse(
                    success=False,
                    shard_id=self.shard_id,
                    last_applied_sequence=last_applied,
                    events_applied=applied_count,
                    error_code=ErrorCode.INTERNAL_ERROR.value,
                    error_message=str(e),
                )

        return replication_pb2.ApplyBatchResponse(
            success=True,
            shard_id=self.shard_id,
            last_applied_sequence=last_applied,
            events_applied=applied_count,
            error_code=ErrorCode.OK.value,
        )

    def GetReplicaCheckpoint(self, request, context):
        """Return the replica's durable last applied sequence and health status."""
        rep_id, last_applied, status = self.store.replica_repo.get_checkpoint(self.shard_id)
        doc_count = self.applier.doc_manager.index.total_documents
        return replication_pb2.ReplicaCheckpointResponse(
            shard_id=self.shard_id,
            replica_id=rep_id,
            last_applied_sequence=last_applied,
            status=status.value if isinstance(status, ReplicaStatus) else str(status),
            total_documents=doc_count,
        )

    def SetReplicaStatus(self, request, context):
        """Update replica synchronization status."""
        try:
            status = ReplicaStatus(request.status)
            _, last_applied, _ = self.store.replica_repo.get_checkpoint(self.shard_id)
            self.store.replica_repo.save_checkpoint(
                shard_id=self.shard_id,
                replica_id=self.replica_id,
                last_applied_sequence=last_applied,
                status=status,
            )
            return replication_pb2.SetReplicaStatusResponse(success=True, current_status=status.value)
        except Exception as e:
            return replication_pb2.SetReplicaStatusResponse(success=False, current_status=request.status)


def create_replica_server(
    shard_id: int,
    replica_id: str,
    db_path: str,
    port: int,
    max_workers: int = 5,
) -> grpc.Server:
    """Factory creating and configuring a Replica gRPC Server."""
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = ReplicaServicer(shard_id=shard_id, replica_id=replica_id, store=store)
    replication_pb2_grpc.add_ReplicaServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{port}")
    return server
