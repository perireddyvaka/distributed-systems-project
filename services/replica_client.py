"""gRPC client for communicating with individual Replica Shard services."""

import time
from datetime import datetime
import grpc
from typing import List, Optional
from proto.generated import replication_pb2, replication_pb2_grpc, shard_pb2
from shared.enums import OperationType, ReplicaStatus
from shared.models import UpdateEvent


def _to_timestamp(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, datetime):
        return val.timestamp()
    return time.time()


class ReplicaClient:
    """Client wrapper for ReplicaService gRPC stub."""

    def __init__(self, target_address: str, timeout: float = 10.0):
        self.target_address = target_address
        self.timeout = timeout
        self.channel = grpc.insecure_channel(target_address)
        self.stub = replication_pb2_grpc.ReplicaServiceStub(self.channel)

    def apply_update(self, event: UpdateEvent) -> replication_pb2.ApplyUpdateResponse:
        """Send a single update event to the replica."""
        proto_event = shard_pb2.ProtoUpdateEvent(
            shard_id=event.shard_id,
            sequence_number=event.sequence_number,
            doc_id=event.doc_id,
            operation=event.operation.value if isinstance(event.operation, OperationType) else str(event.operation),
            version=event.version,
            payload=event.payload or "",
            committed_at=_to_timestamp(event.committed_at),
        )
        return self.stub.ApplyUpdate(proto_event, timeout=self.timeout)

    def apply_batch(self, events: List[UpdateEvent]) -> replication_pb2.ApplyBatchResponse:
        """Send a batch of update events in order to the replica."""
        proto_events = [
            shard_pb2.ProtoUpdateEvent(
                shard_id=e.shard_id,
                sequence_number=e.sequence_number,
                doc_id=e.doc_id,
                operation=e.operation.value if isinstance(e.operation, OperationType) else str(e.operation),
                version=e.version,
                payload=e.payload or "",
                committed_at=_to_timestamp(e.committed_at),
            )
            for e in events
        ]
        request = replication_pb2.ApplyBatchRequest(events=proto_events)
        return self.stub.ApplyBatchUpdates(request, timeout=self.timeout)

    def get_checkpoint(self, shard_id: int) -> replication_pb2.ReplicaCheckpointResponse:
        """Fetch replica's durable checkpoint sequence and synchronization status."""
        request = replication_pb2.ReplicaCheckpointRequest(shard_id=shard_id)
        return self.stub.GetReplicaCheckpoint(request, timeout=self.timeout)

    def set_replica_status(self, shard_id: int, status: ReplicaStatus) -> replication_pb2.SetReplicaStatusResponse:
        """Update replica's synchronization status."""
        request = replication_pb2.SetReplicaStatusRequest(
            shard_id=shard_id,
            status=status.value if isinstance(status, ReplicaStatus) else str(status),
        )
        return self.stub.SetReplicaStatus(request, timeout=self.timeout)

    def is_alive(self) -> bool:
        """Check if replica is reachable."""
        try:
            self.get_checkpoint(0)
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close gRPC channel."""
        self.channel.close()
