"""Unit and integration tests for primary-replica replication, sequence numbers, and checkpoints."""

import os
import tempfile
import threading
import grpc
import pytest
from concurrent import futures

from indexing.document_manager import DocumentManager, TombstoneConflictError
from proto.generated import replication_pb2, replication_pb2_grpc, shard_pb2
from replication.replica_applier import ReplicaApplier
from replication.replication_manager import ReplicationManager
from replication.sequence_manager import SequenceManager
from services.replica_server import ReplicaServicer
from shared.enums import ErrorCode, OperationType, ReplicaStatus
from shared.exceptions import SequenceGapError, StaleVersionError
from shared.models import UpdateEvent
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


def test_sequence_manager_monotonic():
    sm = SequenceManager(shard_id=0, initial_sequence=0)
    assert sm.next_sequence() == 1
    assert sm.next_sequence() == 2
    assert sm.current_sequence == 2

    # Test thread safety
    def increment_many():
        for _ in range(100):
            sm.next_sequence()

    threads = [threading.Thread(target=increment_many) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sm.current_sequence == 502


def test_replica_applier_in_order(tmp_path):
    db_path = str(tmp_path / "replica.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    event1 = UpdateEvent(
        shard_id=0,
        sequence_number=1,
        doc_id="d1",
        operation=OperationType.INSERT,
        version=1,
        payload='{"content": "distributed search"}',
    )
    success, is_dup, _ = applier.apply_event(event1)
    assert success
    assert not is_dup
    assert applier.get_last_applied_sequence() == 1
    assert applier.doc_manager.has_document("d1")
    assert applier.doc_manager.index.get_doc_frequency("search") == 1

    event2 = UpdateEvent(
        shard_id=0,
        sequence_number=2,
        doc_id="d2",
        operation=OperationType.INSERT,
        version=1,
        payload='{"content": "database systems"}',
    )
    success2, is_dup2, _ = applier.apply_event(event2)
    assert success2
    assert not is_dup2
    assert applier.get_last_applied_sequence() == 2
    assert applier.doc_manager.index.total_documents == 2


def test_replica_duplicate_event_handling(tmp_path):
    db_path = str(tmp_path / "replica_dup.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    event1 = UpdateEvent(shard_id=0, sequence_number=1, doc_id="d1", operation=OperationType.INSERT, version=1, payload='{"content": "test"}')
    applier.apply_event(event1)
    assert applier.get_last_applied_sequence() == 1

    # Deliver event 1 again
    success, is_dup, msg = applier.apply_event(event1)
    assert success
    assert is_dup  # Identified as duplicate
    assert applier.get_last_applied_sequence() == 1  # Checkpoint unchanged
    assert applier.doc_manager.index.total_documents == 1


def test_replica_sequence_gap_detection(tmp_path):
    db_path = str(tmp_path / "replica_gap.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    event1 = UpdateEvent(shard_id=0, sequence_number=1, doc_id="d1", operation=OperationType.INSERT, version=1, payload='{"content": "test"}')
    applier.apply_event(event1)

    # Deliver sequence 3 when last applied is 1 (gap: missing sequence 2!)
    event3 = UpdateEvent(shard_id=0, sequence_number=3, doc_id="d3", operation=OperationType.INSERT, version=1, payload='{"content": "test3"}')
    with pytest.raises(SequenceGapError) as exc_info:
        applier.apply_event(event3)

    assert exc_info.value.expected_seq == 2
    assert exc_info.value.received_seq == 3
    # Checkpoint must NOT advance past the gap
    assert applier.get_last_applied_sequence() == 1
    assert not applier.doc_manager.has_document("d3")


def test_replica_stale_version_rejection(tmp_path):
    db_path = str(tmp_path / "replica_stale.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    # Apply version 3
    event1 = UpdateEvent(shard_id=0, sequence_number=1, doc_id="d1", operation=OperationType.INSERT, version=3, payload='{"content": "new version 3"}')
    applier.apply_event(event1)

    # Deliver sequence 2 with stale version 2
    event2 = UpdateEvent(shard_id=0, sequence_number=2, doc_id="d1", operation=OperationType.REPLACE, version=2, payload='{"content": "old version 2"}')
    with pytest.raises(StaleVersionError):
        applier.apply_event(event2)

    # Current document remains version 3
    assert applier.doc_manager.get_document("d1").version == 3


def test_replica_tombstone_anti_resurrection(tmp_path):
    db_path = str(tmp_path / "replica_tombstone.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    # Insert version 1
    applier.apply_event(UpdateEvent(shard_id=0, sequence_number=1, doc_id="d1", operation=OperationType.INSERT, version=1, payload='{"content": "active"}'))
    # Delete at version 2 (tombstone)
    applier.apply_event(UpdateEvent(shard_id=0, sequence_number=2, doc_id="d1", operation=OperationType.DELETE, version=2, payload=''))

    assert applier.doc_manager.get_document("d1") is None
    assert applier.doc_manager.get_document("d1", include_deleted=True).is_deleted is True

    # Delayed older insert with version 1 attempts resurrection
    delayed_event = UpdateEvent(shard_id=0, sequence_number=3, doc_id="d1", operation=OperationType.INSERT, version=1, payload='{"content": "stale insert"}')
    with pytest.raises(TombstoneConflictError):
        applier.apply_event(delayed_event)

    # Tombstone is preserved
    assert applier.doc_manager.get_document("d1") is None


def test_replica_restart_preserves_checkpoint(tmp_path):
    db_path = str(tmp_path / "replica_restart.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)
    applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=store)

    for i in range(1, 6):
        applier.apply_event(
            UpdateEvent(
                shard_id=0,
                sequence_number=i,
                doc_id=f"doc_{i}",
                operation=OperationType.INSERT,
                version=1,
                payload='{"content": "persistent data"}',
            )
        )
    assert applier.get_last_applied_sequence() == 5
    db.close()

    # Restart replica
    fresh_db = DatabaseManager(db_path)
    fresh_store = ShardStore(fresh_db, shard_id=0)
    fresh_applier = ReplicaApplier(shard_id=0, replica_id="rep_0", store=fresh_store)

    assert fresh_applier.get_last_applied_sequence() == 5
    assert fresh_applier.doc_manager.index.total_documents == 5
    fresh_db.close()


def test_live_grpc_replication(tmp_path):
    """Test streaming replication over a live gRPC Replica server."""
    db_path = str(tmp_path / "live_replica.db")
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=0)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer = ReplicaServicer(shard_id=0, replica_id="rep_0", store=store)
    replication_pb2_grpc.add_ReplicaServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    replica_addr = f"127.0.0.1:{port}"
    rep_mgr = ReplicationManager(shard_id=0, replica_address=replica_addr)

    # Replicate event 1
    e1 = UpdateEvent(shard_id=0, sequence_number=1, doc_id="d1", operation=OperationType.INSERT, version=1, payload='{"content": "live replication"}')
    assert rep_mgr.replicate_event(e1)
    assert rep_mgr.get_replication_lag(primary_latest_sequence=1) == 0

    # Replicate event 2
    e2 = UpdateEvent(shard_id=0, sequence_number=2, doc_id="d2", operation=OperationType.INSERT, version=1, payload='{"content": "streaming update"}')
    assert rep_mgr.replicate_event(e2)
    assert rep_mgr.get_replication_lag(primary_latest_sequence=2) == 0

    # Simulate primary writes during replica lag
    assert rep_mgr.get_replication_lag(primary_latest_sequence=5) == 3

    rep_mgr.close()
    server.stop(grace=None)
    db.close()
