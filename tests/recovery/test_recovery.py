"""Integration tests for incremental replica recovery, range replay, interruption, and history unavailability."""

import os
import tempfile
import grpc
import pytest
from concurrent import futures

from proto.generated import replication_pb2, replication_pb2_grpc, shard_pb2, shard_pb2_grpc
from replication.recovery_manager import RecoveryManager
from services.replica_client import ReplicaClient
from services.replica_server import ReplicaServicer
from services.shard_client import ShardClient
from services.shard_server import ShardServicer
from shared.enums import ErrorCode, OperationType, ReplicaStatus, ShardRole
from shared.models import UpdateEvent
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


@pytest.fixture
def primary_and_replica():
    """Spin up live Primary and Replica gRPC servers on dynamic ports with independent SQLite stores."""
    # Create temp database files
    fd_p, path_p = tempfile.mkstemp(suffix="_primary.db")
    fd_r, path_r = tempfile.mkstemp(suffix="_replica.db")
    os.close(fd_p)
    os.close(fd_r)

    db_p = DatabaseManager(path_p)
    db_p.initialize_schema()
    store_p = ShardStore(db_p, shard_id=0)

    db_r = DatabaseManager(path_r)
    db_r.initialize_schema()
    store_r = ShardStore(db_r, shard_id=0)

    # Start Primary Server
    server_p = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer_p = ShardServicer(shard_id=0, store=store_p, role=ShardRole.PRIMARY)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer_p, server_p)
    port_p = server_p.add_insecure_port("127.0.0.1:0")
    server_p.start()

    # Start Replica Server
    server_r = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer_r = ReplicaServicer(shard_id=0, replica_id="replica_0", store=store_r)
    replication_pb2_grpc.add_ReplicaServiceServicer_to_server(servicer_r, server_r)
    port_r = server_r.add_insecure_port("127.0.0.1:0")
    server_r.start()

    client_p = ShardClient(f"127.0.0.1:{port_p}")
    client_r = ReplicaClient(f"127.0.0.1:{port_r}")

    yield (client_p, client_r), (store_p, store_r), (path_p, path_r)

    # Teardown
    client_p.close()
    client_r.close()
    server_p.stop(grace=None)
    server_r.stop(grace=None)
    db_p.close()
    db_r.close()
    for p in [path_p, path_r]:
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(p + ext):
                os.remove(p + ext)


def test_incremental_recovery_basic(primary_and_replica):
    (client_p, client_r), (store_p, store_r), _ = primary_and_replica

    # 1. Insert 5 initial documents on primary and replicate to replica
    for i in range(1, 6):
        resp = client_p.insert_document(doc_id=f"doc_{i}", content=f"initial content {i}")
        assert resp.success
        event = UpdateEvent(
            shard_id=0,
            sequence_number=resp.sequence_number,
            doc_id=f"doc_{i}",
            operation=OperationType.INSERT,
            version=1,
            payload=f'{{"content": "initial content {i}"}}',
        )
        rep_resp = client_r.apply_update(event)
        assert rep_resp.success

    # Verify both at sequence 5
    assert client_p.get_latest_sequence().latest_sequence == 5
    assert client_r.get_checkpoint(0).last_applied_sequence == 5

    # 2. Simulate replica disconnection: perform 15 more writes on primary (sequences 6 to 20)
    for i in range(6, 21):
        resp = client_p.insert_document(doc_id=f"doc_{i}", content=f"missed content {i}")
        assert resp.success
        assert resp.sequence_number == i

    assert client_p.get_latest_sequence().latest_sequence == 20
    assert client_r.get_checkpoint(0).last_applied_sequence == 5

    # 3. Trigger incremental recovery
    recovery_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    start_seq, end_seq, missing_count = recovery_mgr.calculate_missing_range()
    assert start_seq == 6
    assert end_seq == 20
    assert missing_count == 15

    # Execute recovery replay
    result = recovery_mgr.recover(batch_size=5)
    assert result.success
    assert result.replayed_events == 15
    assert result.initial_checkpoint == 5
    assert result.final_checkpoint == 20
    assert result.status == ReplicaStatus.SYNCHRONIZED

    # 4. Verify all 20 documents are indexed on the replica
    checkpoint = client_r.get_checkpoint(0)
    assert checkpoint.last_applied_sequence == 20
    assert checkpoint.total_documents == 20
    assert checkpoint.status == "SYNCHRONIZED"


def test_interrupted_recovery_resumption(primary_and_replica):
    (client_p, client_r), _, _ = primary_and_replica

    # 1. Populate primary with 20 documents
    for i in range(1, 21):
        client_p.insert_document(doc_id=f"doc_{i}", content=f"content {i}")

    assert client_p.get_latest_sequence().latest_sequence == 20
    assert client_r.get_checkpoint(0).last_applied_sequence == 0

    # 2. Start recovery but interrupt after replaying 8 events
    recovery_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    interrupted_result = recovery_mgr.recover(batch_size=4, max_events_to_replay=8)

    assert not interrupted_result.success  # Not yet fully synchronized
    assert interrupted_result.replayed_events == 8
    assert interrupted_result.final_checkpoint == 8
    assert interrupted_result.status == ReplicaStatus.RECOVERING

    # 3. Resume recovery: verify it starts from sequence 9 and replays only 12 events
    start_seq, end_seq, count = recovery_mgr.calculate_missing_range()
    assert start_seq == 9
    assert end_seq == 20
    assert count == 12

    resumed_result = recovery_mgr.recover(batch_size=5)
    assert resumed_result.success
    assert resumed_result.replayed_events == 12
    assert resumed_result.initial_checkpoint == 8
    assert resumed_result.final_checkpoint == 20
    assert resumed_result.status == ReplicaStatus.SYNCHRONIZED


def test_history_unavailable_error(primary_and_replica):
    (client_p, client_r), (store_p, _), _ = primary_and_replica

    # 1. Insert 20 documents on primary
    for i in range(1, 21):
        client_p.insert_document(doc_id=f"doc_{i}", content=f"content {i}")

    # Replica is at checkpoint 0
    # Simulate log pruning on primary: delete update_log entries with sequence <= 10
    with store_p.db.transaction() as cur:
        cur.execute("DELETE FROM update_log WHERE shard_id = 0 AND sequence_number <= 10")

    # Earliest sequence available is now 11
    earliest_seq = client_p.get_latest_sequence().oldest_sequence
    assert earliest_seq == 11

    # 2. Attempt recovery for replica at sequence 0 (requires sequence 1, but oldest is 11)
    recovery_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    result = recovery_mgr.recover()

    assert not result.success
    assert result.status == ReplicaStatus.HISTORY_UNAVAILABLE
    assert result.error_code == ErrorCode.HISTORY_UNAVAILABLE.value
    assert result.replayed_events == 0
    assert result.final_checkpoint == 0
    assert "History unavailable" in result.error_message


def test_recovery_already_synchronized(primary_and_replica):
    (client_p, client_r), _, _ = primary_and_replica

    # Insert 3 documents on primary and replicate each
    for i in range(1, 4):
        resp = client_p.insert_document(doc_id=f"doc_{i}", content=f"content {i}")
        client_r.apply_update(
            UpdateEvent(
                shard_id=0,
                sequence_number=resp.sequence_number,
                doc_id=f"doc_{i}",
                operation=OperationType.INSERT,
                version=1,
                payload=f'{{"content": "content {i}"}}',
            )
        )

    recovery_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    result = recovery_mgr.recover()

    assert result.success
    assert result.replayed_events == 0
    assert result.status == ReplicaStatus.SYNCHRONIZED
