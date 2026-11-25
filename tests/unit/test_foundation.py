"""Milestone M0 smoke tests verifying foundational components."""

import sqlite3
from pathlib import Path
import pytest
from datetime import datetime

from shared.enums import OperationType, ReplicaStatus, ShardRole, ErrorCode, RecoveryStatus
from shared.models import Document, SearchResult, RankingStatsSnapshot, UpdateEvent, ShardStatus
from shared.exceptions import SearchEngineError, StaleVersionError, HistoryUnavailableError, SequenceGapError
from shared.config import ClusterConfig
from proto.generated import coordinator_pb2, shard_pb2, replication_pb2


def test_enums():
    """Verify core enumerations."""
    assert OperationType.INSERT.value == "INSERT"
    assert OperationType.REPLACE.value == "REPLACE"
    assert OperationType.DELETE.value == "DELETE"

    assert ReplicaStatus.SYNCHRONIZED.value == "SYNCHRONIZED"
    assert ReplicaStatus.HISTORY_UNAVAILABLE.value == "HISTORY_UNAVAILABLE"
    assert RecoveryStatus.IDLE.value == "IDLE"
    assert RecoveryStatus.COMPLETED.value == "COMPLETED"

    assert ErrorCode.STALE_VERSION.value == "STALE_VERSION"
    assert ErrorCode.SEQUENCE_GAP.value == "SEQUENCE_GAP"


def test_models():
    """Verify dataclasses initialization and fields."""
    doc = Document(doc_id="doc1", content="hello world", version=1, is_deleted=False, document_length=2)
    assert doc.doc_id == "doc1"
    assert doc.version == 1
    assert not doc.is_deleted
    assert doc.document_length == 2
    assert isinstance(doc.created_at, datetime)

    # Serialization roundtrip
    d_dict = doc.to_dict()
    doc_reconstructed = Document.from_dict(d_dict)
    assert doc_reconstructed.doc_id == doc.doc_id
    assert doc_reconstructed.content == doc.content
    assert doc_reconstructed.version == doc.version

    event = UpdateEvent(
        shard_id=0,
        sequence_number=1,
        doc_id="doc1",
        operation=OperationType.INSERT,
        version=1,
        payload='{"content": "hello world"}'
    )
    assert event.shard_id == 0
    assert event.sequence_number == 1
    assert event.operation == OperationType.INSERT

    # Event serialization roundtrip
    e_dict = event.to_dict()
    event_reconstructed = UpdateEvent.from_dict(e_dict)
    assert event_reconstructed.sequence_number == 1
    assert event_reconstructed.operation == OperationType.INSERT

    stats = RankingStatsSnapshot(total_documents=100, avg_document_length=15.5, document_frequencies={"hello": 40})
    assert stats.total_documents == 100
    assert stats.document_frequencies["hello"] == 40


def test_search_result_tie_breaking():
    """Verify SearchResult ordering: score DESC, doc_id ASC."""
    r1 = SearchResult(doc_id="doc-b", score=3.5, shard_id=0)
    r2 = SearchResult(doc_id="doc-a", score=3.5, shard_id=1)
    r3 = SearchResult(doc_id="doc-c", score=5.0, shard_id=0)

    results = [r1, r2, r3]
    results.sort()

    assert results[0].doc_id == "doc-c"  # Highest score (5.0)
    assert results[1].doc_id == "doc-a"  # Score 3.5, tie-break doc-a < doc-b
    assert results[2].doc_id == "doc-b"  # Score 3.5


def test_exceptions_hierarchy():
    """Verify custom exceptions derive from SearchEngineError."""
    assert issubclass(StaleVersionError, SearchEngineError)
    assert issubclass(HistoryUnavailableError, SearchEngineError)
    assert issubclass(SequenceGapError, SearchEngineError)

    with pytest.raises(SearchEngineError):
        raise StaleVersionError("doc1", current_version=2, incoming_version=1)


def test_config():
    """Verify cluster configuration parsing."""
    config = ClusterConfig.from_env()
    assert config.shard_count >= 1
    assert 0 in config.shard_endpoints
    assert config.shard_endpoints[0].primary_port > 0


def test_sqlite_schema():
    """Verify schema.sql executes cleanly on SQLite."""
    schema_path = Path(__file__).resolve().parent.parent.parent / "storage" / "schema.sql"
    assert schema_path.exists(), "schema.sql does not exist"

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.executescript(schema_path.read_text())

    # Check tables created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {row[0] for row in cursor.fetchall()}
    assert "documents" in tables
    assert "postings" in tables
    assert "update_log" in tables
    assert "shard_metadata" in tables
    assert "replica_state" in tables

    # Test sample inserts
    cursor.execute(
        "INSERT INTO documents (doc_id, content, version, is_deleted, document_length, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("d1", "test content", 1, 0, 2, 1000.0, 1000.0)
    )
    cursor.execute(
        "INSERT INTO postings (term, doc_id, term_frequency) VALUES (?, ?, ?)",
        ("test", "d1", 1)
    )
    cursor.execute(
        "INSERT INTO update_log (shard_id, sequence_number, doc_id, operation, version, payload, committed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (0, 1, "d1", "INSERT", 1, "test content", 1000.0)
    )
    conn.commit()

    cursor.execute("SELECT count(*) FROM documents")
    assert cursor.fetchone()[0] == 1
    cursor.execute("SELECT count(*) FROM update_log")
    assert cursor.fetchone()[0] == 1
    conn.close()


def test_protobuf_messages():
    """Verify protobuf messages can be instantiated and serialized."""
    # Coordinator proto
    insert_req = coordinator_pb2.InsertRequest(doc_id="doc1", content="hello world")
    assert insert_req.doc_id == "doc1"
    serialized = insert_req.SerializeToString()
    deserialized = coordinator_pb2.InsertRequest()
    deserialized.ParseFromString(serialized)
    assert deserialized.doc_id == "doc1"

    # Shard proto
    local_search = shard_pb2.LocalSearchRequest(
        query="hello",
        top_k=5,
        global_stats=shard_pb2.GlobalRankingSnapshot(
            total_documents=10,
            avg_document_length=2.5,
            document_frequencies={"hello": 3},
            stats_version=1
        )
    )
    assert local_search.query == "hello"
    assert local_search.global_stats.total_documents == 10

    # Replication proto
    update_event = shard_pb2.ProtoUpdateEvent(
        shard_id=0,
        sequence_number=42,
        doc_id="doc1",
        operation="INSERT",
        version=1,
        payload="hello"
    )
    apply_resp = replication_pb2.ApplyUpdateResponse(
        success=True,
        shard_id=0,
        applied_sequence=42,
        is_duplicate=False
    )
    assert apply_resp.applied_sequence == 42
