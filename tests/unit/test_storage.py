"""Unit and integration tests for SQLite persistence, atomic transactions, and index reload."""

import os
import tempfile
import pytest
from indexing.bm25 import BM25Scorer
from indexing.document_manager import DocumentManager
from indexing.tokenizer import Tokenizer
from shared.enums import OperationType, ReplicaStatus
from shared.models import Document, UpdateEvent
from storage.database import DatabaseManager
from storage.document_repository import DocumentRepository
from storage.index_repository import IndexRepository
from storage.log_repository import LogRepository
from storage.replica_repository import ReplicaRepository
from storage.shard_store import ShardStore


@pytest.fixture
def temp_db():
    """Create a temporary on-disk SQLite database with WAL mode."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = DatabaseManager(path)
    db.initialize_schema()
    yield db, path
    db.close()
    for ext in ["", "-wal", "-shm"]:
        if os.path.exists(path + ext):
            os.remove(path + ext)


def test_database_wal_mode(temp_db):
    db, path = temp_db
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode;")
    mode = cur.fetchone()[0]
    assert mode.lower() == "wal"
    cur.execute("PRAGMA foreign_keys;")
    fk = cur.fetchone()[0]
    assert fk == 1


def test_document_repository(temp_db):
    db, _ = temp_db
    repo = DocumentRepository(db)

    doc1 = Document(doc_id="d1", content="Hello world", version=1, document_length=2)
    repo.save_document(doc1)

    fetched = repo.get_document("d1")
    assert fetched is not None
    assert fetched.doc_id == "d1"
    assert fetched.version == 1
    assert fetched.content == "Hello world"
    assert not fetched.is_deleted

    # Update with tombstone
    tombstone = Document(doc_id="d1", content="", version=2, is_deleted=True, document_length=0)
    repo.save_document(tombstone)

    assert repo.get_document("d1") is None
    assert repo.get_document("d1", include_deleted=True).is_deleted is True
    assert repo.count_active_documents() == 0


def test_index_repository(temp_db):
    db, _ = temp_db
    doc_repo = DocumentRepository(db)
    index_repo = IndexRepository(db)

    # Need document record for foreign key integrity
    doc_repo.save_document(Document(doc_id="d1", content="test", version=1, document_length=1))
    index_repo.save_postings("d1", {"search": 2, "engine": 1})

    postings_data = index_repo.load_all_postings()
    assert len(postings_data) == 1
    doc_id, tf_map, length = postings_data[0]
    assert doc_id == "d1"
    assert tf_map == {"search": 2, "engine": 1}

    # Replace postings
    index_repo.save_postings("d1", {"python": 3})
    reloaded = index_repo.load_all_postings()
    assert reloaded[0][1] == {"python": 3}


def test_log_repository(temp_db):
    db, _ = temp_db
    log_repo = LogRepository(db)

    event1 = UpdateEvent(
        shard_id=0,
        sequence_number=1,
        doc_id="d1",
        operation=OperationType.INSERT,
        version=1,
        payload='{"content": "first"}',
    )
    event2 = UpdateEvent(
        shard_id=0,
        sequence_number=2,
        doc_id="d2",
        operation=OperationType.INSERT,
        version=1,
        payload='{"content": "second"}',
    )

    log_repo.append_log(event1)
    log_repo.append_log(event2)

    assert log_repo.get_latest_sequence(shard_id=0) == 2
    assert log_repo.get_earliest_sequence(shard_id=0) == 1
    assert log_repo.get_log_count(shard_id=0) == 2

    # Query range
    updates = log_repo.get_updates_after(shard_id=0, since_sequence=1)
    assert len(updates) == 1
    assert updates[0].sequence_number == 2


def test_replica_checkpoint_repository(temp_db):
    db, _ = temp_db
    repo = ReplicaRepository(db)

    repo.save_checkpoint(shard_id=0, replica_id="replica_0", last_applied_sequence=42, status=ReplicaStatus.SYNCHRONIZED)
    rep_id, seq, status = repo.get_checkpoint(shard_id=0)
    assert rep_id == "replica_0"
    assert seq == 42
    assert status == ReplicaStatus.SYNCHRONIZED


def test_atomic_primary_mutation_rollback(temp_db):
    db, _ = temp_db
    store = ShardStore(db, shard_id=0)

    doc = Document(doc_id="d1", content="crash test", version=1, document_length=2)
    tokens, tf, doc_len = Tokenizer.analyze(doc.content)

    # Verify atomic commit
    event = store.execute_primary_mutation(doc, tf, sequence_number=1, operation=OperationType.INSERT)
    assert event.sequence_number == 1
    assert store.log_repo.get_latest_sequence(shard_id=0) == 1
    assert store.doc_repo.get_document("d1") is not None

    # Now verify rollback on failure
    try:
        with db.transaction() as cur:
            store.doc_repo.save_document(Document(doc_id="d2", content="rollback", version=1, document_length=1), cursor=cur)
            # Intentionally raise exception before committing log/metadata
            raise RuntimeError("Simulated crash mid-transaction")
    except RuntimeError:
        pass

    # Ensure document d2 was rolled back and sequence was NOT advanced
    assert store.doc_repo.get_document("d2") is None
    assert store.log_repo.get_latest_sequence(shard_id=0) == 1


def test_restart_and_index_reconstruction(temp_db):
    db, db_path = temp_db
    store = ShardStore(db, shard_id=0)

    # 1. Commit documents into storage
    doc1 = Document(doc_id="doc1", content="distributed systems and search", version=1, document_length=4)
    _, tf1, len1 = Tokenizer.analyze(doc1.content)
    store.execute_primary_mutation(doc1, tf1, sequence_number=1, operation=OperationType.INSERT)

    doc2 = Document(doc_id="doc2", content="python distributed databases", version=1, document_length=3)
    _, tf2, len2 = Tokenizer.analyze(doc2.content)
    store.execute_primary_mutation(doc2, tf2, sequence_number=2, operation=OperationType.INSERT)

    # Delete doc1 (tombstone)
    tombstone = Document(doc_id="doc1", content="", version=2, is_deleted=True, document_length=0)
    store.execute_primary_mutation(tombstone, {}, sequence_number=3, operation=OperationType.DELETE)

    # 2. Simulate process crash / restart: close DB completely
    db.close()

    # 3. Re-open database fresh
    reopened_db = DatabaseManager(db_path)
    fresh_store = ShardStore(reopened_db, shard_id=0)
    reconstructed_mgr = fresh_store.reconstruct_in_memory_state()

    # 4. Verify in-memory state after reload
    assert reconstructed_mgr.has_document("doc2")
    assert not reconstructed_mgr.has_document("doc1")  # doc1 is deleted
    assert reconstructed_mgr.get_document("doc1", include_deleted=True).is_deleted is True
    assert reconstructed_mgr.index.total_documents == 1
    assert reconstructed_mgr.index.get_doc_frequency("distributed") == 1

    # 5. Verify BM25 search on reconstructed index
    scorer = BM25Scorer()
    results = scorer.search("distributed", reconstructed_mgr.index)
    assert len(results) == 1
    assert results[0].doc_id == "doc2"

    reopened_db.close()
