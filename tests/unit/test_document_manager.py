"""Unit tests for DocumentManager with versioning and tombstones."""

import pytest
from indexing.document_manager import (
    DocumentManager,
    DocumentAlreadyExistsError,
    TombstoneConflictError,
)
from shared.exceptions import DocumentNotFoundError, StaleVersionError


def test_insert_document_success():
    mgr = DocumentManager()
    doc = mgr.insert("d1", "The quick brown fox", version=1)
    assert doc.doc_id == "d1"
    assert doc.version == 1
    assert not doc.is_deleted
    assert doc.document_length == 4
    assert mgr.has_document("d1")
    assert mgr.index.get_doc_frequency("fox") == 1
    assert mgr.index.total_documents == 1


def test_insert_duplicate_raises_error():
    mgr = DocumentManager()
    mgr.insert("d1", "Initial content", version=1)
    with pytest.raises(DocumentAlreadyExistsError):
        mgr.insert("d1", "Duplicate content", version=1)


def test_replace_document_monotonic_version():
    mgr = DocumentManager()
    mgr.insert("d1", "Version one content", version=1)

    # Valid replacement with auto-increment
    replaced = mgr.replace("d1", "Version two new content")
    assert replaced.version == 2
    assert replaced.content == "Version two new content"
    assert mgr.get_document("d1").version == 2
    assert mgr.index.get_doc_frequency("one") == 0
    assert mgr.index.get_doc_frequency("two") == 1

    # Replacement with explicit valid version
    replaced3 = mgr.replace("d1", "Version five", version=5)
    assert replaced3.version == 5


def test_replace_with_stale_or_invalid_version():
    mgr = DocumentManager()
    mgr.insert("d1", "Initial content", version=3)

    # Stale version <= current version
    with pytest.raises(StaleVersionError):
        mgr.replace("d1", "Stale update", version=3)
    with pytest.raises(StaleVersionError):
        mgr.replace("d1", "Older update", version=2)

    # Mismatched expected version
    with pytest.raises(StaleVersionError):
        mgr.replace("d1", "Expected version mismatch", expected_version=2)


def test_delete_creates_tombstone():
    mgr = DocumentManager()
    mgr.insert("d1", "Content to be deleted", version=2)
    assert mgr.index.total_documents == 1

    tombstone = mgr.delete("d1")
    assert tombstone.doc_id == "d1"
    assert tombstone.is_deleted is True
    assert tombstone.version == 3  # Auto-incremented to 3
    assert tombstone.document_length == 0

    # Inverted index must exclude deleted document
    assert mgr.index.total_documents == 0
    assert mgr.index.get_doc_frequency("content") == 0
    assert mgr.get_document("d1") is None
    # Tombstone is preserved internally
    assert mgr.get_document("d1", include_deleted=True).is_deleted is True


def test_tombstone_prevents_stale_resurrection():
    mgr = DocumentManager()
    mgr.insert("d1", "Initial", version=1)
    mgr.delete("d1", version=2)  # Tombstone version 2

    # Delayed older insert with version 1 must be rejected
    with pytest.raises(TombstoneConflictError):
        mgr.insert("d1", "Delayed resurrection insert", version=1)

    # Delayed insert with same version 2 must also be rejected
    with pytest.raises(TombstoneConflictError):
        mgr.insert("d1", "Delayed resurrection insert", version=2)

    # But a newer insert with version > tombstone version is allowed
    new_doc = mgr.insert("d1", "Brand new resurrection", version=3)
    assert new_doc.version == 3
    assert not new_doc.is_deleted
    assert mgr.index.total_documents == 1
    assert mgr.index.get_doc_frequency("brand") == 1


def test_delete_nonexistent_or_already_deleted():
    mgr = DocumentManager()
    with pytest.raises(DocumentNotFoundError):
        mgr.delete("nonexistent")

    mgr.insert("d1", "Active", version=1)
    mgr.delete("d1")

    # Deleting already deleted document raises DocumentNotFoundError
    with pytest.raises(DocumentNotFoundError):
        mgr.delete("d1")
