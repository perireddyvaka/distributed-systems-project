"""Unit tests for InvertedIndex."""

import pytest
from indexing.inverted_index import InvertedIndex


def test_insert_single_document():
    index = InvertedIndex()
    index.add_document("doc1", {"distributed": 2, "systems": 1}, 3)
    assert index.total_documents == 1
    assert index.get_document_length("doc1") == 3
    assert index.get_doc_frequency("distributed") == 1
    assert index.get_postings("distributed") == {"doc1": 2}
    assert index.get_postings("systems") == {"doc1": 1}
    assert index.get_doc_frequency("unknown") == 0


def test_insert_multiple_documents():
    index = InvertedIndex()
    index.add_document("doc1", {"distributed": 1, "systems": 1}, 2)
    index.add_document("doc2", {"distributed": 2, "search": 1}, 3)
    index.add_document("doc3", {"search": 1, "engine": 1}, 2)

    assert index.total_documents == 3
    assert index.get_doc_frequency("distributed") == 2
    assert index.get_doc_frequency("search") == 2
    assert index.get_doc_frequency("systems") == 1
    assert index.get_doc_frequency("engine") == 1
    assert index.get_postings("distributed") == {"doc1": 1, "doc2": 2}
    assert index.average_document_length == (2 + 3 + 2) / 3.0


def test_replace_document_updates_postings():
    index = InvertedIndex()
    index.add_document("doc1", {"apple": 2, "banana": 1}, 3)
    assert index.get_doc_frequency("apple") == 1
    assert index.get_doc_frequency("banana") == 1

    # Replace doc1 with completely different terms
    index.update_document("doc1", {"cherry": 4, "date": 1}, 5)
    assert index.total_documents == 1
    assert index.get_document_length("doc1") == 5
    # Old terms should be completely gone
    assert index.get_doc_frequency("apple") == 0
    assert index.get_postings("apple") == {}
    assert index.get_doc_frequency("banana") == 0
    # New terms present
    assert index.get_postings("cherry") == {"doc1": 4}
    assert index.get_postings("date") == {"doc1": 1}


def test_delete_document_removes_postings():
    index = InvertedIndex()
    index.add_document("doc1", {"distributed": 1}, 1)
    index.add_document("doc2", {"distributed": 1, "systems": 1}, 2)
    assert index.get_doc_frequency("distributed") == 2

    index.remove_document("doc1")
    assert index.total_documents == 1
    assert index.get_doc_frequency("distributed") == 1
    assert index.get_postings("distributed") == {"doc2": 1}
    assert not index.contains_document("doc1")
    assert index.contains_document("doc2")

    # Deleting non-existent doc is a safe no-op
    index.remove_document("nonexistent")
    assert index.total_documents == 1


def test_index_reconstruction():
    index = InvertedIndex()
    docs_to_reconstruct = [
        ("doc1", {"hello": 2, "world": 1}, 3),
        ("doc2", {"world": 2, "python": 1}, 3),
    ]
    index.reconstruct(docs_to_reconstruct)
    assert index.total_documents == 2
    assert index.get_doc_frequency("world") == 2
    assert index.get_postings("world") == {"doc1": 1, "doc2": 2}
    assert index.get_postings("hello") == {"doc1": 2}
    assert index.get_postings("python") == {"doc2": 1}
