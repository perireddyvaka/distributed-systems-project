"""Unified Shard Storage coordinator providing atomic transaction boundaries and index reload."""

import json
from typing import Dict, List, Optional, Tuple
from indexing.document_manager import DocumentManager
from indexing.inverted_index import InvertedIndex
from indexing.tokenizer import Tokenizer
from shared.enums import OperationType, ReplicaStatus
from shared.models import Document, UpdateEvent
from storage.database import DatabaseManager
from storage.document_repository import DocumentRepository
from storage.index_repository import IndexRepository
from storage.log_repository import LogRepository
from storage.replica_repository import ReplicaRepository


class ShardStore:
    """Combines repositories to enforce atomic write transactions for Primaries and Replicas."""

    def __init__(self, db: DatabaseManager, shard_id: int):
        self.db = db
        self.shard_id = shard_id
        self.doc_repo = DocumentRepository(db)
        self.index_repo = IndexRepository(db)
        self.log_repo = LogRepository(db)
        self.replica_repo = ReplicaRepository(db)

    def execute_primary_mutation(
        self,
        doc: Document,
        term_frequencies: Dict[str, int],
        sequence_number: int,
        operation: OperationType,
    ) -> UpdateEvent:
        """Atomically commit a primary mutation in a single BEGIN IMMEDIATE SQLite transaction:
        
        1. documents table update
        2. postings table update
        3. update_log insert
        4. shard_metadata sequence update
        """
        payload = json.dumps({"content": doc.content}) if not doc.is_deleted else ""
        event = UpdateEvent(
            shard_id=self.shard_id,
            sequence_number=sequence_number,
            doc_id=doc.doc_id,
            operation=operation,
            version=doc.version,
            payload=payload,
            committed_at=doc.updated_at,
        )

        with self.db.transaction() as cur:
            # 1. Update documents
            self.doc_repo.save_document(doc, cursor=cur)

            # 2. Update postings
            if doc.is_deleted:
                self.index_repo.delete_postings(doc.doc_id, cursor=cur)
            else:
                self.index_repo.save_postings(doc.doc_id, term_frequencies, cursor=cur)

            # 3 & 4. Append to update_log and update shard_metadata
            self.log_repo.append_log(event, cursor=cur)

        return event

    def execute_replica_apply(
        self,
        event: UpdateEvent,
        replica_id: str,
        doc_manager: DocumentManager,
    ) -> None:
        """Atomically apply a replicated update event:
        
        1. Apply mutation to documents & postings
        2. Advance replica checkpoint in replica_state
        3. Update in-memory DocumentManager index
        """
        # Parse payload
        content = ""
        if event.payload:
            try:
                data = json.loads(event.payload)
                content = data.get("content", "")
            except Exception:
                content = event.payload

        is_deleted = event.operation == OperationType.DELETE
        tokens, term_frequencies, doc_length = Tokenizer.analyze(content) if not is_deleted else ([], {}, 0)

        doc = Document(
            doc_id=event.doc_id,
            content=content,
            version=event.version,
            is_deleted=is_deleted,
            document_length=doc_length,
            created_at=event.committed_at,
            updated_at=event.committed_at,
        )

        with self.db.transaction() as cur:
            # 1. Update database documents
            self.doc_repo.save_document(doc, cursor=cur)

            # 2. Update database postings
            if is_deleted:
                self.index_repo.delete_postings(doc.doc_id, cursor=cur)
            else:
                self.index_repo.save_postings(doc.doc_id, term_frequencies, cursor=cur)

            # 3. Advance replica checkpoint
            self.replica_repo.save_checkpoint(
                shard_id=self.shard_id,
                replica_id=replica_id,
                last_applied_sequence=event.sequence_number,
                status=ReplicaStatus.SYNCHRONIZED,
                cursor=cur,
            )

        # 4. Synchronize in-memory index
        doc_manager.restore_document(doc)

    def reconstruct_in_memory_state(self) -> DocumentManager:
        """Reconstruct DocumentManager and InvertedIndex from persistent SQLite storage on boot."""
        doc_manager = DocumentManager()
        # 1. Load all documents (including tombstones)
        all_docs = self.doc_repo.get_all_documents(include_deleted=True)
        for doc in all_docs:
            doc_manager._documents[doc.doc_id] = doc

        # 2. Bulk load postings directly into inverted index
        postings_data = self.index_repo.load_all_postings()
        doc_manager.index.reconstruct(postings_data)
        return doc_manager
