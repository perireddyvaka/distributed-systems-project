"""Document Manager with versioning, tombstones, and inverted index integration."""

import time
from typing import Dict, List, Optional, Tuple

from indexing.inverted_index import InvertedIndex
from indexing.tokenizer import Tokenizer
from shared.enums import ErrorCode
from shared.exceptions import DocumentNotFoundError, StaleVersionError, SearchEngineException
from shared.models import Document


class DocumentAlreadyExistsError(SearchEngineException):
    def __init__(self, doc_id: str):
        super().__init__(f"Document '{doc_id}' already exists", ErrorCode.DOCUMENT_ALREADY_EXISTS)
        self.doc_id = doc_id


class TombstoneConflictError(SearchEngineException):
    def __init__(self, doc_id: str, tombstone_version: int, incoming_version: int):
        super().__init__(
            f"Cannot resurrect deleted document '{doc_id}' with version {incoming_version} <= tombstone version {tombstone_version}",
            ErrorCode.TOMBSTONE_CONFLICT,
        )
        self.doc_id = doc_id
        self.tombstone_version = tombstone_version
        self.incoming_version = incoming_version


class DocumentManager:
    """Manages document lifecycles, versions, tombstones, and index synchronization."""

    def __init__(self, index: Optional[InvertedIndex] = None):
        self.index = index if index is not None else InvertedIndex()
        # doc_id -> Document (stores both active documents and tombstones)
        self._documents: Dict[str, Document] = {}

    def insert(self, doc_id: str, content: str, version: int = 1) -> Document:
        """Insert a new document.
        
        Args:
            doc_id: Unique document identifier.
            content: Document text.
            version: Version number (default 1).
            
        Raises:
            DocumentAlreadyExistsError: If active document already exists.
            TombstoneConflictError: If a tombstone exists and version <= tombstone_version.
            StaleVersionError: If version <= 0.
        """
        if version <= 0:
            raise StaleVersionError(doc_id, current_version=0, incoming_version=version)

        if doc_id in self._documents:
            existing = self._documents[doc_id]
            if existing.is_deleted:
                # Document was previously deleted (tombstone exists)
                if version <= existing.version:
                    raise TombstoneConflictError(doc_id, existing.version, version)
            else:
                # Active document exists
                raise DocumentAlreadyExistsError(doc_id)

        tokens, term_freqs, doc_length = Tokenizer.analyze(content)
        now = time.time()
        doc = Document(
            doc_id=doc_id,
            content=content,
            version=version,
            is_deleted=False,
            document_length=doc_length,
            created_at=now,
            updated_at=now,
        )

        self._documents[doc_id] = doc
        self.index.add_document(doc_id, term_freqs, doc_length)
        return doc

    def replace(
        self,
        doc_id: str,
        content: str,
        version: Optional[int] = None,
        expected_version: Optional[int] = None,
    ) -> Document:
        """Replace an existing document with new content and an incremented version.
        
        Args:
            doc_id: Document ID.
            content: New document text.
            version: Optional explicit version (must be strictly greater than current version).
            expected_version: Optional optimistic concurrency version check.
            
        Raises:
            DocumentNotFoundError: If document does not exist or is deleted.
            StaleVersionError: If expected_version doesn't match or version <= current_version.
        """
        if doc_id not in self._documents or self._documents[doc_id].is_deleted:
            raise DocumentNotFoundError(doc_id)

        current = self._documents[doc_id]

        if expected_version is not None and current.version != expected_version:
            raise StaleVersionError(doc_id, current_version=current.version, incoming_version=expected_version)

        new_version = version if version is not None else (current.version + 1)
        if new_version <= current.version:
            raise StaleVersionError(doc_id, current_version=current.version, incoming_version=new_version)

        tokens, term_freqs, doc_length = Tokenizer.analyze(content)
        now = time.time()
        updated_doc = Document(
            doc_id=doc_id,
            content=content,
            version=new_version,
            is_deleted=False,
            document_length=doc_length,
            created_at=current.created_at,
            updated_at=now,
        )

        self._documents[doc_id] = updated_doc
        self.index.update_document(doc_id, term_freqs, doc_length)
        return updated_doc

    def delete(
        self,
        doc_id: str,
        version: Optional[int] = None,
        expected_version: Optional[int] = None,
    ) -> Document:
        """Delete a document and convert it into a persistent versioned tombstone.
        
        Args:
            doc_id: Document ID.
            version: Optional explicit tombstone version (must be > current version).
            expected_version: Optional optimistic concurrency version check.
            
        Raises:
            DocumentNotFoundError: If document does not exist or is already deleted.
            StaleVersionError: If version check fails.
        """
        if doc_id not in self._documents or self._documents[doc_id].is_deleted:
            raise DocumentNotFoundError(doc_id)

        current = self._documents[doc_id]

        if expected_version is not None and current.version != expected_version:
            raise StaleVersionError(doc_id, current_version=current.version, incoming_version=expected_version)

        tombstone_version = version if version is not None else (current.version + 1)
        if tombstone_version <= current.version:
            raise StaleVersionError(doc_id, current_version=current.version, incoming_version=tombstone_version)

        now = time.time()
        tombstone = Document(
            doc_id=doc_id,
            content="",
            version=tombstone_version,
            is_deleted=True,
            document_length=0,
            created_at=current.created_at,
            updated_at=now,
        )

        # Store tombstone and remove postings from inverted index
        self._documents[doc_id] = tombstone
        self.index.remove_document(doc_id)
        return tombstone

    def get_document(self, doc_id: str, include_deleted: bool = False) -> Optional[Document]:
        """Retrieve a document by ID."""
        doc = self._documents.get(doc_id)
        if doc is None:
            return None
        if doc.is_deleted and not include_deleted:
            return None
        return doc

    def get_all_documents(self, include_deleted: bool = False) -> List[Document]:
        """Retrieve all documents in the manager."""
        if include_deleted:
            return list(self._documents.values())
        return [doc for doc in self._documents.values() if not doc.is_deleted]

    def has_document(self, doc_id: str, include_deleted: bool = False) -> bool:
        """Check if document exists."""
        doc = self._documents.get(doc_id)
        if doc is None:
            return False
        return not doc.is_deleted or include_deleted

    def get_stats(self) -> Tuple[int, float]:
        """Return (total_non_deleted_docs, average_document_length)."""
        return self.index.total_documents, self.index.average_document_length

    def restore_document(self, doc: Document) -> None:
        """Restore a document from storage without validating version progression."""
        self._documents[doc.doc_id] = doc
        if not doc.is_deleted:
            _, term_freqs, doc_length = Tokenizer.analyze(doc.content)
            self.index.add_document(doc.doc_id, term_freqs, doc_length)
        else:
            self.index.remove_document(doc.doc_id)

    def clear(self) -> None:
        """Clear all documents and index."""
        self._documents.clear()
        self.index.clear()
