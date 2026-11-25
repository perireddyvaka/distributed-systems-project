"""Custom Inverted Index using native Python data structures."""

from typing import Dict, Iterable, List, Optional, Set, Tuple


class InvertedIndex:
    """In-memory custom inverted index.
    
    Structure:
        _postings: term -> {doc_id: term_frequency}
        _doc_lengths: doc_id -> total token count
        _doc_terms: doc_id -> set of terms indexed for fast removal/updates
    """

    def __init__(self):
        # term -> {doc_id: tf}
        self._postings: Dict[str, Dict[str, int]] = {}
        # doc_id -> length
        self._doc_lengths: Dict[str, int] = {}
        # doc_id -> set of terms present in document
        self._doc_terms: Dict[str, Set[str]] = {}

    def add_document(self, doc_id: str, term_frequencies: Dict[str, int], doc_length: int) -> None:
        """Add a document's postings to the inverted index."""
        if doc_id in self._doc_lengths:
            # Document already indexed; remove existing postings first
            self.remove_document(doc_id)

        self._doc_lengths[doc_id] = doc_length
        self._doc_terms[doc_id] = set()

        for term, tf in term_frequencies.items():
            if tf <= 0:
                continue
            if term not in self._postings:
                self._postings[term] = {}
            self._postings[term][doc_id] = tf
            self._doc_terms[doc_id].add(term)

    def remove_document(self, doc_id: str) -> None:
        """Remove a document from the inverted index and update postings."""
        if doc_id not in self._doc_lengths:
            return

        del self._doc_lengths[doc_id]
        indexed_terms = self._doc_terms.pop(doc_id, set())

        for term in indexed_terms:
            if term in self._postings:
                self._postings[term].pop(doc_id, None)
                # Clean up empty term entries to keep postings tidy
                if not self._postings[term]:
                    del self._postings[term]

    def update_document(self, doc_id: str, term_frequencies: Dict[str, int], doc_length: int) -> None:
        """Update a document in the index (replaces previous postings)."""
        self.remove_document(doc_id)
        self.add_document(doc_id, term_frequencies, doc_length)

    def get_postings(self, term: str) -> Dict[str, int]:
        """Get copy of posting list for a term: {doc_id: term_frequency}."""
        return dict(self._postings.get(term, {}))

    def get_doc_frequency(self, term: str) -> int:
        """Get document frequency (DF) for a term."""
        return len(self._postings.get(term, {}))

    def get_document_length(self, doc_id: str) -> int:
        """Get the length of an indexed document."""
        return self._doc_lengths.get(doc_id, 0)

    def contains_document(self, doc_id: str) -> bool:
        """Check if document is currently in the inverted index."""
        return doc_id in self._doc_lengths

    @property
    def total_documents(self) -> int:
        """Total number of non-deleted documents in the index."""
        return len(self._doc_lengths)

    @property
    def total_document_length(self) -> int:
        """Sum of lengths of all indexed documents."""
        return sum(self._doc_lengths.values())

    @property
    def average_document_length(self) -> float:
        """Average document length across all indexed documents."""
        if not self._doc_lengths:
            return 0.0
        return self.total_document_length / len(self._doc_lengths)

    def get_all_terms(self) -> Set[str]:
        """Return the set of all unique terms currently indexed."""
        return set(self._postings.keys())

    def get_all_doc_ids(self) -> Set[str]:
        """Return the set of all document IDs currently indexed."""
        return set(self._doc_lengths.keys())

    def clear(self) -> None:
        """Clear all entries from the index."""
        self._postings.clear()
        self._doc_lengths.clear()
        self._doc_terms.clear()

    def reconstruct(self, documents_with_postings: Iterable[Tuple[str, Dict[str, int], int]]) -> None:
        """Reconstruct the entire index from persisted records."""
        self.clear()
        for doc_id, tf_map, length in documents_with_postings:
            self.add_document(doc_id, tf_map, length)
