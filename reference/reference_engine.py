"""Independent single-node reference search engine for correctness verification."""

import math
import re
import unicodedata
from collections import Counter
from typing import Dict, List, Optional, Tuple


class ReferenceEngine:
    """Independent single-node search engine with custom inverted indexing and BM25 ranking.
    
    This engine serves as the trusted oracle for correctness cross-validation.
    It deliberately uses an independent implementation path without sharing code
    with the distributed indexing or RPC packages.
    """

    def __init__(self, k1: float = 1.2, b: float = 0.75):
        self.k1 = k1
        self.b = b
        # doc_id -> {"content": str, "version": int, "is_deleted": bool, "tokens": List[str], "doc_len": int}
        self._documents: Dict[str, dict] = {}
        # term -> {doc_id: tf}
        self._inverted_index: Dict[str, Dict[str, int]] = {}

    def _tokenize(self, text: str) -> List[str]:
        """Independent tokenization with NFKD accent stripping and regex word extraction."""
        if not text:
            return []
        decomposed = unicodedata.normalize("NFD", text)
        stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
        normalized = stripped.lower()
        return re.findall(r"\b[a-zA-Z0-9]+(?:'[a-zA-Z0-9]+)?\b", normalized)

    def insert_document(self, doc_id: str, content: str, version: int = 1) -> None:
        """Insert a document into the reference corpus."""
        if doc_id in self._documents:
            existing = self._documents[doc_id]
            if existing["is_deleted"]:
                if version <= existing["version"]:
                    raise ValueError(f"Cannot resurrect doc '{doc_id}' with version <= tombstone version")
            else:
                raise ValueError(f"Document '{doc_id}' already exists")

        tokens = self._tokenize(content)
        tf_map = dict(Counter(tokens))
        doc_len = len(tokens)

        self._documents[doc_id] = {
            "content": content,
            "version": version,
            "is_deleted": False,
            "tokens": tokens,
            "doc_len": doc_len,
        }

        # Update postings
        for term, tf in tf_map.items():
            if term not in self._inverted_index:
                self._inverted_index[term] = {}
            self._inverted_index[term][doc_id] = tf

    def replace_document(self, doc_id: str, content: str, expected_version: Optional[int] = None) -> None:
        """Replace an existing document."""
        if doc_id not in self._documents or self._documents[doc_id]["is_deleted"]:
            raise KeyError(f"Document '{doc_id}' not found")

        current = self._documents[doc_id]
        if expected_version is not None and current["version"] != expected_version:
            raise ValueError(f"Expected version mismatch for '{doc_id}'")

        new_version = current["version"] + 1

        # Remove old postings
        old_tokens = set(current["tokens"])
        for term in old_tokens:
            if term in self._inverted_index:
                self._inverted_index[term].pop(doc_id, None)
                if not self._inverted_index[term]:
                    del self._inverted_index[term]

        # Insert new postings
        new_tokens = self._tokenize(content)
        tf_map = dict(Counter(new_tokens))
        doc_len = len(new_tokens)

        self._documents[doc_id] = {
            "content": content,
            "version": new_version,
            "is_deleted": False,
            "tokens": new_tokens,
            "doc_len": doc_len,
        }

        for term, tf in tf_map.items():
            if term not in self._inverted_index:
                self._inverted_index[term] = {}
            self._inverted_index[term][doc_id] = tf

    def delete_document(self, doc_id: str, expected_version: Optional[int] = None) -> None:
        """Delete a document and store a versioned tombstone."""
        if doc_id not in self._documents or self._documents[doc_id]["is_deleted"]:
            raise KeyError(f"Document '{doc_id}' not found")

        current = self._documents[doc_id]
        if expected_version is not None and current["version"] != expected_version:
            raise ValueError(f"Expected version mismatch for '{doc_id}'")

        tombstone_version = current["version"] + 1

        # Remove postings
        for term in set(current["tokens"]):
            if term in self._inverted_index:
                self._inverted_index[term].pop(doc_id, None)
                if not self._inverted_index[term]:
                    del self._inverted_index[term]

        self._documents[doc_id] = {
            "content": "",
            "version": tombstone_version,
            "is_deleted": True,
            "tokens": [],
            "doc_len": 0,
        }

    def get_document(self, doc_id: str, include_deleted: bool = False) -> Optional[dict]:
        """Fetch document metadata and content."""
        doc = self._documents.get(doc_id)
        if doc is None:
            return None
        if doc["is_deleted"] and not include_deleted:
            return None
        return dict(doc)

    def total_active_documents(self) -> int:
        """Count of active non-deleted documents."""
        return sum(1 for d in self._documents.values() if not d["is_deleted"])

    def average_document_length(self) -> float:
        """Corpus-wide average document length."""
        active = [d for d in self._documents.values() if not d["is_deleted"]]
        if not active:
            return 0.0
        return sum(d["doc_len"] for d in active) / len(active)

    def document_frequency(self, term: str) -> int:
        """Number of active documents containing term."""
        return len(self._inverted_index.get(term, {}))

    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Execute independent Okapi BM25 search.
        
        Returns:
            List of (doc_id, score) tuples sorted by score DESC, doc_id ASC.
        """
        query_terms = self._tokenize(query)
        if not query_terms:
            return []

        N = self.total_active_documents()
        if N == 0:
            return []

        avgdl = self.average_document_length()

        # Find matching documents
        candidates = set()
        for term in query_terms:
            candidates.update(self._inverted_index.get(term, {}).keys())

        if not candidates:
            return []

        scored = []
        for doc_id in candidates:
            doc_data = self._documents[doc_id]
            if doc_data["is_deleted"]:
                continue

            doc_len = doc_data["doc_len"]
            len_norm = (1.0 - self.b) + self.b * (doc_len / avgdl) if avgdl > 0 else 1.0

            total_score = 0.0
            for term in query_terms:
                postings = self._inverted_index.get(term, {})
                tf = postings.get(doc_id, 0)
                if tf <= 0:
                    continue

                df = len(postings)
                idf = math.log(1.0 + (N - df + 0.5) / (df + 0.5))
                num = tf * (self.k1 + 1.0)
                den = tf + self.k1 * len_norm
                total_score += idf * (num / den)

            if total_score > 0.0:
                scored.append((doc_id, total_score))

        # Deterministic sorting: score DESC, doc_id ASC
        scored.sort(key=lambda item: (-item[1], item[0]))

        if top_k > 0:
            return scored[:top_k]
        return scored
