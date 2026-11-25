"""BM25 (Okapi BM25) Ranking Engine with Global Statistics Snapshot support."""

import math
from typing import Dict, List, Optional, Sequence, Set
from indexing.inverted_index import InvertedIndex
from indexing.tokenizer import Tokenizer
from shared.models import RankingStatsSnapshot, SearchResult


class BM25Scorer:
    """Okapi BM25 ranking engine.
    
    Formula:
        Score(D, Q) = sum_{t in Q} IDF(t) * (f(t, D) * (k1 + 1)) / (f(t, D) + k1 * (1 - b + b * (|D| / avgdl)))
        
        Where:
            IDF(t) = ln(1 + (N - DF(t) + 0.5) / (DF(t) + 0.5))
    """

    def __init__(self, k1: float = 1.2, b: float = 0.75):
        self.k1 = k1
        self.b = b

    def compute_idf(self, total_docs: int, doc_freq: int) -> float:
        """Compute Robertson-Spärck Jones Okapi BM25 IDF with +1 smoothing."""
        if total_docs <= 0:
            return 0.0
        # doc_freq cannot exceed total_docs
        df = min(doc_freq, total_docs)
        numerator = total_docs - df + 0.5
        denominator = df + 0.5
        return math.log(1.0 + (numerator / denominator))

    def score_term(self, tf: int, doc_length: int, avgdl: float, idf: float) -> float:
        """Compute the BM25 contribution of a single query term for a document."""
        if tf <= 0 or idf <= 0:
            return 0.0

        if avgdl <= 0:
            len_norm = 1.0
        else:
            len_norm = (1.0 - self.b) + self.b * (doc_length / avgdl)

        numerator = tf * (self.k1 + 1.0)
        denominator = tf + self.k1 * len_norm

        if denominator <= 0:
            return 0.0

        return idf * (numerator / denominator)

    def score_document(
        self,
        doc_id: str,
        query_terms: Sequence[str],
        index: InvertedIndex,
        stats: RankingStatsSnapshot,
    ) -> float:
        """Calculate total BM25 score for a document using a consistent global statistics snapshot."""
        doc_len = index.get_document_length(doc_id)
        if doc_len == 0 or stats.total_documents == 0:
            return 0.0

        total_score = 0.0
        for term in query_terms:
            postings = index.get_postings(term)
            tf = postings.get(doc_id, 0)
            if tf <= 0:
                continue

            df = stats.document_frequencies.get(term, 0)
            idf = self.compute_idf(stats.total_documents, df)
            term_score = self.score_term(tf, doc_len, stats.avg_document_length, idf)
            total_score += term_score

        return total_score

    def search(
        self,
        query: str,
        index: InvertedIndex,
        stats: Optional[RankingStatsSnapshot] = None,
        top_k: int = 10,
        shard_id: int = 0,
    ) -> List[SearchResult]:
        """Execute BM25 search on an inverted index.
        
        Args:
            query: Raw search query string.
            index: Shard inverted index.
            stats: Optional consistent global ranking statistics snapshot.
                   If None, local statistics from the index are used.
            top_k: Maximum number of results to return.
            shard_id: Shard ID for result labeling.
            
        Returns:
            List of SearchResult objects sorted by score DESC, with deterministic
            tie-breaking by doc_id ASC.
        """
        query_terms = Tokenizer.tokenize(query)
        if not query_terms or index.total_documents == 0:
            return []

        # If global stats are not provided, compute local stats
        if stats is None:
            doc_frequencies = {term: index.get_doc_frequency(term) for term in set(query_terms)}
            stats = RankingStatsSnapshot(
                total_documents=index.total_documents,
                avg_document_length=index.average_document_length,
                document_frequencies=doc_frequencies,
                stats_version=1,
            )

        if stats.total_documents == 0:
            return []

        # Find all candidate documents containing at least one query term
        candidate_doc_ids: Set[str] = set()
        for term in query_terms:
            postings = index.get_postings(term)
            candidate_doc_ids.update(postings.keys())

        if not candidate_doc_ids:
            return []

        # Score candidates
        scored_results: List[SearchResult] = []
        for doc_id in candidate_doc_ids:
            score = self.score_document(doc_id, query_terms, index, stats)
            if score > 0.0:
                scored_results.append(
                    SearchResult(
                        doc_id=doc_id,
                        score=score,
                        shard_id=shard_id,
                    )
                )

        # Deterministic sorting: score DESC, doc_id ASC for tie-breaking
        # SearchResult implements __lt__ with this logic
        scored_results.sort(key=lambda r: (-r.score, r.doc_id))

        if top_k > 0:
            return scored_results[:top_k]
        return scored_results
