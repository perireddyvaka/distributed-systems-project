"""Result merger for global top-k scatter-gather query aggregation."""

from typing import List, Sequence
from shared.models import SearchResult


class ResultMerger:
    """Merges and ranks local top-k candidate lists with deterministic tie-breaking."""

    @staticmethod
    def merge(
        shard_result_batches: Sequence[List[SearchResult]],
        top_k: int = 10,
    ) -> List[SearchResult]:
        """Merge multiple shard result lists and apply deterministic tie-breaking.
        
        Ordering:
            1. score DESC (highest score first)
            2. doc_id ASC (lexicographical tie-breaker for identical scores)
        """
        all_results: List[SearchResult] = []
        for batch in shard_result_batches:
            all_results.extend(batch)

        # Deterministic sorting: -score, then doc_id
        all_results.sort(key=lambda r: (-r.score, r.doc_id))

        if top_k > 0:
            return all_results[:top_k]
        return all_results
