"""Aggregates local shard ranking statistics into a consistent global snapshot."""

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Sequence
from services.shard_client import ShardClient
from shared.models import RankingStatsSnapshot


class StatsAggregator:
    """Collects and aggregates ranking statistics across all primary shards."""

    @staticmethod
    def collect_global_stats(
        terms: Sequence[str],
        primary_clients: Dict[int, ShardClient],
        timeout: float = 5.0,
    ) -> RankingStatsSnapshot:
        """Query all primary shards in parallel and compute global BM25 parameters."""
        unique_terms = list(set(terms))
        if not primary_clients or not unique_terms:
            return RankingStatsSnapshot()

        def _fetch_stats(shard_id_client):
            shard_id, client = shard_id_client
            return client.get_ranking_stats(unique_terms)

        with ThreadPoolExecutor(max_workers=len(primary_clients)) as executor:
            responses = list(executor.map(_fetch_stats, primary_clients.items(), timeout=timeout))

        global_total_docs = 0
        global_total_length = 0
        global_doc_frequencies: Dict[str, int] = {term: 0 for term in unique_terms}

        for resp in responses:
            global_total_docs += resp.total_documents
            global_total_length += resp.total_length
            for stat in resp.term_stats:
                if stat.term in global_doc_frequencies:
                    global_doc_frequencies[stat.term] += stat.doc_frequency

        avg_doc_length = (global_total_length / global_total_docs) if global_total_docs > 0 else 0.0

        return RankingStatsSnapshot(
            total_documents=global_total_docs,
            avg_document_length=avg_doc_length,
            document_frequencies=global_doc_frequencies,
            stats_version=1,
        )
