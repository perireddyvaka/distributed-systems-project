"""Two-phase scatter-gather query dispatcher with consistent global ranking statistics."""

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from coordinator.ranking_stats import StatsAggregator
from coordinator.result_merger import ResultMerger
from coordinator.shard_registry import ShardRegistry
from indexing.tokenizer import Tokenizer
from shared.models import SearchResult


class QueryDispatcher:
    """Orchestrates Phase 1 (Stats Gathering) and Phase 2 (Scattered Retrieval)."""

    def __init__(self, registry: ShardRegistry):
        self.registry = registry

    def dispatch_search(
        self,
        query: str,
        top_k: int = 10,
        timeout: float = 5.0,
    ) -> List[SearchResult]:
        """Execute two-phase distributed scatter-gather search.
        
        Phase 1: Query all primary shards for local document frequencies and lengths
                 to synthesize a consistent GlobalRankingSnapshot.
        Phase 2: Broadcast the query and global snapshot to all primary shards,
                 collect local top-k candidate lists, and merge with deterministic tie-breaking.
        """
        query_terms = Tokenizer.tokenize(query)
        if not query_terms:
            return []

        primary_clients = self.registry.get_all_primary_clients()
        if not primary_clients:
            return []

        # Phase 1: Collect consistent global statistics snapshot
        global_stats = StatsAggregator.collect_global_stats(
            terms=query_terms,
            primary_clients=primary_clients,
            timeout=timeout,
        )

        if global_stats.total_documents == 0:
            return []

        # Phase 2: Dispatch local search queries concurrently
        def _search_shard(shard_id_client):
            shard_id, client = shard_id_client
            try:
                resp = client.search_local(query=query, top_k=top_k, global_stats=global_stats)
                results = []
                for item in resp.results:
                    results.append(
                        SearchResult(
                            doc_id=item.doc_id,
                            score=item.score,
                            shard_id=item.shard_id,
                            snippet=item.snippet,
                        )
                    )
                return results
            except Exception:
                return []

        with ThreadPoolExecutor(max_workers=len(primary_clients)) as executor:
            batch_results = list(executor.map(_search_shard, primary_clients.items(), timeout=timeout))

        # Phase 3: Global Top-K merge with deterministic tie-breaking
        return ResultMerger.merge(batch_results, top_k=top_k)
