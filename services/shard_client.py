"""gRPC client for communicating with individual Shard services."""

import grpc
from typing import List, Optional, Tuple
from proto.generated import shard_pb2, shard_pb2_grpc
from shared.models import RankingStatsSnapshot, SearchResult


class ShardClient:
    """Client wrapper for ShardService gRPC stub."""

    def __init__(self, target_address: str, timeout: float = 10.0):
        self.target_address = target_address
        self.timeout = timeout
        self.channel = grpc.insecure_channel(target_address)
        self.stub = shard_pb2_grpc.ShardServiceStub(self.channel)

    def insert_document(self, doc_id: str, content: str) -> shard_pb2.ShardMutationResponse:
        """Dispatch document insertion to primary shard."""
        request = shard_pb2.ShardInsertRequest(doc_id=doc_id, content=content)
        return self.stub.InsertDocument(request, timeout=self.timeout)

    def replace_document(
        self,
        doc_id: str,
        content: str,
        expected_version: int = 0,
    ) -> shard_pb2.ShardMutationResponse:
        """Dispatch document replacement to primary shard."""
        request = shard_pb2.ShardReplaceRequest(
            doc_id=doc_id,
            content=content,
            expected_version=expected_version,
        )
        return self.stub.ReplaceDocument(request, timeout=self.timeout)

    def delete_document(
        self,
        doc_id: str,
        expected_version: int = 0,
    ) -> shard_pb2.ShardMutationResponse:
        """Dispatch document deletion to primary shard."""
        request = shard_pb2.ShardDeleteRequest(
            doc_id=doc_id,
            expected_version=expected_version,
        )
        return self.stub.DeleteDocument(request, timeout=self.timeout)

    def get_document(self, doc_id: str) -> shard_pb2.GetDocumentResponse:
        """Fetch document from shard."""
        request = shard_pb2.GetDocumentRequest(doc_id=doc_id)
        return self.stub.GetDocument(request, timeout=self.timeout)

    def get_ranking_stats(self, terms: List[str]) -> shard_pb2.RankingStatsResponse:
        """Query shard for document frequency of terms and local corpus stats."""
        request = shard_pb2.RankingStatsRequest(terms=terms)
        return self.stub.GetRankingStats(request, timeout=self.timeout)

    def search_local(
        self,
        query: str,
        top_k: int,
        global_stats: RankingStatsSnapshot,
    ) -> shard_pb2.LocalSearchResponse:
        """Execute local BM25 scoring with consistent global stats snapshot."""
        proto_stats = shard_pb2.GlobalRankingSnapshot(
            total_documents=global_stats.total_documents,
            avg_document_length=global_stats.avg_document_length,
            document_frequencies=global_stats.document_frequencies,
            stats_version=global_stats.stats_version,
        )
        request = shard_pb2.LocalSearchRequest(
            query=query,
            top_k=top_k,
            global_stats=proto_stats,
        )
        return self.stub.SearchLocal(request, timeout=self.timeout)

    def get_shard_status(self) -> shard_pb2.ShardStatusResponse:
        """Fetch status and health."""
        request = shard_pb2.ShardStatusRequest()
        return self.stub.GetShardStatus(request, timeout=self.timeout)

    def get_latest_sequence(self) -> shard_pb2.LatestSequenceResponse:
        """Fetch latest committed sequence."""
        request = shard_pb2.LatestSequenceRequest()
        return self.stub.GetLatestSequence(request, timeout=self.timeout)

    def get_updates_after(
        self,
        since_sequence: int,
        max_events: int = 1000,
    ) -> shard_pb2.GetUpdatesAfterResponse:
        """Fetch update log range for incremental replication or recovery."""
        request = shard_pb2.GetUpdatesAfterRequest(
            since_sequence=since_sequence,
            max_events=max_events,
        )
        return self.stub.GetUpdatesAfter(request, timeout=self.timeout)

    def is_alive(self) -> bool:
        """Check if shard service is reachable."""
        try:
            self.get_shard_status()
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close gRPC channel."""
        self.channel.close()
