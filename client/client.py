"""High-level Python client library for the Distributed Search Engine."""

from __future__ import annotations

import logging
from typing import Any, Optional

import grpc
from proto.generated import coordinator_pb2, coordinator_pb2_grpc

logger = logging.getLogger("SearchEngineClient")


class SearchEngineClient:
    """Client for interacting with the Distributed Search Coordinator."""

    def __init__(self, target: str = "127.0.0.1:50050", timeout: float = 10.0):
        self.target = target
        self.timeout = timeout
        self.channel = grpc.insecure_channel(target)
        self.stub = coordinator_pb2_grpc.CoordinatorServiceStub(self.channel)

    def insert(self, doc_id: str, content: str) -> coordinator_pb2.MutationResponse:
        """Insert a document into the distributed cluster."""
        req = coordinator_pb2.InsertRequest(doc_id=doc_id, content=content)
        return self.stub.InsertDocument(req, timeout=self.timeout)

    def replace(self, doc_id: str, content: str, expected_version: int) -> coordinator_pb2.MutationResponse:
        """Replace an existing document with monotonic version check."""
        req = coordinator_pb2.ReplaceRequest(
            doc_id=doc_id,
            content=content,
            expected_version=expected_version,
        )
        return self.stub.ReplaceDocument(req, timeout=self.timeout)

    def delete(self, doc_id: str, expected_version: int) -> coordinator_pb2.MutationResponse:
        """Delete a document creating a versioned tombstone."""
        req = coordinator_pb2.DeleteRequest(
            doc_id=doc_id,
            expected_version=expected_version,
        )
        return self.stub.DeleteDocument(req, timeout=self.timeout)

    def search(self, query: str, top_k: int = 10) -> coordinator_pb2.SearchResponse:
        """Execute distributed scatter-gather search with consistent BM25 ranking."""
        req = coordinator_pb2.SearchRequest(query=query, top_k=top_k)
        return self.stub.Search(req, timeout=self.timeout)

    def get_cluster_status(self) -> coordinator_pb2.ClusterStatusResponse:
        """Query cluster topology, shard health, sequences, and replication lag."""
        req = coordinator_pb2.ClusterStatusRequest()
        return self.stub.GetClusterStatus(req, timeout=self.timeout)

    def trigger_recovery(self, shard_id: int) -> coordinator_pb2.RecoveryResponse:
        """Trigger incremental recovery for a replica shard."""
        req = coordinator_pb2.RecoveryRequest(shard_id=shard_id)
        return self.stub.TriggerRecovery(req, timeout=self.timeout)

    def close(self) -> None:
        """Close gRPC channel."""
        self.channel.close()

    def __enter__(self) -> SearchEngineClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
