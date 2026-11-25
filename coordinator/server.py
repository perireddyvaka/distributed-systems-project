"""Coordinator gRPC Server coordinating document routing, CRUD dispatch, and scatter-gather search."""

import time
import grpc
from concurrent import futures
from typing import Optional
from proto.generated import coordinator_pb2, coordinator_pb2_grpc
from coordinator.query_dispatcher import QueryDispatcher
from coordinator.router import Router
from coordinator.shard_registry import ShardRegistry
from shared.config import ClusterConfig
from shared.enums import ErrorCode


class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServiceServicer):
    """Implementation of CoordinatorService gRPC contract."""

    def __init__(
        self,
        config: ClusterConfig,
        registry: Optional[ShardRegistry] = None,
        router: Optional[Router] = None,
        dispatcher: Optional[QueryDispatcher] = None,
    ):
        self.config = config
        self.registry = registry if registry is not None else ShardRegistry(config)
        self.router = router if router is not None else Router(config.shard_count)
        self.dispatcher = dispatcher if dispatcher is not None else QueryDispatcher(self.registry)

    def InsertDocument(self, request, context):
        """Route document to owning shard and dispatch insertion."""
        if not request.doc_id:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INVALID_ARGUMENT.value,
                error_message="doc_id cannot be empty",
            )
        try:
            shard_id = self.router.get_shard_id(request.doc_id)
            client = self.registry.get_primary_client(shard_id)
            resp = client.insert_document(request.doc_id, request.content)
            return coordinator_pb2.MutationResponse(
                success=resp.success,
                shard_id=resp.shard_id,
                sequence_number=resp.sequence_number,
                version=resp.version,
                error_code=resp.error_code,
                error_message=resp.error_message,
            )
        except Exception as e:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def ReplaceDocument(self, request, context):
        """Route document to owning shard and dispatch replacement."""
        if not request.doc_id:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INVALID_ARGUMENT.value,
                error_message="doc_id cannot be empty",
            )
        try:
            shard_id = self.router.get_shard_id(request.doc_id)
            client = self.registry.get_primary_client(shard_id)
            resp = client.replace_document(request.doc_id, request.content, request.expected_version)
            return coordinator_pb2.MutationResponse(
                success=resp.success,
                shard_id=resp.shard_id,
                sequence_number=resp.sequence_number,
                version=resp.version,
                error_code=resp.error_code,
                error_message=resp.error_message,
            )
        except Exception as e:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def DeleteDocument(self, request, context):
        """Route document to owning shard and dispatch deletion."""
        if not request.doc_id:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INVALID_ARGUMENT.value,
                error_message="doc_id cannot be empty",
            )
        try:
            shard_id = self.router.get_shard_id(request.doc_id)
            client = self.registry.get_primary_client(shard_id)
            resp = client.delete_document(request.doc_id, request.expected_version)
            return coordinator_pb2.MutationResponse(
                success=resp.success,
                shard_id=resp.shard_id,
                sequence_number=resp.sequence_number,
                version=resp.version,
                error_code=resp.error_code,
                error_message=resp.error_message,
            )
        except Exception as e:
            return coordinator_pb2.MutationResponse(
                success=False,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def Search(self, request, context):
        """Execute two-phase scatter-gather search across cluster."""
        t0 = time.time()
        top_k = request.top_k if request.top_k > 0 else 10
        try:
            results = self.dispatcher.dispatch_search(request.query, top_k=top_k)
            exec_time = (time.time() - t0) * 1000.0

            proto_items = [
                coordinator_pb2.SearchResultItem(
                    doc_id=r.doc_id,
                    score=r.score,
                    shard_id=r.shard_id,
                    version=r.version,
                    snippet=r.snippet,
                )
                for r in results
            ]
            return coordinator_pb2.SearchResponse(
                results=proto_items,
                total_hits=len(results),
                execution_time_ms=exec_time,
                error_code=ErrorCode.OK.value,
            )
        except Exception as e:
            return coordinator_pb2.SearchResponse(
                results=[],
                total_hits=0,
                execution_time_ms=(time.time() - t0) * 1000.0,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def GetClusterStatus(self, request, context):
        """Aggregate health status of all primary and replica shards."""
        statuses = self.registry.get_cluster_status()
        total_cluster_docs = sum(s.doc_count for s in statuses)
        is_healthy = all(s.is_primary_alive for s in statuses)

        shard_infos = [
            coordinator_pb2.ShardInfo(
                shard_id=s.shard_id,
                primary_address=s.primary_address,
                replica_address=s.replica_address,
                latest_sequence=s.latest_sequence,
                replica_applied_sequence=s.replica_applied_sequence,
                replication_lag=s.replication_lag,
                replica_status=s.replica_status.value if s.replica_status else "DISCONNECTED",
                is_primary_alive=s.is_primary_alive,
                is_replica_alive=s.is_replica_alive,
                total_documents=s.doc_count,
            )
            for s in statuses
        ]

        return coordinator_pb2.ClusterStatusResponse(
            shards=shard_infos,
            total_cluster_documents=total_cluster_docs,
            is_healthy=is_healthy,
        )

    def TriggerRecovery(self, request, context):
        """Coordination endpoint for replica recovery (detailed in M6)."""
        return coordinator_pb2.RecoveryResponse(
            success=True,
            shard_id=request.shard_id,
            status="RECOVERING",
            replayed_events=0,
        )


def create_coordinator_server(config: ClusterConfig, max_workers: int = 10) -> grpc.Server:
    """Factory creating and configuring Coordinator gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = CoordinatorServicer(config)
    coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"{config.coordinator_host}:{config.coordinator_port}")
    return server
