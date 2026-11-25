"""Integration tests for Sharding, Router, and Coordinator scatter-gather search."""

import os
import tempfile
import time
import grpc
import pytest
from concurrent import futures

from coordinator.query_dispatcher import QueryDispatcher
from coordinator.router import Router
from coordinator.server import CoordinatorServicer
from coordinator.shard_registry import ShardRegistry
from proto.generated import coordinator_pb2, shard_pb2, shard_pb2_grpc
from services.shard_server import ShardServicer
from shared.config import ClusterConfig, ShardEndpoint
from shared.enums import ErrorCode, ShardRole
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


@pytest.fixture
def two_shard_cluster():
    """Spin up two primary shard gRPC servers on ephemeral ports and return coordinator servicer."""
    # Create temp databases for Shard 0 and Shard 1
    fd0, path0 = tempfile.mkstemp(suffix="_shard0.db")
    fd1, path1 = tempfile.mkstemp(suffix="_shard1.db")
    os.close(fd0)
    os.close(fd1)

    db0 = DatabaseManager(path0)
    db0.initialize_schema()
    store0 = ShardStore(db0, shard_id=0)

    db1 = DatabaseManager(path1)
    db1.initialize_schema()
    store1 = ShardStore(db1, shard_id=1)

    # Start Shard 0 Primary gRPC Server
    server0 = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer0 = ShardServicer(shard_id=0, store=store0, role=ShardRole.PRIMARY)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer0, server0)
    port0 = server0.add_insecure_port("127.0.0.1:0")
    server0.start()

    # Start Shard 1 Primary gRPC Server
    server1 = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer1 = ShardServicer(shard_id=1, store=store1, role=ShardRole.PRIMARY)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer1, server1)
    port1 = server1.add_insecure_port("127.0.0.1:0")
    server1.start()

    # Configure cluster topology
    endpoints = {
        0: ShardEndpoint(primary_host="127.0.0.1", primary_port=port0, replica_host="127.0.0.1", replica_port=0),
        1: ShardEndpoint(primary_host="127.0.0.1", primary_port=port1, replica_host="127.0.0.1", replica_port=0),
    }
    config = ClusterConfig(
        coordinator_host="127.0.0.1",
        coordinator_port=0,
        shard_count=2,
        shard_endpoints=endpoints,
    )

    registry = ShardRegistry(config)
    router = Router(num_shards=2)
    dispatcher = QueryDispatcher(registry)
    coord_servicer = CoordinatorServicer(config, registry=registry, router=router, dispatcher=dispatcher)

    yield coord_servicer, (servicer0, servicer1), (port0, port1)

    # Teardown
    registry.close()
    server0.stop(grace=None)
    server1.stop(grace=None)
    db0.close()
    db1.close()
    for p in [path0, path1]:
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(p + ext):
                os.remove(p + ext)


def test_coordinator_insert_and_routing(two_shard_cluster):
    coord, (s0, s1), _ = two_shard_cluster

    # Insert document via coordinator
    resp1 = coord.InsertDocument(
        coordinator_pb2.InsertRequest(doc_id="doc_alpha", content="distributed systems search engine"),
        None,
    )
    assert resp1.success
    assert resp1.sequence_number == 1
    assert resp1.version == 1

    resp2 = coord.InsertDocument(
        coordinator_pb2.InsertRequest(doc_id="doc_beta", content="python distributed databases"),
        None,
    )
    assert resp2.success

    # Check total documents across cluster
    status = coord.GetClusterStatus(coordinator_pb2.ClusterStatusRequest(), None)
    assert status.total_cluster_documents == 2
    assert status.is_healthy


def test_coordinator_crud_lifecycle(two_shard_cluster):
    coord, _, _ = two_shard_cluster

    # Insert
    ins = coord.InsertDocument(
        coordinator_pb2.InsertRequest(doc_id="d100", content="initial content"),
        None,
    )
    assert ins.success

    # Replace
    rep = coord.ReplaceDocument(
        coordinator_pb2.ReplaceRequest(doc_id="d100", content="updated content", expected_version=1),
        None,
    )
    assert rep.success
    assert rep.version == 2

    # Delete
    dlt = coord.DeleteDocument(
        coordinator_pb2.DeleteRequest(doc_id="d100", expected_version=2),
        None,
    )
    assert dlt.success
    assert dlt.version == 3

    # Stale insert should fail
    stale_ins = coord.InsertDocument(
        coordinator_pb2.InsertRequest(doc_id="d100", content="resurrect attempt"),
        None,
    )
    assert not stale_ins.success
    assert stale_ins.error_code == ErrorCode.TOMBSTONE_CONFLICT.value


def test_distributed_scatter_gather_search(two_shard_cluster):
    coord, (s0, s1), _ = two_shard_cluster

    # Insert documents across both shards
    docs = [
        ("d1", "distributed search engine indexing"),
        ("d2", "distributed database systems"),
        ("d3", "information retrieval and search systems"),
        ("d4", "unrelated machine learning algorithms"),
    ]
    for doc_id, text in docs:
        resp = coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id=doc_id, content=text), None)
        assert resp.success

    # Search for "distributed search"
    search_resp = coord.Search(coordinator_pb2.SearchRequest(query="distributed search", top_k=5), None)
    assert search_resp.total_hits >= 2

    # The top document should be d1 because it matches both "distributed" and "search"
    top_doc = search_resp.results[0]
    assert top_doc.doc_id == "d1"
    assert top_doc.score > search_resp.results[1].score

    # Unrelated document d4 should not be in the results
    result_ids = [r.doc_id for r in search_resp.results]
    assert "d4" not in result_ids
    assert "d1" in result_ids
    assert "d2" in result_ids
    assert "d3" in result_ids


def test_deterministic_tie_breaking_across_shards(two_shard_cluster):
    coord, _, _ = two_shard_cluster

    # Insert two documents with identical content
    coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id="doc_z", content="common query term"), None)
    coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id="doc_a", content="common query term"), None)

    search_resp = coord.Search(coordinator_pb2.SearchRequest(query="common query term", top_k=5), None)
    assert search_resp.total_hits == 2
    # Identical score -> doc_a precedes doc_z
    assert search_resp.results[0].doc_id == "doc_a"
    assert search_resp.results[1].doc_id == "doc_z"
