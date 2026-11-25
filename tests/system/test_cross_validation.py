"""System cross-validation test suite comparing the distributed cluster against the independent ReferenceEngine."""

import os
import tempfile
import grpc
import pytest
from concurrent import futures

from coordinator.query_dispatcher import QueryDispatcher
from coordinator.router import Router
from coordinator.server import CoordinatorServicer
from coordinator.shard_registry import ShardRegistry
from proto.generated import coordinator_pb2, shard_pb2, shard_pb2_grpc
from reference.reference_engine import ReferenceEngine
from replication.recovery_manager import RecoveryManager
from services.replica_client import ReplicaClient
from services.replica_server import ReplicaServicer
from services.shard_client import ShardClient
from services.shard_server import ShardServicer
from shared.config import ClusterConfig, ShardEndpoint
from shared.enums import OperationType, ReplicaStatus, ShardRole
from shared.models import UpdateEvent
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


@pytest.fixture
def verification_cluster():
    """Spin up 2-shard primary cluster with Coordinator and standalone ReferenceEngine."""
    fd0, p0 = tempfile.mkstemp(suffix="_val_p0.db")
    fd1, p1 = tempfile.mkstemp(suffix="_val_p1.db")
    os.close(fd0)
    os.close(fd1)

    db0 = DatabaseManager(p0)
    db0.initialize_schema()
    store0 = ShardStore(db0, shard_id=0)

    db1 = DatabaseManager(p1)
    db1.initialize_schema()
    store1 = ShardStore(db1, shard_id=1)

    # Shard 0 Primary Server
    s0 = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer0 = ShardServicer(shard_id=0, store=store0, role=ShardRole.PRIMARY)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer0, s0)
    port0 = s0.add_insecure_port("127.0.0.1:0")
    s0.start()

    # Shard 1 Primary Server
    s1 = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer1 = ShardServicer(shard_id=1, store=store1, role=ShardRole.PRIMARY)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer1, s1)
    port1 = s1.add_insecure_port("127.0.0.1:0")
    s1.start()

    # Coordinator
    endpoints = {
        0: ShardEndpoint(primary_host="127.0.0.1", primary_port=port0, replica_host="127.0.0.1", replica_port=0),
        1: ShardEndpoint(primary_host="127.0.0.1", primary_port=port1, replica_host="127.0.0.1", replica_port=0),
    }
    config = ClusterConfig(coordinator_host="127.0.0.1", coordinator_port=0, shard_count=2, shard_endpoints=endpoints)
    registry = ShardRegistry(config)
    router = Router(num_shards=2)
    dispatcher = QueryDispatcher(registry)
    coord = CoordinatorServicer(config, registry=registry, router=router, dispatcher=dispatcher)

    # Independent reference engine
    ref = ReferenceEngine(k1=1.2, b=0.75)

    yield coord, ref, (store0, store1), (port0, port1)

    registry.close()
    s0.stop(grace=None)
    s1.stop(grace=None)
    db0.close()
    db1.close()
    for p in [p0, p1]:
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(p + ext):
                os.remove(p + ext)


def test_distributed_vs_reference_ingestion_and_search(verification_cluster):
    coord, ref, _, _ = verification_cluster

    # Diverse test corpus
    documents = [
        ("doc_1", "distributed systems rely on consensus and replication algorithms"),
        ("doc_2", "information retrieval and search engines index large text documents"),
        ("doc_3", "python backend distributed database search engine architecture"),
        ("doc_4", "custom inverted index data structures using python dictionaries"),
        ("doc_5", "elasticsearch inspired distributed text search with incremental recovery"),
        ("doc_6", "probabilistic relevance ranking with okapi bm25 and term frequencies"),
        ("doc_7", "machine learning algorithms for natural language processing and search"),
        ("doc_8", "fault tolerant replication using monotonic per shard sequence logs"),
        ("doc_9", "sqlite write ahead logging mode for crash resilient persistence"),
        ("doc_10", "distributed scatter gather query processing and result merging"),
    ]

    # Ingest into both distributed cluster and reference engine
    for doc_id, content in documents:
        # Distributed
        resp = coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id=doc_id, content=content), None)
        assert resp.success, f"Failed to insert {doc_id} into distributed cluster"
        # Reference
        ref.insert_document(doc_id=doc_id, content=content, version=1)

    assert ref.total_active_documents() == len(documents)

    # Queries covering single-term, multi-term, rare, and frequent words
    queries = [
        "distributed search",
        "search engine",
        "algorithms",
        "replication",
        "python",
        "sqlite persistence",
        "okapi bm25",
        "consensus",
    ]

    for q in queries:
        dist_resp = coord.Search(coordinator_pb2.SearchRequest(query=q, top_k=10), None)
        ref_results = ref.search(query=q, top_k=10)

        # 1. Total hit count must match
        assert dist_resp.total_hits == len(ref_results), f"Hit count mismatch for query '{q}'"

        # 2. Exact ranking order and document IDs must match
        dist_ids = [item.doc_id for item in dist_resp.results]
        ref_ids = [item[0] for item in ref_results]
        assert dist_ids == ref_ids, f"Ranking order mismatch for query '{q}': dist={dist_ids}, ref={ref_ids}"

        # 3. BM25 scores must match within floating point precision (1e-5)
        for i in range(len(ref_results)):
            dist_score = dist_resp.results[i].score
            ref_score = ref_results[i][1]
            diff = abs(dist_score - ref_score)
            assert diff < 1e-5, f"Score discrepancy on query '{q}', doc '{dist_ids[i]}': dist={dist_score}, ref={ref_score}, diff={diff}"


def test_distributed_vs_reference_document_replacement(verification_cluster):
    coord, ref, _, _ = verification_cluster

    # Ingest doc
    coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id="d1", content="original content about databases"), None)
    ref.insert_document("d1", "original content about databases")

    # Replace with updated text
    coord.ReplaceDocument(coordinator_pb2.ReplaceRequest(doc_id="d1", content="updated text about distributed search", expected_version=1), None)
    ref.replace_document("d1", "updated text about distributed search", expected_version=1)

    # Search for "databases" -> 0 hits in both
    dist_dbs = coord.Search(coordinator_pb2.SearchRequest(query="databases", top_k=5), None)
    ref_dbs = ref.search("databases")
    assert dist_dbs.total_hits == 0
    assert len(ref_dbs) == 0

    # Search for "search" -> 1 hit in both with equal score
    dist_search = coord.Search(coordinator_pb2.SearchRequest(query="search", top_k=5), None)
    ref_search = ref.search("search")
    assert dist_search.total_hits == 1
    assert len(ref_search) == 1
    assert abs(dist_search.results[0].score - ref_search[0][1]) < 1e-5


def test_distributed_vs_reference_deletion_and_tombstones(verification_cluster):
    coord, ref, _, _ = verification_cluster

    coord.InsertDocument(coordinator_pb2.InsertRequest(doc_id="del_doc", content="temporary test document"), None)
    ref.insert_document("del_doc", "temporary test document")

    # Delete
    coord.DeleteDocument(coordinator_pb2.DeleteRequest(doc_id="del_doc", expected_version=1), None)
    ref.delete_document("del_doc", expected_version=1)

    # Must not appear in search
    assert coord.Search(coordinator_pb2.SearchRequest(query="temporary", top_k=5), None).total_hits == 0
    assert len(ref.search("temporary")) == 0

    # Tombstone version check
    ref_doc = ref.get_document("del_doc", include_deleted=True)
    assert ref_doc["is_deleted"] is True
    assert ref_doc["version"] == 2


def test_post_recovery_convergence_with_reference():
    """Verify that after replica incremental recovery, replica converges 100% with ReferenceEngine."""
    fd_p, p_path = tempfile.mkstemp(suffix="_conv_p.db")
    fd_r, r_path = tempfile.mkstemp(suffix="_conv_r.db")
    os.close(fd_p)
    os.close(fd_r)

    db_p = DatabaseManager(p_path)
    db_p.initialize_schema()
    store_p = ShardStore(db_p, shard_id=0)

    db_r = DatabaseManager(r_path)
    db_r.initialize_schema()
    store_r = ShardStore(db_r, shard_id=0)

    # Start Primary Server
    s_p = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    shard_pb2_grpc.add_ShardServiceServicer_to_server(ShardServicer(shard_id=0, store=store_p, role=ShardRole.PRIMARY), s_p)
    port_p = s_p.add_insecure_port("127.0.0.1:0")
    s_p.start()

    # Start Replica Server
    s_r = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    from proto.generated import replication_pb2_grpc
    replication_pb2_grpc.add_ReplicaServiceServicer_to_server(ReplicaServicer(shard_id=0, replica_id="rep_0", store=store_r), s_r)
    port_r = s_r.add_insecure_port("127.0.0.1:0")
    s_r.start()

    client_p = ShardClient(f"127.0.0.1:{port_p}")
    client_r = ReplicaClient(f"127.0.0.1:{port_r}")
    ref = ReferenceEngine()

    # 1. Ingest 5 initial documents into both Primary, Replica, and ReferenceEngine
    for i in range(1, 6):
        doc_id = f"doc_{i}"
        content = f"cluster document number {i} with searchable content"
        resp = client_p.insert_document(doc_id, content)
        client_r.apply_update(UpdateEvent(shard_id=0, sequence_number=resp.sequence_number, doc_id=doc_id, operation=OperationType.INSERT, version=1, payload=f'{{"content": "{content}"}}'))
        ref.insert_document(doc_id, content)

    # 2. Disconnect replica and execute 10 more mutations on Primary and ReferenceEngine
    for i in range(6, 16):
        doc_id = f"doc_{i}"
        content = f"missed during disconnection article {i} advanced search"
        client_p.insert_document(doc_id, content)
        ref.insert_document(doc_id, content)

    # Replace doc_1
    client_p.replace_document("doc_1", "updated cluster document number 1")
    ref.replace_document("doc_1", "updated cluster document number 1")

    # Delete doc_2
    client_p.delete_document("doc_2")
    ref.delete_document("doc_2")

    # 3. Perform incremental recovery
    rec_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    result = rec_mgr.recover()
    assert result.success
    assert result.status == ReplicaStatus.SYNCHRONIZED

    # 4. Compare recovered replica state directly against ReferenceEngine
    recovered_mgr = store_r.reconstruct_in_memory_state()

    # Total active documents must match
    assert recovered_mgr.index.total_documents == ref.total_active_documents()

    # doc_2 must be deleted in both
    assert not recovered_mgr.has_document("doc_2")
    assert ref.get_document("doc_2") is None
    assert recovered_mgr.get_document("doc_2", include_deleted=True).is_deleted is True

    # Search on recovered replica's index must match ReferenceEngine exactly
    from indexing.bm25 import BM25Scorer
    scorer = BM25Scorer()
    replica_results = scorer.search("advanced search", recovered_mgr.index, top_k=10)
    ref_results = ref.search("advanced search", top_k=10)

    assert len(replica_results) == len(ref_results)
    for k in range(len(ref_results)):
        assert replica_results[k].doc_id == ref_results[k][0]
        assert abs(replica_results[k].score - ref_results[k][1]) < 1e-5

    # Teardown
    client_p.close()
    client_r.close()
    s_p.stop(grace=None)
    s_r.stop(grace=None)
    db_p.close()
    db_r.close()
    for p in [p_path, r_path]:
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(p + ext):
                os.remove(p + ext)
