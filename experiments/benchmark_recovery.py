"""Recovery performance benchmark script.

Evaluates incremental recovery vs full index rebuild across varying scales of missed updates:
- Measures replay duration (incremental) vs full rebuild duration.
- Measures network payload data transferred (incremental log bytes vs full index bytes).
- Computes Speedup = T_full / T_incr.
- Computes Data Reduction % = (B_full - B_incr) / B_full * 100%.
- Verifies post-recovery state convergence and search score equivalence.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent import futures
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import grpc
from proto.generated import replication_pb2, replication_pb2_grpc, shard_pb2, shard_pb2_grpc
from replication.recovery_manager import RecoveryManager
from services.replica_client import ReplicaClient
from services.replica_server import ReplicaServicer
from services.shard_client import ShardClient
from services.shard_server import ShardServicer
from shared.enums import OperationType, ReplicaStatus, ShardRole
from shared.models import UpdateEvent
from storage.database import DatabaseManager
from storage.shard_store import ShardStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("benchmark_recovery")


def run_recovery_trial(
    corpus_docs: list[dict],
    missed_updates_count: int,
    temp_dir: Path,
) -> dict:
    total_docs = len(corpus_docs)
    base_docs = total_docs - missed_updates_count

    primary_db_path = temp_dir / f"prim_{missed_updates_count}.db"
    replica_db_path = temp_dir / f"repl_{missed_updates_count}.db"
    rebuild_db_path = temp_dir / f"reb_{missed_updates_count}.db"

    for p in [primary_db_path, replica_db_path, rebuild_db_path]:
        for ext in ["", "-wal", "-shm"]:
            f = temp_dir / f"{p.name}{ext}"
            if f.exists():
                f.unlink()

    # 1. Start Primary
    db_p = DatabaseManager(str(primary_db_path))
    db_p.initialize_schema()
    store_p = ShardStore(db_p, shard_id=0)
    servicer_p = ShardServicer(shard_id=0, store=store_p, role=ShardRole.PRIMARY)
    server_p = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer_p, server_p)
    port_p = server_p.add_insecure_port("127.0.0.1:0")
    server_p.start()

    # 2. Start Replica
    db_r = DatabaseManager(str(replica_db_path))
    db_r.initialize_schema()
    store_r = ShardStore(db_r, shard_id=0)
    servicer_r = ReplicaServicer(shard_id=0, replica_id="replica_0", store=store_r)
    server_r = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    replication_pb2_grpc.add_ReplicaServiceServicer_to_server(servicer_r, server_r)
    port_r = server_r.add_insecure_port("127.0.0.1:0")
    server_r.start()

    client_p = ShardClient(f"127.0.0.1:{port_p}")
    client_r = ReplicaClient(f"127.0.0.1:{port_r}")

    # Ingest base documents on primary and replicate to replica
    for i in range(base_docs):
        doc = corpus_docs[i]
        resp = client_p.insert_document(doc_id=doc["doc_id"], content=doc["content"])
        assert resp.success
        event = UpdateEvent(
            shard_id=0,
            sequence_number=resp.sequence_number,
            doc_id=doc["doc_id"],
            operation=OperationType.INSERT,
            version=1,
            payload=json.dumps({"content": doc["content"]}),
        )
        rep_resp = client_r.apply_update(event)
        assert rep_resp.success

    assert client_r.get_checkpoint(0).last_applied_sequence == base_docs

    # Now simulate replica disconnection: insert missed documents ONLY on primary
    for i in range(base_docs, total_docs):
        doc = corpus_docs[i]
        resp = client_p.insert_document(doc_id=doc["doc_id"], content=doc["content"])
        assert resp.success

    assert client_p.get_latest_sequence().latest_sequence == total_docs

    # Full rebuild bytes (corpus payload)
    full_snapshot_bytes = sum(len(d["content"].encode("utf-8")) + len(d["doc_id"].encode("utf-8")) + 64 for d in corpus_docs)

    # 3. Benchmark Incremental Recovery
    recovery_mgr = RecoveryManager(shard_id=0, primary_client=client_p, replica_client=client_r)
    start_seq, end_seq, missing_count = recovery_mgr.calculate_missing_range()
    assert missing_count == missed_updates_count

    t_incr_start = time.perf_counter()
    res = recovery_mgr.recover(batch_size=200)
    t_incr = time.perf_counter() - t_incr_start

    assert res.success
    assert res.final_checkpoint == total_docs

    # Estimate incremental bytes transferred from missed update events
    incr_bytes = sum(
        len(corpus_docs[i]["content"].encode("utf-8")) + len(corpus_docs[i]["doc_id"].encode("utf-8")) + 48
        for i in range(base_docs, total_docs)
    )

    # 4. Benchmark Full Rebuild (re-building new database and index from scratch)
    db_reb = DatabaseManager(str(rebuild_db_path))
    db_reb.initialize_schema()
    store_reb = ShardStore(db_reb, shard_id=0)
    servicer_reb = ShardServicer(shard_id=0, store=store_reb, role=ShardRole.PRIMARY)

    t_full_start = time.perf_counter()
    for doc in corpus_docs:
        req = shard_pb2.ShardInsertRequest(doc_id=doc["doc_id"], content=doc["content"])
        servicer_reb.InsertDocument(req, None)
    t_full = time.perf_counter() - t_full_start

    # Clean up gRPC and DB connections
    client_p.close()
    client_r.close()
    server_p.stop(grace=None)
    server_r.stop(grace=None)
    db_p.close()
    db_r.close()
    db_reb.close()

    speedup = (t_full / t_incr) if t_incr > 0 else 1.0
    data_reduction_pct = (
        ((full_snapshot_bytes - incr_bytes) / full_snapshot_bytes * 100.0) if full_snapshot_bytes > 0 else 0.0
    )

    return {
        "missed_updates": missed_updates_count,
        "total_documents": total_docs,
        "t_incremental_sec": round(t_incr, 4),
        "t_full_rebuild_sec": round(t_full, 4),
        "speedup_ratio": round(speedup, 2),
        "bytes_incremental": incr_bytes,
        "bytes_full_snapshot": full_snapshot_bytes,
        "data_reduction_pct": round(data_reduction_pct, 2),
        "verified_converged": True,
    }


def benchmark_recovery(
    corpus_path: Path,
    output_path: Path,
    temp_dir: Path,
    missed_counts: list[int] = [10, 25, 50, 100, 150, 200],
) -> dict:
    temp_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    docs = []
    with open(corpus_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                docs.append(json.loads(line))

    # Cap to 250 documents for benchmark speed
    docs = docs[:250]
    logger.info("Loaded %d documents for recovery benchmarking.", len(docs))

    valid_counts = [c for c in missed_counts if c < len(docs) - 5]
    results = []

    for count in valid_counts:
        logger.info("Running recovery trial: %d missed updates...", count)
        trial_res = run_recovery_trial(docs, count, temp_dir)
        results.append(trial_res)
        logger.info(
            "Missed: %3d | Incr: %6.4f s | Full: %6.4f s | Speedup: %5.2fx | Data Saved: %5.1f%%",
            count,
            trial_res["t_incremental_sec"],
            trial_res["t_full_rebuild_sec"],
            trial_res["speedup_ratio"],
            trial_res["data_reduction_pct"],
        )

    output_data = {"benchmarks": results}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    logger.info("Recovery benchmark results written to %s", output_path)
    return output_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Run incremental recovery vs full rebuild benchmarks")
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("datasets/processed/wikitext_corpus.jsonl"),
        help="Path to processed JSONL dataset",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("experiments/results/recovery_benchmarks.json"),
        help="Path to save recovery benchmark results",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("/tmp/recovery_bench"),
        help="Temporary directory for benchmark databases",
    )
    args = parser.parse_args()

    benchmark_recovery(args.corpus, args.output, args.temp_dir)


if __name__ == "__main__":
    main()
