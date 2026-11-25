"""Search performance benchmark script.

Evaluates search latency distributions (p50, p95, p99), QPS throughput,
and scatter-gather overhead across 1, 2, and 4 shard configurations with varying top_k values.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from concurrent import futures
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import grpc
from coordinator.query_dispatcher import QueryDispatcher
from coordinator.router import Router
from coordinator.shard_registry import ShardRegistry
from proto.generated import shard_pb2, shard_pb2_grpc
from services.shard_server import ShardServicer
from shared.config import ClusterConfig, ShardEndpoint
from shared.enums import ShardRole
from storage.database import DatabaseManager
from storage.shard_store import ShardStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("benchmark_search")


class BenchCluster:
    """Manages an in-process gRPC cluster of N shards for realistic benchmarking."""

    def __init__(self, num_shards: int, temp_dir: Path):
        self.num_shards = num_shards
        self.temp_dir = temp_dir
        self.servers: list[grpc.Server] = []
        self.db_managers: list[DatabaseManager] = []
        self.stores: list[ShardStore] = []
        self.servicers: list[ShardServicer] = []
        self.endpoints: dict[int, ShardEndpoint] = {}
        self.registry: ShardRegistry | None = None
        self.dispatcher: QueryDispatcher | None = None
        self.router = Router(num_shards=num_shards)

    def start(self) -> None:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        for shard_id in range(self.num_shards):
            db_path = self.temp_dir / f"bench_shard_{self.num_shards}_{shard_id}.db"
            if db_path.exists():
                for ext in ["", "-wal", "-shm"]:
                    p = self.temp_dir / f"{db_path.name}{ext}"
                    if p.exists():
                        p.unlink()

            db = DatabaseManager(str(db_path))
            db.initialize_schema()
            store = ShardStore(db, shard_id=shard_id)
            servicer = ShardServicer(shard_id=shard_id, store=store, role=ShardRole.PRIMARY)

            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer, server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()

            self.servers.append(server)
            self.db_managers.append(db)
            self.stores.append(store)
            self.servicers.append(servicer)
            self.endpoints[shard_id] = ShardEndpoint(
                primary_host="127.0.0.1",
                primary_port=port,
                replica_host="127.0.0.1",
                replica_port=0,
            )

        config = ClusterConfig(
            coordinator_host="127.0.0.1",
            coordinator_port=0,
            shard_count=self.num_shards,
            shard_endpoints=self.endpoints,
        )
        self.registry = ShardRegistry(config)
        self.dispatcher = QueryDispatcher(self.registry)

    def populate(self, corpus_path: Path) -> int:
        count = 0
        with open(corpus_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                doc_id = record["doc_id"]
                content = record["content"]
                shard_id = self.router.get_shard_id(doc_id)
                servicer = self.servicers[shard_id]
                req = shard_pb2.ShardInsertRequest(doc_id=doc_id, content=content)
                servicer.InsertDocument(req, None)
                count += 1
        return count

    def stop(self) -> None:
        if self.registry:
            self.registry.close()
        for s in self.servers:
            s.stop(grace=None)
        for db in self.db_managers:
            db.close()


def benchmark_search(
    corpus_path: Path,
    output_path: Path,
    temp_dir: Path,
    num_shards_list: list[int] = [1, 2, 4],
    top_k_list: list[int] = [5, 10, 20, 50],
    queries: list[str] | None = None,
    repetitions: int = 25,
) -> dict:
    if queries is None:
        queries = [
            "distributed systems concurrency",
            "search engine inverted index",
            "okapi bm25 ranking algorithm",
            "replication write ahead logging",
            "virtual memory operating systems",
            "network protocol transmission",
            "fault tolerance recovery checkpointing",
            "database consistency",
        ]

    temp_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    benchmark_data: dict[str, dict] = {}

    for num_shards in num_shards_list:
        logger.info("Setting up %d-shard live cluster...", num_shards)
        cluster = BenchCluster(num_shards, temp_dir)
        cluster.start()
        docs_loaded = cluster.populate(corpus_path)
        logger.info("Loaded %d documents across %d shards.", docs_loaded, num_shards)

        shard_key = f"{num_shards}_shards"
        benchmark_data[shard_key] = {}

        for top_k in top_k_list:
            latencies_ms: list[float] = []

            # Warmup
            for q in queries[:2]:
                cluster.dispatcher.dispatch_search(query=q, top_k=top_k)

            # Benchmark runs
            start_total = time.perf_counter()
            total_queries = 0

            for _ in range(repetitions):
                for q in queries:
                    t0 = time.perf_counter()
                    results = cluster.dispatcher.dispatch_search(query=q, top_k=top_k)
                    t_diff = time.perf_counter() - t0
                    latencies_ms.append(t_diff * 1000.0)
                    total_queries += 1

            total_elapsed = time.perf_counter() - start_total
            qps = total_queries / total_elapsed if total_elapsed > 0 else 0.0

            sorted_lat = sorted(latencies_ms)
            p50 = sorted_lat[int(0.50 * len(sorted_lat))]
            p95 = sorted_lat[int(0.95 * len(sorted_lat))]
            p99 = sorted_lat[int(0.99 * len(sorted_lat))]
            mean_lat = statistics.mean(latencies_ms)

            k_key = f"top_{top_k}"
            benchmark_data[shard_key][k_key] = {
                "num_shards": num_shards,
                "top_k": top_k,
                "total_queries": total_queries,
                "qps": round(qps, 2),
                "mean_latency_ms": round(mean_lat, 3),
                "p50_latency_ms": round(p50, 3),
                "p95_latency_ms": round(p95, 3),
                "p99_latency_ms": round(p99, 3),
            }

            logger.info(
                "Shards: %d | Top-K: %2d | QPS: %7.1f | p50: %6.3f ms | p95: %6.3f ms | p99: %6.3f ms",
                num_shards,
                top_k,
                qps,
                p50,
                p95,
                p99,
            )

        cluster.stop()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(benchmark_data, f, indent=2)

    logger.info("Search benchmark results successfully written to %s", output_path)
    return benchmark_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Run search performance benchmarks")
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("datasets/processed/wikitext_corpus.jsonl"),
        help="Path to processed JSONL dataset",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("experiments/results/search_benchmarks.json"),
        help="Path to save benchmark JSON results",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path("/tmp/search_engine_bench"),
        help="Temporary directory for benchmark shard databases",
    )
    parser.add_argument("--reps", type=int, default=25, help="Repetitions per query")
    args = parser.parse_args()

    benchmark_search(
        corpus_path=args.corpus,
        output_path=args.output,
        temp_dir=args.temp_dir,
        repetitions=args.reps,
    )


if __name__ == "__main__":
    main()
