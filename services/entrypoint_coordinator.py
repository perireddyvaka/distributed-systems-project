"""Container entrypoint for the Coordinator microservice."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from concurrent import futures
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import grpc
from coordinator.server import CoordinatorServicer
from proto.generated import coordinator_pb2_grpc
from shared.config import ClusterConfig, ShardEndpoint

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("coordinator_entrypoint")


def build_cluster_config() -> ClusterConfig:
    """Build cluster configuration from environment variables."""
    port = int(os.environ.get("COORDINATOR_PORT", "50050"))
    shard_count = int(os.environ.get("SHARD_COUNT", "2"))
    endpoints: dict[int, ShardEndpoint] = {}

    # Support JSON-encoded endpoints if provided
    endpoints_json = os.environ.get("SHARD_ENDPOINTS_JSON")
    if endpoints_json:
        data = json.loads(endpoints_json)
        for s_id_str, ep_dict in data.items():
            endpoints[int(s_id_str)] = ShardEndpoint(**ep_dict)
    else:
        # Standard convention: SHARD_{i}_PRIMARY_HOST, etc.
        for i in range(shard_count):
            p_host = os.environ.get(f"SHARD_{i}_PRIMARY_HOST", f"shard{i}-primary")
            p_port = int(os.environ.get(f"SHARD_{i}_PRIMARY_PORT", str(50051 + i)))
            r_host = os.environ.get(f"SHARD_{i}_REPLICA_HOST", f"shard{i}-replica")
            r_port = int(os.environ.get(f"SHARD_{i}_REPLICA_PORT", str(50061 + i)))
            endpoints[i] = ShardEndpoint(
                primary_host=p_host,
                primary_port=p_port,
                replica_host=r_host,
                replica_port=r_port,
            )

    return ClusterConfig(
        coordinator_host="0.0.0.0",
        coordinator_port=port,
        shard_count=shard_count,
        shard_endpoints=endpoints,
    )


def run_coordinator_service() -> None:
    config = build_cluster_config()
    max_workers = int(os.environ.get("MAX_WORKERS", "15"))

    logger.info("Starting Coordinator Service on 0.0.0.0:%d with %d shards...", config.coordinator_port, config.shard_count)
    for shard_id, ep in config.shard_endpoints.items():
        logger.info("  - Shard %d: Primary -> %s:%d | Replica -> %s:%d", shard_id, ep.primary_host, ep.primary_port, ep.replica_host, ep.replica_port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = CoordinatorServicer(config)
    coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{config.coordinator_port}")
    server.start()
    logger.info("Coordinator online and listening on 0.0.0.0:%d", config.coordinator_port)

    # Graceful shutdown handler
    stop_event = threading.Event()

    def _sig_handler(sig, frame):
        logger.info("Received signal %s, initiating graceful shutdown of Coordinator...", sig)
        server.stop(grace=5.0)
        servicer.registry.close()
        stop_event.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    stop_event.wait()
    logger.info("Coordinator terminated cleanly.")


if __name__ == "__main__":
    run_coordinator_service()
