"""Container entrypoint for Primary and Replica Shard microservices."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import grpc
from proto.generated import replication_pb2_grpc, shard_pb2_grpc
from services.replica_server import ReplicaServicer
from services.shard_server import ShardServicer
from shared.enums import ShardRole
from storage.database import DatabaseManager
from storage.shard_store import ShardStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("shard_entrypoint")


def run_shard_service() -> None:
    shard_id = int(os.environ.get("SHARD_ID", "0"))
    role_str = os.environ.get("ROLE", "PRIMARY").upper()
    role = ShardRole[role_str]
    port = int(os.environ.get("PORT", "50051" if role == ShardRole.PRIMARY else "50061"))
    db_path = os.environ.get("DB_PATH", f"/data/shard_{shard_id}_{role.value.lower()}.db")
    replica_id = os.environ.get("REPLICA_ID", f"replica_{shard_id}")
    max_workers = int(os.environ.get("MAX_WORKERS", "10"))

    logger.info("Starting Shard Node: ID=%d | Role=%s | Port=%d | DB=%s", shard_id, role.value, port, db_path)

    # Ensure parent directory of database exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id=shard_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    if role == ShardRole.PRIMARY:
        servicer = ShardServicer(shard_id=shard_id, store=store, role=ShardRole.PRIMARY)
        shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer, server)
    else:
        servicer = ReplicaServicer(shard_id=shard_id, replica_id=replica_id, store=store)
        replication_pb2_grpc.add_ReplicaServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    logger.info("Shard %d (%s) successfully listening on 0.0.0.0:%d", shard_id, role.value, port)

    # Graceful shutdown handler
    stop_event = threading.Event()

    def _sig_handler(sig, frame):
        logger.info("Received termination signal %s, initiating graceful shutdown...", sig)
        server.stop(grace=5.0)
        db.close()
        stop_event.set()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    stop_event.wait()
    logger.info("Shard %d (%s) terminated cleanly.", shard_id, role.value)


if __name__ == "__main__":
    run_shard_service()
