"""Centralized Configuration for Distributed Search Engine."""

import os
from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class ShardEndpoint:
    primary_host: str
    primary_port: int
    replica_host: str
    replica_port: int


@dataclass
class ClusterConfig:
    coordinator_host: str = "0.0.0.0"
    coordinator_port: int = 50050
    shard_count: int = 2
    shard_endpoints: Dict[int, ShardEndpoint] = None
    data_dir: str = "./data"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ClusterConfig":
        coord_host = os.getenv("COORDINATOR_HOST", "0.0.0.0")
        coord_port = int(os.getenv("COORDINATOR_PORT", "50050"))
        shard_count = int(os.getenv("SHARD_COUNT", "2"))
        data_dir = os.getenv("DATA_DIR", "./data")
        log_level = os.getenv("LOG_LEVEL", "INFO")

        endpoints: Dict[int, ShardEndpoint] = {}
        for s in range(shard_count):
            p_host = os.getenv(f"SHARD_{s}_PRIMARY_HOST", "127.0.0.1")
            p_port = int(os.getenv(f"SHARD_{s}_PRIMARY_PORT", str(50051 + s)))
            r_host = os.getenv(f"SHARD_{s}_REPLICA_HOST", "127.0.0.1")
            r_port = int(os.getenv(f"SHARD_{s}_REPLICA_PORT", str(50061 + s)))
            endpoints[s] = ShardEndpoint(
                primary_host=p_host,
                primary_port=p_port,
                replica_host=r_host,
                replica_port=r_port,
            )

        return cls(
            coordinator_host=coord_host,
            coordinator_port=coord_port,
            shard_count=shard_count,
            shard_endpoints=endpoints,
            data_dir=data_dir,
            log_level=log_level,
        )
