"""Shard Registry for managing cluster topology and active shard connections."""

from typing import Dict, List, Optional
from services.replica_client import ReplicaClient
from services.shard_client import ShardClient
from shared.config import ClusterConfig, ShardEndpoint
from shared.enums import ReplicaStatus
from shared.models import ShardStatus


class ShardRegistry:
    """Maintains active endpoints and connections to primary and replica shards."""

    def __init__(self, config: ClusterConfig):
        self.config = config
        self.shard_count = config.shard_count
        # shard_id -> primary ShardClient
        self._primary_clients: Dict[int, ShardClient] = {}
        self._replica_clients: Dict[int, ReplicaClient] = {}
        self._endpoints: Dict[int, ShardEndpoint] = dict(config.shard_endpoints or {})

    def get_replica_client(self, shard_id: int) -> ReplicaClient:
        """Get or create cached ReplicaClient for a shard."""
        if shard_id not in self._replica_clients:
            if shard_id not in self._endpoints:
                raise ValueError(f"No endpoint configuration for replica shard {shard_id}")
            endpoint = self._endpoints[shard_id]
            addr = f"{endpoint.replica_host}:{endpoint.replica_port}"
            self._replica_clients[shard_id] = ReplicaClient(addr)
        return self._replica_clients[shard_id]

    def register_shard(
        self,
        shard_id: int,
        primary_host: str,
        primary_port: int,
        replica_host: str,
        replica_port: int,
    ) -> None:
        """Register or update a shard's endpoints."""
        endpoint = ShardEndpoint(
            primary_host=primary_host,
            primary_port=primary_port,
            replica_host=replica_host,
            replica_port=replica_port,
        )
        self._endpoints[shard_id] = endpoint
        # Invalidate cached client if existing
        if shard_id in self._primary_clients:
            self._primary_clients[shard_id].close()
            del self._primary_clients[shard_id]

    def get_primary_client(self, shard_id: int) -> ShardClient:
        """Get or create cached ShardClient for the primary shard."""
        if shard_id not in self._primary_clients:
            if shard_id not in self._endpoints:
                raise KeyError(f"Shard {shard_id} is not configured in the registry")
            ep = self._endpoints[shard_id]
            address = f"{ep.primary_host}:{ep.primary_port}"
            self._primary_clients[shard_id] = ShardClient(address)
        return self._primary_clients[shard_id]

    def get_all_primary_clients(self) -> Dict[int, ShardClient]:
        """Return primary clients for all configured shards."""
        for shard_id in range(self.shard_count):
            self.get_primary_client(shard_id)
        return dict(self._primary_clients)

    def get_cluster_status(self) -> List[ShardStatus]:
        """Query and return health and sequence status for all shards."""
        statuses = []
        for shard_id in range(self.shard_count):
            ep = self._endpoints.get(shard_id)
            p_addr = f"{ep.primary_host}:{ep.primary_port}" if ep else "unknown"
            r_addr = f"{ep.replica_host}:{ep.replica_port}" if ep else "unknown"

            is_p_alive = False
            latest_seq = 0
            doc_count = 0

            try:
                client = self.get_primary_client(shard_id)
                resp = client.get_shard_status()
                is_p_alive = resp.is_healthy
                latest_seq = resp.latest_sequence
                doc_count = resp.total_documents
            except Exception:
                is_p_alive = False

            replica_applied_seq = 0
            is_r_alive = False
            replica_status = ReplicaStatus.DISCONNECTED

            try:
                if ep and ep.replica_port > 0:
                    r_client = self.get_replica_client(shard_id)
                    chk = r_client.get_checkpoint(shard_id)
                    replica_applied_seq = chk.last_applied_sequence
                    is_r_alive = True
                    try:
                        replica_status = ReplicaStatus(chk.status)
                    except ValueError:
                        replica_status = ReplicaStatus.SYNCHRONIZED
            except Exception as e:
                is_r_alive = False

            lag = max(0, latest_seq - replica_applied_seq)

            statuses.append(
                ShardStatus(
                    shard_id=shard_id,
                    primary_address=p_addr,
                    replica_address=r_addr,
                    latest_sequence=latest_seq,
                    replica_applied_sequence=replica_applied_seq,
                    replication_lag=lag,
                    replica_status=replica_status,
                    is_primary_alive=is_p_alive,
                    is_replica_alive=is_r_alive,
                    doc_count=doc_count,
                )
            )
        return statuses

    def close(self) -> None:
        """Close all cached client channels."""
        for client in self._primary_clients.values():
            client.close()
        for r_client in self._replica_clients.values():
            r_client.close()
        self._primary_clients.clear()
        self._replica_clients.clear()
