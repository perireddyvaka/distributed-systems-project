"""Replica repository for tracking durable applied checkpoints."""

import sqlite3
from typing import Optional, Tuple
from shared.enums import ReplicaStatus
from storage.database import DatabaseManager


class ReplicaRepository:
    """Manages replica_state table for checkpointing and recovery tracking."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def save_checkpoint(
        self,
        shard_id: int,
        replica_id: str,
        last_applied_sequence: int,
        status: ReplicaStatus = ReplicaStatus.SYNCHRONIZED,
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> None:
        """Durable checkpoint update."""
        sql = """
        INSERT INTO replica_state (shard_id, replica_id, last_applied_sequence, status, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(shard_id) DO UPDATE SET
            replica_id = excluded.replica_id,
            last_applied_sequence = excluded.last_applied_sequence,
            status = excluded.status,
            updated_at = excluded.updated_at;
        """
        params = (
            shard_id,
            replica_id,
            last_applied_sequence,
            status.value if isinstance(status, ReplicaStatus) else str(status),
        )

        if cursor is not None:
            cursor.execute(sql, params)
        else:
            with self.db.transaction() as cur:
                cur.execute(sql, params)

    def get_checkpoint(self, shard_id: int) -> Tuple[str, int, ReplicaStatus]:
        """Fetch (replica_id, last_applied_sequence, status) for a shard."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT replica_id, last_applied_sequence, status FROM replica_state WHERE shard_id = ?",
            (shard_id,),
        )
        row = cur.fetchone()
        cur.close()

        if row:
            return (
                row["replica_id"],
                row["last_applied_sequence"],
                ReplicaStatus(row["status"]),
            )
        return ("default", 0, ReplicaStatus.DISCONNECTED)
