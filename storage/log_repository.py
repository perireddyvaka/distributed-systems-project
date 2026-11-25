"""Log repository for persisting per-shard update logs."""

import sqlite3
import json
import time
from datetime import datetime
from typing import List, Optional
from shared.enums import OperationType
from shared.models import UpdateEvent
from storage.database import DatabaseManager


def _to_epoch(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, datetime):
        return val.timestamp()
    return time.time()


class LogRepository:
    """Manages update_log and shard_metadata tables."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def append_log(self, event: UpdateEvent, cursor: Optional[sqlite3.Cursor] = None) -> None:
        """Insert update log event and advance shard metadata sequence atomically."""
        log_sql = """
        INSERT INTO update_log (shard_id, sequence_number, doc_id, operation, version, payload, committed_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'))
        """
        log_params = (
            event.shard_id,
            event.sequence_number,
            event.doc_id,
            event.operation.value if isinstance(event.operation, OperationType) else str(event.operation),
            event.version,
            event.payload,
            _to_epoch(event.committed_at),
        )

        meta_sql = """
        INSERT INTO shard_metadata (shard_id, latest_committed_sequence, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(shard_id) DO UPDATE SET
            latest_committed_sequence = excluded.latest_committed_sequence,
            updated_at = excluded.updated_at
        """
        meta_params = (event.shard_id, event.sequence_number)

        if cursor is not None:
            cursor.execute(log_sql, log_params)
            cursor.execute(meta_sql, meta_params)
        else:
            with self.db.transaction() as cur:
                cur.execute(log_sql, log_params)
                cur.execute(meta_sql, meta_params)

    def get_updates_after(
        self,
        shard_id: int,
        since_sequence: int,
        max_events: int = 1000,
    ) -> List[UpdateEvent]:
        """Fetch update log events with sequence_number > since_sequence in ascending order."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        sql = """
        SELECT shard_id, sequence_number, doc_id, operation, version, payload, strftime('%s', committed_at) as committed_ts
        FROM update_log
        WHERE shard_id = ? AND sequence_number > ?
        ORDER BY sequence_number ASC
        LIMIT ?
        """
        cur.execute(sql, (shard_id, since_sequence, max_events))
        rows = cur.fetchall()
        cur.close()

        events = []
        for row in rows:
            events.append(
                UpdateEvent(
                    shard_id=row["shard_id"],
                    sequence_number=row["sequence_number"],
                    doc_id=row["doc_id"],
                    operation=OperationType(row["operation"]),
                    version=row["version"],
                    payload=row["payload"],
                    committed_at=float(row["committed_ts"]) if row["committed_ts"] else 0.0,
                )
            )
        return events

    def get_latest_sequence(self, shard_id: int) -> int:
        """Get the latest committed sequence number for a shard."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT latest_committed_sequence FROM shard_metadata WHERE shard_id = ?",
            (shard_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return row["latest_committed_sequence"]
        return 0

    def get_earliest_sequence(self, shard_id: int) -> Optional[int]:
        """Get the minimum available sequence number in the log (for history availability check)."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT min(sequence_number) as min_seq FROM update_log WHERE shard_id = ?",
            (shard_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row and row["min_seq"] is not None:
            return row["min_seq"]
        return None

    def get_log_count(self, shard_id: int) -> int:
        """Get total number of log entries for a shard."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM update_log WHERE shard_id = ?", (shard_id,))
        count = cur.fetchone()[0]
        cur.close()
        return count
