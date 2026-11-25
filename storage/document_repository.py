"""Document repository for persistent SQLite storage."""

import sqlite3
import time
from datetime import datetime
from typing import List, Optional
from shared.models import Document
from storage.database import DatabaseManager


def _to_epoch(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, datetime):
        return val.timestamp()
    return time.time()


class DocumentRepository:
    """Handles CRUD persistence for documents and versioned tombstones."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def save_document(self, doc: Document, cursor: Optional[sqlite3.Cursor] = None) -> None:
        """Insert or replace document in SQLite."""
        sql = """
        INSERT INTO documents (doc_id, content, version, is_deleted, document_length, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime(?, 'unixepoch'), datetime(?, 'unixepoch'))
        ON CONFLICT(doc_id) DO UPDATE SET
            content = excluded.content,
            version = excluded.version,
            is_deleted = excluded.is_deleted,
            document_length = excluded.document_length,
            updated_at = excluded.updated_at;
        """
        params = (
            doc.doc_id,
            doc.content,
            doc.version,
            1 if doc.is_deleted else 0,
            doc.document_length,
            _to_epoch(doc.created_at),
            _to_epoch(doc.updated_at),
        )

        if cursor is not None:
            cursor.execute(sql, params)
        else:
            with self.db.transaction() as cur:
                cur.execute(sql, params)

    def get_document(self, doc_id: str, include_deleted: bool = False) -> Optional[Document]:
        """Fetch document by doc_id."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        sql = "SELECT doc_id, content, version, is_deleted, document_length, strftime('%s', created_at) as created_ts, strftime('%s', updated_at) as updated_ts FROM documents WHERE doc_id = ?"
        cur.execute(sql, (doc_id,))
        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        is_deleted = bool(row["is_deleted"])
        if is_deleted and not include_deleted:
            return None

        created_ts = float(row["created_ts"]) if row["created_ts"] else 0.0
        updated_ts = float(row["updated_ts"]) if row["updated_ts"] else 0.0

        return Document(
            doc_id=row["doc_id"],
            content=row["content"],
            version=row["version"],
            is_deleted=is_deleted,
            document_length=row["document_length"],
            created_at=created_ts,
            updated_at=updated_ts,
        )

    def get_all_documents(self, include_deleted: bool = False) -> List[Document]:
        """Fetch all documents."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        if include_deleted:
            sql = "SELECT doc_id, content, version, is_deleted, document_length, strftime('%s', created_at) as created_ts, strftime('%s', updated_at) as updated_ts FROM documents"
            cur.execute(sql)
        else:
            sql = "SELECT doc_id, content, version, is_deleted, document_length, strftime('%s', created_at) as created_ts, strftime('%s', updated_at) as updated_ts FROM documents WHERE is_deleted = 0"
            cur.execute(sql)

        rows = cur.fetchall()
        cur.close()

        results = []
        for row in rows:
            created_ts = float(row["created_ts"]) if row["created_ts"] else 0.0
            updated_ts = float(row["updated_ts"]) if row["updated_ts"] else 0.0
            results.append(
                Document(
                    doc_id=row["doc_id"],
                    content=row["content"],
                    version=row["version"],
                    is_deleted=bool(row["is_deleted"]),
                    document_length=row["document_length"],
                    created_at=created_ts,
                    updated_at=updated_ts,
                )
            )
        return results

    def count_active_documents(self) -> int:
        """Count non-deleted documents."""
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM documents WHERE is_deleted = 0")
        count = cur.fetchone()[0]
        cur.close()
        return count
