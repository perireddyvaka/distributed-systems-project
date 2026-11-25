"""Index repository for persisting postings and bulk index reconstruction."""

import sqlite3
from typing import Dict, List, Optional, Tuple
from storage.database import DatabaseManager


class IndexRepository:
    """Manages postings table in SQLite."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def save_postings(
        self,
        doc_id: str,
        term_frequencies: Dict[str, int],
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> None:
        """Atomically replace postings for a document."""
        delete_sql = "DELETE FROM postings WHERE doc_id = ?"
        insert_sql = "INSERT INTO postings (term, doc_id, term_frequency) VALUES (?, ?, ?)"
        insert_params = [(term, doc_id, tf) for term, tf in term_frequencies.items() if tf > 0]

        if cursor is not None:
            cursor.execute(delete_sql, (doc_id,))
            if insert_params:
                cursor.executemany(insert_sql, insert_params)
        else:
            with self.db.transaction() as cur:
                cur.execute(delete_sql, (doc_id,))
                if insert_params:
                    cur.executemany(insert_sql, insert_params)

    def delete_postings(self, doc_id: str, cursor: Optional[sqlite3.Cursor] = None) -> None:
        """Delete all postings for a document."""
        sql = "DELETE FROM postings WHERE doc_id = ?"
        if cursor is not None:
            cursor.execute(sql, (doc_id,))
        else:
            with self.db.transaction() as cur:
                cur.execute(sql, (doc_id,))

    def load_all_postings(self) -> List[Tuple[str, Dict[str, int], int]]:
        """Load all active documents and their postings for index reconstruction on boot.
        
        Returns:
            List of (doc_id, {term: tf}, document_length) for all non-deleted documents.
        """
        conn = self.db.get_connection()
        cur = conn.cursor()

        # Fetch active documents
        cur.execute("SELECT doc_id, document_length FROM documents WHERE is_deleted = 0")
        active_docs = {row["doc_id"]: row["document_length"] for row in cur.fetchall()}

        if not active_docs:
            cur.close()
            return []

        # Fetch all postings
        cur.execute("SELECT term, doc_id, term_frequency FROM postings")
        postings_rows = cur.fetchall()
        cur.close()

        doc_tfs: Dict[str, Dict[str, int]] = {doc_id: {} for doc_id in active_docs}
        for row in postings_rows:
            doc_id = row["doc_id"]
            if doc_id in doc_tfs:
                doc_tfs[doc_id][row["term"]] = row["term_frequency"]

        return [(doc_id, doc_tfs[doc_id], active_docs[doc_id]) for doc_id in active_docs]
