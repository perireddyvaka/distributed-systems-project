"""SQLite Database connection manager with WAL mode and ACID transaction boundaries."""

import sqlite3
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional


class DatabaseManager:
    """Manages SQLite connections, pragma settings, and atomic transactions."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._init_connection()

    def _init_connection(self) -> None:
        """Initialize connection with WAL mode and foreign key enforcement."""
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0,
            isolation_level=None,  # Autocommit mode; we manage BEGIN/COMMIT manually
        )
        self._conn.row_factory = sqlite3.Row

        # Set durability and concurrency pragmas
        cursor = self._conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        if self.db_path != ":memory:":
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
        cursor.close()

    def get_connection(self) -> sqlite3.Connection:
        """Return raw SQLite connection."""
        if self._conn is None:
            self._init_connection()
        return self._conn

    def initialize_schema(self, schema_path: Optional[str] = None) -> None:
        """Apply schema DDL script."""
        if schema_path is None:
            schema_path = str(Path(__file__).resolve().parent / "schema.sql")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        cursor = self.get_connection().cursor()
        cursor.executescript(schema_sql)
        cursor.close()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Cursor, None, None]:
        """Context manager providing an atomic BEGIN IMMEDIATE transaction block.
        
        Guarantees that write transactions acquire write locks immediately,
        preventing deadlock between concurrent writers in WAL mode.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE;")
        try:
            yield cursor
            cursor.execute("COMMIT;")
        except Exception:
            cursor.execute("ROLLBACK;")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
