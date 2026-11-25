-- Database schema for Distributed Search Engine (SQLite)
-- Applied to each primary and replica independent database.

-- documents table: Stores document content, version, and tombstone state
CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    version INTEGER NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    document_length INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- postings table: Inverted index mapping term to document with term frequency
CREATE TABLE IF NOT EXISTS postings (
    term TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    term_frequency INTEGER NOT NULL,
    PRIMARY KEY (term, doc_id),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term);
CREATE INDEX IF NOT EXISTS idx_postings_doc_id ON postings(doc_id);

-- update_log table: Persistent write-ahead log for primary-to-replica replication and incremental recovery
CREATE TABLE IF NOT EXISTS update_log (
    shard_id INTEGER NOT NULL,
    sequence_number INTEGER NOT NULL,
    doc_id TEXT NOT NULL,
    operation TEXT NOT NULL, -- 'INSERT', 'REPLACE', 'DELETE'
    version INTEGER NOT NULL,
    payload TEXT,            -- JSON-serialized document or null for delete
    committed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_update_log_seq ON update_log(shard_id, sequence_number);

-- shard_metadata table: Tracks latest sequence number and shard-level statistics
CREATE TABLE IF NOT EXISTS shard_metadata (
    shard_id INTEGER PRIMARY KEY,
    latest_committed_sequence INTEGER NOT NULL DEFAULT 0,
    total_documents INTEGER NOT NULL DEFAULT 0,
    total_length INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- replica_state table: Tracks replica applied sequence checkpoint and recovery status
CREATE TABLE IF NOT EXISTS replica_state (
    shard_id INTEGER PRIMARY KEY,
    replica_id TEXT NOT NULL,
    last_applied_sequence INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'DISCONNECTED', -- 'DISCONNECTED', 'CONNECTING', 'RECOVERING', 'SYNCHRONIZED', 'FAILED', 'HISTORY_UNAVAILABLE'
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
