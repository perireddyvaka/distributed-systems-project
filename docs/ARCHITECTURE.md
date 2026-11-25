# System Architecture

## Distributed Search Engine with Incremental Index Recovery

This document describes the architectural design, component interactions, data models, protocols, and workflows of the distributed search engine.

---

## 1. Architectural Overview

The system is a distributed, sharded text search engine inspired by Elasticsearch, engineered to provide high search throughput, partitioning, primary-replica fault tolerance, and incremental index recovery after disconnection.

### 1.1 High-Level Architecture Diagram

```mermaid
flowchart TB
    subgraph ClientLayer [Client Layer]
        CLI[Client / CLI]
    end

    subgraph CoordinatorLayer [Coordinator Node]
        Router[Deterministic Router]
        Registry[Shard Registry]
        StatsCollector[Global Stats Collector]
        Dispatcher[Query Dispatcher]
        Merger[Global Result Merger]
        RecoveryCoord[Recovery Coordinator]
    end

    subgraph Shard0 [Shard 0 Cluster]
        subgraph Primary0 [Primary Shard 0]
            P0_Handler[Shard gRPC Service]
            P0_DocMgr[Document Manager]
            P0_Index[Custom Inverted Index]
            P0_BM25[BM25 Scorer]
            P0_Seq[Sequence Manager]
            P0_Log[Update Logger]
            P0_DB[(SQLite DB Primary 0)]
        end
        subgraph Replica0 [Replica Shard 0]
            R0_Handler[Replica gRPC Service]
            R0_Applier[Replica Applier]
            R0_Index[Custom Inverted Index]
            R0_BM25[BM25 Scorer]
            R0_Checkpoint[Checkpoint Tracker]
            R0_DB[(SQLite DB Replica 0)]
        end
    end

    subgraph Shard1 [Shard 1 Cluster]
        subgraph Primary1 [Primary Shard 1]
            P1_Handler[Shard gRPC Service]
            P1_DocMgr[Document Manager]
            P1_Index[Custom Inverted Index]
            P1_BM25[BM25 Scorer]
            P1_Seq[Sequence Manager]
            P1_Log[Update Logger]
            P1_DB[(SQLite DB Primary 1)]
        end
        subgraph Replica1 [Replica Shard 1]
            R1_Handler[Replica gRPC Service]
            R1_Applier[Replica Applier]
            R1_Index[Custom Inverted Index]
            R1_BM25[BM25 Scorer]
            R1_Checkpoint[Checkpoint Tracker]
            R1_DB[(SQLite DB Replica 1)]
        end
    end

    subgraph Validation [Correctness Validation]
        RefEngine[Independent Single-Node Reference Engine]
    end

    CLI -->|CRUD / Search / Recovery| CoordinatorLayer
    Router -->|Route doc_id| P0_Handler
    Router -->|Route doc_id| P1_Handler
    Dispatcher -->|Scatter Query + Global Stats| P0_Handler
    Dispatcher -->|Scatter Query + Global Stats| P1_Handler
    P0_Handler -->|Local Top-K| Merger
    P1_Handler -->|Local Top-K| Merger
    Merger -->|Global Top-K| CLI

    %% Primary 0 Internal
    P0_Handler --> P0_DocMgr
    P0_DocMgr --> P0_Index
    P0_DocMgr --> P0_Seq
    P0_Seq --> P0_Log
    P0_Log --> P0_DB
    P0_Handler --> P0_BM25
    P0_BM25 --> P0_Index

    %% Primary 1 Internal
    P1_Handler --> P1_DocMgr
    P1_DocMgr --> P1_Index
    P1_DocMgr --> P1_Seq
    P1_Seq --> P1_Log
    P1_Log --> P1_DB
    P1_Handler --> P1_BM25
    P1_BM25 --> P1_Index

    %% Replication
    P0_Log -.->|gRPC Replicate Stream / Recovery| R0_Applier
    R0_Applier --> R0_Index
    R0_Applier --> R0_Checkpoint
    R0_Checkpoint --> R0_DB

    P1_Log -.->|gRPC Replicate Stream / Recovery| R1_Applier
    R1_Applier --> R1_Index
    R1_Applier --> R1_Checkpoint
    R1_Checkpoint --> R1_DB

    %% Reference Verification
    CLI -.->|Mirror Operations| RefEngine
```

---

## 2. Core Components

### 2.1 Coordinator
- **Deterministic Router:** Maps each `doc_id` to its owning `shard_id` via MD5 hash modulo `num_shards`.
- **Shard Registry:** Maintains network endpoints (gRPC host/port) and health status of all primary and replica shards.
- **Global Stats Collector:** Queries primary shards to aggregate global corpus statistics ($N_{total}$, $avgdl$, term document frequencies $DF(t)$) needed for globally comparable BM25 scoring.
- **Query Dispatcher:** Scatters search requests along with the global ranking statistics snapshot to all primary shards concurrently.
- **Result Merger:** Collects local top-k result sets from shards, merges them into a global top-k ranking, and breaks ties deterministically by `doc_id`.
- **Recovery Coordinator:** Provides client-facing triggers and status inspection for shard recovery operations.

### 2.2 Primary Shard
- **Shard gRPC Service:** Exposes document CRUD, local search, statistics reporting, and update-log streaming.
- **Document Manager:** Validates document versions, prevents stale writes, creates tombstones upon deletion, and manages posting modifications.
- **Custom Inverted Index:** In-memory index maintaining term postings (`term -> {doc_id: tf}`), term document frequencies, and document lengths.
- **BM25 Scorer:** Evaluates multi-term queries against local postings using the globally supplied ranking statistics snapshot.
- **Sequence Manager:** Allocates monotonically increasing sequence numbers (1, 2, 3...) per shard.
- **Update Logger:** Atomically commits mutation records to SQLite `update_log`.

### 2.3 Replica Shard
- **Replica gRPC Service:** Exposes replication endpoints (`ApplyUpdate`, `GetCheckpoint`) and status queries.
- **Replica Applier:** Enforces strict ordering, detects sequence gaps, ignores duplicate events, rejects stale document versions, and preserves tombstones.
- **Checkpoint Tracker:** Atomically persists `last_applied_sequence` to SQLite `replica_state`.
- **Independent Index:** Maintains its own in-memory inverted index and SQLite storage.

### 2.4 Recovery Manager
- Coordinates incremental recovery when a replica reconnects after downtime or disconnection.
- Computes missing sequence ranges $[S_{replica} + 1, S_{primary}]$.
- Retrieves ordered log events from the primary's `update_log` table.
- Streams events to the replica, which validates, applies, and commits each event.
- Verifies that log history is intact; reports `HISTORY_UNAVAILABLE` if logs are missing.

### 2.5 Independent Reference Engine
- A completely independent, single-node in-memory search engine.
- Implements text normalization, tokenization, index storage, and BM25 ranking without sharding, RPCs, or coordinator dependencies.
- Used in tests to execute identical workloads and verify distributed search rankings, BM25 scores, and replica convergence.

---

## 3. Data Flow & Workflows

### 3.1 Document Write Workflow (Insert / Replace / Delete)
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Coordinator
    participant Primary
    participant PrimaryDB as Primary SQLite (WAL)
    participant Replica
    participant ReplicaDB as Replica SQLite (WAL)

    Client->>Coordinator: InsertDocument(doc_id, content)
    Coordinator->>Coordinator: shard_id = MD5(doc_id) % num_shards
    Coordinator->>Primary: InsertDocument(doc_id, content)
    Primary->>Primary: Validate version (new_version > current_version)
    Primary->>Primary: seq = next_sequence_number()
    Primary->>PrimaryDB: BEGIN IMMEDIATE
    Primary->>PrimaryDB: INSERT/UPDATE documents & postings
    Primary->>PrimaryDB: INSERT update_log (seq, op, doc_id, payload)
    Primary->>PrimaryDB: UPDATE shard_metadata (latest_seq)
    Primary->>PrimaryDB: COMMIT
    Primary->>Primary: Update in-memory inverted index
    Primary-->>Coordinator: Success(seq, version)
    Coordinator-->>Client: Success(shard_id, seq, version)

    opt Asynchronous / Stream Replication
        Primary->>Replica: ApplyUpdate(seq, op, doc_id, payload)
        Replica->>Replica: Validate sequence (seq == last_applied + 1)
        Replica->>ReplicaDB: BEGIN IMMEDIATE
        Replica->>ReplicaDB: Apply mutation & UPDATE replica_state (last_applied = seq)
        Replica->>ReplicaDB: COMMIT
        Replica->>Replica: Update in-memory inverted index
        Replica-->>Primary: Ack(seq)
    end
```

### 3.2 Distributed Search Workflow (Scatter-Gather with Global Stats)
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Coordinator
    participant Shard0 as Primary Shard 0
    participant Shard1 as Primary Shard 1

    Client->>Coordinator: Search(query="distributed system", top_k=10)
    Coordinator->>Coordinator: Extract terms: ["distributed", "system"]
    par Get Stats Snapshot
        Coordinator->>Shard0: GetRankingStats(terms)
        Coordinator->>Shard1: GetRankingStats(terms)
    end
    Coordinator->>Coordinator: Aggregate Global Stats (N, avgdl, DF(t))
    par Scatter Search
        Coordinator->>Shard0: SearchLocal(query, global_stats, top_k=10)
        Coordinator->>Shard1: SearchLocal(query, global_stats, top_k=10)
    end
    Shard0-->>Coordinator: Local Results (doc_id, score)
    Shard1-->>Coordinator: Local Results (doc_id, score)
    Coordinator->>Coordinator: Merge results + Tie-break by doc_id
    Coordinator-->>Client: Global Top-K Results
```

### 3.3 Incremental Replica Recovery Workflow
```mermaid
sequenceDiagram
    autonumber
    participant Replica
    participant RecoveryMgr as Recovery Manager
    participant Primary

    Note over Replica: Replica reconnects after outage
    Replica->>RecoveryMgr: StartRecovery(shard_id)
    RecoveryMgr->>Replica: GetReplicaCheckpoint()
    Replica-->>RecoveryMgr: last_applied_sequence = 950
    RecoveryMgr->>Primary: GetLatestSequence()
    Primary-->>RecoveryMgr: latest_sequence = 1200
    RecoveryMgr->>RecoveryMgr: Missing Range: [951, 1200] (250 events)
    RecoveryMgr->>Primary: GetUpdatesAfter(since_seq=950, max_seq=1200)
    alt Update log available
        Primary-->>RecoveryMgr: Stream UpdateEvents [951..1200]
        loop For each event in order
            RecoveryMgr->>Replica: ApplyUpdate(event)
            Replica->>Replica: Apply to SQLite + Checkpoint + Inverted Index
            Replica-->>RecoveryMgr: Ack(seq)
        end
        RecoveryMgr-->>Replica: Recovery Complete (Status: SYNCHRONIZED)
    else Log truncated / missing
        Primary-->>RecoveryMgr: Error(HISTORY_UNAVAILABLE)
        RecoveryMgr-->>Replica: Recovery Failed (Status: HISTORY_UNAVAILABLE)
    end
```

---

## 4. SQLite Storage Schema

Each primary and replica node manages an independent SQLite database with Write-Ahead Logging (`PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;`).

```sql
-- documents: Stores document state and tombstones
CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    version INTEGER NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    document_length INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- postings: Stores inverted index term frequencies
CREATE TABLE IF NOT EXISTS postings (
    term TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    term_frequency INTEGER NOT NULL,
    PRIMARY KEY (term, doc_id),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term);

-- update_log: Persistent per-shard write-ahead log for replication
CREATE TABLE IF NOT EXISTS update_log (
    shard_id INTEGER NOT NULL,
    sequence_number INTEGER NOT NULL,
    doc_id TEXT NOT NULL,
    operation TEXT NOT NULL, -- INSERT, REPLACE, DELETE
    version INTEGER NOT NULL,
    payload TEXT,            -- JSON-serialized document or delta
    committed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shard_id, sequence_number)
);
CREATE INDEX IF NOT EXISTS idx_log_seq ON update_log(shard_id, sequence_number);

-- shard_metadata: Stores shard sequence counter and statistics
CREATE TABLE IF NOT EXISTS shard_metadata (
    shard_id INTEGER PRIMARY KEY,
    latest_committed_sequence INTEGER NOT NULL DEFAULT 0,
    total_documents INTEGER NOT NULL DEFAULT 0,
    total_length INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- replica_state: Tracks checkpoint sequence of the replica
CREATE TABLE IF NOT EXISTS replica_state (
    shard_id INTEGER PRIMARY KEY,
    replica_id TEXT NOT NULL,
    last_applied_sequence INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'DISCONNECTED',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 5. Ranking Formulation: BM25 with Global Statistics

The system implements the Okapi BM25 ranking algorithm:

$$\text{Score}(D, Q) = \sum_{t \in Q} \text{IDF}(t) \cdot \frac{f(t, D) \cdot (k_1 + 1)}{f(t, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

Where:
- $f(t, D)$ is the term frequency of $t$ in document $D$ (from local shard postings).
- $|D|$ is the length of document $D$ in tokens.
- $\text{avgdl} = \frac{\sum_{i=1}^{N} |D_i|}{N}$ is the **global average document length** aggregated across all primary shards.
- $N$ is the **global count of non-deleted documents** across all primary shards.
- $\text{IDF}(t) = \ln\left(1 + \frac{N - DF(t) + 0.5}{DF(t) + 0.5}\right)$ is calculated using the **global document frequency** $DF(t)$ of term $t$.
- Default hyperparameters: $k_1 = 1.2$, $b = 0.75$.

Deterministic tie-breaking: If two documents have identical scores within $10^{-6}$, ties are broken by ascending `doc_id`.

For complete mathematical derivations, parameter sensitivity analysis, distributed aggregation invariants, and recovery benchmark metrics, see [docs/MATHEMATICAL_FORMULATIONS.md](MATHEMATICAL_FORMULATIONS.md).
