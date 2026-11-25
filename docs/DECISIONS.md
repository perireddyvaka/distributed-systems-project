# Architecture Decision Records (ADRs)

This document records the foundational and continuing architectural design decisions for the **Distributed Search Engine with Incremental Index Recovery**.

---

## ADR-001: Shard Routing via Deterministic MD5 Modulo

- **Decision ID:** ADR-001
- **Date:** 2026-11-25
- **Context:**
  In a sharded distributed search engine, documents must be deterministically assigned to a single owning shard. Python's built-in `hash()` function employs randomized seed salting (SIPHASH-based) across process restarts (`PYTHONHASHSEED`), making it unsuitable for persistent distributed routing decisions.
- **Decision:**
  Use MD5 hexadecimal hashing on the UTF-8 encoded `doc_id`, parsing the first 8 hex characters as an integer and taking modulo `num_shards`:
  ```python
  shard_id = int(hashlib.md5(doc_id.encode('utf-8')).hexdigest()[:8], 16) % num_shards
  ```
- **Alternatives Considered:**
  1. *Python's built-in `hash()`*: Non-deterministic across process restarts and independent nodes.
  2. *Consistent Hashing (Ring)*: Added complexity with virtual nodes; unnecessary for fixed shard topologies (e.g., 2 or 4 shards without dynamic resharding).
  3. *Murmur3 / xxHash*: Requires external C-extension libraries; MD5 is built into Python standard library (`hashlib`) with zero dependency footprint and uniform distribution.
- **Reason for Selection:**
  Provides deterministic, stable, reproducible routing across all processes and nodes with zero external dependencies.
- **Affected Modules:**
  - `coordinator/router.py`
  - `client/client.py`
- **Related Tests:**
  - `tests/unit/test_router.py`

---

## ADR-002: Consistent Global Ranking-Statistics Snapshot for Distributed BM25

- **Decision ID:** ADR-002
- **Date:** 2026-11-25
- **Context:**
  In standard Okapi BM25, the Inverse Document Frequency $\text{IDF}(t)$ and normalization factor depend on corpus-wide statistics: total document count $N$, average document length $\text{avgdl}$, and document frequency $DF(t)$ of query terms. In a sharded architecture, if each shard computes BM25 scores using only its local statistics, shards with fewer documents or different term densities will produce skewed scores that cannot be meaningfully merged in a global top-$k$ heap.
- **Decision:**
  Implement a two-phase scatter-gather query execution:
  1. **Phase 1 (Stats Gathering):** The coordinator extracts query terms and requests term statistics (`DF(t)`, local document count, and total length) from all primary shards.
  2. **Snapshot Creation:** The coordinator aggregates these into a consistent `GlobalRankingSnapshot` ($N_{global}$, $\text{avgdl}_{global}$, $DF_{global}(t)$).
  3. **Phase 2 (Scored Retrieval):** The coordinator dispatches the query and the `GlobalRankingSnapshot` to all primary shards.
  4. Each shard scores its matching documents against the identical global parameters and returns its local top-$k$ candidates.
  5. The coordinator merges the results using a max-heap with deterministic tie-breaking.
- **Alternatives Considered:**
  1. *Local shard scoring with score normalization*: Skews relevance and fails when rare terms appear disproportionately on one shard.
  2. *Periodic background stats gossip*: Stale statistics lead to inconsistent ranking during rapid ingestion.
- **Reason for Selection:**
  Guarantees 100% mathematical equivalence between distributed scatter-gather search and the single-node reference engine.
- **Affected Modules:**
  - `coordinator/ranking_stats.py`
  - `coordinator/query_dispatcher.py`
  - `coordinator/result_merger.py`
  - `indexing/bm25.py`
  - `services/shard_server.py`
- **Related Tests:**
  - `tests/unit/test_bm25.py`
  - `tests/integration/test_sharded_search.py`
  - `tests/system/test_cross_validation.py`

---

## ADR-003: Monotonic Document Versioning and Tombstone Semantics

- **Decision ID:** ADR-003
- **Date:** 2026-11-25
- **Context:**
  In an asynchronous distributed environment with potential network delays and retries, stale updates or delayed insertions might arrive out-of-order at replicas. Specifically, if a document is deleted and a delayed `INSERT` arrives later, it could resurrect the deleted document.
- **Decision:**
  - Every document has an integer `version` initialized to 1 upon creation and incremented on replacement.
  - A `DELETE` operation creates a versioned tombstone: `is_deleted = 1` with `version = current_version + 1`.
  - Replicas and primaries strictly enforce `incoming_version > current_version` for mutations. If `current.is_deleted == 1`, incoming inserts with `version <= tombstone_version` are rejected with `TOMBSTONE_CONFLICT` / `STALE_VERSION`.
- **Alternatives Considered:**
  1. *Physical hard-delete*: Removes all record of the document; delayed packets or log replays will interpret the document as brand new and resurrect it.
  2. *Timestamp-based Last-Write-Wins (LWW)*: Clock skew across machines can cause erroneous overwrites or drops.
- **Reason for Selection:**
  Prevents stale resurrection and ensures deterministic convergence between primary and replica.
- **Affected Modules:**
  - `indexing/document_manager.py`
  - `replication/replica_applier.py`
  - `storage/document_repository.py`
- **Related Tests:**
  - `tests/unit/test_crud.py`
  - `tests/replication/test_stale_rejection.py`

---

## ADR-004: Monotonic Per-Shard Sequence Numbers and Atomic Write Boundaries

- **Decision ID:** ADR-004
- **Date:** 2026-11-25
- **Context:**
  Replication and incremental recovery require a total order of operations per shard to detect gaps, skip duplicates, and replay missed ranges. A process crash must never result in a state where a document change is persisted without an update log record.
- **Decision:**
  - Each primary shard maintains a monotonic integer sequence counter ($1, 2, 3, \dots$) stored durably in `shard_metadata`.
  - Every mutation atomically executes within a single SQLite transaction (`BEGIN IMMEDIATE`):
    1. Update `documents` and `postings` tables.
    2. Insert record into `update_log (shard_id, sequence_number, doc_id, operation, version, payload, committed_at)`.
    3. Update `shard_metadata` with new `latest_committed_sequence`.
  - Replicas atomically apply incoming events and advance their checkpoint in `replica_state` (`last_applied_sequence = seq`) within a single transaction.
- **Alternatives Considered:**
  1. *Global cross-shard sequence counter*: Requires distributed coordination (e.g., Raft/ZooKeeper), creating a bottleneck and violating shard independence.
  2. *Separate logging file (WAL on disk) outside SQLite*: Complicates atomic synchronization between data and log.
- **Reason for Selection:**
  Leverages SQLite's native ACID transaction guarantees to eliminate split-brain or partial commits between document data and replication logs.
- **Affected Modules:**
  - `replication/sequence_manager.py`
  - `replication/update_logger.py`
  - `storage/database.py`
  - `storage/log_repository.py`
- **Related Tests:**
  - `tests/unit/test_sequence_manager.py`
  - `tests/integration/test_persistence.py`

---

## ADR-005: Incremental Replica Recovery via Sequence-Range Replay

- **Decision ID:** ADR-005
- **Date:** 2026-11-25
- **Context:**
  When a replica experiences transient disconnection or restart, rebuilding the full index from scratch causes high I/O, network bandwidth, and CPU waste. The replica needs a mechanism to synchronize only what was missed while remaining resilient to interruptions.
- **Decision:**
  Implement incremental sequence-range recovery:
  1. Replica exposes its persisted checkpoint: $S_{checkpoint} = \text{last\_applied\_sequence}$.
  2. Primary exposes its current committed sequence: $S_{latest} = \text{latest\_committed\_sequence}$.
  3. Recovery manager requests missing range: $(S_{checkpoint}, S_{latest}]$.
  4. Primary streams ordered `UpdateEvent` objects from `update_log`.
  5. Replica processes each event sequentially, updating its in-memory index and atomically advancing its checkpoint.
  6. If log entries have been pruned or are missing ($S_{checkpoint} < S_{oldest} - 1$), the recovery manager aborts with status `HISTORY_UNAVAILABLE`.
  7. If recovery is interrupted midway, the next recovery attempt safely resumes from the latest committed checkpoint.
- **Alternatives Considered:**
  1. *Full snapshot transfer*: Requires snapshot creation, transmission of entire SQLite DB, and complete index rebuilding.
  2. *Document-diff scanning*: Comparing document IDs and versions across nodes requires expensive full-table scans.
- **Reason for Selection:**
  Achieves dramatic speedup and minimal network transfer during transient partitions.
- **Affected Modules:**
  - `replication/recovery_manager.py`
  - `services/shard_server.py`
  - `services/replica_server.py`
- **Related Tests:**
  - `tests/recovery/test_disconnection.py`
  - `tests/recovery/test_interrupted.py`
  - `tests/recovery/test_history_unavailable.py`

---

## ADR-006: SQLite WAL Mode for High-Performance Concurrency

- **Decision ID:** ADR-006
- **Date:** 2026-11-25
- **Context:**
  Primary and replica shards must handle concurrent read queries (search, statistics retrieval) while processing incoming document writes or replication streams. Default SQLite rollback journal mode locks the entire database during writes.
- **Decision:**
  Enable Write-Ahead Logging (`PRAGMA journal_mode = WAL;`) and relaxed synchronous mode (`PRAGMA synchronous = NORMAL;`) on all SQLite databases at startup. Enable foreign key enforcement (`PRAGMA foreign_keys = ON;`).
- **Alternatives Considered:**
  1. *Default DELETE journal mode*: Readers block writers and writers block readers.
  2. *In-memory only SQLite (`:memory:`) without disk*: Fails durability and crash recovery requirements.
- **Reason for Selection:**
  WAL mode allows multiple concurrent readers to proceed uninterrupted while a writer executes transactions, essential for low search latency during ingestion.
- **Affected Modules:**
  - `storage/database.py`
- **Related Tests:**
  - `tests/unit/test_database.py`
  - `tests/integration/test_persistence.py`

---

## ADR-007: Independent Reference Engine for Correctness Verification

- **Decision ID:** ADR-007
- **Date:** 2026-11-25
- **Context:**
  Verifying the correctness and convergence of a distributed system requires an independent, trusted source of truth to avoid circular assertions (where a bug in the shared implementation affects both test subject and verifier).
- **Decision:**
  Build a completely independent, single-node `ReferenceEngine` that maintains its own corpus, tokenization, in-memory inverted index, and BM25 scoring algorithm with zero code sharing with coordinator or RPC layers.
  Test suites execute identical workloads on both the reference engine and the distributed cluster, asserting:
  - Exact match of document existence, versions, and tombstone states.
  - 100% agreement on retrieved top-$k$ `doc_id` rankings.
  - BM25 score equality within float precision ($10^{-5}$).
- **Alternatives Considered:**
  1. *Using Elasticsearch as verification*: Violates project instruction ("Do not introduce external technologies like Elasticsearch").
  2. *Testing distributed components solely against hardcoded mocks*: Lacks the ability to verify dynamic multi-step workloads.
- **Reason for Selection:**
  Provides mathematical certainty of correctness and reproducibility for course grading and TA evaluation.
- **Affected Modules:**
  - `reference/reference_engine.py`
  - `tests/system/test_cross_validation.py`
- **Related Tests:**
  - `tests/system/test_cross_validation.py`
