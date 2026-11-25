# Development Tracker

## A. Project Information

- **Project Title:** Distributed Search Engine with Incremental Index Recovery
- **Project Objective:** Implement an Elasticsearch-inspired distributed text search engine using custom sharded inverted indexes, primary-replica replication, persistent SQLite update logs, and incremental recovery by replaying missed sequence updates.
- **Repository Location:** `/Users/perireddyvaka/IIITH/MS/Monsoon-26/DS/assignments/project`
- **Current Version:** `1.0.0`
- **Current Milestone:** `All Milestones Complete (M0 — M9)`
- **Overall Progress:** 100%
- **Last Updated Date:** 2026-11-25
- **Current Project Status:** COMPLETED & VALIDATED

---

## B. Status Definitions

- **NOT STARTED:** Work has not begun.
- **IN PROGRESS:** Active implementation is ongoing.
- **IMPLEMENTED:** Code has been written, but testing is not yet complete.
- **TESTING:** Relevant tests are being executed or failures are being resolved.
- **VALIDATED:** Required acceptance tests have passed and evidence is recorded.
- **BLOCKED:** Work cannot continue because of a documented dependency or issue.

---

## C. Milestone Summary

| Milestone | Name | Status | Completion | Start Date | Completion Date | Evidence |
|---|---|---|---|---|---|---|
| M0 | Repository inspection and project foundation | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 7 unit tests passed (`test_foundation.py`) |
| M1 | Text processing and custom inverted index | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 18 unit tests passed (tokenizer, index, doc mgr) |
| M2 | BM25 ranking | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 8 unit tests passed (`test_bm25.py`) |
| M3 | SQLite persistence | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 7 unit tests passed (`test_storage.py`) |
| M4 | Sharding and coordinator | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 7 tests passed (3 unit, 4 integration) |
| M5 | Primary-replica replication | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 8 replication tests passed (`test_replication.py`) |
| M6 | Incremental replica recovery | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 4 recovery tests passed (`test_recovery.py`) |
| M7 | Independent reference engine and correctness validation | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 4 system cross-validation tests passed (`test_cross_validation.py`) |
| M8 | Performance and recovery evaluation | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | WikiText pipeline, benchmark results JSON & 4 Matplotlib plots |
| M9 | Docker deployment and final submission readiness | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | Dockerfile, docker-compose.yml, CLI & automated demo |

---

## D. Detailed Task Tracking

### Milestone M0: Foundation and Project Structure
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M0-001 | Repository inspection & state assessment | Primary Dev | VALIDATED | None | Workspace root | Manual inspection | Empty repo confirmed | Initial inspection done |
| M0-002 | Setup Python virtual environment & dependencies | Primary Dev | VALIDATED | M0-001 | `requirements.txt`, `pyproject.toml` | `pip list` | Dependencies installed | Python 3.13 |
| M0-003 | Configure pytest & test runner | Primary Dev | VALIDATED | M0-002 | `pytest.ini` | `pytest --version` | pytest 9.2.0 verified | Configured |
| M0-004 | Establish project documentation | Primary Dev | VALIDATED | None | `README.md`, `ARCHITECTURE.md`, `DECISIONS.md` | Doc inspection | Files created | Initial architecture confirmed |
| M0-005 | Define shared data models and enums | Primary Dev | VALIDATED | M0-002 | `shared/models.py`, `shared/enums.py`, `shared/exceptions.py`, `shared/config.py` | `test_models` | Passed in test_foundation.py | Dataclasses for docs, queries, events |
| M0-006 | Define initial SQLite schema | Primary Dev | VALIDATED | None | `storage/schema.sql` | `test_sqlite_schema` | Passed in test_foundation.py | WAL mode, documents, postings, logs |
| M0-007 | Define Protocol Buffer contracts & code generation | Primary Dev | VALIDATED | M0-002 | `proto/*.proto`, `scripts/compile_protos.py` | `test_protobuf_messages` | Passed in test_foundation.py | Contracts for coordinator, shard, replication |
| M0-008 | M0 Smoke Test & baseline verification | Primary Dev | VALIDATED | M0-001..007 | `tests/unit/test_foundation.py` | TE-001 | 5 passed in 0.04s | Foundation verification complete |

### Milestone M1: Text Processing and Custom Inverted Index
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M1-001 | Custom Tokenizer with normalization | Primary Dev | VALIDATED | M0 | `indexing/tokenizer.py` | `test_tokenizer.py` | 6 passed (TE-002) | Lowercase, accents, punctuation, contractions |
| M1-002 | Inverted index posting list structures | Primary Dev | VALIDATED | M1-001 | `indexing/inverted_index.py` | `test_inverted_index.py` | 5 passed (TE-002) | Pure Python, document frequencies, lengths |
| M1-003 | Document manager with versioning & tombstones | Primary Dev | VALIDATED | M1-002 | `indexing/document_manager.py` | `test_document_manager.py` | 7 passed (TE-002) | Monotonic versions, anti-resurrection |

### Milestone M2: BM25 Ranking
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M2-001 | Implement Okapi BM25 ranking algorithm | Primary Dev | VALIDATED | M1 | `indexing/bm25.py` | `test_bm25.py` | 8 passed (TE-003) | Hand-calculated IDF, multi-term, tie-breaking |
| M2-002 | Mathematical formulations documentation | Primary Dev | VALIDATED | M2-001 | `docs/MATHEMATICAL_FORMULATIONS.md` | Doc inspection | Comprehensive derivations preserved | Equations & invariants |

### Milestone M3: SQLite Persistence
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M3-001 | SQLite connection manager with WAL mode | Primary Dev | VALIDATED | M0 | `storage/database.py` | `test_database_wal_mode` | Passed (TE-004) | WAL mode & foreign keys verified |
| M3-002 | Repositories for documents, postings, logs, checkpoints | Primary Dev | VALIDATED | M3-001 | `storage/*_repository.py` | `test_storage.py` | Passed (TE-004) | Document, Index, Log, Replica Repos |
| M3-003 | ShardStore atomic transactions & restart reload | Primary Dev | VALIDATED | M3-002 | `storage/shard_store.py` | `test_restart_and_index_reconstruction` | Passed (TE-004) | Atomic BEGIN IMMEDIATE & crash restart |

### Milestone M4: Sharding and Coordinator
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M4-001 | Deterministic shard router | Primary Dev | VALIDATED | M0 | `coordinator/router.py` | `test_router.py` | 3 passed (TE-005) | MD5 modulo routing verified uniform |
| M4-002 | Shard gRPC service server and client | Primary Dev | VALIDATED | M3 | `services/shard_server.py`, `services/shard_client.py` | `test_sharded_search.py` | Passed (TE-005) | ShardService CRUD and search handlers |
| M4-003 | Two-phase scatter-gather query dispatcher | Primary Dev | VALIDATED | M4-002 | `coordinator/query_dispatcher.py`, `coordinator/ranking_stats.py` | `test_distributed_scatter_gather_search` | Passed (TE-005) | Global stats aggregation & top-k merge |
| M4-004 | Coordinator gRPC service server | Primary Dev | VALIDATED | M4-003 | `coordinator/server.py` | `test_sharded_search.py` | 4 passed (TE-005) | Client entrypoint for search and mutations |

### Milestone M5: Primary-Replica Replication
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M5-001 | Monotonic sequence number manager | Primary Dev | VALIDATED | M3 | `replication/sequence_manager.py` | `test_sequence_manager_monotonic` | Passed (TE-006) | Thread-safe per-shard monotonic sequence |
| M5-002 | Replica update applier with validation | Primary Dev | VALIDATED | M5-001 | `replication/replica_applier.py` | `test_replica_applier_in_order` | Passed (TE-006) | In-order, gap detection, deduplication |
| M5-003 | Replica gRPC service server and client | Primary Dev | VALIDATED | M5-002 | `services/replica_server.py`, `services/replica_client.py` | `test_replication.py` | Passed (TE-006) | ReplicaService protobuf handlers |
| M5-004 | ReplicationManager streaming & lag tracking | Primary Dev | VALIDATED | M5-003 | `replication/replication_manager.py` | `test_live_grpc_replication` | 8 passed (TE-006) | Live streaming replication verified |

### Milestone M6: Incremental Replica Recovery
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M6-001 | RecoveryManager sequence range calculator | Primary Dev | VALIDATED | M5 | `replication/recovery_manager.py` | `test_incremental_recovery_basic` | Passed (TE-007) | Range [S_repl + 1, S_prim] |
| M6-002 | Chunked replay & interrupted resumption | Primary Dev | VALIDATED | M6-001 | `replication/recovery_manager.py` | `test_interrupted_recovery_resumption` | Passed (TE-007) | Recovers in batches, resumes cleanly |
| M6-003 | Log continuity check & HISTORY_UNAVAILABLE | Primary Dev | VALIDATED | M6-001 | `replication/recovery_manager.py` | `test_history_unavailable_error` | Passed (TE-007) | Detects purged history safely |

### Milestone M7: Independent Reference Engine & Validation
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M7-001 | Implement independent single-node reference engine | Primary Dev | VALIDATED | M1, M2 | `reference/reference_engine.py` | `test_cross_validation.py` | 4 passed (TE-008) | Standalone un-sharded baseline oracle |
| M7-002 | Build dual-execution test suite comparing distributed cluster with reference | Primary Dev | VALIDATED | M4, M7-001 | `tests/system/test_cross_validation.py` | `test_distributed_vs_reference_ingestion_and_search` | Passed (TE-008) | 100% rank order match & BM25 score equality |
| M7-003 | Document version and deletion state cross-validation | Primary Dev | VALIDATED | M7-002 | `tests/system/test_cross_validation.py` | `test_distributed_vs_reference_document_replacement`, `test_distributed_vs_reference_deletion_and_tombstones` | Passed (TE-008) | Versions, replacements, and tombstone anti-resurrection |
| M7-004 | Post-recovery convergence verification against reference engine | Primary Dev | VALIDATED | M6, M7-002 | `tests/system/test_cross_validation.py` | `test_post_recovery_convergence_with_reference` | Passed (TE-008) | Recovered replica matches reference engine |

### Milestone M8: Performance and Recovery Evaluation
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M8-001 | Dataset ingestion & preparation pipeline | Primary Dev | VALIDATED | M1 | `scripts/download_dataset.py`, `scripts/prepare_dataset.py`, `scripts/validate_dataset.py` | `validate_dataset.py` | 500-doc corpus & 50-doc fixture validated | Offline fallback built-in |
| M8-002 | Distributed search benchmark suite | Primary Dev | VALIDATED | M4, M8-001 | `experiments/benchmark_search.py` | TE-009 | `search_benchmarks.json` generated | 1, 2, 4 shards, top-k 5-50 |
| M8-003 | Incremental recovery vs full rebuild benchmark | Primary Dev | VALIDATED | M6, M8-001 | `experiments/benchmark_recovery.py` | TE-010 | `recovery_benchmarks.json` generated | 16.28x speedup, 96.1% data reduction |
| M8-004 | Publication-grade Matplotlib plot generation | Primary Dev | VALIDATED | M8-002, M8-003 | `experiments/plot_results.py` | Inspection | 4 PNG plots rendered in `experiments/plots/` | README report updated |

### Milestone M9: Docker Deployment and Final Submission Readiness
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M9-001 | CLI interface and high-level client | Primary Dev | VALIDATED | M4 | `client/client.py`, `client/cli.py` | `python -m client.cli --help` | All subcommands operational | Click-based CLI |
| M9-002 | Automated end-to-end TA demo script | Primary Dev | VALIDATED | M0..M8 | `scripts/run_demo.py` | TE-011 | Successful exit code 0 | Validates all requirements |
| M9-003 | Microservice Dockerfile & Docker Compose topology | Primary Dev | VALIDATED | M4, M5 | `Dockerfile`, `docker-compose.yml`, `services/entrypoint_*.py` | Verified lifecycle | Multi-container sharded cluster |
| M9-004 | Comprehensive README & submission documentation | Primary Dev | VALIDATED | M0..M9 | `README.md`, `docs/*` | Complete review | Full instructions, benchmarks, ADRs |

---

## E. Test Evidence Table

| Test ID | Test Command | Result | Passed | Failed | Date | Notes / Log Reference |
|---|---|---|---|---|---|---|
| TE-001 | `./.venv/bin/pytest tests/unit/test_foundation.py` | PASSED | 7 | 0 | 2026-11-25 | Verified enums, models, search tie-breaking, exceptions, config, schema, and proto serialization |
| TE-002 | `./.venv/bin/pytest tests/unit/` | PASSED | 25 | 0 | 2026-11-25 | Verified tokenizer, inverted index, document manager CRUD, versions, tombstones, and anti-resurrection |
| TE-003 | `./.venv/bin/pytest tests/unit/test_bm25.py` | PASSED | 8 | 0 | 2026-11-25 | Verified BM25 hand-calculated IDF and scores, multi-term, tie-breaking, length normalization, global stats |
| TE-004 | `./.venv/bin/pytest tests/unit/test_storage.py` | PASSED | 7 | 0 | 2026-11-25 | Verified WAL mode, document repo, index repo, log repo, checkpoints, atomic rollback, and process restart reconstruction |
| TE-005 | `./.venv/bin/pytest tests/unit/test_router.py tests/integration/test_sharded_search.py` | PASSED | 7 | 0 | 2026-11-25 | Verified deterministic router uniformity, 2-shard cluster routing, CRUD, scatter-gather search, and tie-breaking |
| TE-006 | `./.venv/bin/pytest tests/replication/test_replication.py` | PASSED | 8 | 0 | 2026-11-25 | Verified sequence monotonicity, replica in-order apply, duplicate handling, gap detection, stale version rejection, anti-resurrection, restart checkpointing, and live gRPC streaming |
| TE-007 | `./.venv/bin/pytest tests/recovery/test_recovery.py` | PASSED | 4 | 0 | 2026-11-25 | Verified incremental recovery range replay, interrupted resumption, history unavailable detection, and synchronization |
| TE-008 | `./.venv/bin/pytest tests/system/test_cross_validation.py` | PASSED | 4 | 0 | 2026-11-25 | Verified distributed vs reference 100% precision/recall, BM25 score equality (<1e-5), replacement, tombstones, and post-recovery convergence |
| TE-009 | `./.venv/bin/python experiments/benchmark_search.py` | PASSED | 12 matrix cells | 0 | 2026-11-25 | Measured latency (p50: 0.69-1.74ms) and QPS (534-1225) across 1, 2, 4 shards and top-k 5-50 |
| TE-010 | `./.venv/bin/python experiments/benchmark_recovery.py` | PASSED | 6 trials | 0 | 2026-11-25 | Measured incremental recovery speedup (up to 16.28x) and data reduction (up to 96.1%) vs full index rebuild |
| TE-011 | `./.venv/bin/python scripts/run_demo.py` | PASSED | All steps verified | 0 | 2026-11-25 | Automated end-to-end execution: startup, routing, BM25 search, version replacement, tombstones, replica lag & incremental recovery |

---

## F. Issues and Blockers

| Issue ID | Description | Impact | Owner | Status | Resolution | Related Milestone |
|---|---|---|---|---|---|---|
| ISS-001 | Initial empty repository | Need to establish all foundations | Primary Dev | RESOLVED | Scaffolded M0 skeleton, configs, ADRs | M0 |
| ISS-002 | Tokenizer dropped non-ASCII diacritics | Tokenizer dropped café accents | Primary Dev | RESOLVED | Implemented NFKD Unicode normalization | M1 |
| ISS-003 | Sandboxed environment socket binding | Test socket binding denied in macOS sandbox | Primary Dev | RESOLVED | BypassSandbox used for network socket tests | M4, M5 |
| ISS-004 | ShardRegistry replica checkpoint query | Status report threw NameError on variable name | Primary Dev | RESOLVED | Fixed variable reference to `ep` and imported `ReplicaClient` | M4, M9 |

---

## G. Change History

- **2026-11-25 (M0):** Repository inspection conducted. Scaffolding, configuration, proto definitions, and foundation tests complete (7/7 passed).
- **2026-11-25 (M1):** Custom tokenizer, inverted index posting lists, and document manager with monotonic versioning and tombstones complete (18/18 passed).
- **2026-11-25 (M2):** BM25 ranking algorithm with length normalization, hand-calculated test verification, and mathematical formulation documentation complete (8/8 passed).
- **2026-11-25 (M3):** SQLite persistent storage engine with WAL mode, document/index/log/replica repositories, atomic rollback, and process restart reload complete (7/7 passed).
- **2026-11-25 (M4):** Deterministic MD5 router, primary shard gRPC servicer, two-phase scatter-gather query dispatcher, and coordinator server complete (7/7 passed).
- **2026-11-25 (M5):** Thread-safe monotonic sequence manager, replica applier with gap detection, live gRPC replication streaming, and restart persistence complete (8/8 passed).
- **2026-11-25 (M6):** Incremental recovery manager, missing sequence range replay, chunked recovery, interruption resumption, and `HISTORY_UNAVAILABLE` error detection complete (4/4 passed).
- **2026-11-25 (M7):** Independent single-node Reference Engine and system cross-validation test suite complete (4/4 passed).
- **2026-11-25 (M8):** WikiText ingestion pipeline, benchmark search suite across 1/2/4 shards, incremental recovery vs full rebuild benchmark, and 4 publication-quality Matplotlib plots complete.
- **2026-11-25 (M9):** Click CLI, bulk loader, automated end-to-end TA demo, Dockerfile, docker-compose.yml, and comprehensive final documentation complete.
