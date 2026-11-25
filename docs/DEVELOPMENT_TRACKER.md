# Development Tracker

## A. Project Information

- **Project Title:** Distributed Search Engine with Incremental Index Recovery
- **Project Objective:** Implement an Elasticsearch-inspired distributed text search engine using custom sharded inverted indexes, primary-replica replication, persistent SQLite update logs, and incremental recovery by replaying missed sequence updates.
- **Repository Location:** `/Users/perireddyvaka/IIITH/MS/Monsoon-26/DS/assignments/project`
- **Current Version:** `0.1.0`
- **Current Milestone:** `M3 — SQLite persistence`
- **Overall Progress:** 35%
- **Last Updated Date:** 2026-11-25
- **Current Project Status:** IN PROGRESS

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
| M0 | Repository inspection and project foundation | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 7 unit tests passed (test_foundation.py) |
| M1 | Text processing and custom inverted index | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 18 unit tests passed (tokenizer, index, doc mgr) |
| M2 | BM25 ranking | VALIDATED | 100% | 2026-11-25 | 2026-11-25 | 8 unit tests passed (test_bm25.py) |
| M3 | SQLite persistence | NOT STARTED | 0% | — | — | — |
| M4 | Sharding and coordinator | NOT STARTED | 0% | — | — | — |
| M5 | Primary-replica replication | NOT STARTED | 0% | — | — | — |
| M6 | Incremental replica recovery | NOT STARTED | 0% | — | — | — |
| M7 | Independent reference engine and correctness validation | NOT STARTED | 0% | — | — | — |
| M8 | Performance and recovery evaluation | NOT STARTED | 0% | — | — | — |
| M9 | Docker deployment and final submission readiness | NOT STARTED | 0% | — | — | — |

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
| M1-001 | Implement text tokenizer & normalizer | Primary Dev | VALIDATED | M0 | `indexing/tokenizer.py` | `test_tokenizer.py` | 6 passed (TE-002) | Lowercase, punct, accents, contractions |
| M1-002 | Implement in-memory inverted index | Primary Dev | VALIDATED | M1-001 | `indexing/inverted_index.py` | `test_inverted_index.py` | 5 passed (TE-002) | Postings map, doc length, DF, clear, reconstruct |
| M1-003 | Implement document manager with versioning & tombstones | Primary Dev | VALIDATED | M1-002 | `indexing/document_manager.py` | `test_document_manager.py` | 7 passed (TE-002) | Monotonic versions, tombstones, anti-resurrection |
| M1-004 | CRUD operations unit test suite | Primary Dev | VALIDATED | M1-003 | `tests/unit/test_document_manager.py` | `pytest tests/unit/` | 25 total passed (TE-002) | Insert, replace, delete, tombstone conflict |

### Milestone M2: BM25 Ranking
| Task ID | Task Description | Owner | Status | Dependencies | Files/Modules | Test IDs | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|
| M2-001 | Implement BM25 scoring algorithm | Primary Dev | VALIDATED | M1 | `indexing/bm25.py` | `test_bm25.py` | 8 passed (TE-003) | Okapi BM25 formula with smoothing |
| M2-002 | Implement global/local corpus statistics structures | Primary Dev | VALIDATED | M2-001 | `indexing/bm25.py` | `test_bm25.py` | Passed (TE-003) | N, avgdl, DF(t) snapshot support |
| M2-003 | Implement multi-term query scoring & tie-breaking | Primary Dev | VALIDATED | M2-002 | `indexing/bm25.py` | `test_bm25.py` | Passed (TE-003) | Score DESC, doc_id ASC tie-break |
| M2-004 | Known-answer ranking tests | Primary Dev | VALIDATED | M2-003 | `tests/unit/test_bm25.py` | `test_bm25_idf_hand_calculated` | Passed (TE-003) | Hand-calculated values validated |

---

## E. Test Evidence Table

| Test ID | Test Command | Result | Passed | Failed | Date | Notes / Log Reference |
|---|---|---|---|---|---|---|
| TE-001 | `./.venv/bin/pytest tests/unit/test_foundation.py` | PASSED | 7 | 0 | 2026-11-25 | Verified enums, models, search tie-breaking, exceptions, config, schema, and proto serialization |
| TE-002 | `./.venv/bin/pytest tests/unit/` | PASSED | 25 | 0 | 2026-11-25 | Verified tokenizer, inverted index, document manager CRUD, versions, tombstones, and anti-resurrection |
| TE-003 | `./.venv/bin/pytest tests/unit/test_bm25.py` | PASSED | 8 | 0 | 2026-11-25 | Verified BM25 hand-calculated IDF and scores, multi-term, tie-breaking, length normalization, global stats |

---

## F. Issues and Blockers

| Issue ID | Description | Impact | Owner | Status | Resolution | Related Milestone |
|---|---|---|---|---|---|---|
| ISS-001 | Initial empty repository | Need to establish all foundations | Primary Dev | RESOLVED | Scaffolded M0 skeleton, configs, ADRs | M0 |

---

## G. Change History

- **2026-11-25:** Repository inspection conducted. Confirmed clean state. Created directory layout, `requirements.txt`, `pyproject.toml`, `pytest.ini`, `.env.example`, `.gitignore`, `docs/DECISIONS.md`, `docs/ARCHITECTURE.md`, `docs/DEVELOPMENT_TRACKER.md`. Initialized Python virtualenv and installed dependencies. Compiled protobufs and validated schema, enums, models, config, and proto serialization in `tests/unit/test_foundation.py` (5/5 passed). M0 is complete.
