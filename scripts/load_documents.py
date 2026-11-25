"""Bulk loads JSONL corpus into the distributed search engine via the coordinator."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from client.client import SearchEngineClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("load_documents")


def load_documents(
    corpus_path: Path,
    coordinator_target: str = "127.0.0.1:50050",
    max_docs: int = 1000,
    batch_log: int = 50,
) -> int:
    """Read documents from corpus_path and insert into cluster."""
    if not corpus_path.exists():
        logger.error("Corpus file does not exist: %s", corpus_path)
        return 0

    client = SearchEngineClient(coordinator_target)
    loaded = 0
    t0 = time.perf_counter()

    logger.info("Connecting to coordinator at %s...", coordinator_target)
    logger.info("Ingesting corpus from %s (max_docs=%d)...", corpus_path, max_docs)

    try:
        with open(corpus_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                if loaded >= max_docs:
                    break
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                doc_id = record["doc_id"]
                content = record["content"]

                resp = client.insert(doc_id=doc_id, content=content)
                if not resp.success:
                    logger.warning("Failed to insert doc %s: %s", doc_id, resp.error_message)
                else:
                    loaded += 1

                if loaded % batch_log == 0:
                    elapsed = time.perf_counter() - t0
                    qps = loaded / elapsed if elapsed > 0 else 0
                    logger.info("Ingested %4d documents (%.1f docs/sec)", loaded, qps)

        elapsed = time.perf_counter() - t0
        qps = loaded / elapsed if elapsed > 0 else 0
        logger.info("SUCCESS: Ingested %d documents in %.2f seconds (%.1f docs/sec)", loaded, elapsed, qps)
        return loaded
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk load documents into distributed cluster")
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("datasets/processed/wikitext_corpus.jsonl"),
        help="Path to JSONL corpus file",
    )
    parser.add_argument(
        "--coordinator",
        type=str,
        default="127.0.0.1:50050",
        help="Coordinator host:port",
    )
    parser.add_argument("--max-docs", type=int, default=1000, help="Maximum documents to load")
    args = parser.parse_args()

    load_documents(args.corpus, args.coordinator, args.max_docs)


if __name__ == "__main__":
    main()
