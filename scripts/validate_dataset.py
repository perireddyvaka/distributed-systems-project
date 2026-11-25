"""Validates JSONL dataset format, schema, document uniqueness, and statistics."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from coordinator.router import Router
from indexing.tokenizer import Tokenizer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("validate_dataset")


def validate_dataset(file_path: Path, num_shards: int = 4) -> bool:
    """Validate JSONL file structure, document schema, and distribution across shards."""
    if not file_path.exists():
        logger.error("File does not exist: %s", file_path)
        return False

    tokenizer = Tokenizer()
    router = Router(num_shards=num_shards)

    seen_ids: set[str] = set()
    total_docs = 0
    total_words = 0
    vocab: set[str] = set()
    shard_counts: dict[int, int] = {i: 0 for i in range(num_shards)}

    logger.info("Validating dataset at: %s", file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error("Line %d: Invalid JSON - %s", line_num, e)
                return False

            if not isinstance(record, dict):
                logger.error("Line %d: Record is not a dictionary", line_num)
                return False

            doc_id = record.get("doc_id")
            content = record.get("content")

            if not doc_id or not isinstance(doc_id, str):
                logger.error("Line %d: Missing or invalid doc_id", line_num)
                return False

            if not content or not isinstance(content, str):
                logger.error("Line %d: Missing or empty content", line_num)
                return False

            if doc_id in seen_ids:
                logger.error("Line %d: Duplicate doc_id '%s'", line_num, doc_id)
                return False
            seen_ids.add(doc_id)

            tokens = tokenizer.tokenize(content)
            if not tokens:
                logger.error("Line %d: Document has 0 tokens after tokenization", line_num)
                return False

            total_docs += 1
            total_words += len(tokens)
            vocab.update(tokens)

            shard_id = router.get_shard_id(doc_id)
            shard_counts[shard_id] += 1

    if total_docs == 0:
        logger.error("Validation failed: dataset is empty")
        return False

    avg_words = total_words / total_docs
    logger.info("Validation SUCCESSFUL!")
    logger.info("Total Documents : %d", total_docs)
    logger.info("Unique Words    : %d", len(vocab))
    logger.info("Total Words     : %d", total_words)
    logger.info("Avg Doc Length  : %.2f terms", avg_words)
    logger.info("Shard Distribution (N=%d shards):", num_shards)
    for shard_id, count in shard_counts.items():
        pct = (count / total_docs) * 100
        logger.info("  Shard %d: %4d docs (%.1f%%)", shard_id, count, pct)

    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate JSONL dataset format and schema")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("datasets/processed/wikitext_corpus.jsonl"),
        help="JSONL dataset file to validate",
    )
    parser.add_argument("--num-shards", type=int, default=4, help="Number of shards for routing check")
    args = parser.parse_args()

    success = validate_dataset(args.file, num_shards=args.num_shards)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
