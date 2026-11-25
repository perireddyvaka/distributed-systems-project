"""Prepares raw text into clean, structured JSONL documents for distributed search.

Splits raw WikiText or offline text into coherent document units, generates deterministic
document IDs, and writes to datasets/processed/wikitext_corpus.jsonl and sample fixtures.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexing.tokenizer import Tokenizer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("prepare_dataset")


def clean_wikitext_line(line: str) -> str:
    """Clean WikiText formatting artifacts like '= = Header = =' and unknown tokens."""
    # Remove WikiText section headers e.g. = = = Heading = = =
    line = re.sub(r"=\s*=\s*[^=]+=\s*=", " ", line)
    line = re.sub(r"=\s*[^=]+=", " ", line)
    # Remove <unk> tokens
    line = line.replace("<unk>", "").strip()
    return line


def prepare_dataset(
    input_file: Path,
    output_file: Path,
    sample_file: Path | None = None,
    fixture_file: Path | None = None,
    min_doc_words: int = 15,
    max_docs: int = 2000,
    sample_size: int = 50,
) -> int:
    """Read raw input file, process into structured JSONL documents."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if sample_file:
        sample_file.parent.mkdir(parents=True, exist_ok=True)
    if fixture_file:
        fixture_file.parent.mkdir(parents=True, exist_ok=True)

    tokenizer = Tokenizer()
    documents = []
    current_paragraphs: list[str] = []

    with open(input_file, "r", encoding="utf-8") as f:
        for raw_line in f:
            cleaned = clean_wikitext_line(raw_line)
            if not cleaned:
                if current_paragraphs:
                    full_text = " ".join(current_paragraphs).strip()
                    tokens = tokenizer.tokenize(full_text)
                    if len(tokens) >= min_doc_words:
                        doc_id = f"doc_{len(documents) + 1:05d}"
                        documents.append({"doc_id": doc_id, "content": full_text})
                        if len(documents) >= max_docs:
                            break
                    current_paragraphs = []
            else:
                current_paragraphs.append(cleaned)

    # If any remaining paragraph
    if current_paragraphs and len(documents) < max_docs:
        full_text = " ".join(current_paragraphs).strip()
        tokens = tokenizer.tokenize(full_text)
        if len(tokens) >= min_doc_words:
            doc_id = f"doc_{len(documents) + 1:05d}"
            documents.append({"doc_id": doc_id, "content": full_text})

    logger.info("Prepared %d documents from %s", len(documents), input_file)

    # Write processed corpus
    with open(output_file, "w", encoding="utf-8") as f_out:
        for doc in documents:
            f_out.write(json.dumps(doc) + "\n")
    logger.info("Wrote full processed corpus to %s", output_file)

    # Write sample files
    sample_docs = documents[:sample_size]
    if sample_file:
        with open(sample_file, "w", encoding="utf-8") as f_samp:
            for doc in sample_docs:
                f_samp.write(json.dumps(doc) + "\n")
        logger.info("Wrote %d sample docs to %s", len(sample_docs), sample_file)

    if fixture_file:
        with open(fixture_file, "w", encoding="utf-8") as f_fix:
            for doc in sample_docs:
                f_fix.write(json.dumps(doc) + "\n")
        logger.info("Wrote %d fixture docs to %s", len(sample_docs), fixture_file)

    return len(documents)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare raw text into JSONL document corpus")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("datasets/raw/wikitext_raw.txt"),
        help="Input raw text file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("datasets/processed/wikitext_corpus.jsonl"),
        help="Output JSONL path",
    )
    parser.add_argument(
        "--sample",
        type=Path,
        default=Path("datasets/samples/sample_docs.jsonl"),
        help="Output sample JSONL path",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path("tests/fixtures/sample_corpus.jsonl"),
        help="Output test fixture JSONL path",
    )
    parser.add_argument("--min-words", type=int, default=15, help="Minimum words per document")
    parser.add_argument("--max-docs", type=int, default=2000, help="Maximum documents to produce")
    parser.add_argument("--sample-size", type=int, default=50, help="Number of docs for sample/fixture")

    args = parser.parse_args()
    prepare_dataset(
        input_file=args.input,
        output_file=args.output,
        sample_file=args.sample,
        fixture_file=args.fixture,
        min_doc_words=args.min_words,
        max_docs=args.max_docs,
        sample_size=args.sample_size,
    )


if __name__ == "__main__":
    main()
