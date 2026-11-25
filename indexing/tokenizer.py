"""Text tokenization and normalization for custom inverted index."""

import re
import unicodedata
from collections import Counter
from typing import Dict, List, Tuple


# Regex pattern to match word tokens (alphanumeric words, keeping internal contractions/apostrophes)
TOKEN_PATTERN = re.compile(r"\b[a-zA-Z0-9]+(?:'[a-zA-Z0-9]+)?\b")


class Tokenizer:
    """Deterministic text tokenizer and normalizer."""

    @staticmethod
    def normalize(text: str) -> str:
        """Normalize text: NFKD decomposition, accent stripping, and lowercase."""
        if not text:
            return ""
        # Strip diacritical marks (e.g. café -> cafe, résumé -> resume)
        decomposed = unicodedata.normalize("NFD", text)
        stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
        return stripped.lower()

    @classmethod
    def tokenize(cls, text: str) -> List[str]:
        """Tokenize input text into normalized token stream."""
        if not text or not isinstance(text, str):
            return []
        normalized_text = cls.normalize(text)
        return TOKEN_PATTERN.findall(normalized_text)

    @classmethod
    def analyze(cls, text: str) -> Tuple[List[str], Dict[str, int], int]:
        """Analyze text and produce tokens, term frequencies, and document length.
        
        Returns:
            Tuple of:
            - tokens: List[str] of extracted tokens in sequence
            - term_frequencies: Dict[str, int] mapping term -> frequency count
            - document_length: int total number of tokens
        """
        tokens = cls.tokenize(text)
        doc_length = len(tokens)
        term_frequencies = dict(Counter(tokens))
        return tokens, term_frequencies, doc_length
