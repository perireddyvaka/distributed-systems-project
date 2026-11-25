"""Unit tests for Tokenizer."""

import pytest
from indexing.tokenizer import Tokenizer


def test_tokenizer_lowercase_normalization():
    text = "Hello World! DISTRIBUTED Systems"
    tokens = Tokenizer.tokenize(text)
    assert tokens == ["hello", "world", "distributed", "systems"]


def test_tokenizer_punctuation_handling():
    text = "Search-engine, with (punctuation)... and: symbols; $100% #hash!"
    tokens = Tokenizer.tokenize(text)
    assert "search" in tokens
    assert "engine" in tokens
    assert "punctuation" in tokens
    assert "100" in tokens
    assert "hash" in tokens


def test_tokenizer_empty_and_whitespace():
    assert Tokenizer.tokenize("") == []
    assert Tokenizer.tokenize("   \n\t  ") == []
    assert Tokenizer.tokenize(None) == []


def test_tokenizer_repeated_words_and_tf():
    text = "cat dog cat cat bird dog"
    tokens, tf, doc_len = Tokenizer.analyze(text)
    assert doc_len == 6
    assert tf["cat"] == 3
    assert tf["dog"] == 2
    assert tf["bird"] == 1


def test_tokenizer_unicode_normalization():
    # Accented / special characters normalized consistently
    text = "café naïve RESUMÉ"
    tokens = Tokenizer.tokenize(text)
    assert len(tokens) == 3
    # Check lowercased
    assert all(t.islower() for t in tokens)


def test_tokenizer_contractions():
    text = "don't can't it's"
    tokens = Tokenizer.tokenize(text)
    assert "don't" in tokens
    assert "can't" in tokens
    assert "it's" in tokens
