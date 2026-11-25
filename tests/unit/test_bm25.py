"""Unit tests for BM25 ranking algorithm and scoring."""

import math
import pytest
from indexing.bm25 import BM25Scorer
from indexing.inverted_index import InvertedIndex
from indexing.tokenizer import Tokenizer
from shared.models import RankingStatsSnapshot


def test_bm25_idf_hand_calculated():
    scorer = BM25Scorer()
    # N = 10, DF = 2
    # IDF = ln(1 + (10 - 2 + 0.5) / (2 + 0.5)) = ln(1 + 8.5 / 2.5) = ln(4.4)
    expected_idf = math.log(1.0 + 8.5 / 2.5)
    computed_idf = scorer.compute_idf(total_docs=10, doc_freq=2)
    assert abs(computed_idf - expected_idf) < 1e-6
    assert abs(computed_idf - 1.4816045) < 1e-5


def test_bm25_idf_edge_cases():
    scorer = BM25Scorer()
    # Total docs 0 returns 0
    assert scorer.compute_idf(total_docs=0, doc_freq=0) == 0.0
    # DF 0
    assert scorer.compute_idf(total_docs=5, doc_freq=0) > 0.0
    # DF == N (term in all documents): IDF remains positive because of +1 smoothing
    idf_all = scorer.compute_idf(total_docs=10, doc_freq=10)
    assert idf_all > 0.0
    assert abs(idf_all - math.log(1.0 + 0.5 / 10.5)) < 1e-6


def test_bm25_single_term_hand_calculated():
    scorer = BM25Scorer(k1=1.2, b=0.75)
    index = InvertedIndex()
    # doc1: "distributed systems" (length = 2, tf(distributed) = 1)
    # doc2: "database systems" (length = 2, tf(distributed) = 0)
    index.add_document("doc1", {"distributed": 1, "systems": 1}, 2)
    index.add_document("doc2", {"database": 1, "systems": 1}, 2)

    # N = 2, avgdl = 2.0, DF(distributed) = 1
    # len_norm = (1 - 0.75) + 0.75 * (2 / 2.0) = 1.0
    # term_weight = 1 * (1.2 + 1.0) / (1 + 1.2 * 1.0) = 2.2 / 2.2 = 1.0
    # score = IDF * 1.0 = ln(1 + (2 - 1 + 0.5) / (1 + 0.5)) = ln(1 + 1.5 / 1.5) = ln(2.0)
    expected_score = math.log(2.0)

    results = scorer.search("distributed", index, top_k=10)
    assert len(results) == 1
    assert results[0].doc_id == "doc1"
    assert abs(results[0].score - expected_score) < 1e-6


def test_bm25_multi_term_scoring():
    scorer = BM25Scorer()
    index = InvertedIndex()
    # doc1 has both query terms
    index.add_document("doc1", {"distributed": 2, "search": 1}, 3)
    # doc2 has only one query term
    index.add_document("doc2", {"distributed": 1, "systems": 2}, 3)
    # doc3 has none
    index.add_document("doc3", {"compiler": 1, "design": 1}, 2)

    results = scorer.search("distributed search", index, top_k=10)
    assert len(results) == 2
    # doc1 should rank higher than doc2 because it contains both terms and higher TF
    assert results[0].doc_id == "doc1"
    assert results[1].doc_id == "doc2"
    assert results[0].score > results[1].score


def test_bm25_document_length_normalization():
    scorer = BM25Scorer(k1=1.2, b=0.75)
    index = InvertedIndex()
    # doc_short: "search engine" (len = 2, tf(search) = 1)
    # doc_long: "search engine with lots of extra unrelated words here" (len = 9, tf(search) = 1)
    index.add_document("doc_short", {"search": 1, "engine": 1}, 2)
    index.add_document(
        "doc_long",
        {"search": 1, "engine": 1, "extra": 1, "words": 6},
        9,
    )

    results = scorer.search("search", index)
    assert len(results) == 2
    # The shorter document has higher term density so BM25 gives it a higher score
    assert results[0].doc_id == "doc_short"
    assert results[1].doc_id == "doc_long"
    assert results[0].score > results[1].score


def test_deterministic_tie_breaking():
    scorer = BM25Scorer()
    index = InvertedIndex()
    # Two documents with identical term frequencies and lengths
    index.add_document("doc_z", {"apple": 1, "banana": 1}, 2)
    index.add_document("doc_a", {"apple": 1, "banana": 1}, 2)

    results = scorer.search("apple", index, top_k=10)
    assert len(results) == 2
    assert abs(results[0].score - results[1].score) < 1e-9
    # Must tie-break by doc_id ASC: doc_a before doc_z
    assert results[0].doc_id == "doc_a"
    assert results[1].doc_id == "doc_z"


def test_top_k_truncation():
    scorer = BM25Scorer()
    index = InvertedIndex()
    for i in range(10):
        index.add_document(f"doc_{i}", {"test": 1}, 1)

    top3 = scorer.search("test", index, top_k=3)
    assert len(top3) == 3
    # Sorted by doc_id ASC due to identical scores
    assert [r.doc_id for r in top3] == ["doc_0", "doc_1", "doc_2"]


def test_global_ranking_stats_consistency():
    scorer = BM25Scorer()
    # Shard 1 has 1 doc
    index1 = InvertedIndex()
    index1.add_document("d1", {"search": 1}, 2)

    # Shard 2 has 9 docs
    index2 = InvertedIndex()
    index2.add_document("d2", {"search": 1}, 2)
    for i in range(3, 11):
        index2.add_document(f"d{i}", {"other": 1}, 5)

    # If each scored using local stats, IDF would be wildly different:
    local_score_s1 = scorer.search("search", index1)[0].score
    local_score_s2 = scorer.search("search", index2)[0].score
    assert abs(local_score_s1 - local_score_s2) > 0.5  # S1 and S2 disagree locally!

    # Now use a consistent GlobalRankingSnapshot across both shards:
    global_stats = RankingStatsSnapshot(
        total_documents=10,
        avg_document_length=4.7,
        document_frequencies={"search": 2},
        stats_version=1,
    )

    global_score_s1 = scorer.search("search", index1, stats=global_stats)[0].score
    global_score_s2 = scorer.search("search", index2, stats=global_stats)[0].score

    # With consistent global stats, identical documents get EXACTLY the same score!
    assert abs(global_score_s1 - global_score_s2) < 1e-9
