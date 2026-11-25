"""Unit tests for deterministic document router."""

import pytest
from coordinator.router import Router


def test_router_deterministic_consistency():
    router = Router(num_shards=2)
    doc_id = "wiki_article_1059"
    shard1 = router.get_shard_id(doc_id)
    shard2 = router.get_shard_id(doc_id)
    assert shard1 == shard2
    assert shard1 in {0, 1}


def test_router_uniform_distribution():
    num_shards = 4
    router = Router(num_shards=num_shards)
    counts = {s: 0 for s in range(num_shards)}

    # Test across 1,000 synthetic IDs
    for i in range(1000):
        shard_id = router.get_shard_id(f"doc_{i}")
        counts[shard_id] += 1

    # Every shard should get a reasonable share (~250 +- 70)
    for s in range(num_shards):
        assert counts[s] > 180, f"Shard {s} received too few documents: {counts[s]}"


def test_router_invalid_arguments():
    with pytest.raises(ValueError):
        Router(num_shards=0)

    router = Router(num_shards=2)
    with pytest.raises(ValueError):
        router.get_shard_id("")
