"""Deterministic document routing using stable MD5 hashing."""

import hashlib


class Router:
    """Routes documents to shards deterministically."""

    def __init__(self, num_shards: int):
        if num_shards <= 0:
            raise ValueError("Number of shards must be greater than zero.")
        self.num_shards = num_shards

    def get_shard_id(self, doc_id: str) -> int:
        """Compute owning shard ID using stable MD5 modulo.
        
        Formula:
            int(MD5(doc_id)[:8], 16) % num_shards
        """
        if not doc_id:
            raise ValueError("doc_id cannot be empty")
        # Compute MD5 digest of UTF-8 encoded doc_id
        digest = hashlib.md5(doc_id.encode("utf-8")).hexdigest()
        # Take first 8 hexadecimal characters as 32-bit unsigned int
        hash_val = int(digest[:8], 16)
        return hash_val % self.num_shards
