"""Per-shard sequence manager allocating monotonic integers."""

import threading
from typing import Optional
from storage.log_repository import LogRepository


class SequenceManager:
    """Allocates monotonically increasing sequence numbers per shard.
    
    Guarantees thread-safe atomic sequence increments S_{t+1} = S_t + 1.
    """

    def __init__(self, shard_id: int, initial_sequence: int = 0):
        self.shard_id = shard_id
        self._current_sequence = initial_sequence
        self._lock = threading.Lock()

    @classmethod
    def from_log_repository(cls, shard_id: int, log_repo: LogRepository) -> "SequenceManager":
        """Initialize SequenceManager by loading the latest committed sequence from storage."""
        latest = log_repo.get_latest_sequence(shard_id)
        return cls(shard_id=shard_id, initial_sequence=latest)

    def next_sequence(self) -> int:
        """Atomically increment and return the next sequence number."""
        with self._lock:
            self._current_sequence += 1
            return self._current_sequence

    @property
    def current_sequence(self) -> int:
        """Get the current sequence number without incrementing."""
        with self._lock:
            return self._current_sequence

    def set_sequence(self, seq: int) -> None:
        """Update current sequence (e.g. after database load or restore)."""
        with self._lock:
            if seq < self._current_sequence:
                raise ValueError(
                    f"Cannot set sequence backwards: current={self._current_sequence}, new={seq}"
                )
            self._current_sequence = seq
