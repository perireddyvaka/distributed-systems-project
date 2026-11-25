"""Shared Data Models for Distributed Search Engine."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from shared.enums import OperationType, ReplicaStatus, ShardRole


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Document:
    """Represents a document in the search engine."""
    doc_id: str
    content: str
    version: int = 1
    is_deleted: bool = False
    document_length: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = _now()
        if self.updated_at is None:
            self.updated_at = _now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "content": self.content,
            "version": self.version,
            "is_deleted": self.is_deleted,
            "document_length": self.document_length,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Document":
        created = datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None
        updated = datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else None
        return cls(
            doc_id=data["doc_id"],
            content=data["content"],
            version=data.get("version", 1),
            is_deleted=bool(data.get("is_deleted", False)),
            document_length=data.get("document_length", 0),
            created_at=created,
            updated_at=updated,
        )


@dataclass
class SearchResult:
    """Represents a single scored search result with deterministic tie-breaking."""
    doc_id: str
    score: float
    shard_id: int
    content: Optional[str] = None
    version: int = 1
    snippet: str = ""

    def __lt__(self, other: "SearchResult") -> bool:
        # Score descending; tie-break by doc_id ascending
        if abs(self.score - other.score) > 1e-9:
            return self.score > other.score
        return self.doc_id < other.doc_id


@dataclass
class RankingStatsSnapshot:
    """Consistent ranking statistics across the distributed corpus."""
    total_documents: int = 0
    avg_document_length: float = 0.0
    document_frequencies: Dict[str, int] = field(default_factory=dict)
    stats_version: int = 1


RankingStats = RankingStatsSnapshot


@dataclass
class UpdateEvent:
    """Represents a logged mutation event for replication and recovery."""
    shard_id: int
    sequence_number: int
    doc_id: str
    operation: OperationType
    version: int
    payload: Optional[str] = None  # JSON or text content
    committed_at: Optional[datetime] = None

    def __post_init__(self):
        if self.committed_at is None:
            self.committed_at = _now()
        if isinstance(self.operation, str) and not isinstance(self.operation, OperationType):
            self.operation = OperationType(self.operation)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "sequence_number": self.sequence_number,
            "doc_id": self.doc_id,
            "operation": self.operation.value,
            "version": self.version,
            "payload": self.payload,
            "committed_at": self.committed_at.isoformat() if self.committed_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UpdateEvent":
        committed = datetime.fromisoformat(data["committed_at"]) if data.get("committed_at") else None
        return cls(
            shard_id=data["shard_id"],
            sequence_number=data["sequence_number"],
            doc_id=data["doc_id"],
            operation=OperationType(data["operation"]),
            version=data["version"],
            payload=data.get("payload"),
            committed_at=committed,
        )


@dataclass
class ShardStatus:
    """Status information for a shard instance."""
    shard_id: int
    role: ShardRole = ShardRole.PRIMARY
    latest_sequence: int = 0
    total_documents: int = 0
    is_healthy: bool = True
    primary_address: Optional[str] = None
    replica_address: Optional[str] = None
    replication_lag: Optional[int] = 0
    replica_applied_sequence: Optional[int] = 0
    replica_status: Optional[ReplicaStatus] = ReplicaStatus.DISCONNECTED
