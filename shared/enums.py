"""Shared Enumerations for Distributed Search Engine."""

from enum import Enum


class OperationType(str, Enum):
    """Operation types recorded in update log and replication events."""
    INSERT = "INSERT"
    REPLACE = "REPLACE"
    DELETE = "DELETE"


class ReplicaStatus(str, Enum):
    """Lifecycle status of a replica shard."""
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    RECOVERING = "RECOVERING"
    SYNCHRONIZED = "SYNCHRONIZED"
    FAILED = "FAILED"
    HISTORY_UNAVAILABLE = "HISTORY_UNAVAILABLE"


class RecoveryStatus(str, Enum):
    """Status model for incremental recovery operations."""
    IDLE = "IDLE"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    HISTORY_UNAVAILABLE = "HISTORY_UNAVAILABLE"


class ShardRole(str, Enum):
    """Role of a shard instance."""
    PRIMARY = "PRIMARY"
    REPLICA = "REPLICA"


class ErrorCode(str, Enum):
    """Standardized error codes across system."""
    OK = "OK"
    DOCUMENT_NOT_FOUND = "DOCUMENT_NOT_FOUND"
    DOCUMENT_ALREADY_EXISTS = "DOCUMENT_ALREADY_EXISTS"
    STALE_VERSION = "STALE_VERSION"
    TOMBSTONE_CONFLICT = "TOMBSTONE_CONFLICT"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    DUPLICATE_SEQUENCE = "DUPLICATE_SEQUENCE"
    HISTORY_UNAVAILABLE = "HISTORY_UNAVAILABLE"
    SHARD_UNAVAILABLE = "SHARD_UNAVAILABLE"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    INTERNAL_ERROR = "INTERNAL_ERROR"
