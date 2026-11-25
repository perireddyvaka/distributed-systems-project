"""Shared Exceptions for Distributed Search Engine."""

from shared.enums import ErrorCode


class SearchEngineException(Exception):
    """Base exception for search engine errors."""
    def __init__(self, message: str, error_code: ErrorCode = ErrorCode.INTERNAL_ERROR):
        super().__init__(message)
        self.error_code = error_code
        self.message = message


class DocumentNotFoundError(SearchEngineException):
    def __init__(self, doc_id: str):
        super().__init__(f"Document '{doc_id}' not found", ErrorCode.DOCUMENT_NOT_FOUND)
        self.doc_id = doc_id


class StaleVersionError(SearchEngineException):
    def __init__(self, doc_id: str, current_version: int, incoming_version: int):
        super().__init__(
            f"Stale version for document '{doc_id}': current={current_version}, incoming={incoming_version}",
            ErrorCode.STALE_VERSION
        )
        self.doc_id = doc_id
        self.current_version = current_version
        self.incoming_version = incoming_version


class SequenceGapError(SearchEngineException):
    def __init__(self, shard_id: int, expected_seq: int, received_seq: int):
        super().__init__(
            f"Sequence gap on shard {shard_id}: expected={expected_seq}, received={received_seq}",
            ErrorCode.SEQUENCE_GAP
        )
        self.shard_id = shard_id
        self.expected_seq = expected_seq
        self.received_seq = received_seq


class HistoryUnavailableError(SearchEngineException):
    def __init__(self, shard_id: int, requested_seq: int, oldest_available_seq: int):
        super().__init__(
            f"History unavailable on shard {shard_id}: requested sequence {requested_seq} is older than oldest available {oldest_available_seq}",
            ErrorCode.HISTORY_UNAVAILABLE
        )
        self.shard_id = shard_id
        self.requested_seq = requested_seq
        self.oldest_available_seq = oldest_available_seq


class ShardUnavailableError(SearchEngineException):
    def __init__(self, shard_id: int, address: str):
        super().__init__(
            f"Shard {shard_id} at {address} is unavailable",
            ErrorCode.SHARD_UNAVAILABLE
        )
        self.shard_id = shard_id
        self.address = address


# Alias for compatibility
SearchEngineError = SearchEngineException
