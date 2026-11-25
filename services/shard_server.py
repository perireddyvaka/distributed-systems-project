"""gRPC Shard Service implementation for Primary and Replica nodes."""

import grpc
from concurrent import futures
from typing import Optional
from proto.generated import shard_pb2, shard_pb2_grpc
from indexing.bm25 import BM25Scorer
from indexing.document_manager import (
    DocumentManager,
    DocumentAlreadyExistsError,
    TombstoneConflictError,
)
from indexing.tokenizer import Tokenizer
from shared.enums import ErrorCode, OperationType, ShardRole
from shared.exceptions import DocumentNotFoundError, StaleVersionError
from shared.models import RankingStatsSnapshot
from storage.database import DatabaseManager
from storage.shard_store import ShardStore


class ShardServicer(shard_pb2_grpc.ShardServiceServicer):
    """Implementation of ShardService gRPC contract."""

    def __init__(
        self,
        shard_id: int,
        store: ShardStore,
        role: ShardRole = ShardRole.PRIMARY,
        doc_manager: Optional[DocumentManager] = None,
        bm25: Optional[BM25Scorer] = None,
    ):
        self.shard_id = shard_id
        self.store = store
        self.role = role
        self.doc_manager = doc_manager if doc_manager is not None else store.reconstruct_in_memory_state()
        self.bm25 = bm25 if bm25 is not None else BM25Scorer()

    def InsertDocument(self, request, context):
        """Insert a new document on the primary shard."""
        try:
            # 1. Update in-memory document manager
            doc = self.doc_manager.insert(request.doc_id, request.content, version=1)
            _, term_freqs, _ = Tokenizer.analyze(doc.content)

            # 2. Allocate sequence number and atomically commit to SQLite
            latest_seq = self.store.log_repo.get_latest_sequence(self.shard_id)
            seq = latest_seq + 1
            self.store.execute_primary_mutation(doc, term_freqs, seq, OperationType.INSERT)

            return shard_pb2.ShardMutationResponse(
                success=True,
                shard_id=self.shard_id,
                sequence_number=seq,
                version=doc.version,
                error_code=ErrorCode.OK.value,
            )
        except DocumentAlreadyExistsError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.DOCUMENT_ALREADY_EXISTS.value,
                error_message=str(e),
            )
        except TombstoneConflictError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.TOMBSTONE_CONFLICT.value,
                error_message=str(e),
            )
        except Exception as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def ReplaceDocument(self, request, context):
        """Replace an existing document on the primary shard."""
        try:
            expected_ver = request.expected_version if request.expected_version > 0 else None
            doc = self.doc_manager.replace(request.doc_id, request.content, expected_version=expected_ver)
            _, term_freqs, _ = Tokenizer.analyze(doc.content)

            latest_seq = self.store.log_repo.get_latest_sequence(self.shard_id)
            seq = latest_seq + 1
            self.store.execute_primary_mutation(doc, term_freqs, seq, OperationType.REPLACE)

            return shard_pb2.ShardMutationResponse(
                success=True,
                shard_id=self.shard_id,
                sequence_number=seq,
                version=doc.version,
                error_code=ErrorCode.OK.value,
            )
        except DocumentNotFoundError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.DOCUMENT_NOT_FOUND.value,
                error_message=str(e),
            )
        except StaleVersionError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.STALE_VERSION.value,
                error_message=str(e),
            )
        except Exception as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def DeleteDocument(self, request, context):
        """Delete document and create versioned tombstone."""
        try:
            expected_ver = request.expected_version if request.expected_version > 0 else None
            tombstone = self.doc_manager.delete(request.doc_id, expected_version=expected_ver)

            latest_seq = self.store.log_repo.get_latest_sequence(self.shard_id)
            seq = latest_seq + 1
            self.store.execute_primary_mutation(tombstone, {}, seq, OperationType.DELETE)

            return shard_pb2.ShardMutationResponse(
                success=True,
                shard_id=self.shard_id,
                sequence_number=seq,
                version=tombstone.version,
                error_code=ErrorCode.OK.value,
            )
        except DocumentNotFoundError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.DOCUMENT_NOT_FOUND.value,
                error_message=str(e),
            )
        except StaleVersionError as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.STALE_VERSION.value,
                error_message=str(e),
            )
        except Exception as e:
            return shard_pb2.ShardMutationResponse(
                success=False,
                shard_id=self.shard_id,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def GetDocument(self, request, context):
        """Fetch document from shard."""
        doc = self.doc_manager.get_document(request.doc_id, include_deleted=True)
        if doc is None:
            return shard_pb2.GetDocumentResponse(found=False, doc_id=request.doc_id)
        return shard_pb2.GetDocumentResponse(
            found=True,
            doc_id=doc.doc_id,
            content=doc.content,
            version=doc.version,
            is_deleted=doc.is_deleted,
            document_length=doc.document_length,
        )

    def GetRankingStats(self, request, context):
        """Compute local statistics for terms to aggregate globally."""
        term_stats = []
        for term in request.terms:
            df = self.doc_manager.index.get_doc_frequency(term)
            term_stats.append(shard_pb2.TermFrequencyStat(term=term, doc_frequency=df))

        return shard_pb2.RankingStatsResponse(
            shard_id=self.shard_id,
            total_documents=self.doc_manager.index.total_documents,
            total_length=self.doc_manager.index.total_document_length,
            term_stats=term_stats,
        )

    def SearchLocal(self, request, context):
        """Execute local BM25 scoring with consistent global stats snapshot."""
        try:
            proto_stats = request.global_stats
            global_stats = RankingStatsSnapshot(
                total_documents=proto_stats.total_documents,
                avg_document_length=proto_stats.avg_document_length,
                document_frequencies=dict(proto_stats.document_frequencies),
                stats_version=proto_stats.stats_version,
            )

            results = self.bm25.search(
                query=request.query,
                index=self.doc_manager.index,
                stats=global_stats,
                top_k=request.top_k,
                shard_id=self.shard_id,
            )

            items = [
                shard_pb2.LocalSearchResultItem(
                    doc_id=r.doc_id,
                    score=r.score,
                    shard_id=self.shard_id,
                    snippet="",
                )
                for r in results
            ]
            return shard_pb2.LocalSearchResponse(
                results=items,
                local_hits=len(items),
                error_code=ErrorCode.OK.value,
            )
        except Exception as e:
            return shard_pb2.LocalSearchResponse(
                results=[],
                local_hits=0,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )

    def GetShardStatus(self, request, context):
        """Return shard health and sequence progression."""
        latest_seq = self.store.log_repo.get_latest_sequence(self.shard_id)
        return shard_pb2.ShardStatusResponse(
            shard_id=self.shard_id,
            role=self.role.value,
            latest_sequence=latest_seq,
            total_documents=self.doc_manager.index.total_documents,
            is_healthy=True,
        )

    def GetLatestSequence(self, request, context):
        """Return latest committed sequence and oldest sequence in update log."""
        latest_seq = self.store.log_repo.get_latest_sequence(self.shard_id)
        earliest_seq = self.store.log_repo.get_earliest_sequence(self.shard_id) or 0
        return shard_pb2.LatestSequenceResponse(
            shard_id=self.shard_id,
            latest_sequence=latest_seq,
            oldest_sequence=earliest_seq,
        )

    def GetUpdatesAfter(self, request, context):
        """Fetch update events for incremental recovery."""
        try:
            events = self.store.log_repo.get_updates_after(
                self.shard_id,
                since_sequence=request.since_sequence,
                max_events=request.max_events,
            )
            proto_events = [
                shard_pb2.ProtoUpdateEvent(
                    shard_id=e.shard_id,
                    sequence_number=e.sequence_number,
                    doc_id=e.doc_id,
                    operation=e.operation.value if isinstance(e.operation, OperationType) else str(e.operation),
                    version=e.version,
                    payload=e.payload or "",
                    committed_at=e.committed_at,
                )
                for e in events
            ]
            has_more = len(events) == request.max_events
            return shard_pb2.GetUpdatesAfterResponse(
                events=proto_events,
                has_more=has_more,
                error_code=ErrorCode.OK.value,
            )
        except Exception as e:
            return shard_pb2.GetUpdatesAfterResponse(
                events=[],
                has_more=False,
                error_code=ErrorCode.INTERNAL_ERROR.value,
                error_message=str(e),
            )


def create_shard_server(
    shard_id: int,
    db_path: str,
    port: int,
    role: ShardRole = ShardRole.PRIMARY,
    max_workers: int = 10,
) -> grpc.Server:
    """Factory creating and configuring a Shard gRPC Server."""
    db = DatabaseManager(db_path)
    db.initialize_schema()
    store = ShardStore(db, shard_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = ShardServicer(shard_id=shard_id, store=store, role=role)
    shard_pb2_grpc.add_ShardServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{port}")
    return server
