"""Shared records used by adapters, validation, and persistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any


class CrossBatchOverlapPolicy(StrEnum):
    """Supported behavior when an incoming key already exists in DuckDB."""

    REJECT_INCOMING = "reject_incoming"


@dataclass(frozen=True)
class ProvenanceMetadata:
    """Required source metadata for one ingestion batch."""

    source_file: str | None
    source_url: str | None
    retrieved_at: datetime | None
    interval: str | None


@dataclass(frozen=True)
class ExistingBatch:
    """A previously committed batch found by its source artifact hash."""

    batch_id: str
    row_count: int
    clean_row_count: int
    rejected_row_count: int
    status: str
    ingested_at: datetime
    archive_member: str | None


@dataclass(frozen=True)
class NormalizedCandidate:
    """Adapter output before the generic validators accept or reject it."""

    source_line: int
    timestamp_text: str | None
    asset_id_text: str | None
    asset_name_text: str | None
    generation_text: str | None
    raw_record: dict[str, Any]


@dataclass(frozen=True)
class CleanRecord:
    timestamp: datetime
    asset_id: str
    asset_name: str | None
    generation_mw: float
    source_line: int


@dataclass(frozen=True)
class RejectedRecord:
    source_line: int | None
    raw_timestamp: str | None
    raw_asset_id: str | None
    raw_asset_name: str | None
    raw_generation: str | None
    raw_record: dict[str, Any]
    reason_code: str
    reason_detail: str


@dataclass
class BatchResult:
    batch_id: str
    input_path: Path
    ingested_at: datetime
    input_row_count: int = 0
    clean_records: list[CleanRecord] = field(default_factory=list)
    rejected_records: list[RejectedRecord] = field(default_factory=list)
    source_sha256: str | None = None
    archive_member: str | None = None
    provenance_written: bool = True

    @property
    def status(self) -> str:
        if self.clean_records and self.rejected_records:
            return "partial"
        if self.rejected_records:
            return "rejected"
        return "loaded"

    @property
    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.rejected_records:
            counts[record.reason_code] = counts.get(record.reason_code, 0) + 1
        return dict(sorted(counts.items()))
