"""Dataset-independent, deterministic validation functions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Callable, Mapping, MutableSet, Sequence

from .models import CrossBatchOverlapPolicy


class ReasonCode(StrEnum):
    MISSING_TIMESTAMP = "MISSING_TIMESTAMP"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    MISSING_ASSET_ID = "MISSING_ASSET_ID"
    NON_NUMERIC_GENERATION = "NON_NUMERIC_GENERATION"
    DUPLICATE_RECORD = "DUPLICATE_RECORD"
    CROSS_BATCH_OVERLAP = "CROSS_BATCH_OVERLAP"
    WRONG_COLUMN_STRUCTURE = "WRONG_COLUMN_STRUCTURE"
    EMPTY_FILE = "EMPTY_FILE"
    OTHER_MALFORMED_ROW = "OTHER_MALFORMED_ROW"
    MISSING_PROVENANCE_METADATA = "MISSING_PROVENANCE_METADATA"


@dataclass(frozen=True)
class ValidationIssue:
    code: ReasonCode
    detail: str


def _is_blank(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def validate_timestamp_present(value: str | None) -> ValidationIssue | None:
    if _is_blank(value):
        return ValidationIssue(ReasonCode.MISSING_TIMESTAMP, "timestamp is blank")
    return None


def parse_timestamp(
    value: str,
    parser: Callable[[str], datetime],
) -> tuple[datetime | None, ValidationIssue | None]:
    try:
        return parser(value.strip()), None
    except (TypeError, ValueError, OverflowError) as exc:
        return None, ValidationIssue(
            ReasonCode.INVALID_TIMESTAMP,
            f"timestamp could not be parsed: {exc}",
        )


def validate_asset_identifier(value: str | None) -> ValidationIssue | None:
    if _is_blank(value):
        return ValidationIssue(ReasonCode.MISSING_ASSET_ID, "asset identifier is blank")
    return None


def parse_generation(value: str | None) -> tuple[float | None, ValidationIssue | None]:
    if _is_blank(value):
        return None, ValidationIssue(
            ReasonCode.NON_NUMERIC_GENERATION,
            "generation value is blank",
        )
    try:
        parsed = float(value.strip())
    except (AttributeError, TypeError, ValueError):
        return None, ValidationIssue(
            ReasonCode.NON_NUMERIC_GENERATION,
            f"generation value is not numeric: {value!r}",
        )
    if not math.isfinite(parsed):
        return None, ValidationIssue(
            ReasonCode.NON_NUMERIC_GENERATION,
            f"generation value must be finite: {value!r}",
        )
    return parsed, None


def validate_duplicate_record(
    asset_id: str,
    timestamp: datetime,
    seen_keys: MutableSet[tuple[str, datetime]],
) -> ValidationIssue | None:
    key = (asset_id, timestamp)
    if key in seen_keys:
        return ValidationIssue(
            ReasonCode.DUPLICATE_RECORD,
            "duplicate (asset_id, timestamp) within this input batch",
        )
    seen_keys.add(key)
    return None


def validate_cross_batch_overlap(
    asset_id: str,
    timestamp: datetime,
    existing_keys: Mapping[tuple[str, datetime], str],
    policy: CrossBatchOverlapPolicy,
) -> ValidationIssue | None:
    """Apply the explicit policy for a key already committed by another batch."""

    key = (asset_id, timestamp)
    if key not in existing_keys:
        return None
    if policy is CrossBatchOverlapPolicy.REJECT_INCOMING:
        existing_source = existing_keys[key]
        return ValidationIssue(
            ReasonCode.CROSS_BATCH_OVERLAP,
            "incoming (asset_id, timestamp) overlaps an existing batch; "
            f"policy=reject_incoming; existing_source_file={existing_source!r}",
        )
    raise ValueError(f"unsupported cross-batch overlap policy: {policy}")


def validate_column_structure(
    actual_columns: Sequence[str],
    expected_columns: Sequence[str],
    *,
    require_order: bool,
) -> ValidationIssue | None:
    if len(actual_columns) != len(expected_columns):
        return ValidationIssue(
            ReasonCode.WRONG_COLUMN_STRUCTURE,
            f"expected {len(expected_columns)} columns, received {len(actual_columns)}",
        )
    if require_order and tuple(actual_columns) != tuple(expected_columns):
        return ValidationIssue(
            ReasonCode.WRONG_COLUMN_STRUCTURE,
            "column names or order do not match the observed AESO schema",
        )
    return None


def validate_file_not_empty(has_header: bool) -> ValidationIssue | None:
    if not has_header:
        return ValidationIssue(ReasonCode.EMPTY_FILE, "file is entirely empty")
    return None


def validate_provenance(metadata: Mapping[str, object]) -> ValidationIssue | None:
    required = ("source_file", "source_url", "retrieved_at", "interval")
    missing = [name for name in required if _is_blank(metadata.get(name))]
    if missing:
        return ValidationIssue(
            ReasonCode.MISSING_PROVENANCE_METADATA,
            "missing provenance fields: " + ", ".join(missing),
        )
    return None


def malformed_row_issue(exc: Exception) -> ValidationIssue:
    """Convert an unexpected row-level exception into the catch-all reason."""

    return ValidationIssue(
        ReasonCode.OTHER_MALFORMED_ROW,
        f"malformed row: {type(exc).__name__}: {exc}",
    )
