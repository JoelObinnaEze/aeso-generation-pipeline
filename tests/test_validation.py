from __future__ import annotations

from datetime import UTC, datetime

from aeso_pipeline.adapter import AesoCsdHourlyAdapter
from aeso_pipeline.models import CrossBatchOverlapPolicy
from aeso_pipeline.validation import (
    ReasonCode,
    malformed_row_issue,
    parse_generation,
    parse_timestamp,
    validate_asset_identifier,
    validate_column_structure,
    validate_cross_batch_overlap,
    validate_duplicate_record,
    validate_file_not_empty,
    validate_provenance,
    validate_timestamp_present,
)


def test_missing_timestamp_has_specific_reason() -> None:
    issue = validate_timestamp_present("  ")
    assert issue is not None
    assert issue.code is ReasonCode.MISSING_TIMESTAMP


def test_unparseable_timestamp_has_specific_reason() -> None:
    parsed, issue = parse_timestamp(
        "June-ish",
        AesoCsdHourlyAdapter().parse_timestamp,
    )
    assert parsed is None
    assert issue is not None
    assert issue.code is ReasonCode.INVALID_TIMESTAMP


def test_missing_asset_identifier_has_specific_reason() -> None:
    issue = validate_asset_identifier(None)
    assert issue is not None
    assert issue.code is ReasonCode.MISSING_ASSET_ID


def test_non_numeric_generation_has_specific_reason() -> None:
    parsed, issue = parse_generation("not reported")
    assert parsed is None
    assert issue is not None
    assert issue.code is ReasonCode.NON_NUMERIC_GENERATION


def test_duplicate_asset_timestamp_has_specific_reason() -> None:
    timestamp = datetime(2026, 6, 1, tzinfo=UTC)
    seen = {("ACD1", timestamp)}
    issue = validate_duplicate_record("ACD1", timestamp, seen)
    assert issue is not None
    assert issue.code is ReasonCode.DUPLICATE_RECORD


def test_cross_batch_overlap_has_specific_reason() -> None:
    timestamp = datetime(2026, 6, 1, tzinfo=UTC)
    issue = validate_cross_batch_overlap(
        "ACD1",
        timestamp,
        {("ACD1", timestamp): "earlier.csv"},
        CrossBatchOverlapPolicy.REJECT_INCOMING,
    )
    assert issue is not None
    assert issue.code is ReasonCode.CROSS_BATCH_OVERLAP
    assert "existing_source_file='earlier.csv'" in issue.detail


def test_wrong_column_order_has_specific_reason() -> None:
    expected = ("timestamp", "asset", "volume")
    issue = validate_column_structure(
        ("asset", "timestamp", "volume"),
        expected,
        require_order=True,
    )
    assert issue is not None
    assert issue.code is ReasonCode.WRONG_COLUMN_STRUCTURE


def test_empty_file_has_specific_reason() -> None:
    issue = validate_file_not_empty(has_header=False)
    assert issue is not None
    assert issue.code is ReasonCode.EMPTY_FILE


def test_other_malformed_row_has_specific_reason() -> None:
    issue = malformed_row_issue(RuntimeError("synthetic failure"))
    assert issue.code is ReasonCode.OTHER_MALFORMED_ROW


def test_missing_provenance_has_specific_reason() -> None:
    issue = validate_provenance(
        {
            "source_file": "batch.csv",
            "source_url": "",
            "retrieved_at": datetime(2026, 6, 2, tzinfo=UTC),
            "interval": "1hour",
        }
    )
    assert issue is not None
    assert issue.code is ReasonCode.MISSING_PROVENANCE_METADATA


def test_valid_generation_is_finite_float() -> None:
    parsed, issue = parse_generation("-0.25")
    assert issue is None
    assert parsed == -0.25
