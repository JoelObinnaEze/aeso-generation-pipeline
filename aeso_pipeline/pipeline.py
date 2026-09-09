"""Orchestration of reading, normalization, validation, and persistence."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .adapter import AesoCsdHourlyAdapter, GenerationAdapter
from .models import (
    BatchResult,
    CleanRecord,
    CrossBatchOverlapPolicy,
    ExistingBatch,
    NormalizedCandidate,
    ProvenanceMetadata,
    RejectedRecord,
)
from .readers import calculate_sha256, read_csv_batch
from .storage import (
    find_batch_by_source_hash,
    find_existing_generation_keys,
    persist_batch,
)
from .validation import (
    ValidationIssue,
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


def _reject_candidate(
    candidate: NormalizedCandidate,
    issue: ValidationIssue,
) -> RejectedRecord:
    return RejectedRecord(
        source_line=candidate.source_line,
        raw_timestamp=candidate.timestamp_text,
        raw_asset_id=candidate.asset_id_text,
        raw_asset_name=candidate.asset_name_text,
        raw_generation=candidate.generation_text,
        raw_record=candidate.raw_record,
        reason_code=issue.code.value,
        reason_detail=issue.detail,
    )


def _reject_batch(issue: ValidationIssue, raw_record: dict[str, Any]) -> RejectedRecord:
    return RejectedRecord(
        source_line=None,
        raw_timestamp=None,
        raw_asset_id=None,
        raw_asset_name=None,
        raw_generation=None,
        raw_record=raw_record,
        reason_code=issue.code.value,
        reason_detail=issue.detail,
    )


def _validate_candidate(
    candidate: NormalizedCandidate,
    adapter: GenerationAdapter,
    seen_keys: set[tuple[str, datetime]],
) -> tuple[CleanRecord | None, RejectedRecord | None]:
    issue = validate_timestamp_present(candidate.timestamp_text)
    if issue:
        return None, _reject_candidate(candidate, issue)

    timestamp, issue = parse_timestamp(candidate.timestamp_text or "", adapter.parse_timestamp)
    if issue:
        return None, _reject_candidate(candidate, issue)

    issue = validate_asset_identifier(candidate.asset_id_text)
    if issue:
        return None, _reject_candidate(candidate, issue)

    generation, issue = parse_generation(candidate.generation_text)
    if issue:
        return None, _reject_candidate(candidate, issue)

    asset_id = (candidate.asset_id_text or "").strip()
    assert timestamp is not None and generation is not None
    issue = validate_duplicate_record(asset_id, timestamp, seen_keys)
    if issue:
        return None, _reject_candidate(candidate, issue)

    asset_name = (candidate.asset_name_text or "").strip() or None
    return (
        CleanRecord(
            timestamp=timestamp,
            asset_id=asset_id,
            asset_name=asset_name,
            generation_mw=generation,
            source_line=candidate.source_line,
        ),
        None,
    )


def _report_for_result(
    result: BatchResult,
    db_path: Path,
    metadata: ProvenanceMetadata,
    adapter: GenerationAdapter,
    overlap_policy: CrossBatchOverlapPolicy,
    existing_batch: ExistingBatch | None = None,
) -> dict[str, Any]:
    common = {
        "attempted_batch_id": result.batch_id,
        "input_file": str(result.input_path.resolve()),
        "database": str(db_path.resolve()),
        "source_file": metadata.source_file,
        "source_url": metadata.source_url,
        "source_interval": metadata.interval,
        "source_sha256": result.source_sha256,
        "archive_member": result.archive_member,
        "adapter_schema_version": adapter.schema_version,
        "overlap_policy": overlap_policy.value,
    }
    if existing_batch:
        return {
            **common,
            "batch_id": existing_batch.batch_id,
            "archive_member": existing_batch.archive_member,
            "input_row_count": existing_batch.row_count,
            "clean_row_count": 0,
            "rejected_row_count": 0,
            "rows_written": 0,
            "reason_counts": {},
            "provenance_written": False,
            "idempotent_replay": True,
            "status": "skipped_idempotent",
            "original_batch": {
                "batch_id": existing_batch.batch_id,
                "clean_row_count": existing_batch.clean_row_count,
                "rejected_row_count": existing_batch.rejected_row_count,
                "status": existing_batch.status,
                "ingested_at": existing_batch.ingested_at.astimezone(UTC).isoformat(),
            },
        }
    return {
        **common,
        "batch_id": result.batch_id,
        "input_row_count": result.input_row_count,
        "clean_row_count": len(result.clean_records),
        "rejected_row_count": len(result.rejected_records),
        "rows_written": len(result.clean_records) + len(result.rejected_records),
        "reason_counts": result.reason_counts,
        "provenance_written": result.provenance_written,
        "idempotent_replay": False,
        "status": result.status,
        "ingested_at": result.ingested_at.isoformat(),
    }


def _write_report(report: dict[str, Any], report_path: Path | None) -> None:
    if report_path is None:
        return
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def ingest_file(
    input_path: Path,
    db_path: Path,
    metadata: ProvenanceMetadata,
    *,
    report_path: Path | None = None,
    adapter: GenerationAdapter | None = None,
    overlap_policy: CrossBatchOverlapPolicy = (
        CrossBatchOverlapPolicy.REJECT_INCOMING
    ),
) -> dict[str, Any]:
    """Ingest one CSV/ZIP input and return its serializable validation report."""

    input_path = Path(input_path)
    db_path = Path(db_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"input file does not exist: {input_path}")

    adapter = adapter or AesoCsdHourlyAdapter()
    overlap_policy = CrossBatchOverlapPolicy(overlap_policy)
    result = BatchResult(
        batch_id=str(uuid4()),
        input_path=input_path,
        ingested_at=datetime.now(UTC),
    )

    provenance_issue = validate_provenance(
        {
            "source_file": metadata.source_file,
            "source_url": metadata.source_url,
            "retrieved_at": metadata.retrieved_at,
            "interval": metadata.interval,
        }
    )
    accepted_candidates: dict[tuple[str, datetime], NormalizedCandidate] = {}
    if provenance_issue:
        result.provenance_written = False
        result.rejected_records.append(
            _reject_batch(provenance_issue, {"input_path": str(input_path)})
        )
    else:
        result.source_sha256 = calculate_sha256(input_path)
        existing_batch = find_batch_by_source_hash(
            db_path, result.source_sha256
        )
        if existing_batch:
            report = _report_for_result(
                result,
                db_path,
                metadata,
                adapter,
                overlap_policy,
                existing_batch,
            )
            _write_report(report, report_path)
            return report

        try:
            raw_batch = read_csv_batch(
                input_path,
                source_sha256=result.source_sha256,
            )
            result.archive_member = raw_batch.archive_member
            result.input_row_count = len(raw_batch.rows)

            empty_issue = validate_file_not_empty(raw_batch.header is not None)
            if empty_issue:
                result.rejected_records.append(_reject_batch(empty_issue, {}))
            else:
                assert raw_batch.header is not None
                header_issue = validate_column_structure(
                    raw_batch.header,
                    adapter.expected_columns,
                    require_order=True,
                )
                if header_issue:
                    result.rejected_records.append(
                        _reject_batch(header_issue, {"header": raw_batch.header})
                    )
                else:
                    seen_keys: set[tuple[str, datetime]] = set()
                    for line_number, values in raw_batch.rows:
                        structure_issue = validate_column_structure(
                            values,
                            adapter.expected_columns,
                            require_order=False,
                        )
                        if structure_issue:
                            result.rejected_records.append(
                                RejectedRecord(
                                    source_line=line_number,
                                    raw_timestamp=values[0] if values else None,
                                    raw_asset_id=values[2] if len(values) > 2 else None,
                                    raw_asset_name=values[3] if len(values) > 3 else None,
                                    raw_generation=values[5] if len(values) > 5 else None,
                                    raw_record={"values": values},
                                    reason_code=structure_issue.code.value,
                                    reason_detail=structure_issue.detail,
                                )
                            )
                            continue
                        try:
                            candidate = adapter.normalize(values, line_number)
                            clean, rejected = _validate_candidate(
                                candidate, adapter, seen_keys
                            )
                        except Exception as exc:
                            issue = malformed_row_issue(exc)
                            result.rejected_records.append(
                                RejectedRecord(
                                    source_line=line_number,
                                    raw_timestamp=values[0] if values else None,
                                    raw_asset_id=values[2] if len(values) > 2 else None,
                                    raw_asset_name=values[3] if len(values) > 3 else None,
                                    raw_generation=values[5] if len(values) > 5 else None,
                                    raw_record={"values": values},
                                    reason_code=issue.code.value,
                                    reason_detail=issue.detail,
                                )
                            )
                            continue

                        if clean:
                            result.clean_records.append(clean)
                            accepted_candidates[(clean.asset_id, clean.timestamp)] = candidate
                        if rejected:
                            result.rejected_records.append(rejected)
        except Exception as exc:
            issue = malformed_row_issue(exc)
            result.rejected_records.append(
                _reject_batch(issue, {"input_path": str(input_path)})
            )

        if result.clean_records:
            existing_keys = find_existing_generation_keys(
                db_path, result.clean_records
            )
            non_overlapping_records: list[CleanRecord] = []
            for clean_record in result.clean_records:
                issue = validate_cross_batch_overlap(
                    clean_record.asset_id,
                    clean_record.timestamp,
                    existing_keys,
                    overlap_policy,
                )
                if issue:
                    candidate = accepted_candidates[
                        (clean_record.asset_id, clean_record.timestamp)
                    ]
                    result.rejected_records.append(
                        _reject_candidate(candidate, issue)
                    )
                else:
                    non_overlapping_records.append(clean_record)
            result.clean_records = non_overlapping_records

    concurrent_existing_batch = persist_batch(
        db_path,
        result,
        metadata,
        adapter_schema_version=adapter.schema_version,
        overlap_policy=overlap_policy,
    )
    report = _report_for_result(
        result,
        db_path,
        metadata,
        adapter,
        overlap_policy,
        concurrent_existing_batch,
    )
    _write_report(report, report_path)
    return report
