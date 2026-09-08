"""DuckDB schema and transactional batch persistence."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from .models import BatchResult, ProvenanceMetadata


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS clean_generation (
    timestamp TIMESTAMPTZ NOT NULL,
    asset_id VARCHAR NOT NULL,
    asset_name VARCHAR,
    generation_mw DOUBLE NOT NULL,
    source_file VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    source_interval VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected_rows (
    batch_id VARCHAR NOT NULL,
    source_line BIGINT,
    raw_timestamp VARCHAR,
    raw_asset_id VARCHAR,
    raw_asset_name VARCHAR,
    raw_generation VARCHAR,
    raw_record VARCHAR NOT NULL,
    reason_code VARCHAR NOT NULL,
    reason_detail VARCHAR NOT NULL,
    source_file VARCHAR,
    source_url VARCHAR,
    ingested_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS provenance (
    batch_id VARCHAR PRIMARY KEY,
    source_file VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    row_count BIGINT NOT NULL,
    clean_row_count BIGINT NOT NULL,
    rejected_row_count BIGINT NOT NULL,
    interval VARCHAR NOT NULL,
    source_sha256 VARCHAR,
    archive_member VARCHAR,
    status VARCHAR NOT NULL
);
"""


def _write_clean_staging_csv(
    path: Path,
    result: BatchResult,
    metadata: ProvenanceMetadata,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerows(
            (
                record.timestamp.isoformat(),
                record.asset_id,
                record.asset_name,
                record.generation_mw,
                metadata.source_file,
                metadata.source_url,
                result.ingested_at.isoformat(),
                metadata.interval,
            )
            for record in result.clean_records
        )


def _write_rejected_staging_csv(
    path: Path,
    result: BatchResult,
    metadata: ProvenanceMetadata,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerows(
            (
                result.batch_id,
                record.source_line,
                record.raw_timestamp,
                record.raw_asset_id,
                record.raw_asset_name,
                record.raw_generation,
                json.dumps(record.raw_record, ensure_ascii=False, sort_keys=True),
                record.reason_code,
                record.reason_detail,
                metadata.source_file,
                metadata.source_url,
                result.ingested_at.isoformat(),
            )
            for record in result.rejected_records
        )


def persist_batch(
    db_path: Path,
    result: BatchResult,
    metadata: ProvenanceMetadata,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="aeso_pipeline_") as temporary_directory:
        staging_directory = Path(temporary_directory)
        clean_csv = staging_directory / "clean.csv"
        rejected_csv = staging_directory / "rejected.csv"
        if result.clean_records:
            _write_clean_staging_csv(clean_csv, result, metadata)
        if result.rejected_records:
            _write_rejected_staging_csv(rejected_csv, result, metadata)

        connection = duckdb.connect(str(db_path))
        try:
            connection.execute(SCHEMA_SQL)
            connection.execute("BEGIN TRANSACTION")

            if result.clean_records:
                connection.execute(
                    """
                    COPY clean_generation FROM ?
                    (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '')
                    """,
                    [str(clean_csv)],
                )

            if result.rejected_records:
                connection.execute(
                    """
                    COPY rejected_rows FROM ?
                    (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '')
                    """,
                    [str(rejected_csv)],
                )

            if result.provenance_written:
                connection.execute(
                    """
                    INSERT INTO provenance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        result.batch_id,
                        metadata.source_file,
                        metadata.source_url,
                        metadata.retrieved_at,
                        result.ingested_at,
                        result.input_row_count,
                        len(result.clean_records),
                        len(result.rejected_records),
                        metadata.interval,
                        result.source_sha256,
                        result.archive_member,
                        result.status,
                    ],
                )

            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
