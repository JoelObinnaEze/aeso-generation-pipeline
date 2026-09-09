"""DuckDB schema, migrations, lookup helpers, and transactional persistence."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Sequence

import duckdb

from .models import (
    BatchResult,
    CleanRecord,
    CrossBatchOverlapPolicy,
    ExistingBatch,
    ProvenanceMetadata,
)


DATABASE_SCHEMA_VERSION = 2

CREATE_TABLES_SQL = """
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
    status VARCHAR NOT NULL,
    adapter_schema_version VARCHAR NOT NULL,
    overlap_policy VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_schema (
    component VARCHAR PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""


class DatabaseMigrationError(RuntimeError):
    """Raised when a legacy database cannot be upgraded without data loss."""


def _column_names(connection: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        row[1]
        for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    }


def _migrate_legacy_provenance(connection: duckdb.DuckDBPyConnection) -> None:
    columns = _column_names(connection, "provenance")
    if "adapter_schema_version" not in columns:
        connection.execute(
            """
            ALTER TABLE provenance
            ADD COLUMN adapter_schema_version VARCHAR DEFAULT 'legacy-unversioned'
            """
        )
    if "overlap_policy" not in columns:
        connection.execute(
            """
            ALTER TABLE provenance
            ADD COLUMN overlap_policy VARCHAR DEFAULT 'legacy-unenforced'
            """
        )


def _assert_supported_schema_version(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    row = connection.execute(
        """
        SELECT schema_version FROM pipeline_schema
        WHERE component = 'aeso_pipeline'
        """
    ).fetchone()
    if row and row[0] > DATABASE_SCHEMA_VERSION:
        raise DatabaseMigrationError(
            f"database schema version {row[0]} is newer than this component's "
            f"supported version {DATABASE_SCHEMA_VERSION}; upgrade the application"
        )


def _assert_legacy_uniqueness(connection: duckdb.DuckDBPyConnection) -> None:
    duplicate_hash_groups = connection.execute(
        """
        SELECT count(*)
        FROM (
            SELECT source_sha256
            FROM provenance
            WHERE source_sha256 IS NOT NULL
            GROUP BY source_sha256
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_hash_groups:
        raise DatabaseMigrationError(
            "legacy database contains repeated source hashes; automatic migration "
            "would require choosing which provenance batch to keep. Start with a new "
            "database or resolve those duplicate batches explicitly."
        )

    duplicate_key_groups = connection.execute(
        """
        SELECT count(*)
        FROM (
            SELECT asset_id, timestamp
            FROM clean_generation
            GROUP BY asset_id, timestamp
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_key_groups:
        raise DatabaseMigrationError(
            "legacy database contains overlapping (asset_id, timestamp) keys; "
            "automatic migration will not guess which observation is authoritative. "
            "Start with a new database or resolve the overlaps explicitly."
        )


def _record_schema_version(connection: duckdb.DuckDBPyConnection) -> None:
    row = connection.execute(
        """
        SELECT schema_version FROM pipeline_schema
        WHERE component = 'aeso_pipeline'
        """
    ).fetchone()
    if row is None:
        connection.execute(
            "INSERT INTO pipeline_schema VALUES ('aeso_pipeline', ?, ?)",
            [DATABASE_SCHEMA_VERSION, datetime.now(UTC)],
        )
    elif row[0] < DATABASE_SCHEMA_VERSION:
        connection.execute(
            """
            UPDATE pipeline_schema
            SET schema_version = ?, updated_at = ?
            WHERE component = 'aeso_pipeline'
            """,
            [DATABASE_SCHEMA_VERSION, datetime.now(UTC)],
        )


def initialize_database(db_path: Path) -> None:
    """Create the current schema or safely migrate a non-conflicting v1 database."""

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(CREATE_TABLES_SQL)
        _assert_supported_schema_version(connection)
        _migrate_legacy_provenance(connection)
        _assert_legacy_uniqueness(connection)
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_provenance_source_sha256
            ON provenance (source_sha256)
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_clean_generation_key
            ON clean_generation (asset_id, timestamp)
            """
        )
        _record_schema_version(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def _existing_batch_from_row(tuple_row: tuple[object, ...]) -> ExistingBatch:
    return ExistingBatch(
        batch_id=str(tuple_row[0]),
        row_count=int(tuple_row[1]),
        clean_row_count=int(tuple_row[2]),
        rejected_row_count=int(tuple_row[3]),
        status=str(tuple_row[4]),
        ingested_at=tuple_row[5],
        archive_member=tuple_row[6],
    )


def _find_batch_on_connection(
    connection: duckdb.DuckDBPyConnection,
    source_sha256: str | None,
) -> ExistingBatch | None:
    if not source_sha256:
        return None
    row = connection.execute(
        """
        SELECT batch_id, row_count, clean_row_count, rejected_row_count,
               status, ingested_at, archive_member
        FROM provenance
        WHERE source_sha256 = ?
        """,
        [source_sha256],
    ).fetchone()
    return _existing_batch_from_row(row) if row else None


def find_batch_by_source_hash(
    db_path: Path,
    source_sha256: str,
) -> ExistingBatch | None:
    initialize_database(db_path)
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        return _find_batch_on_connection(connection, source_sha256)
    finally:
        connection.close()


def find_existing_generation_keys(
    db_path: Path,
    records: Sequence[CleanRecord],
) -> dict[tuple[str, datetime], str]:
    """Return only incoming keys that already exist, without loading the full table."""

    if not records:
        return {}
    initialize_database(db_path)
    with TemporaryDirectory(prefix="aeso_overlap_") as temporary_directory:
        key_csv = Path(temporary_directory) / "keys.csv"
        with key_csv.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerows(
                (record.timestamp.isoformat(), record.asset_id) for record in records
            )

        connection = duckdb.connect(str(db_path))
        try:
            connection.execute(
                "CREATE TEMP TABLE incoming_keys "
                "(timestamp TIMESTAMPTZ NOT NULL, asset_id VARCHAR NOT NULL)"
            )
            connection.execute(
                """
                COPY incoming_keys FROM ?
                (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', ESCAPE '"')
                """,
                [str(key_csv)],
            )
            rows = connection.execute(
                """
                SELECT incoming.asset_id, incoming.timestamp, existing.source_file
                FROM incoming_keys AS incoming
                JOIN clean_generation AS existing
                  ON existing.asset_id = incoming.asset_id
                 AND existing.timestamp = incoming.timestamp
                """
            ).fetchall()
            return {
                (asset_id, timestamp): source_file
                for asset_id, timestamp, source_file in rows
            }
        finally:
            connection.close()


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
    *,
    adapter_schema_version: str,
    overlap_policy: CrossBatchOverlapPolicy,
) -> ExistingBatch | None:
    """Atomically persist a batch, or return the prior batch for a hash replay."""

    initialize_database(db_path)
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
            connection.execute("BEGIN TRANSACTION")
            existing_batch = _find_batch_on_connection(
                connection, result.source_sha256
            )
            if existing_batch:
                connection.execute("ROLLBACK")
                return existing_batch

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
                    INSERT INTO provenance (
                        batch_id, source_file, source_url, retrieved_at, ingested_at,
                        row_count, clean_row_count, rejected_row_count, interval,
                        source_sha256, archive_member, status,
                        adapter_schema_version, overlap_policy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        adapter_schema_version,
                        overlap_policy.value,
                    ],
                )

            connection.execute("COMMIT")
            return None
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
