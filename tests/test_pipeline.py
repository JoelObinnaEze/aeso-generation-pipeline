from __future__ import annotations

import csv
import shutil
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb

from aeso_pipeline.adapter import AesoCsdHourlyAdapter
from aeso_pipeline.models import ProvenanceMetadata
from aeso_pipeline.pipeline import ingest_file
from aeso_pipeline.storage import (
    DATABASE_SCHEMA_VERSION,
    DatabaseMigrationError,
    initialize_database,
)
from aeso_pipeline.validation import ReasonCode


ADAPTER = AesoCsdHourlyAdapter()
SOURCE_URL = "https://example.test/aeso/hourly.csv"
RETRIEVED_AT = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)


def _row(
    *,
    timestamp: str = "2026-06-01 00:00:00",
    asset_id: str = "ACD1",
    asset_name: str = "ACD1 Big Sky Solar",
    generation: str = "1.25",
) -> list[str]:
    return [
        timestamp,
        "2026-06-01 01:00:00",
        asset_id,
        asset_name,
        asset_id,
        generation,
        "140.0",
        "140.0",
        "SOLAR",
        "SOLAR",
        "48",
        "South",
    ]


def _write_csv(
    path: Path,
    rows: list[list[str]],
    header: tuple[str, ...] = ADAPTER.expected_columns,
) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)


def _metadata(path: Path) -> ProvenanceMetadata:
    return ProvenanceMetadata(
        source_file=path.name,
        source_url=SOURCE_URL,
        retrieved_at=RETRIEVED_AT,
        interval=ADAPTER.interval,
    )


def _table_count(db_path: Path, table: str) -> int:
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_fully_valid_batch_flows_to_clean_and_provenance(tmp_path: Path) -> None:
    source = tmp_path / "valid.csv"
    db_path = tmp_path / "valid.duckdb"
    report_path = tmp_path / "report.json"
    _write_csv(
        source,
        [
            _row(),
            _row(
                timestamp="2026-06-01 01:00:00",
                asset_name="A test asset, with a comma",
                generation="2.5",
            ),
        ],
    )

    report = ingest_file(
        source,
        db_path,
        _metadata(source),
        report_path=report_path,
    )

    assert report["status"] == "loaded"
    assert report["clean_row_count"] == 2
    assert report["rejected_row_count"] == 0
    assert report["provenance_written"] is True
    assert report_path.is_file()
    assert _table_count(db_path, "clean_generation") == 2
    assert _table_count(db_path, "rejected_rows") == 0
    assert _table_count(db_path, "provenance") == 1

    with duckdb.connect(str(db_path), read_only=True) as connection:
        timestamp, interval = connection.execute(
            "SELECT timestamp, source_interval FROM clean_generation ORDER BY timestamp LIMIT 1"
        ).fetchone()
        provenance = connection.execute(
            "SELECT row_count, clean_row_count, rejected_row_count, status FROM provenance"
        ).fetchone()
    assert timestamp == datetime(2026, 6, 1, 7, 0, tzinfo=UTC)
    assert interval == "1hour"
    assert provenance == (2, 2, 0, "loaded")


def test_mixed_batch_splits_clean_and_rejected_rows(tmp_path: Path) -> None:
    source = tmp_path / "mixed.csv"
    db_path = tmp_path / "mixed.duckdb"
    _write_csv(
        source,
        [
            _row(),
            _row(timestamp=""),
            _row(timestamp="not-a-date"),
            _row(timestamp="2026-06-01 02:00:00", asset_id=""),
            _row(timestamp="2026-06-01 03:00:00", generation="offline"),
            _row(),
        ],
    )

    report = ingest_file(source, db_path, _metadata(source))

    assert report["status"] == "partial"
    assert report["clean_row_count"] == 1
    assert report["rejected_row_count"] == 5
    assert report["reason_counts"] == {
        ReasonCode.DUPLICATE_RECORD.value: 1,
        ReasonCode.INVALID_TIMESTAMP.value: 1,
        ReasonCode.MISSING_ASSET_ID.value: 1,
        ReasonCode.MISSING_TIMESTAMP.value: 1,
        ReasonCode.NON_NUMERIC_GENERATION.value: 1,
    }

    with duckdb.connect(str(db_path), read_only=True) as connection:
        reasons = connection.execute(
            "SELECT reason_code FROM rejected_rows ORDER BY reason_code"
        ).fetchall()
    assert [value for (value,) in reasons] == sorted(report["reason_counts"])


def test_row_with_wrong_column_count_is_quarantined(tmp_path: Path) -> None:
    source = tmp_path / "short-row.csv"
    db_path = tmp_path / "short-row.duckdb"
    _write_csv(source, [["2026-06-01 00:00:00", "2026-06-01 01:00:00"]])

    report = ingest_file(source, db_path, _metadata(source))

    assert report["clean_row_count"] == 0
    assert report["reason_counts"] == {ReasonCode.WRONG_COLUMN_STRUCTURE.value: 1}


def test_wrong_header_order_rejects_the_batch(tmp_path: Path) -> None:
    source = tmp_path / "wrong-header.csv"
    db_path = tmp_path / "wrong-header.duckdb"
    header = list(ADAPTER.expected_columns)
    header[0], header[1] = header[1], header[0]
    _write_csv(source, [_row()], tuple(header))

    report = ingest_file(source, db_path, _metadata(source))

    assert report["clean_row_count"] == 0
    assert report["input_row_count"] == 1
    assert report["reason_counts"] == {ReasonCode.WRONG_COLUMN_STRUCTURE.value: 1}
    assert _table_count(db_path, "provenance") == 1


def test_entirely_empty_file_is_quarantined(tmp_path: Path) -> None:
    source = tmp_path / "empty.csv"
    db_path = tmp_path / "empty.duckdb"
    source.touch()

    report = ingest_file(source, db_path, _metadata(source))

    assert report["input_row_count"] == 0
    assert report["reason_counts"] == {ReasonCode.EMPTY_FILE.value: 1}
    assert _table_count(db_path, "provenance") == 1


def test_missing_provenance_rejects_batch_without_provenance_row(tmp_path: Path) -> None:
    source = tmp_path / "no-provenance.csv"
    db_path = tmp_path / "no-provenance.duckdb"
    _write_csv(source, [_row()])
    incomplete = ProvenanceMetadata(
        source_file=source.name,
        source_url=None,
        retrieved_at=RETRIEVED_AT,
        interval=ADAPTER.interval,
    )

    report = ingest_file(source, db_path, incomplete)

    assert report["clean_row_count"] == 0
    assert report["provenance_written"] is False
    assert report["reason_counts"] == {
        ReasonCode.MISSING_PROVENANCE_METADATA.value: 1
    }
    assert _table_count(db_path, "provenance") == 0
    assert _table_count(db_path, "rejected_rows") == 1


def test_single_csv_zip_is_supported(tmp_path: Path) -> None:
    csv_path = tmp_path / "inside.csv"
    zip_path = tmp_path / "hourly.zip"
    db_path = tmp_path / "zip.duckdb"
    _write_csv(csv_path, [_row()])
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.write(csv_path, arcname="CSD Generation (Hourly) - test.csv")

    report = ingest_file(zip_path, db_path, _metadata(zip_path))

    assert report["clean_row_count"] == 1
    assert report["archive_member"] == "CSD Generation (Hourly) - test.csv"
    assert report["source_sha256"]


def test_same_artifact_hash_is_an_explicit_idempotent_noop(tmp_path: Path) -> None:
    first_source = tmp_path / "original.csv"
    copied_source = tmp_path / "renamed-copy.csv"
    db_path = tmp_path / "idempotent.duckdb"
    _write_csv(first_source, [_row()])
    shutil.copyfile(first_source, copied_source)

    first = ingest_file(first_source, db_path, _metadata(first_source))
    replay = ingest_file(copied_source, db_path, _metadata(copied_source))

    assert first["status"] == "loaded"
    assert replay["status"] == "skipped_idempotent"
    assert replay["idempotent_replay"] is True
    assert replay["rows_written"] == 0
    assert replay["provenance_written"] is False
    assert replay["batch_id"] == first["batch_id"]
    assert replay["attempted_batch_id"] != first["attempted_batch_id"]
    assert replay["source_sha256"] == first["source_sha256"]
    assert _table_count(db_path, "clean_generation") == 1
    assert _table_count(db_path, "rejected_rows") == 0
    assert _table_count(db_path, "provenance") == 1


def test_cross_batch_overlap_rejects_incoming_and_keeps_existing(
    tmp_path: Path,
) -> None:
    first_source = tmp_path / "first.csv"
    second_source = tmp_path / "second.csv"
    db_path = tmp_path / "overlap.duckdb"
    _write_csv(first_source, [_row(generation="1.25")])
    _write_csv(
        second_source,
        [
            _row(generation="999.0"),
            _row(timestamp="2026-06-01 01:00:00", generation="2.5"),
        ],
    )

    first = ingest_file(first_source, db_path, _metadata(first_source))
    second = ingest_file(second_source, db_path, _metadata(second_source))

    assert first["status"] == "loaded"
    assert second["status"] == "partial"
    assert second["overlap_policy"] == "reject_incoming"
    assert second["clean_row_count"] == 1
    assert second["rejected_row_count"] == 1
    assert second["reason_counts"] == {
        ReasonCode.CROSS_BATCH_OVERLAP.value: 1
    }
    assert _table_count(db_path, "clean_generation") == 2
    assert _table_count(db_path, "rejected_rows") == 1
    assert _table_count(db_path, "provenance") == 2

    with duckdb.connect(str(db_path), read_only=True) as connection:
        retained = connection.execute(
            """
            SELECT generation_mw, source_file
            FROM clean_generation
            WHERE asset_id = 'ACD1'
            ORDER BY timestamp
            LIMIT 1
            """
        ).fetchone()
        reject = connection.execute(
            "SELECT reason_code, raw_generation FROM rejected_rows"
        ).fetchone()
        policies = connection.execute(
            """
            SELECT DISTINCT adapter_schema_version, overlap_policy
            FROM provenance
            """
        ).fetchall()
    assert retained == (1.25, first_source.name)
    assert reject == (ReasonCode.CROSS_BATCH_OVERLAP.value, "999.0")
    assert policies == [(ADAPTER.schema_version, "reject_incoming")]


def _create_legacy_database(
    db_path: Path,
    *,
    duplicate_keys: bool = False,
    duplicate_hashes: bool = False,
) -> None:
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE clean_generation (
                timestamp TIMESTAMPTZ NOT NULL,
                asset_id VARCHAR NOT NULL,
                asset_name VARCHAR,
                generation_mw DOUBLE NOT NULL,
                source_file VARCHAR NOT NULL,
                source_url VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                source_interval VARCHAR NOT NULL
            );
            CREATE TABLE rejected_rows (
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
            CREATE TABLE provenance (
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
        )
        connection.execute(
            """
            INSERT INTO provenance VALUES
            ('legacy-1', 'one.csv', 'https://example.test/one', now(), now(),
             1, 1, 0, '1hour', 'legacy-hash', NULL, 'loaded')
            """
        )
        if duplicate_hashes:
            connection.execute(
                """
                INSERT INTO provenance VALUES
                ('legacy-2', 'two.csv', 'https://example.test/two', now(), now(),
                 1, 1, 0, '1hour', 'legacy-hash', NULL, 'loaded')
                """
            )
        if duplicate_keys:
            connection.execute(
                """
                INSERT INTO clean_generation VALUES
                ('2026-06-01 07:00:00+00', 'ACD1', 'Asset', 1.0,
                 'one.csv', 'https://example.test/one', now(), '1hour'),
                ('2026-06-01 07:00:00+00', 'ACD1', 'Asset', 2.0,
                 'two.csv', 'https://example.test/two', now(), '1hour')
                """
            )


def test_non_conflicting_legacy_database_migrates_in_place(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.duckdb"
    _create_legacy_database(db_path)

    initialize_database(db_path)

    with duckdb.connect(str(db_path), read_only=True) as connection:
        provenance_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('provenance')"
            ).fetchall()
        }
        version = connection.execute(
            """
            SELECT schema_version FROM pipeline_schema
            WHERE component = 'aeso_pipeline'
            """
        ).fetchone()[0]
        legacy_labels = connection.execute(
            """
            SELECT adapter_schema_version, overlap_policy
            FROM provenance
            WHERE batch_id = 'legacy-1'
            """
        ).fetchone()
        indexes = {
            row[0]
            for row in connection.execute(
                """
                SELECT index_name FROM duckdb_indexes()
                WHERE index_name LIKE 'uq_%'
                """
            ).fetchall()
        }
    assert {"adapter_schema_version", "overlap_policy"} <= provenance_columns
    assert version == DATABASE_SCHEMA_VERSION
    assert legacy_labels == ("legacy-unversioned", "legacy-unenforced")
    assert indexes == {
        "uq_clean_generation_key",
        "uq_provenance_source_sha256",
    }


def test_conflicting_legacy_database_refuses_automatic_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-overlap.duckdb"
    _create_legacy_database(db_path, duplicate_keys=True)

    try:
        initialize_database(db_path)
    except DatabaseMigrationError as exc:
        assert "will not guess" in str(exc)
    else:
        raise AssertionError("conflicting legacy database should not migrate")


def test_repeated_legacy_source_hash_refuses_automatic_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-repeated-hash.duckdb"
    _create_legacy_database(db_path, duplicate_hashes=True)

    try:
        initialize_database(db_path)
    except DatabaseMigrationError as exc:
        assert "repeated source hashes" in str(exc)
    else:
        raise AssertionError("repeated legacy source hashes should not migrate")


def test_newer_database_schema_refuses_downgrade(tmp_path: Path) -> None:
    db_path = tmp_path / "future.duckdb"
    initialize_database(db_path)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            UPDATE pipeline_schema
            SET schema_version = ?
            WHERE component = 'aeso_pipeline'
            """,
            [DATABASE_SCHEMA_VERSION + 1],
        )

    try:
        initialize_database(db_path)
    except DatabaseMigrationError as exc:
        assert "newer than this component" in str(exc)
    else:
        raise AssertionError("a newer database schema should not be downgraded")
