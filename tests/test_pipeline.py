from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb

from aeso_pipeline.adapter import AesoCsdHourlyAdapter
from aeso_pipeline.models import ProvenanceMetadata
from aeso_pipeline.pipeline import ingest_file
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
