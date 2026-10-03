from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from aeso_pipeline.adapter import AesoCsdHourlyAdapter
from aeso_pipeline.inspect import main, summarize_database
from aeso_pipeline.models import ProvenanceMetadata
from aeso_pipeline.pipeline import ingest_file


def _create_database(tmp_path: Path) -> Path:
    adapter = AesoCsdHourlyAdapter()
    source = tmp_path / "inspect.csv"
    database = tmp_path / "inspect.duckdb"
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(adapter.expected_columns)
        writer.writerows(
            [
                [
                    "2026-06-01 00:00:00",
                    "2026-06-01 01:00:00",
                    "ACD1",
                    "Big Sky Solar",
                    "ACD1",
                    "1.25",
                    "140.0",
                    "140.0",
                    "SOLAR",
                    "SOLAR",
                    "48",
                    "South",
                ],
                [
                    "2026-06-01 01:00:00",
                    "2026-06-01 02:00:00",
                    "AKE1",
                    "Little Smoky",
                    "AKE1",
                    "2.5",
                    "45.0",
                    "45.0",
                    "GAS",
                    "GAS",
                    "40",
                    "Northwest",
                ],
            ]
        )
    ingest_file(
        source,
        database,
        ProvenanceMetadata(
            source_file=source.name,
            source_url="https://example.test/aeso/inspect.csv",
            retrieved_at=datetime(2026, 6, 2, tzinfo=UTC),
            interval=adapter.interval,
        ),
    )
    return database


def test_summary_reports_operational_database_health(tmp_path: Path) -> None:
    database = _create_database(tmp_path)

    summary = summarize_database(database)

    assert summary == {
        "database": str(database.resolve()),
        "database_schema_version": 2,
        "batch_count": 1,
        "clean_row_count": 2,
        "rejected_row_count": 0,
        "distinct_asset_count": 2,
        "duplicate_key_group_count": 0,
        "time_range_utc": {
            "start": "2026-06-01T07:00:00+00:00",
            "end": "2026-06-01T08:00:00+00:00",
        },
    }


def test_cli_prints_summary_as_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = _create_database(tmp_path)

    exit_code = main(["--db", str(database)])

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["clean_row_count"] == 2
    assert output["duplicate_key_group_count"] == 0


def test_summary_refuses_a_missing_database(tmp_path: Path) -> None:
    missing_database = tmp_path / "missing.duckdb"

    with pytest.raises(FileNotFoundError, match="database does not exist"):
        summarize_database(missing_database)
