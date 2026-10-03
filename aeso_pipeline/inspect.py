"""Read-only operational summary for a pipeline DuckDB database."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import duckdb


class TimeRange(TypedDict):
    start: str | None
    end: str | None


class DatabaseSummary(TypedDict):
    database: str
    database_schema_version: int
    batch_count: int
    clean_row_count: int
    rejected_row_count: int
    distinct_asset_count: int
    duplicate_key_group_count: int
    time_range_utc: TimeRange


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def summarize_database(db_path: Path) -> DatabaseSummary:
    """Return deterministic health and scale metrics without modifying the database."""

    db_path = Path(db_path)
    if not db_path.is_file():
        raise FileNotFoundError(f"database does not exist: {db_path}")

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        connection.execute("SET TimeZone = 'UTC'")
        (
            schema_version,
            batch_count,
            clean_row_count,
            rejected_row_count,
            distinct_asset_count,
            first_timestamp,
            last_timestamp,
            duplicate_key_group_count,
        ) = connection.execute(
            """
            SELECT
                (SELECT schema_version FROM pipeline_schema
                 WHERE component = 'aeso_pipeline'),
                (SELECT count(*) FROM provenance),
                (SELECT count(*) FROM clean_generation),
                (SELECT count(*) FROM rejected_rows),
                (SELECT count(DISTINCT asset_id) FROM clean_generation),
                (SELECT min(timestamp) FROM clean_generation),
                (SELECT max(timestamp) FROM clean_generation),
                (SELECT count(*) FROM (
                    SELECT asset_id, timestamp
                    FROM clean_generation
                    GROUP BY asset_id, timestamp
                    HAVING count(*) > 1
                ))
            """
        ).fetchone()
    finally:
        connection.close()

    return {
        "database": str(db_path.resolve()),
        "database_schema_version": int(schema_version),
        "batch_count": int(batch_count),
        "clean_row_count": int(clean_row_count),
        "rejected_row_count": int(rejected_row_count),
        "distinct_asset_count": int(distinct_asset_count),
        "duplicate_key_group_count": int(duplicate_key_group_count),
        "time_range_utc": {
            "start": _isoformat(first_timestamp),
            "end": _isoformat(last_timestamp),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize an AESO pipeline DuckDB database without modifying it."
    )
    parser.add_argument("--db", required=True, type=Path, help="DuckDB database path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        summary = summarize_database(args.db)
    except (FileNotFoundError, duckdb.Error) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
