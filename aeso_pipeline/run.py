"""Command-line entry point: python -m aeso_pipeline.run."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from .adapter import AesoCsdHourlyAdapter
from .models import CrossBatchOverlapPolicy, ProvenanceMetadata
from .pipeline import ingest_file


DEFAULT_SOURCE_URL = (
    "https://aeso.box.com/s/qofgn9axnnw6uq3ip1goiq2ngb11txe5/"
    "folder/196178549071"
)


def _parse_retrieved_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--retrieved-at must include a UTC offset")
    return parsed.astimezone(UTC)


def _default_report_path(db_path: Path) -> Path:
    return db_path.with_suffix(".validation.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest and validate one AESO CSD hourly CSV or ZIP file."
    )
    parser.add_argument("--input", required=True, type=Path, help="AESO .csv or .zip")
    parser.add_argument("--db", required=True, type=Path, help="Output DuckDB path")
    parser.add_argument(
        "--report",
        type=Path,
        help="Validation report path (default: DB name with .validation.json)",
    )
    parser.add_argument(
        "--source-url",
        default=DEFAULT_SOURCE_URL,
        help="Retrieval URL recorded in provenance",
    )
    parser.add_argument(
        "--retrieved-at",
        type=_parse_retrieved_at,
        help="ISO-8601 retrieval time; defaults to the input file modification time",
    )
    parser.add_argument(
        "--overlap-policy",
        choices=[policy.value for policy in CrossBatchOverlapPolicy],
        default=CrossBatchOverlapPolicy.REJECT_INCOMING.value,
        help="Cross-batch key policy (default: reject_incoming)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path: Path = args.input
    retrieved_at = args.retrieved_at
    if retrieved_at is None:
        if not input_path.is_file():
            raise SystemExit(f"input file does not exist: {input_path}")
        retrieved_at = datetime.fromtimestamp(input_path.stat().st_mtime, tz=UTC)

    adapter = AesoCsdHourlyAdapter()
    metadata = ProvenanceMetadata(
        source_file=input_path.name,
        source_url=args.source_url,
        retrieved_at=retrieved_at,
        interval=adapter.interval,
    )
    report_path = args.report or _default_report_path(args.db)
    report = ingest_file(
        input_path,
        args.db,
        metadata,
        report_path=report_path,
        adapter=adapter,
        overlap_policy=CrossBatchOverlapPolicy(args.overlap_policy),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
