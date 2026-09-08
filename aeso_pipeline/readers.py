"""Raw CSV and single-member ZIP ingestion."""

from __future__ import annotations

import csv
import hashlib
import io
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO
from zipfile import BadZipFile, ZipFile


@dataclass(frozen=True)
class RawCsvBatch:
    header: list[str] | None
    rows: list[tuple[int, list[str]]]
    source_sha256: str
    archive_member: str | None


class RawInputError(ValueError):
    """Raised when an input container cannot yield one CSV stream."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _open_text(path: Path) -> Iterator[tuple[TextIO, str | None]]:
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            yield stream, None
        return

    if path.suffix.lower() != ".zip":
        raise RawInputError("input must be a .csv or .zip file")

    try:
        with ZipFile(path) as archive:
            members = [
                name
                for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".csv")
            ]
            if len(members) != 1:
                raise RawInputError(
                    f"ZIP must contain exactly one CSV file; found {len(members)}"
                )
            member = members[0]
            with archive.open(member) as binary_stream:
                with io.TextIOWrapper(
                    binary_stream, encoding="utf-8-sig", newline=""
                ) as text_stream:
                    yield text_stream, member
    except BadZipFile as exc:
        raise RawInputError("input is not a readable ZIP archive") from exc


def read_csv_batch(path: Path) -> RawCsvBatch:
    with _open_text(path) as (stream, member):
        reader = csv.reader(stream, strict=True)
        try:
            header = next(reader)
        except StopIteration:
            header = None
            rows: list[tuple[int, list[str]]] = []
        else:
            rows = [(line_number, row) for line_number, row in enumerate(reader, start=2)]

    return RawCsvBatch(
        header=header,
        rows=rows,
        source_sha256=_sha256(path),
        archive_member=member,
    )

