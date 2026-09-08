"""AESO CSD-specific raw schema and normalization rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Protocol, Sequence

from .models import NormalizedCandidate


class GenerationAdapter(Protocol):
    """Interface a future source-specific generation adapter must implement."""

    interval: str
    expected_columns: Sequence[str]

    def normalize(
        self, values: Sequence[str], source_line: int
    ) -> NormalizedCandidate: ...

    def parse_timestamp(self, value: str) -> datetime: ...


class AesoCsdHourlyAdapter:
    """Map the observed AESO hourly CSV layout to canonical raw fields."""

    interval = "1hour"
    expected_columns: tuple[str, ...] = (
        "Date (MST)",
        "Date (MPT)",
        "Asset Short Name",
        "Asset Name",
        "Asset Grouping",
        "Volume",
        "Maximum Capability",
        "System Capability",
        "Fuel Type",
        "Sub Fuel Type",
        "Planning Area",
        "Region",
    )

    _mst = timezone(timedelta(hours=-7), name="MST")

    def normalize(self, values: Sequence[str], source_line: int) -> NormalizedCandidate:
        raw = dict(zip(self.expected_columns, values, strict=True))
        return NormalizedCandidate(
            source_line=source_line,
            timestamp_text=raw["Date (MST)"],
            asset_id_text=raw["Asset Short Name"],
            asset_name_text=raw["Asset Name"],
            generation_text=raw["Volume"],
            raw_record=raw,
        )

    def parse_timestamp(self, value: str) -> datetime:
        """Interpret the explicitly labelled MST column and normalize to UTC."""

        local = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return local.replace(tzinfo=self._mst).astimezone(UTC)
