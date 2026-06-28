"""Cardio CSV parser for the cardio ingestion connector (PR-C1).

Converts raw cardio CSV rows (column-name -> string-cell mappings) into typed
``CardioRecord`` values. Per the event-contracts spec "Raw Topic JSON Shape",
the raw layer carries source fields **verbatim** - the parser therefore only
validates required fields and coerces CSV strings to their typed values.

Mirrors ``ingestion/wellness/parser.py`` symbol-for-symbol.

Cardio CSV columns:
  Required: athlete_id (str), activity_type (str), duration_sec (int),
            timestamp (ISO-8601 datetime str)
  Optional/nullable: distance_km (float | None), avg_hr (int | None), tss (float | None)

Design notes:
  - ``timestamp`` is the primary column name; ``start_date`` is accepted as a
    Strava export variant alias (defensively mapped to timestamp before parsing).
  - ``activity_type`` is a free-form string — never rejected for unknown values
    (decision #203, ADR-C3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class CardioRecord:
    """A typed, source-faithful representation of one cardio CSV row.

    ``timestamp`` is kept as the original ISO datetime string (raw-layer fidelity);
    the producer computes ``event_time`` as epoch-ms from it.
    """

    athlete_id: str
    activity_type: str
    duration_sec: int
    timestamp: str
    distance_km: float | None
    avg_hr: int | None
    tss: float | None


class MalformedRowError(ValueError):
    """Raised when a cardio CSV row cannot be parsed into a CardioRecord."""


@dataclass
class ParseResult:
    """Outcome of parsing a batch of rows: valid records plus per-row errors."""

    records: list[CardioRecord]
    errors: list[MalformedRowError]


def _normalise_row(row: Mapping[str, str]) -> Mapping[str, str]:
    """Alias ``start_date`` -> ``timestamp`` if ``timestamp`` is absent (Strava variant)."""
    if "timestamp" not in row or str(row.get("timestamp", "")).strip() == "":
        start_date = row.get("start_date")
        if start_date is not None:
            # Return a new dict with the alias mapped
            merged = dict(row)
            merged["timestamp"] = start_date
            return merged
    return row


def _require(row: Mapping[str, str], field: str) -> str:
    """Return a non-empty required string field, else raise MalformedRowError."""
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise MalformedRowError(f"missing required field: {field!r}")
    return str(value)


def _to_int(row: Mapping[str, str], field: str) -> int:
    raw = _require(row, field)
    try:
        return int(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable int for {field!r}: {raw!r}") from exc


def _to_optional_float(row: Mapping[str, str], field: str) -> float | None:
    """Nullable float field: absent/blank -> None, non-numeric -> raise."""
    raw = row.get(field)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable float for {field!r}: {raw!r}") from exc


def _to_optional_int(row: Mapping[str, str], field: str) -> int | None:
    """Nullable int field: absent/blank -> None, non-integer -> raise."""
    raw = row.get(field)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable int for {field!r}: {raw!r}") from exc


def parse_row(raw_row: Mapping[str, str]) -> CardioRecord:
    """Parse a single cardio CSV row into a typed CardioRecord.

    Raises MalformedRowError if a required field is missing/empty or a
    numeric value cannot be parsed.

    Defensively aliases ``start_date`` -> ``timestamp`` for Strava export variants
    (design ADR note).
    """
    row = _normalise_row(raw_row)
    return CardioRecord(
        athlete_id=_require(row, "athlete_id"),
        activity_type=_require(row, "activity_type"),
        duration_sec=_to_int(row, "duration_sec"),
        timestamp=_require(row, "timestamp"),
        distance_km=_to_optional_float(row, "distance_km"),
        avg_hr=_to_optional_int(row, "avg_hr"),
        tss=_to_optional_float(row, "tss"),
    )


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of rows, collecting valid records and per-row errors.

    Malformed rows are skipped (not raised) so a single bad row does not abort
    an entire batch upload; each failure is captured in ``ParseResult.errors``
    for downstream reporting/DLQ wiring.
    """
    records: list[CardioRecord] = []
    errors: list[MalformedRowError] = []
    for row in rows:
        try:
            records.append(parse_row(row))
        except MalformedRowError as exc:
            errors.append(exc)
    return ParseResult(records=records, errors=errors)
